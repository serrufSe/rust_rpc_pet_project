mod settings;
mod service;

#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::future::Future;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use futures::lock::Mutex;
use tonic::transport::Server;
use serruf_rpc::rpc_processing::rpc_processing_server::RpcProcessingServer;
use serruf_rpc::rpc_processing::RequestMessage;
use futures::{StreamExt, TryFutureExt};
use tokio::join;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use serruf_rpc::rpc_processing::rpc_processing_client::RpcProcessingClient;
use backoff::ExponentialBackoff;
use backoff::future::retry;
use futures::future::join_all;

lazy_static! {
    static ref CONFIG: settings::Settings =
        settings::Settings::new().expect("config can be loaded");
}

lazy_static! {
    static ref ROUTING: HashMap<String, Vec<String>> = HashMap::from([
    (String::from("start"), vec![String::from("server1")]),
    (String::from("server1"), vec![String::from("server2"), String::from("client")]),
    (String::from("server2"), vec![String::from("client")]),
]);
}

lazy_static! {
    static ref NETWORK: HashMap<String, String> = HashMap::from([
    (String::from("server1"), String::from("http://localhost:50051")),
    (String::from("server2"), String::from("http://localhost:50052")),
    (String::from("client"), String::from("http://localhost:50053")),
]);
}

type MySender = async_channel::Sender<RequestMessage>;
type Senders = Arc<Mutex<HashMap<String, MySender>>>;

struct NodeInfo {
    node_name: String,
    node_address: String,
}

fn start_client(senders: Senders, node_info: Arc<NodeInfo>) -> MySender {
    let (result_sender, client_receiver) = async_channel::unbounded::<RequestMessage>();
    println!("Connect to server {}", node_info.node_name);

    tokio::spawn(async move {
        retry(ExponentialBackoff::default(), || async {
            let receiver_for_retry = client_receiver.clone();
            let node_for_retry = (*node_info).node_address.clone();
            println!("Establish connect to server {}", node_info.node_address);

            RpcProcessingClient::connect(node_for_retry)
                .map_err(|e| backoff::Error::from(e.to_string()))
                .and_then(|mut client: RpcProcessingClient<tonic::transport::Channel>| async move {
                    client.transmit(receiver_for_retry).map_err(|e| backoff::Error::from(e.to_string())).await
                }).await
        }).await.unwrap();

        let mut sender_opt = senders.lock().await;
        sender_opt.remove(&node_info.node_name);
        println!("Dispose sender")
    });

    result_sender
}

pub async fn run<Fn, F>(
    mut logic: Fn
) -> Result<(), Box<dyn std::error::Error>>
where
    Fn: FnMut(RequestMessage) -> F,
    F: Future<Output=RequestMessage>,
{
    println!("Start server for {}", CONFIG.server.addr);
    //handle message from server pass it through consumer and throw it to client
    let (server_sender, server_receiver) = mpsc::channel::<RequestMessage>(1);

    let connect_to_nodes = ROUTING.get(&CONFIG.service_discovery.name)
        .expect(&format!("{} service not found in static routing", CONFIG.service_discovery.name))
        .to_owned();

    let nodes_info: Vec<Arc<NodeInfo>> = connect_to_nodes.into_iter().map(|node| {
        let address = NETWORK.get(&node).expect(&format!("{} node not found in static SD", node)).to_owned();
        Arc::new(NodeInfo { node_name: node, node_address: address })
    }).collect();

    let senders: Senders = Arc::new(Mutex::new(HashMap::new()));

    let consumer_future = ReceiverStream::new(server_receiver)
        .for_each_concurrent(2, |element| {
            let pending_computation = logic(element);
            async {
                let result = pending_computation.await;
                let broadcasting = nodes_info.iter().map(|node_info| async {
                    println!("try to acquire lock");
                    let mut sender_opt = senders.lock().await;

                    let result_sender = sender_opt.entry(node_info.node_name.clone())
                        .or_insert_with(|| start_client(Arc::clone(&senders), Arc::clone(node_info)));
                    println!("get sender");

                    let send_res = result_sender.send(result.clone()).await;

                    match send_res {
                        Ok(_) => {
                            println!("Send")
                        }
                        Err(ex) => {
                            println!("Drop {}", ex.0.id)
                        }
                    }
                });
                join_all(broadcasting).await;
            }
        });

    let rpc_service = RpcProcessingService { sender: server_sender };
    let server_future = Server::builder()
        .add_service(RpcProcessingServer::new(rpc_service))
        .serve(CONFIG.server.addr.to_socket_addrs().unwrap().next().unwrap());

    join!(server_future, consumer_future);
    println!("after all");
    Ok(())
}

pub use crate::settings::Settings;
pub use crate::service::RpcProcessingService;

