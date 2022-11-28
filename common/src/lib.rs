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

lazy_static! {
    static ref CONFIG: settings::Settings =
        settings::Settings::new().expect("config can be loaded");
}

lazy_static! {
    static ref ROUTING: HashMap<String, Vec<String>> = HashMap::from([
    (String::from("start"), vec![String::from("server1")]),
    (String::from("server1"), vec![String::from("server2")]),
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
type SenderOpt = Arc<Mutex<Option<MySender>>>;

fn start_client(client_sender: SenderOpt, connect_to: Arc<String>) -> MySender {
    let (result_sender, client_receiver) = async_channel::unbounded::<RequestMessage>();
    println!("Connect to server {}", connect_to);

    tokio::spawn(async move {
        retry(ExponentialBackoff::default(), || async {
            let r_clone = client_receiver.clone();
            println!("Establish connect to server {}", connect_to);
            RpcProcessingClient::connect((*connect_to).clone())
                .map_err(|e| backoff::Error::from(e.to_string()))
                .and_then(|mut client: RpcProcessingClient<tonic::transport::Channel>| async move {
                    client.transmit(r_clone).map_err(|e| backoff::Error::from(e.to_string())).await
                }).await
        }).await.unwrap();
        let mut sender_opt = client_sender.lock().await;
        *sender_opt = None;
        println!("Dispose sender")
    });

    result_sender
}

pub async fn run<Fn, F>(mut logic: Fn) -> Result<(), Box<dyn std::error::Error>> where Fn: FnMut(RequestMessage) -> F, F: Future<Output=RequestMessage> {
    println!("Start server for {}", CONFIG.server.addr);
    //handle message from server pass it through consumer and throw it to client
    let (server_sender, server_receiver) = mpsc::channel::<RequestMessage>(1);

    let connect_to_node = ROUTING.get(&CONFIG.service_discovery.name)
        .expect("SD error")
        .first() // TODO multiple nodes
        .expect("Empty connect_to part");

    let connect_to_address = NETWORK.get(connect_to_node)
        .expect("Node not found")
        .to_owned();

    let connect_to_pointer = Arc::new(connect_to_address);
    let client_sender: SenderOpt = Arc::new(Mutex::new(None));

    let consumer_future = ReceiverStream::new(server_receiver)
        .for_each_concurrent(2, |element| {
            let pending_computation = logic(element);
            async {
                println!("try to acquire lock");
                let mut sender_opt = client_sender.lock().await;
                let result_sender = sender_opt.get_or_insert_with(||
                    start_client(Arc::clone(&client_sender), Arc::clone(&connect_to_pointer))
                );
                println!("get sender");
                let result = pending_computation.await;
                let send_res = result_sender.send(result).await;

                match send_res {
                    Ok(_) => {
                        println!("Send")
                    }
                    Err(ex) => {
                        println!("Drop {}", ex.0.id)
                    }
                }
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

