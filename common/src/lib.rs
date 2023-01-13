mod settings;
mod service;
mod service_discovery;
mod routing;

#[macro_use]
extern crate lazy_static;

use std::future::Future;
use std::net::ToSocketAddrs;
use std::sync::Arc;
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
use dashmap::DashMap;
use async_channel::unbounded;

lazy_static! {static ref CONFIG: settings::Settings = settings::Settings::new().expect("config can be loaded");}

static PARALLELISM: usize = 2;

type MySender = async_channel::Sender<RequestMessage>;
type Senders = Arc<DashMap<String, MySender>>;

fn start_client(senders: Senders, node_info: Arc<NodeInfo>) -> MySender {
    let (result_sender, client_receiver) = unbounded::<RequestMessage>();
    println!("Connect to server {}", node_info.node_name);

    tokio::spawn(async move {
        retry(ExponentialBackoff::default(), || async {
            let receiver_for_retry = client_receiver.clone();
            let node_for_retry = (*node_info).node_address.clone();
            println!("Establish connect to server {} {}", node_info.node_name, node_info.node_address);

            RpcProcessingClient::connect(node_for_retry)
                .map_err(|e| backoff::Error::from(e.to_string()))
                .and_then(|mut client: RpcProcessingClient<tonic::transport::Channel>| async move {
                    client.transmit(receiver_for_retry).map_err(|e| backoff::Error::from(e.to_string())).await
                }).await
        }).await.unwrap();

        senders.remove(&node_info.node_name);
        println!("Dispose sender {}", node_info.node_name);
        drop(client_receiver);
    });

    result_sender
}

async fn broadcast_result<F>(
    pending_computation: F,
    nodes_info: &Vec<Arc<NodeInfo>>,
    senders: &Senders,
)
where F: Future<Output=RequestMessage> {
    let result = pending_computation.await;
    let broadcasting = nodes_info.iter().map(|node_info| async {
        let result_sender = senders.entry(node_info.node_name.clone())
            .or_insert_with(|| start_client(Arc::clone(senders), Arc::clone(node_info)));
        let send_res = result_sender.send(result.clone()).await;
        if let Err(ex) = send_res {
            println!("Drop {}", ex.0.id)
        }
    });
    join_all(broadcasting).await;
}

pub async fn run<Fn, F>(
    mut logic: Fn
) -> Result<(), Box<dyn std::error::Error>>
where
    Fn: FnMut(RequestMessage) -> F,
    F: Future<Output=RequestMessage>,
{
    let ds = DiscoveryService::new(&CONFIG);
    ds.register().await;
    ds.stat_check().await;

    let rs = StaticRouting::init();

    println!("Start server for {}", CONFIG.server.addr);
    //handle message from server pass it through consumer and throw it to client
    let (server_sender, server_receiver) = mpsc::channel::<RequestMessage>(1);
    let nodes_info = ds.get_nodes(rs.connect_to_nodes(&CONFIG.service_discovery.name)).await;
    let nodes_refs = nodes_info.into_iter()
        .map(|node_info| Arc::new(node_info))
        .collect::<Vec<Arc<NodeInfo>>>();
    let senders: Senders = Arc::new(DashMap::new());

    let consumer_future = ReceiverStream::new(server_receiver)
        .for_each_concurrent(PARALLELISM, |element|
            broadcast_result(logic(element), &nodes_refs, &senders)
        );

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
pub use crate::service_discovery::DiscoveryService;
use crate::service_discovery::NodeInfo;
pub use crate::routing::{RoutingService, StaticRouting};

