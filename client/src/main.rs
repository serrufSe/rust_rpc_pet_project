#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use atomic_counter::{AtomicCounter, RelaxedCounter};
use tonic::transport::Server;
use serruf_rpc::rpc_processing::RequestMessage;
use serruf_rpc::rpc_processing::rpc_processing_server::{RpcProcessingServer, RpcProcessing};
use futures::stream::StreamExt;
use serruf_rpc::rpc_processing::rpc_processing_client::RpcProcessingClient;
use tokio::sync::oneshot;
use futures::FutureExt;
use tonic::{Request, Response, Status, Streaming};
use common::{DiscoveryService, Settings, StaticRouting, RoutingService};

lazy_static! {static ref CONFIG: Settings = Settings::new().expect("config can be loaded");}

struct ClientRpcService {
    stop_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    awaited_count: usize,
    counters: HashMap<String, RelaxedCounter>,
    awaited_nodes_counter: RelaxedCounter,
}

impl ClientRpcService {
    fn new(stop_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
           awaited_count: usize,
           counters: HashMap<String, RelaxedCounter>) -> ClientRpcService {
        ClientRpcService { stop_sender, awaited_count, counters, awaited_nodes_counter: RelaxedCounter::new(1) }
    }
}

#[tonic::async_trait]
impl RpcProcessing for ClientRpcService {
    async fn transmit(&self, request: Request<Streaming<RequestMessage>>) -> Result<Response<()>, Status> {
        let mut in_stream = request.into_inner();
        while let Some(result) = in_stream.next().await {
            match result {
                Ok(_) => {
                    let message = result.unwrap();
                    let counter = self.counters.get(&message.last_node).expect("Missing counter");
                    let got = counter.inc();
                    println!("Got {} {} {}", message.id, message.data, got);
                    if got >= self.awaited_count {
                        println!("Stop success {}", message.last_node);
                        break;
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
        let finished_nodes = self.awaited_nodes_counter.inc();
        if finished_nodes >= self.counters.len() {
            let mut l = self.stop_sender.lock().unwrap();
            l.take().unwrap().send(()).unwrap();
        }
        Ok(Response::new(()))
    }
}

async fn start_client<R>(request: impl tonic::IntoStreamingRequest<Message=RequestMessage>,
                         ds: &DiscoveryService<'static>,
                         rs: &R,
) -> Result<Response<()>, Status>
where
    R: RoutingService
{
    println!("Start client");
    let addresses = ds.get_nodes(rs.connect_to_nodes("start")).await;
    let start_node_address = addresses.first().expect("Empty addresses for client").node_address.clone();
    let mut client = RpcProcessingClient::connect(start_node_address).await.unwrap();
    client.transmit(request).await
}

async fn finite() -> Result<(), Box<dyn std::error::Error>> {
    let ds = DiscoveryService::new(&CONFIG);
    ds.register().await;
    ds.stat_check().await;

    let rs = StaticRouting::init();

    let range: Range<u32> = 1..11;
    let (tx, rx) = oneshot::channel::<()>();
    let sender = Arc::new(Mutex::new(Some(tx)));
    let counters: HashMap<String, RelaxedCounter> = rs.receive_from_nodes(&CONFIG.service_discovery.name)
        .into_iter()
        .map(|node| (node, RelaxedCounter::new(1)))
        .collect();

    let svc = RpcProcessingServer::new(ClientRpcService::new(sender, range.len(), counters));

    let server_jh = tokio::spawn(async move {
        let address = CONFIG.server.addr.to_socket_addrs().unwrap().next().unwrap();
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(address, rx.map(drop))
            .await
    });

    let request = tokio_stream::iter(range.map(|x: u32| RequestMessage { id: x, data: "kek".to_string(), last_node: "client".to_string() }));
    start_client(request, &ds, &rs).await.unwrap();

    server_jh.await.unwrap().unwrap();
    println!("After server stop");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    finite().await
}
