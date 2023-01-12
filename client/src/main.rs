#[macro_use]
extern crate lazy_static;
extern crate core;

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use atomic_counter::{AtomicCounter, RelaxedCounter};
use tonic::transport::{Channel, Server};
use serruf_rpc::rpc_processing::RequestMessage;
use serruf_rpc::rpc_processing::rpc_processing_server::{RpcProcessingServer, RpcProcessing};
use futures::stream::StreamExt;
use serruf_rpc::rpc_processing::rpc_processing_client::RpcProcessingClient;
use tokio::sync::oneshot;
use tokio_stream::wrappers::BroadcastStream;
use futures::FutureExt;
use tonic::{Request, Response, Status, Streaming};
use common::{DiscoveryService, Settings, StaticRouting, RoutingService};
use futures::future::join_all;
use tokio::sync::broadcast;

lazy_static! {static ref CONFIG: Settings = Settings::new().expect("config can be loaded");}

struct Statistic {
    is_done: bool,
    current_number: usize,
}

struct Counter {
    actual: RelaxedCounter,
    expected: usize,
}

struct SendStream(RpcProcessingClient<Channel>, BroadcastStream<RequestMessage>);

impl SendStream {
    async fn start(mut self) -> Result<Response<()>, Status> {
        let fixed_stream = self.1.map(|element| match element {
            Ok(value) => { value }
            Err(_) => panic!("") // TODO fix me
        });
        self.0.transmit(fixed_stream).await
    }
}

impl Counter {
    fn get_statistic(&self) -> Statistic {
        let got = self.actual.inc();
        Statistic { is_done: got >= self.expected, current_number: got }
    }
}

struct ClientRpcService {
    stop_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    counters: HashMap<String, Counter>,
    awaited_nodes_counter: RelaxedCounter,
}

impl ClientRpcService {
    fn new(stop_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
           counters: HashMap<String, Counter>) -> ClientRpcService {
        ClientRpcService { stop_sender, counters, awaited_nodes_counter: RelaxedCounter::new(1) }
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
                    let statistic = counter.get_statistic();
                    println!("Got {} {} {}", message.id, message.data, statistic.current_number);
                    if statistic.is_done {
                        println!("Stop success {}", message.last_node);
                        break;
                    }
                }
                Err(_) => break
            }
        }
        let finished_nodes = self.awaited_nodes_counter.inc();
        if finished_nodes >= self.counters.len() {
            let mut l = self.stop_sender.lock().unwrap();
            l.take().unwrap().send(()).unwrap();
            println!("Stop success client");
        }
        Ok(Response::new(()))
    }
}

async fn subscribe_clients<R>(sender: &broadcast::Sender<RequestMessage>,
                              ds: &DiscoveryService<'static>,
                              rs: &R,
) -> Vec<SendStream> where
    R: RoutingService {
    let addresses = ds.get_nodes(rs.connect_to_nodes("start")).await;
    join_all(addresses.into_iter().map(|node| async {
        let client = RpcProcessingClient::connect(node.node_address).await.unwrap();
        SendStream(client, BroadcastStream::from(sender.subscribe()))
    })).await
}

fn make_data_mock(id: u32) -> RequestMessage {
    RequestMessage { id: id, data: "kek".to_string(), last_node: "client".to_string() }
}

async fn finite() -> Result<(), Box<dyn std::error::Error>> {
    let ds = Arc::new(DiscoveryService::new(&CONFIG));
    let rs = StaticRouting::init();

    let range: Range<u32> = 1..11;
    let (tx, rx) = oneshot::channel::<()>();
    let sender = Arc::new(Mutex::new(Some(tx)));
    let receive_from = rs.receive_from_scale(&CONFIG.service_discovery.name);
    let counters = receive_from.into_iter()
        .map(|(key, value)|
            (key, Counter { actual: RelaxedCounter::new(1), expected: range.len() * value })
        )
        .collect::<HashMap<String, Counter>>();
    let svc = RpcProcessingServer::new(ClientRpcService::new(sender, counters));
    let server_ds = Arc::clone(&ds);
    let server_jh = tokio::spawn(async move {
        let address = CONFIG.server.addr.to_socket_addrs().unwrap().next().unwrap();
        server_ds.register().await;
        server_ds.stat_check().await;
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(address, rx.map(drop))
            .await
            .expect("Server stop error");
    });
    let (result_sender, _rx) = broadcast::channel::<RequestMessage>(1000);

    println!("Start client");
    let subs = subscribe_clients(&result_sender, &ds, &rs).await;

    for request in range.map(|x| make_data_mock(x)) {
        result_sender.send(request)?;
    }
    drop(result_sender);

    let mut res = join_all(subs.into_iter().map(|sub| sub.start())).await;
    res.swap_remove(0).unwrap();
    server_jh.await.unwrap();
    println!("Done");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    finite().await
}
