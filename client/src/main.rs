#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use atomic_counter::{AtomicCounter, RelaxedCounter};
use tokio::join;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use serruf_rpc::rpc_processing::RequestMessage;
use serruf_rpc::rpc_processing::rpc_processing_server::{RpcProcessingServer, RpcProcessing};
use futures::stream::{self, StreamExt};
use common::RpcProcessingService;
use serruf_rpc::rpc_processing::rpc_processing_client::RpcProcessingClient;
use tokio::time::{sleep, Duration};
use tokio::sync::oneshot;
use futures::FutureExt;
use tonic::{Request, Response, Status, Streaming};
use common::Settings;
use common::sd_from_file;

lazy_static! {
    static ref ROUTING: HashMap<String, Vec<String>> = HashMap::from([
    (String::from("start"), vec![String::from("server1")]),
    (String::from("server1"), vec![String::from("client"), String::from("server2")]),
    (String::from("server2"), vec![String::from("client")]),
]);
}

lazy_static! {static ref NETWORK: HashMap<String, String> = sd_from_file();}

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

async fn start_client(request: impl tonic::IntoStreamingRequest<Message=RequestMessage>) -> Result<Response<()>, Status> {
    println!("Start client");
    let start_node = ROUTING.get("start")
        .expect("Start node not found")
        .first()
        .expect("Empty start node");
    let start_node_address = NETWORK.get(start_node).expect("Start node address not found").to_owned();
    let mut client = RpcProcessingClient::connect(start_node_address).await.unwrap();
    client.transmit(request).await
}

async fn infinite() -> Result<(), Box<dyn std::error::Error>> {
    let requests = stream::unfold(0, |id| async move {
        let next_state = id + 1;
        sleep(Duration::from_millis(1000)).await;
        println!("Return {} for sending", id);
        Some((RequestMessage { id, data: "kek".to_string(), last_node: "client".to_string() }, next_state))
    });
    let resp = start_client(requests);

    let (server_sender, server_receiver) = mpsc::channel::<RequestMessage>(1);

    let consumer = ReceiverStream::new(server_receiver)
        .for_each_concurrent(2, |element| async move {
            println!("Got {} in client consumer", element.id)
        });

    let rpc_service = RpcProcessingService { sender: server_sender };
    let address = CONFIG.server.addr.to_socket_addrs().unwrap().next().unwrap();
    let server_future = Server::builder()
        .add_service(RpcProcessingServer::new(rpc_service))
        .serve(address);

    join!(server_future, consumer, resp);
    println!("after");

    Ok(())
}

async fn finite() -> Result<(), Box<dyn std::error::Error>> {
    let range: Range<u32> = 1..11;
    let (tx, rx) = oneshot::channel::<()>();
    let sender = Arc::new(Mutex::new(Some(tx)));
    let counters: HashMap<String, RelaxedCounter> = ROUTING.iter().filter_map(|(key, value)| {
        if value.contains(&CONFIG.service_discovery.name) {
            Some((key.clone(), RelaxedCounter::new(1)))
        } else { None }
    }).collect();

    let svc = RpcProcessingServer::new(ClientRpcService::new(sender, range.len(), counters));

    let server_jh = tokio::spawn(async move {
        println!("Start server");
        let address = CONFIG.server.addr.to_socket_addrs().unwrap().next().unwrap();
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(address, rx.map(drop))
            .await
    });

    let request = tokio_stream::iter(range.map(|x: u32| RequestMessage { id: x, data: "kek".to_string(), last_node: "client".to_string() }));
    start_client(request).await.unwrap();

    server_jh.await.unwrap().unwrap();
    println!("After server stop");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    infinite().await
}
