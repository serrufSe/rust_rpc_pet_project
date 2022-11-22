use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};
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

// TODO stolen from tonic tests, simplify
struct Svc(Arc<Mutex<Option<oneshot::Sender<()>>>>);

#[tonic::async_trait]
impl RpcProcessing for Svc {
    async fn transmit(&self, request: Request<Streaming<RequestMessage>>) -> Result<Response<()>, Status> {
        let mut in_stream = request.into_inner();
        match in_stream.next().await{
            None => {}
            Some(data) => {
                let request = data.unwrap();
                println!("Got {} with data {}", request.id, request.data)
            }
        }
        let mut l = self.0.lock().unwrap();
        l.take().unwrap().send(()).unwrap();
        Ok(Response::new(()))
    }
}

async fn infinite() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RpcProcessingClient::connect("http://localhost:50051").await.unwrap();
    let requests = stream::unfold(0, |id| async move {
        let next_state = id + 1;
        sleep(Duration::from_millis(1000)).await;
        println!("Return {} for sending", id);
        Some((RequestMessage {id, data: "kek".to_string()}, next_state))
    });
    let resp = client.transmit(requests);


    println!("Start client");
    let (server_sender, server_receiver): (Sender<RequestMessage>, Receiver<RequestMessage>) = mpsc::channel(1);

    let consumer = ReceiverStream::new(server_receiver)
        .for_each_concurrent(2, |element| async move {
            println!("Got {} in client consumer", element.id)
        });

    let rpc_service = RpcProcessingService { sender: server_sender };
    let server_future = Server::builder()
        .add_service(RpcProcessingServer::new(rpc_service))
        .serve("localhost:50053".to_socket_addrs().unwrap().next().unwrap());

    join!(server_future, consumer, resp);
    println!("after");

    Ok(())
}

async fn finite_single() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = oneshot::channel::<()>();
    let sender = Arc::new(Mutex::new(Some(tx)));
    let svc = RpcProcessingServer::new(Svc(sender));

    let server_jh = tokio::spawn(async move {
        println!("Start server");
        let address = "localhost:50053".to_socket_addrs().unwrap().next().unwrap();
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(address, rx.map(drop))
            .await
    });

    println!("Start client");
    let mut client = RpcProcessingClient::connect("http://localhost:50051").await.unwrap();
    let request = tokio_stream::iter(vec![RequestMessage {id: 1, data: "kek".to_string()}]);
    client.transmit(request).await.unwrap();

    server_jh.await.unwrap().unwrap();
    println!("After server stop");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    finite_single().await
}
