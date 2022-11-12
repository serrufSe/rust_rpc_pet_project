use std::net::ToSocketAddrs;
use tokio::join;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use serruf_rpc::rpc_processing::RequestMessage;
use serruf_rpc::rpc_processing::rpc_processing_server::RpcProcessingServer;
use futures::stream::{self, StreamExt};
use common::RpcProcessingService;
use serruf_rpc::rpc_processing::rpc_processing_client::RpcProcessingClient;
use tokio::time::{sleep, Duration};

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    infinite().await
}
