use std::net::ToSocketAddrs;
use tokio::join;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use serruf_rpc::rpc_processing::RequestMessage;
use serruf_rpc::rpc_processing::rpc_processing_server::RpcProcessingServer;
use futures::StreamExt;
use common::RpcProcessingService;
use serruf_rpc::rpc_processing::rpc_processing_client::RpcProcessingClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RpcProcessingClient::connect("http://localhost:50051").await.unwrap();
    let request = tokio_stream::iter(vec![RequestMessage {id: 1, data: "kek".to_string()}]);
    let resp = client.transmit(request);


    println!("Start server2");
    let (server_sender, server_receiver): (Sender<RequestMessage>, Receiver<RequestMessage>) = mpsc::channel(1);

    let consumer = ReceiverStream::new(server_receiver)
        .for_each_concurrent(2, |element| async move {
            println!("Got {} in client consumer", element.data)
        });

    let rpc_service = RpcProcessingService { sender: server_sender };
    let server_future = Server::builder()
        .add_service(RpcProcessingServer::new(rpc_service))
        .serve("localhost:50053".to_socket_addrs().unwrap().next().unwrap());

    join!(server_future, consumer, resp);
    println!("after");

    Ok(())
}
