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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Start server3");
    //handle message from server pass it through consumer and throw it to client
    let (server_sender, server_receiver): (Sender<RequestMessage>, Receiver<RequestMessage>) = mpsc::channel(1);

    let consumer = ReceiverStream::new(server_receiver)
        .for_each_concurrent(2, |element| async move {
            println!("Got {} in client consumer", element.data)
        });

    let rpc_service = RpcProcessingService { sender: server_sender };
    let server_future = Server::builder()
        .add_service(RpcProcessingServer::new(rpc_service))
        .serve("localhost:50052".to_socket_addrs().unwrap().next().unwrap());

    join!(server_future, consumer);
    println!("after");
    Ok(())
}
