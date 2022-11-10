mod settings;
mod service;

use std::cell::RefCell;
use std::future::Future;
use std::net::ToSocketAddrs;
use std::sync::Mutex; // TODO try futures::lock::Mutex in advance
use tonic::transport::Server;
use serruf_rpc::rpc_processing::rpc_processing_server::RpcProcessingServer;
use serruf_rpc::rpc_processing::RequestMessage;
use futures::{StreamExt, TryFutureExt};
use tokio::join;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio_stream::wrappers::ReceiverStream;
use serruf_rpc::rpc_processing::rpc_processing_client::RpcProcessingClient;
use backoff::ExponentialBackoff;
use backoff::future::retry;

fn start_client() -> async_channel::Sender<RequestMessage> {
    let (result_sender, client_receiver): (async_channel::Sender<RequestMessage>, async_channel::Receiver<RequestMessage>) = async_channel::unbounded();

    println!("Connect to server3");

    tokio::spawn(async move {
        retry(ExponentialBackoff::default(), || async {
            let r_clone = client_receiver.clone();
            println!("Establish connect to server3");
            RpcProcessingClient::connect("http://localhost:50052")
                .map_err(|e| backoff::Error::from(e.to_string()))
                .and_then(|mut client: RpcProcessingClient<tonic::transport::Channel>| async move {
                    client.transmit(r_clone).map_err(|e| backoff::Error::from(e.to_string())).await
                }).await
        }).await
    });

    result_sender
}

pub async fn run<Fn, F>(mut logic: Fn) -> Result<(), Box<dyn std::error::Error>> where Fn: FnMut(RequestMessage) -> F, F: Future<Output=RequestMessage> {
    println!("Start server1");
    let settings = Settings::new()?;
    println!("{}", settings.server.addr);
    //handle message from server pass it through consumer and throw it to client
    let (server_sender, server_receiver): (Sender<RequestMessage>, Receiver<RequestMessage>) = mpsc::channel(1);

    let client_sender: Mutex<Option<async_channel::Sender<RequestMessage>>> = Mutex::new(None);

    let consumer_future = ReceiverStream::new(server_receiver)
        .for_each_concurrent(2, |element| {
            let pending_computation = logic(element);
            async {
                match client_sender.lock() {
                    Ok(mut sender_opt) => {
                        let result_sender = sender_opt.get_or_insert_with(|| start_client());
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
                    Err(_) => {
                        println!("Lock is poisoned");
                    }
                };
            }
        });

    let rpc_service = RpcProcessingService { sender: server_sender };
    let server_future = Server::builder()
        .add_service(RpcProcessingServer::new(rpc_service))
        .serve(settings.server.addr.to_socket_addrs().unwrap().next().unwrap());

    join!(server_future, consumer_future);
    println!("after all");
    Ok(())
}

pub use crate::settings::Settings;
pub use crate::service::RpcProcessingService;

