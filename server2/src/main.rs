use serruf_rpc::rpc_processing::rpc_processing_client::RpcProcessingClient;
use tokio_stream::StreamExt;
use tonic::{Response, Status};
use serruf_rpc::rpc_processing::RequestMessage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RpcProcessingClient::connect("http://localhost:50051").await.unwrap();
    let request = tokio_stream::iter(vec![RequestMessage {id: 1, data: "kek".to_string()}]);
    let resp = client.transmit(request).await;
    match resp {
        Ok(_) => {
            println!("Done")
        }
        Err(error) => {
            println!("Error: {}", error)
        }
    }
    Ok(())
}
