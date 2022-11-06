use tokio::sync::mpsc::Sender;
use tonic::{Response, Status, Streaming, Request};
use serruf_rpc::rpc_processing::RequestMessage;
use serruf_rpc::rpc_processing::rpc_processing_server::RpcProcessing;
use futures::StreamExt;

pub struct RpcProcessingService {
    pub sender: Sender<RequestMessage>,
}

#[tonic::async_trait]
impl RpcProcessing for RpcProcessingService {
    async fn transmit(&self, request: Request<Streaming<RequestMessage>>) -> Result<Response<()>, Status> {
        let mut in_stream = request.into_inner();

        while let Some(result) = in_stream.next().await {
            match result {
                Ok(v) => {
                    println!("Got {} in server", v.data);
                    self.sender.send(v).await.unwrap();
                }
                Err(_) => {
                    break;
                }
            }
        }

        Ok(Response::new(()))
    }
}

