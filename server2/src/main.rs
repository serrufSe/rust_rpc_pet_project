use tokio::time::{sleep, Duration};
use common::run;
use serruf_rpc::rpc_processing::RequestMessage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run(|element| async move {
        println!("Before long work");
        sleep(Duration::from_millis(1000)).await;
        println!("After long work");
        RequestMessage{id: 1, data: format!("{} + server 2", element.data)}
    }).await
}
