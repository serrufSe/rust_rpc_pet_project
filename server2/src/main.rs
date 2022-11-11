use tokio::time::{sleep, Duration};
use common::run;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run(|element| async {
        println!("Before long work");
        sleep(Duration::from_millis(1000)).await;
        println!("After long work");
        element
    }).await
}
