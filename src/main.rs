use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use crate::lb::LoadBalancer;

mod lb;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut balancer = lb::RoundRobin::new();
    balancer.register_server("google.com:80".to_string());
    balancer.register_server("amazon.com:80".to_string());

    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    let balancer = Arc::new(balancer);

    loop {
        let balancer = balancer.clone();
        let (socket, _) = listener.accept().await?;
        tokio::spawn( async move {
            match balancer.process(socket).await {
                Ok(server) => {println!("Connection to {server} closed")}
                Err(err) => {println!("Load balancer failure: {err}")}
            };
        });
    }
}
