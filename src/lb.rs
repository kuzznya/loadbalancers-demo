use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpStream;

pub trait LoadBalancer {
    fn register_server(&mut self, addr: String);
    fn deregister_server(&mut self, addr: String);
    fn process<'a>(&'a self, socket: TcpStream) -> Pin<Box<dyn Future<Output=Result<String, Box<dyn Error>>> + Send + 'a>>;
}

pub struct RoundRobin {
    servers: Vec<String>,
    pos: AtomicUsize
}

impl RoundRobin {
    pub fn new() -> RoundRobin {
        return RoundRobin{servers: Vec::new(), pos: AtomicUsize::new(0)}
    }
}

impl LoadBalancer for RoundRobin {
    fn register_server(&mut self, addr: String) {
        self.servers.push(addr);
    }

    fn deregister_server(&mut self, addr: String) {
        if let Some(idx) = self.servers.iter().position(|el| *el == addr) {
            self.servers.remove(idx);
        }
    }

    fn process<'a>(&'a self, mut socket: TcpStream) -> Pin<Box<dyn Future<Output=Result<String, Box<dyn Error>>> + Send + 'a>> {
        return Box::pin(async move {
            let pos = self.pos.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |x| { Some((x + 1) % self.servers.len()) }
            ).unwrap();
            let server = &self.servers[pos];

            println!("Using server {server}");

            let (mut reader, mut writer) = socket.split();

            let mut upstream = TcpStream::connect(server).await?;
            let (mut upstream_reader, mut upstream_writer) = upstream.split();

            let (res1, res2) = tokio::join!(
                tokio::io::copy(&mut reader, &mut upstream_writer),
                tokio::io::copy(&mut upstream_reader, &mut writer)
            );
            if let Err(err) = res1 {
                return Err(Box::from(err))
            }
            if let Err(err) = res2 {
                return Err(Box::from(err))
            }

            return Ok(String::from(server));
        });
    }
}
