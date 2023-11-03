use std::error::Error;
use std::fmt;
use std::fmt::{Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub trait LoadBalancer {
    fn register_server<'a>(&'a mut self, addr: String) -> Pin<Box<dyn Future<Output=()> + Send + 'a>>;
    fn deregister_server<'a>(&'a mut self, addr: String) -> Pin<Box<dyn Future<Output=()> + Send + 'a>>;
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
    fn register_server<'a>(&'a mut self, addr: String) -> Pin<Box<dyn Future<Output=()> + Send + 'a>> {
        Box::pin(async move {
            self.servers.push(addr);
        })
    }

    fn deregister_server<'a>(&'a mut self, addr: String) -> Pin<Box<dyn Future<Output=()> + Send + 'a>> {
        Box::pin(async move {
            if let Some(idx) = self.servers.iter().position(|el| *el == addr) {
                self.servers.remove(idx);
            }
        })
    }

    fn process<'a>(&'a self, socket: TcpStream) -> Pin<Box<dyn Future<Output=Result<String, Box<dyn Error>>> + Send + 'a>> {
        return Box::pin(async move {
            let pos = self.pos.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |x| { Some((x + 1) % self.servers.len()) }
            ).unwrap();
            let server = &self.servers[pos];

            println!("Using server {server}");

            let upstream = TcpStream::connect(server).await?;

            copy_to_upstream(socket, upstream).await;

            return Ok(String::from(server));
        });
    }
}

pub struct LeastConnections {
    servers: Vec<String>,
    connections: Mutex<Vec<u32>>
}

impl LeastConnections {
    pub fn new() -> LeastConnections {
        return LeastConnections {servers: Vec::new(), connections: Mutex::new(Vec::new())};
    }
}

impl LoadBalancer for LeastConnections {
    fn register_server<'a>(&'a mut self, addr: String) -> Pin<Box<dyn Future<Output=()> + Send + 'a>> {
        Box::pin(async {
            let mut connections = self.connections.lock().await;
            self.servers.push(addr);
            connections.push(0);
        })
    }

    fn deregister_server<'a>(&'a mut self, addr: String) -> Pin<Box<dyn Future<Output=()> + Send + 'a>> {
        Box::pin(async move {
            let mut connections = self.connections.lock().await;
            if let Some(idx) = self.servers.iter().position(|el| *el == addr) {
                self.servers.remove(idx);
                connections.remove(idx);
            }
        })
    }

    fn process<'a>(&'a self, socket: TcpStream) -> Pin<Box<dyn Future<Output=Result<String, Box<dyn Error>>> + Send + 'a>> {
        return Box::pin(async move {
            let server = {
                let mut connections = self.connections.lock().await;
                let (server_idx, _) = connections.iter().enumerate().min_by_key(|(_, el)| **el).ok_or(NoUpstreamsRegistered)?;
                connections[server_idx] += 1;
                Ok::<&String, NoUpstreamsRegistered>(&self.servers[server_idx])
            }?;

            println!("Using server {server}");

            let upstream = TcpStream::connect(server).await?;

            copy_to_upstream(socket, upstream).await;
            {
                let mut connections = self.connections.lock().await;
                if let Some(idx) = self.servers.iter().position(|s| *s == server.clone()) {
                    connections[idx] -= 1
                }
            };

            return Ok(server.to_string());
        });
    }
}

async fn copy_to_upstream(mut socket: TcpStream, mut upstream: TcpStream) {
    let (mut reader, mut writer) = socket.split();
    let (mut upstream_reader, mut upstream_writer) = upstream.split();

    tokio::join!(
        async move {
            let _ = tokio::io::copy(&mut reader, &mut upstream_writer).await;
            let _ = upstream_writer.shutdown().await;
        },
        async move {
            let _ = tokio::io::copy(&mut upstream_reader, &mut writer).await;
            let _ = writer.shutdown().await;
        }
    );
}

#[derive(Debug)]
struct NoUpstreamsRegistered;

impl Error for NoUpstreamsRegistered {}

impl fmt::Display for NoUpstreamsRegistered {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "No upstreams registered for load balancing")
    }
}
