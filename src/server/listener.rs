use crate::config::FlushPolicy;
use crate::server::engine::StorageEngine;
use crate::server::handler::handle_connection;
use socket2::{Domain, Protocol, Socket, Type};
use std::io::Result as IoResult;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::time::sleep;

/// High-throughput Tokio TCP server backed by Windows IOCP
pub struct Server {
    engine: StorageEngine,
}

impl Server {
    pub fn new(engine: StorageEngine) -> Self {
        Self { engine }
    }

    /// Binds TCP socket and returns bound TcpListener and local SocketAddr
    pub fn bind(&self) -> IoResult<(TcpListener, SocketAddr)> {
        let bind_addr = self.engine.config().bind_addr.clone();
        let addr: SocketAddr = bind_addr
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

        // On Windows, setting SO_REUSEADDR allows multiple sockets to forcibly bind to the same port (WinSock port hijacking).
        // Disabling reuse_address on Windows ensures strict port exclusivity (error 10048 if port is in use).
        #[cfg(windows)]
        {
            socket.set_reuse_address(false)?;
        }
        #[cfg(not(windows))]
        {
            socket.set_reuse_address(true)?;
            // let _ = socket.set_reuse_port(true);
        }

        socket.bind(&addr.into())?;
        socket.listen(1024)?;
        let std_listener: std::net::TcpListener = socket.into();
        std_listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(std_listener)?;
        let local_addr = listener.local_addr()?;
        Ok((listener, local_addr))
    }

    /// Runs server event loop with an already bound TcpListener
    pub async fn run_with_listener(&self, listener: TcpListener) -> IoResult<()> {
        let local_addr = listener.local_addr()?;
        tracing::info!("TCP Storage Server listening on http://{}", local_addr);

        if let FlushPolicy::AsyncPeriodic { interval, .. } = self.engine.config().flush_policy {
            let engine_clone = self.engine.clone();
            tokio::spawn(async move {
                loop {
                    sleep(interval).await;
                    if let Err(err) = engine_clone.flush_all() {
                        tracing::error!("Background WAL periodic flush error: {}", err);
                    }
                }
            });
        }

        let retention_engine = self.engine.clone();
        let retention_interval = self.engine.config().retention_check_interval;
        tokio::spawn(async move {
            loop {
                sleep(retention_interval).await;
                match retention_engine.apply_retention_all() {
                    Ok(count) if count > 0 => {
                        tracing::info!("Retention Garbage Collector: Unlinked {} expired log segment files.", count);
                    }
                    Err(e) => {
                        tracing::error!("Retention Garbage Collector error: {}", e);
                    }
                    _ => {}
                }
            }
        });

        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((socket, addr)) => {
                            tracing::debug!("Accepted incoming connection from {}", addr);
                            let engine_clone = self.engine.clone();
                            tokio::spawn(async move {
                                handle_connection(socket, engine_clone).await;
                            });
                        }
                        Err(err) => {
                            tracing::error!("Failed to accept incoming TCP socket: {}", err);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Shutdown signal received. Flushing storage engine partitions...");
                    if let Err(e) = self.engine.flush_all() {
                        tracing::error!("Failed to flush partitions during shutdown: {}", e);
                    }
                    tracing::info!("Server shut down gracefully.");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Run TCP listener loop
    pub async fn run(&self) -> IoResult<()> {
        let (listener, _) = self.bind()?;
        self.run_with_listener(listener).await
    }
}
