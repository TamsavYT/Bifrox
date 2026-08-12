use crate::config::FlushPolicy;
use crate::server::engine::StorageEngine;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use socket2::{Domain, Protocol, Socket, Type};
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::sleep;

pub fn build_tls_acceptor(
    config: &crate::config::EngineConfig,
) -> IoResult<tokio_rustls::TlsAcceptor> {
    let (certs, key) =
        if let (Some(cert_path), Some(key_path)) = (&config.ssl_cert_path, &config.ssl_key_path) {
            let cert_file = std::fs::File::open(cert_path)?;
            let mut cert_reader = std::io::BufReader::new(cert_file);
            let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

            let key_file = std::fs::File::open(key_path)?;
            let mut key_reader = std::io::BufReader::new(key_file);
            let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No private key found in SSL key file",
                )
            })?;

            (certs, key)
        } else {
            // Auto-generate self-signed TLS cert for testing & zero-config SSL mode
            let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
            let cert = rcgen::generate_simple_self_signed(subject_alt_names)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let cert_der = cert.cert.der().to_vec();
            let key_der = cert.key_pair.serialize_der();
            (
                vec![CertificateDer::from(cert_der)],
                PrivateKeyDer::Pkcs8(key_der.into()),
            )
        };

    let _ = rustls::crypto::ring::default_provider().install_default();

    let client_auth_enabled = matches!(
        config.ssl_client_auth.to_lowercase().as_str(),
        "required" | "requested" | "true"
    );

    let server_config = if client_auth_enabled {
        let mut root_store = rustls::RootCertStore::empty();
        if let Some(ca_path) = &config.ssl_ca_path {
            let ca_file = std::fs::File::open(ca_path)?;
            let mut ca_reader = std::io::BufReader::new(ca_file);
            let ca_certs = rustls_pemfile::certs(&mut ca_reader).collect::<Result<Vec<_>, _>>()?;
            for c in ca_certs {
                root_store.add(c).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
            }
        } else {
            for c in &certs {
                let _ = root_store.add(c.clone());
            }
        }
        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;

        rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?
    };

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)))
}

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

        let tls_acceptor = if matches!(
            self.engine.config().security_protocol,
            crate::config::SecurityProtocol::Ssl | crate::config::SecurityProtocol::SaslSsl
        ) {
            match build_tls_acceptor(self.engine.config()) {
                Ok(acceptor) => Some(acceptor),
                Err(e) => {
                    tracing::error!("Failed to initialize TLS/SSL acceptor: {}", e);
                    return Err(e);
                }
            }
        } else {
            None
        };

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
                        tracing::info!(
                            "Retention Garbage Collector: Unlinked {} expired log segment files.",
                            count
                        );
                    }
                    Err(e) => {
                        tracing::error!("Retention Garbage Collector error: {}", e);
                    }
                    _ => {}
                }
            }
        });

        let metrics_engine = self.engine.clone();
        tokio::spawn(async move {
            let metrics_port = local_addr.port().checked_add(1000).unwrap_or(9090);
            let metrics_addr = SocketAddr::from(([127, 0, 0, 1], metrics_port));
            if let Ok(listener) = TcpListener::bind(metrics_addr).await {
                tracing::info!(
                    "Prometheus Metrics Exporter listening on http://{}/metrics",
                    metrics_addr
                );
                loop {
                    if let Ok((mut socket, _)) = listener.accept().await {
                        let engine = metrics_engine.clone();
                        tokio::spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};
                            let mut buf = [0u8; 1024];
                            let _ = socket.read(&mut buf).await;
                            let body = engine.metrics().render_prometheus(
                                engine.list_topics().len(),
                                engine.broker_endpoints().len(),
                            );
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = socket.write_all(response.as_bytes()).await;
                        });
                    }
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
                            let acceptor = tls_acceptor.clone();
                            tokio::spawn(async move {
                                if let Some(acceptor) = acceptor {
                                    match acceptor.accept(socket).await {
                                        Ok(tls_stream) => {
                                            crate::server::handler::handle_connection_stream(
                                                tls_stream,
                                                engine_clone,
                                                addr.to_string(),
                                            )
                                            .await;
                                        }
                                        Err(e) => {
                                            tracing::warn!("TLS handshake failed for {}: {}", addr, e);
                                        }
                                    }
                                } else {
                                    crate::server::handler::handle_connection_stream(
                                        socket,
                                        engine_clone,
                                        addr.to_string(),
                                    )
                                    .await;
                                }
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
