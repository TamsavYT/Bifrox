use crate::config::FlushPolicy;
use crate::server::engine::StorageEngine;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use socket2::{Domain, Protocol, Socket, Type};
use std::io::Result as IoResult;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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
                    // Same reasoning as the retention sweep below: `flush_all` is
                    // blocking fsync I/O across every partition.
                    let engine_for_blocking = engine_clone.clone();
                    let result =
                        tokio::task::spawn_blocking(move || engine_for_blocking.flush_all()).await;
                    match result {
                        Ok(Err(err)) => {
                            tracing::error!("Background WAL periodic flush error: {}", err);
                        }
                        Err(e) => {
                            tracing::error!("Background WAL periodic flush task join error: {}", e);
                        }
                        _ => {}
                    }
                }
            });
        }

        let retention_engine = self.engine.clone();
        let retention_interval = self.engine.config().retention_check_interval;
        tokio::spawn(async move {
            loop {
                sleep(retention_interval).await;
                // `apply_retention_all` internally fans each partition's blocking file
                // I/O out onto Tokio's blocking thread pool (bounded by
                // `compaction_worker_threads`), so this task itself just awaits the
                // aggregate result rather than wrapping the whole call in its own
                // spawn_blocking.
                match retention_engine.apply_retention_all().await {
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

        let share_reaper_engine = self.engine.clone();
        tokio::spawn(async move {
            loop {
                sleep(std::time::Duration::from_millis(500)).await;
                share_reaper_engine.sweep_share_lock_timeouts();
            }
        });

        let tx_timeout_engine = self.engine.clone();
        tokio::spawn(async move {
            loop {
                sleep(std::time::Duration::from_secs(5)).await;
                // Aborts hanging transactions (including ones restored from
                // `__transaction_state` at startup that no producer ever resumed) once
                // they exceed `transaction.timeout.ms` — see `sweep_expired_transactions`.
                tx_timeout_engine.sweep_expired_transactions();
            }
        });

        let metrics_engine = self.engine.clone();
        tokio::spawn(async move {
            let config = metrics_engine.config();
            let metrics_addr: SocketAddr = if let Some(ref bind_str) = config.metrics_bind_addr {
                bind_str.parse().unwrap_or_else(|_| {
                    let metrics_port = local_addr.port().checked_add(1000).unwrap_or(9090);
                    SocketAddr::from(([127, 0, 0, 1], metrics_port))
                })
            } else {
                let metrics_port = local_addr.port().checked_add(1000).unwrap_or(9090);
                SocketAddr::from(([127, 0, 0, 1], metrics_port))
            };

            if let Ok(listener) = TcpListener::bind(metrics_addr).await {
                tracing::info!(
                    "Prometheus Metrics Exporter listening on http://{}/metrics",
                    metrics_addr
                );
                loop {
                    if let Ok((mut socket, peer_addr)) = listener.accept().await {
                        let engine = metrics_engine.clone();
                        tokio::spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};

                            // Network control: allowed IPs
                            let allowed_ips = &engine.config().metrics_allowed_ips;
                            if !is_allowed_metrics_peer(allowed_ips, peer_addr.ip()) {
                                let res = "HTTP/1.1 403 Forbidden\r\nContent-Length: 15\r\nConnection: close\r\n\r\n403 Forbidden\n";
                                let _ = socket.write_all(res.as_bytes()).await;
                                return;
                            }

                            let mut buf = [0u8; 1024];
                            let read_bytes = match socket.read(&mut buf).await {
                                Ok(n) if n > 0 => n,
                                _ => return,
                            };

                            // Scrape auth control: token auth
                            if let Some(ref token) = engine.config().metrics_auth_token {
                                let req_str = String::from_utf8_lossy(&buf[..read_bytes]);
                                let auth_header = format!("Authorization: Bearer {}", token);
                                if !req_str.contains(&auth_header) {
                                    let res = "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer\r\nContent-Length: 18\r\nConnection: close\r\n\r\n401 Unauthorized\n";
                                    let _ = socket.write_all(res.as_bytes()).await;
                                    return;
                                }
                            }

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

fn is_allowed_metrics_peer(allowed_ips: &[String], peer_ip: IpAddr) -> bool {
    if allowed_ips.is_empty() {
        return true;
    }

    allowed_ips.iter().any(|rule| {
        let rule = rule.trim();
        if rule == "*" {
            return true;
        }
        if let Ok(ip) = rule.parse::<IpAddr>() {
            return ip == peer_ip;
        }
        cidr_contains(rule, peer_ip)
    })
}

pub(crate) fn cidr_contains(cidr: &str, peer_ip: IpAddr) -> bool {
    let (net_str, prefix_str) = match cidr.split_once('/') {
        Some(parts) => parts,
        None => return false,
    };
    let network_ip: IpAddr = match net_str.parse() {
        Ok(ip) => ip,
        Err(_) => return false,
    };
    let prefix: u8 = match prefix_str.parse() {
        Ok(p) => p,
        Err(_) => return false,
    };

    match (network_ip, peer_ip) {
        (IpAddr::V4(net), IpAddr::V4(peer)) => ipv4_cidr_contains(net, prefix, peer),
        (IpAddr::V6(net), IpAddr::V6(peer)) => ipv6_cidr_contains(net, prefix, peer),
        _ => false,
    }
}

fn ipv4_cidr_contains(network: Ipv4Addr, prefix: u8, peer: Ipv4Addr) -> bool {
    if prefix > 32 {
        return false;
    }
    if prefix == 0 {
        return true;
    }
    let mask = u32::MAX << (32 - prefix);
    (u32::from(network) & mask) == (u32::from(peer) & mask)
}

fn ipv6_cidr_contains(network: Ipv6Addr, prefix: u8, peer: Ipv6Addr) -> bool {
    if prefix > 128 {
        return false;
    }
    if prefix == 0 {
        return true;
    }
    let mask = u128::MAX << (128 - prefix);
    (u128::from(network) & mask) == (u128::from(peer) & mask)
}

#[cfg(test)]
mod tests {
    use super::is_allowed_metrics_peer;

    #[test]
    fn metrics_allowlist_supports_exact_ip_wildcard_and_cidr() {
        assert!(is_allowed_metrics_peer(
            &["*".to_string()],
            "10.1.2.3".parse().unwrap()
        ));
        assert!(is_allowed_metrics_peer(
            &["127.0.0.1".to_string()],
            "127.0.0.1".parse().unwrap()
        ));
        assert!(!is_allowed_metrics_peer(
            &["127.0.0.1".to_string()],
            "127.0.0.2".parse().unwrap()
        ));
        assert!(is_allowed_metrics_peer(
            &["10.0.0.0/8".to_string()],
            "10.11.12.13".parse().unwrap()
        ));
        assert!(!is_allowed_metrics_peer(
            &["10.0.0.0/8".to_string()],
            "11.0.0.1".parse().unwrap()
        ));
        assert!(is_allowed_metrics_peer(
            &["2001:db8::/32".to_string()],
            "2001:db8:abcd::1".parse().unwrap()
        ));
        assert!(!is_allowed_metrics_peer(
            &["2001:db8::/32".to_string()],
            "2001:dead::1".parse().unwrap()
        ));
    }
}
