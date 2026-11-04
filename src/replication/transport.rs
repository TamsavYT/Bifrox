//! Verified TLS for broker RPCs. Insecure replication requires an explicit opt-in.
use rustls::pki_types::pem::PemObject;
use std::{io, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

pub trait PeerIo: AsyncRead + AsyncWrite + Send + Unpin + std::fmt::Debug {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + std::fmt::Debug> PeerIo for T {}
pub type PeerStream = Box<dyn PeerIo>;
#[derive(Clone, Default)]
pub struct PeerTransport {
    tls: Option<Arc<rustls::ClientConfig>>,
    insecure: bool,
    pins: Vec<Vec<u8>>,
}
impl std::fmt::Debug for PeerTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTransport")
            .field("tls", &self.tls.is_some())
            .finish()
    }
}
impl PeerTransport {
    pub fn from_config(config: &crate::config::EngineConfig) -> io::Result<Self> {
        if config.allow_insecure_replication
            && (config.security_protocol != crate::config::SecurityProtocol::Plaintext
                || config.acls_enabled
                || config.auth_token.is_some())
        {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "insecure replication cannot be combined with TLS, SASL, ACLs or token authentication"));
        }
        let mut pins = Vec::new();
        for (id, path) in &config.replication_peer_certificates {
            let cert = rustls::pki_types::CertificateDer::from_pem_file(path)
                .map_err(io::Error::other)?
                .to_vec();
            if *id == config.node_id || pins.contains(&cert) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "peer certificate identities must be unique and exclude this node",
                ));
            }
            pins.push(cert);
        }
        let tls = matches!(
            config.security_protocol,
            crate::config::SecurityProtocol::Ssl | crate::config::SecurityProtocol::SaslSsl
        );
        if !tls && !pins.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "peer certificates require TLS",
            ));
        }
        if !tls {
            return Ok(Self {
                pins: Vec::new(),
                tls: None,
                insecure: config.allow_insecure_replication
                    && !config.acls_enabled
                    && config.auth_token.is_none()
                    && config.security_protocol == crate::config::SecurityProtocol::Plaintext,
            });
        }
        // Standalone TLS servers need not have broker-client credentials configured.
        if config.peer_addrs.is_empty() && config.replication_peer_certificates.is_empty() {
            return Ok(Self::default());
        }
        if pins.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS replication requires replication.peer.cert.<node-id> pins",
            ));
        }
        let required = |p: &Option<std::path::PathBuf>| {
            p.clone().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "TLS replication requires ssl.ca.path, ssl.cert.path and ssl.key.path",
                )
            })
        };
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls::pki_types::CertificateDer::pem_file_iter(required(&config.ssl_ca_path)?)
            .map_err(io::Error::other)?
        {
            roots
                .add(cert.map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
        }
        let certs =
            rustls::pki_types::CertificateDer::pem_file_iter(required(&config.ssl_cert_path)?)
                .map_err(io::Error::other)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(io::Error::other)?;
        let key = rustls::pki_types::PrivateKeyDer::from_pem_file(required(&config.ssl_key_path)?)
            .map_err(io::Error::other)?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .map_err(io::Error::other)?;
        Ok(Self {
            pins,
            tls: Some(Arc::new(tls)),
            insecure: false,
        })
    }
    pub async fn connect(&self, address: &str) -> io::Result<PeerStream> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            if self.tls.is_none() && !self.insecure {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "replication requires authenticated TLS; insecure replication is disabled",
                ));
            }
            let stream = TcpStream::connect(address).await?;
            if let Some(tls) = &self.tls {
                let host = address
                    .parse::<std::net::SocketAddr>()
                    .map(|a| a.ip().to_string())
                    .unwrap_or_else(|_| {
                        address
                            .rsplit_once(':')
                            .map(|(h, _)| h.to_string())
                            .unwrap_or_default()
                    });
                let name =
                    rustls::pki_types::ServerName::try_from(host).map_err(io::Error::other)?;
                let stream = tokio_rustls::TlsConnector::from(tls.clone())
                    .connect(name, stream)
                    .await?;
                let cert = stream
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|c| c.first());
                if !cert.is_some_and(|c| self.pins.iter().any(|p| p.as_slice() == c.as_ref())) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "server is not a pinned cluster peer",
                    ));
                }
                Ok(Box::new(stream) as PeerStream)
            } else {
                Ok(Box::new(stream) as PeerStream)
            }
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "peer connection/handshake timed out",
            )
        })?
    }
    /// Compatibility for low-level codec tests, never used by StorageEngine.
    pub fn insecure_for_tests() -> Self {
        Self {
            pins: Vec::new(),
            tls: None,
            insecure: true,
        }
    }
}
