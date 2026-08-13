use crate::protocol::{CommandCode, RecordFrame, WireResponse};
use crate::scram;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bytes::{Buf, BufMut};
use std::io::Result as IoResult;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_CLIENT_RESPONSE_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProduceResult {
    pub assigned_partition: u32,
    pub first_offset: u64,
    pub last_offset: u64,
}

#[derive(Debug)]
pub struct ConsumerCoordinator {
    pub group_id: String,
    pub member_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslAuthResponse {
    pub error_code: i16,
    pub error_message: String,
    pub auth_bytes: Vec<u8>,
    pub session_lifetime_ms: i64,
}

impl ConsumerCoordinator {
    pub fn new(group_id: String, member_id: String) -> Self {
        Self {
            group_id,
            member_id,
        }
    }

    // A mock background task loop for Rebalance and Heartbeat
    pub async fn run_background_task(&self, mut client: TestClient) {
        loop {
            // Heartbeat
            let _ = client.heartbeat(&self.group_id, 1, &self.member_id).await;
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeekResult {
    pub base_offset: u64,
    pub physical_position: u64,
}

#[derive(Debug, Clone)]
pub struct ClusterDescription {
    pub cluster_id: String,
    pub node_id: u32,
    pub is_leader: bool,
    pub brokers: Vec<(u32, String)>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ClientStream {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl tokio::io::AsyncRead for ClientStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            ClientStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for ClientStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match &mut *self {
            ClientStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match &mut *self {
            ClientStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match &mut *self {
            ClientStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}

/// Helper client for testing protocol commands over TCP
#[derive(Debug)]
pub struct TestClient {
    addr: SocketAddr,
    stream: Option<ClientStream>,
}

impl TestClient {
    async fn read_wire_response<S: AsyncReadExt + Unpin>(stream: &mut S) -> IoResult<WireResponse> {
        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        if payload_len > MAX_CLIENT_RESPONSE_PAYLOAD_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Response payload {} exceeds maximum {} bytes",
                    payload_len, MAX_CLIENT_RESPONSE_PAYLOAD_BYTES
                ),
            ));
        }
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;
        Ok(WireResponse {
            status,
            payload: resp_payload,
        })
    }

    pub async fn connect(addr: SocketAddr) -> IoResult<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            addr,
            stream: Some(ClientStream::Plain(stream)),
        })
    }

    pub async fn connect_tls(addr: SocketAddr) -> IoResult<Self> {
        Self::connect_tls_full(addr, None, None, false).await
    }

    pub async fn connect_tls_with_ca(
        addr: SocketAddr,
        ca_path: &std::path::Path,
    ) -> IoResult<Self> {
        Self::connect_tls_full(addr, Some(ca_path), None, false).await
    }

    pub async fn connect_mtls(
        addr: SocketAddr,
        ca_path: &std::path::Path,
        client_cert_path: &std::path::Path,
        client_key_path: &std::path::Path,
    ) -> IoResult<Self> {
        Self::connect_tls_full(
            addr,
            Some(ca_path),
            Some((client_cert_path, client_key_path)),
            false,
        )
        .await
    }

    pub async fn connect_tls_full(
        addr: SocketAddr,
        ca_path: Option<&std::path::Path>,
        client_auth: Option<(&std::path::Path, &std::path::Path)>,
        insecure_skip_verify: bool,
    ) -> IoResult<Self> {
        Self::connect_tls_full_with_domain(addr, ca_path, client_auth, insecure_skip_verify, None)
            .await
    }

    pub async fn connect_tls_full_with_domain(
        addr: SocketAddr,
        ca_path: Option<&std::path::Path>,
        client_auth: Option<(&std::path::Path, &std::path::Path)>,
        insecure_skip_verify: bool,
        server_name: Option<&str>,
    ) -> IoResult<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tcp_stream = TcpStream::connect(addr).await?;

        let config = if insecure_skip_verify {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(NoVerify))
                .with_no_client_auth()
        } else {
            let mut root_store = rustls::RootCertStore::empty();
            if let Some(ca) = ca_path {
                let ca_file = std::fs::File::open(ca)?;
                let mut ca_reader = std::io::BufReader::new(ca_file);
                let ca_certs =
                    rustls_pemfile::certs(&mut ca_reader).collect::<Result<Vec<_>, _>>()?;
                for c in ca_certs {
                    root_store.add(c).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?;
                }
            } else {
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);
            if let Some((cert_path, key_path)) = client_auth {
                let cert_file = std::fs::File::open(cert_path)?;
                let mut cert_reader = std::io::BufReader::new(cert_file);
                let client_certs =
                    rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

                let key_file = std::fs::File::open(key_path)?;
                let mut key_reader = std::io::BufReader::new(key_file);
                let client_key =
                    rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "No private key found in client SSL key file",
                        )
                    })?;

                builder
                    .with_client_auth_cert(client_certs, client_key)
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
                    })?
            } else {
                builder.with_no_client_auth()
            }
        };

        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
        let domain_str = match server_name {
            Some(name) => name.to_string(),
            None => {
                let ip = addr.ip();
                if ip.is_loopback() {
                    "localhost".to_string()
                } else {
                    ip.to_string()
                }
            }
        };

        let domain = rustls::pki_types::ServerName::try_from(domain_str.as_str())
            .map(|s| s.to_owned())
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid TLS server name '{}': {}", domain_str, e),
                )
            })?;

        let tls_stream = connector.connect(domain, tcp_stream).await?;
        Ok(Self {
            addr,
            stream: Some(ClientStream::Tls(tls_stream)),
        })
    }

    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    pub fn disconnect(&mut self) {
        self.stream = None;
    }

    pub async fn reconnect(&mut self) -> IoResult<()> {
        let is_tls = matches!(self.stream, Some(ClientStream::Tls(_)));
        if is_tls {
            let tls_client = Self::connect_tls(self.addr).await?;
            self.stream = tls_client.stream;
        } else {
            let stream = TcpStream::connect(self.addr).await?;
            self.stream = Some(ClientStream::Plain(stream));
        }
        Ok(())
    }

    pub async fn ping_handshake(&mut self) -> IoResult<bool> {
        match self.latest_offset("__ping_check__", 0).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    pub async fn produce_single(
        &mut self,
        topic: &str,
        key: &str,
        transaction_id: Option<&str>,
        num_partitions: u32,
        payload: impl AsRef<[u8]>,
    ) -> IoResult<ProduceResult> {
        self.produce_batch(
            topic,
            key,
            transaction_id,
            num_partitions,
            &[payload.as_ref()],
        )
        .await
    }

    pub async fn produce_batch(
        &mut self,
        topic: &str,
        key: &str,
        transaction_id: Option<&str>,
        num_partitions: u32,
        records: &[impl AsRef<[u8]>],
    ) -> IoResult<ProduceResult> {
        self.produce_batch_eos(topic, key, transaction_id, num_partitions, 0, 0, 0, records)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn produce_batch_eos(
        &mut self,
        topic: &str,
        key: &str,
        transaction_id: Option<&str>,
        num_partitions: u32,
        producer_id: u64,
        producer_epoch: i16,
        base_sequence: i32,
        records: &[impl AsRef<[u8]>],
    ) -> IoResult<ProduceResult> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::ProduceBatch as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        crate::protocol::wire::write_pascal_string(&mut inner, key);
        crate::protocol::wire::write_pascal_string(&mut inner, transaction_id.unwrap_or(""));
        inner.put_u32(num_partitions);
        inner.put_u64(producer_id);
        inner.put_i16(producer_epoch);
        inner.put_i32(base_sequence);
        inner.put_u32(records.len() as u32);
        for rec in records {
            let slice = rec.as_ref();
            inner.put_u32(slice.len() as u32);
            inner.put_slice(slice);
        }

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            if resp_payload.len() < 20 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Payload too short",
                ));
            }
            let assigned_partition = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap());
            let first_offset = u64::from_be_bytes(resp_payload[4..12].try_into().unwrap());
            let last_offset = u64::from_be_bytes(resp_payload[12..20].try_into().unwrap());
            Ok(ProduceResult {
                assigned_partition,
                first_offset,
                last_offset,
            })
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::Fetch as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);
        inner.put_u64(offset);
        inner.put_u32(max_bytes);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            if resp_payload.len() < 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Payload too short",
                ));
            }
            let count = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap()) as usize;
            let mut frames = Vec::with_capacity(count);
            let mut cursor = 4usize;
            for _ in 0..count {
                if cursor >= resp_payload.len() {
                    break;
                }
                match RecordFrame::decode(&resp_payload[cursor..]) {
                    Ok((mut frame, consumed)) => {
                        cursor += consumed;
                        if let Ok(decompressed) = frame.decompress_payload() {
                            frame.payload = decompressed;
                        }
                        frames.push(frame);
                    }
                    Err(_) => break,
                }
            }
            Ok(frames)
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn commit_offset(
        &mut self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::CommitOffset as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);
        inner.put_u64(offset);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn fetch_offset(
        &mut self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> IoResult<u64> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::FetchOffset as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            if resp_payload.len() < 8 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Payload too short",
                ));
            }
            let offset = u64::from_be_bytes(resp_payload[0..8].try_into().unwrap());
            Ok(offset)
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn describe_topic(
        &mut self,
        topic: &str,
    ) -> IoResult<(String, Vec<crate::protocol::wire::DescribedPartition>)> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::DescribeTopic as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            use bytes::Buf;
            let mut cursor = &resp_payload[..];
            let t_len = cursor.get_u16() as usize;
            let res_topic = String::from_utf8_lossy(&cursor[..t_len]).to_string();
            cursor = &cursor[t_len..];
            let count = cursor.get_u32() as usize;
            let mut partitions = Vec::with_capacity(count);
            for _ in 0..count {
                let partition_id = cursor.get_u32();
                let high_watermark = cursor.get_u64();
                let leader_id = cursor.get_u32();
                let rep_len = cursor.get_u32() as usize;
                let mut replicas = Vec::with_capacity(rep_len);
                for _ in 0..rep_len {
                    replicas.push(cursor.get_u32());
                }
                partitions.push(crate::protocol::wire::DescribedPartition {
                    partition_id,
                    high_watermark,
                    leader_id,
                    replicas,
                });
            }
            Ok((res_topic, partitions))
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn describe_group(
        &mut self,
        group_id: &str,
    ) -> IoResult<(String, Vec<crate::protocol::wire::DescribedGroupMember>)> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::DescribeGroup as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            use bytes::Buf;
            let mut cursor = &resp_payload[..];
            let s_len = cursor.get_u16() as usize;
            let state_str = String::from_utf8_lossy(&cursor[..s_len]).to_string();
            cursor = &cursor[s_len..];
            let member_count = cursor.get_u32() as usize;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let m_len = cursor.get_u16() as usize;
                let member_id = String::from_utf8_lossy(&cursor[..m_len]).to_string();
                cursor = &cursor[m_len..];
                let assign_count = cursor.get_u32() as usize;
                let mut assigned_partitions = Vec::with_capacity(assign_count);
                for _ in 0..assign_count {
                    let top_len = cursor.get_u16() as usize;
                    let top = String::from_utf8_lossy(&cursor[..top_len]).to_string();
                    cursor = &cursor[top_len..];
                    let part = cursor.get_u32();
                    assigned_partitions.push((top, part));
                }
                members.push(crate::protocol::wire::DescribedGroupMember {
                    member_id,
                    assigned_partitions,
                });
            }
            Ok((state_str, members))
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn latest_offset(&mut self, topic: &str, partition: u32) -> IoResult<u64> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::LatestOffset as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            if resp_payload.len() < 8 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Payload too short",
                ));
            }
            let watermark = u64::from_be_bytes(resp_payload[0..8].try_into().unwrap());
            Ok(watermark)
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn seek(&mut self, topic: &str, partition: u32, offset: u64) -> IoResult<SeekResult> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::Seek as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);
        inner.put_u64(offset);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            if resp_payload.len() < 16 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Payload too short",
                ));
            }
            let base_offset = u64::from_be_bytes(resp_payload[0..8].try_into().unwrap());
            let physical_position = u64::from_be_bytes(resp_payload[8..16].try_into().unwrap());
            Ok(SeekResult {
                base_offset,
                physical_position,
            })
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn register_broker(&mut self, node_id: u32, endpoint: &str) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::RegisterBroker as u8);

        let mut inner = Vec::new();
        inner.put_u32(node_id);
        crate::protocol::wire::write_pascal_string(&mut inner, endpoint);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        }
    }

    pub async fn unregister_broker(&mut self, node_id: u32) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::UnregisterBroker as u8);

        let mut inner = Vec::new();
        inner.put_u32(node_id);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        }
    }

    pub async fn sasl_handshake(&mut self, mechanism: &str) -> IoResult<(i16, Vec<String>)> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::SaslHandshake as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, mechanism);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;
        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let mut buf = &resp.payload[..];
        if buf.len() < 6 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid SaslHandshake payload",
            ));
        }
        let err_code = buf.get_i16();
        let count = buf.get_u32() as usize;
        let mut mechs = Vec::with_capacity(count);
        for _ in 0..count {
            let len = buf.get_u16() as usize;
            let m = String::from_utf8_lossy(&buf[..len]).to_string();
            buf = &buf[len..];
            mechs.push(m);
        }
        Ok((err_code, mechs))
    }

    pub async fn sasl_authenticate_full(
        &mut self,
        auth_bytes: &[u8],
    ) -> IoResult<SaslAuthResponse> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::SaslAuthenticate as u8);
        req_buf.put_u32(auth_bytes.len() as u32);
        req_buf.extend_from_slice(auth_bytes);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;
        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let mut buf = &resp.payload[..];
        if buf.len() < 2 {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid SaslAuthenticate payload",
            ))
        } else {
            let error_code = buf.get_i16();
            let error_message = if buf.len() >= 2 {
                let len = buf.get_u16() as usize;
                if len > 0 && buf.len() >= len {
                    let msg = String::from_utf8_lossy(&buf[..len]).to_string();
                    buf = &buf[len..];
                    msg
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            let auth_bytes = if buf.len() >= 4 {
                let len = buf.get_u32() as usize;
                if buf.len() >= len {
                    let data = buf[..len].to_vec();
                    buf = &buf[len..];
                    data
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            let session_lifetime_ms = if buf.len() >= 8 { buf.get_i64() } else { 0 };
            Ok(SaslAuthResponse {
                error_code,
                error_message,
                auth_bytes,
                session_lifetime_ms,
            })
        }
    }

    pub async fn sasl_authenticate(&mut self, auth_bytes: &[u8]) -> IoResult<i16> {
        Ok(self.sasl_authenticate_full(auth_bytes).await?.error_code)
    }

    pub async fn sasl_authenticate_scram_sha256(
        &mut self,
        username: &str,
        password: &str,
    ) -> IoResult<i16> {
        let client_nonce = generate_scram_client_nonce()?;
        let client_first = format!("n,,n={},r={}", username, client_nonce);
        let first_response = self.sasl_authenticate_full(client_first.as_bytes()).await?;
        if first_response.error_code != 0 {
            return Ok(first_response.error_code);
        }

        let server_first = String::from_utf8(first_response.auth_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let (combined_nonce, salt, iterations) = parse_scram_server_first(&server_first)?;
        let client_final = build_scram_client_final(
            password,
            &client_first[3..],
            &server_first,
            &combined_nonce,
            &salt,
            iterations,
        )?;
        let final_response = self.sasl_authenticate_full(client_final.as_bytes()).await?;
        Ok(final_response.error_code)
    }

    pub async fn create_acl(&mut self, binding: &crate::server::acl::AclBinding) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::CreateAcls as u8);
        let mut inner = Vec::new();
        inner.put_u8(binding.resource_type);
        crate::protocol::wire::write_pascal_string(&mut inner, &binding.resource_name);
        inner.put_u8(binding.pattern_type);
        crate::protocol::wire::write_pascal_string(&mut inner, &binding.principal);
        crate::protocol::wire::write_pascal_string(&mut inner, &binding.host);
        inner.put_u8(binding.operation);
        inner.put_u8(binding.permission_type);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;
        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        }
    }

    pub async fn describe_acls(
        &mut self,
        filter: &crate::server::acl::AclBinding,
    ) -> IoResult<Vec<crate::server::acl::AclBinding>> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::DescribeAcls as u8);
        let mut inner = Vec::new();
        inner.put_u8(filter.resource_type);
        crate::protocol::wire::write_pascal_string(&mut inner, &filter.resource_name);
        inner.put_u8(filter.pattern_type);
        crate::protocol::wire::write_pascal_string(&mut inner, &filter.principal);
        crate::protocol::wire::write_pascal_string(&mut inner, &filter.host);
        inner.put_u8(filter.operation);
        inner.put_u8(filter.permission_type);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;
        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        if resp.status == 0 {
            let mut buf = &resp.payload[..];
            if buf.len() < 6 {
                return Ok(Vec::new());
            }
            let _err_code = buf.get_i16();
            let count = buf.get_u32() as usize;
            let mut result = Vec::with_capacity(count);
            for _ in 0..count {
                let resource_type = buf.get_u8();
                let r_len = buf.get_u16() as usize;
                let resource_name = String::from_utf8_lossy(&buf[..r_len]).to_string();
                buf = &buf[r_len..];
                let pattern_type = buf.get_u8();
                let p_len = buf.get_u16() as usize;
                let principal = String::from_utf8_lossy(&buf[..p_len]).to_string();
                buf = &buf[p_len..];
                let h_len = buf.get_u16() as usize;
                let host = String::from_utf8_lossy(&buf[..h_len]).to_string();
                buf = &buf[h_len..];
                let operation = buf.get_u8();
                let permission_type = buf.get_u8();
                result.push(crate::server::acl::AclBinding {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }
            Ok(result)
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        }
    }

    pub async fn begin_transaction(
        &mut self,
        transaction_id: &str,
        producer_id: u64,
    ) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::BeginTx as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, transaction_id);
        inner.put_u64(producer_id);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn commit_transaction(&mut self, transaction_id: &str) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::CommitTx as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, transaction_id);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn abort_transaction(&mut self, transaction_id: &str) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::AbortTx as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, transaction_id);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn init_producer_id(&mut self, transactional_id: &str) -> IoResult<(u64, i16)> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::InitProducerId as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, transactional_id);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            if resp_payload.len() < 10 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Payload too short",
                ));
            }
            let producer_id = u64::from_be_bytes(resp_payload[0..8].try_into().unwrap());
            let producer_epoch = i16::from_be_bytes(resp_payload[8..10].try_into().unwrap());
            Ok((producer_id, producer_epoch))
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn add_partitions_to_txn(
        &mut self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: i16,
        topics: &[(&str, &[u32])],
    ) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::AddPartitionsToTxn as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, transactional_id);
        inner.put_u64(producer_id);
        inner.put_i16(producer_epoch);
        inner.put_u32(topics.len() as u32);
        for (t_name, parts) in topics {
            crate::protocol::wire::write_pascal_string(&mut inner, t_name);
            inner.put_u32(parts.len() as u32);
            for p in *parts {
                inner.put_u32(*p);
            }
        }

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn end_txn(
        &mut self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: i16,
        committed: bool,
    ) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::EndTxn as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, transactional_id);
        inner.put_u64(producer_id);
        inner.put_i16(producer_epoch);
        inner.put_u8(if committed { 1 } else { 0 });

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn fetch_by_timestamp(
        &mut self,
        topic: &str,
        partition: u32,
        target_timestamp: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::FetchByTimestamp as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);
        inner.put_u64(target_timestamp);
        inner.put_u32(max_bytes);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            if resp_payload.len() < 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Payload too short",
                ));
            }
            let count = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap()) as usize;
            let mut frames = Vec::with_capacity(count);
            let mut cursor = 4usize;
            for _ in 0..count {
                if cursor >= resp_payload.len() {
                    break;
                }
                match RecordFrame::decode(&resp_payload[cursor..]) {
                    Ok((mut frame, consumed)) => {
                        cursor += consumed;
                        if let Ok(decompressed) = frame.decompress_payload() {
                            frame.payload = decompressed;
                        }
                        frames.push(frame);
                    }
                    Err(_) => break,
                }
            }
            Ok(frames)
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    /// P1: Read-committed fetch — hides uncommitted/aborted records (LSO filtered).
    /// Same wire shape as `fetch` but uses command code 0x0B (FetchCommitted).
    pub async fn fetch_committed(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::FetchCommitted as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);
        inner.put_u64(offset);
        inner.put_u32(max_bytes);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(&req_buf).await?;
        let resp = Self::read_wire_response(stream).await?;
        let status = resp.status;
        let resp_payload = resp.payload;

        if status == 0 {
            if resp_payload.len() < 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Payload too short",
                ));
            }
            let count = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap()) as usize;
            let mut frames = Vec::with_capacity(count);
            let mut cursor = 4usize;
            for _ in 0..count {
                if cursor >= resp_payload.len() {
                    break;
                }
                match RecordFrame::decode(&resp_payload[cursor..]) {
                    Ok((mut frame, consumed)) => {
                        cursor += consumed;
                        if let Ok(decompressed) = frame.decompress_payload() {
                            frame.payload = decompressed;
                        }
                        frames.push(frame);
                    }
                    Err(_) => break,
                }
            }
            Ok(frames)
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn send_raw_bytes_no_wait(&mut self, raw: &[u8]) -> IoResult<()> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(raw).await?;
        Ok(())
    }

    pub async fn send_raw_bytes(&mut self, raw: &[u8]) -> IoResult<WireResponse> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
        })?;

        stream.write_all(raw).await?;
        Self::read_wire_response(stream).await
    }

    pub async fn set_client_id(&mut self, client_id: &str) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::SetClientId as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, client_id);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        }
    }

    pub async fn upsert_scram_user(&mut self, username: &str, password: &str) -> IoResult<()> {
        let credential = scram::ScramCredential::generate(
            username,
            password,
            scram::DEFAULT_SCRAM_SHA256_ITERATIONS,
        )
        .map_err(|_| std::io::Error::other("Failed to generate SCRAM credential"))?;
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::UpsertScramUser as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, username);
        inner.put_u32(credential.iterations);
        write_len_prefixed_bytes(&mut inner, &credential.salt);
        write_len_prefixed_bytes(&mut inner, &credential.stored_key);
        write_len_prefixed_bytes(&mut inner, &credential.server_key);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        }
    }

    pub async fn delete_scram_user(&mut self, username: &str) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::DeleteScramUser as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, username);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        }
    }

    pub async fn ping(&mut self) -> IoResult<bool> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::Ping as u8);
        req_buf.put_u32(0);
        let resp = self.send_raw_bytes(&req_buf).await?;
        Ok(resp.status == 0 && resp.payload == b"PONG")
    }

    pub async fn describe_cluster(&mut self) -> IoResult<ClusterDescription> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::DescribeCluster as u8);
        req_buf.put_u32(0);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status != 0 {
            return Err(std::io::Error::other("DescribeCluster failed"));
        }

        let mut payload = &resp.payload[..];
        if payload.len() < 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "DescribeCluster payload too short",
            ));
        }
        let cluster_len = payload.get_u16() as usize;
        if payload.len() < cluster_len + 5 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "DescribeCluster payload incomplete",
            ));
        }
        let cluster_id = String::from_utf8_lossy(&payload[..cluster_len]).to_string();
        payload = &payload[cluster_len..];
        let node_id = payload.get_u32();
        let is_leader = payload.get_u8() != 0;

        let mut brokers = Vec::new();
        if payload.len() >= 4 {
            let broker_count = payload.get_u32() as usize;
            for _ in 0..broker_count {
                if payload.len() < 6 {
                    break;
                }
                let b_id = payload.get_u32();
                let len = payload.get_u16() as usize;
                if payload.len() < len {
                    break;
                }
                let addr = String::from_utf8_lossy(&payload[..len]).to_string();
                payload = &payload[len..];
                brokers.push((b_id, addr));
            }
        }

        Ok(ClusterDescription {
            cluster_id,
            node_id,
            is_leader,
            brokers,
        })
    }

    pub async fn list_topics(&mut self) -> IoResult<Vec<String>> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::ListTopics as u8);
        req_buf.put_u32(0);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            let mut src = &resp.payload[..];
            if src.len() < 4 {
                return Ok(Vec::new());
            }
            let count = src.get_u32() as usize;
            let mut topics = Vec::with_capacity(count);
            for _ in 0..count {
                if src.len() < 2 {
                    break;
                }
                let len = src.get_u16() as usize;
                if src.len() < len {
                    break;
                }
                let t = String::from_utf8_lossy(&src[..len]).to_string();
                src = &src[len..];
                topics.push(t);
            }
            Ok(topics)
        } else {
            Err(std::io::Error::other("ListTopics failed"))
        }
    }

    pub async fn delete_topic(&mut self, topic: &str) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::DeleteTopic as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other("DeleteTopic failed"))
        }
    }

    pub async fn create_topic(&mut self, topic: &str, partitions: u32) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::CreateTopic as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partitions);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other("CreateTopic failed"))
        }
    }

    pub async fn join_group(
        &mut self,
        group_id: &str,
        member_id: &str,
        protocols: &[&str],
    ) -> IoResult<String> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::JoinGroup as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);
        crate::protocol::wire::write_pascal_string(&mut inner, member_id);
        inner.put_u32(protocols.len() as u32);
        for p in protocols {
            crate::protocol::wire::write_pascal_string(&mut inner, p);
        }
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            let mut payload = &resp.payload[..];
            if payload.len() >= 2 {
                let len = bytes::Buf::get_u16(&mut payload) as usize;
                let m_id = String::from_utf8_lossy(&payload[..len]).to_string();
                Ok(m_id)
            } else {
                Ok(member_id.to_string())
            }
        } else {
            Err(std::io::Error::other("JoinGroup failed"))
        }
    }

    pub async fn sync_group(
        &mut self,
        group_id: &str,
        generation_id: u32,
        member_id: &str,
        assignments: &[crate::protocol::wire::MemberAssignment],
    ) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::SyncGroup as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);
        inner.put_u32(generation_id);
        crate::protocol::wire::write_pascal_string(&mut inner, member_id);

        inner.put_u32(assignments.len() as u32);
        for a in assignments {
            crate::protocol::wire::write_pascal_string(&mut inner, &a.member_id);
            crate::protocol::wire::write_pascal_string(&mut inner, &a.topic);
            inner.put_u32(a.partitions.len() as u32);
            for p in &a.partitions {
                inner.put_u32(*p);
            }
        }

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other("SyncGroup failed"))
        }
    }

    pub async fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: u32,
        member_id: &str,
    ) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::Heartbeat as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);
        inner.put_u32(generation_id);
        crate::protocol::wire::write_pascal_string(&mut inner, member_id);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other("Heartbeat failed"))
        }
    }

    pub async fn leave_group(&mut self, group_id: &str, member_id: &str) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::LeaveGroup as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);
        crate::protocol::wire::write_pascal_string(&mut inner, member_id);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other("LeaveGroup failed"))
        }
    }

    pub async fn offset_commit(
        &mut self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        metadata: &str,
    ) -> IoResult<()> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::OffsetCommit as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);
        inner.put_u64(offset);
        crate::protocol::wire::write_pascal_string(&mut inner, metadata);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other("OffsetCommit failed"))
        }
    }

    pub async fn offset_fetch(
        &mut self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> IoResult<(u64, String)> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::OffsetFetch as u8);
        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, group_id);
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(partition);
        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);
        let resp = self.send_raw_bytes(&req_buf).await?;
        if resp.status == 0 {
            let mut payload = &resp.payload[..];
            if payload.len() < 8 {
                return Err(std::io::Error::other(
                    "Incomplete OffsetFetch response payload",
                ));
            }
            let offset = payload.get_u64();
            let metadata = if payload.len() >= 2 {
                let len = payload.get_u16() as usize;
                if payload.len() >= len {
                    let str_bytes = &payload[..len];
                    String::from_utf8_lossy(str_bytes).to_string()
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            Ok((offset, metadata))
        } else {
            Err(std::io::Error::other("OffsetFetch failed"))
        }
    }
}

fn generate_scram_client_nonce() -> IoResult<String> {
    let rng = ring::rand::SystemRandom::new();
    let mut nonce = [0u8; 18];
    ring::rand::SecureRandom::fill(&rng, &mut nonce)
        .map_err(|_| std::io::Error::other("Failed to generate SCRAM nonce"))?;
    Ok(scram::hex_encode(&nonce))
}

fn parse_scram_server_first(server_first: &str) -> IoResult<(String, Vec<u8>, u32)> {
    let mut nonce = None;
    let mut salt_b64 = None;
    let mut iterations = None;
    for part in server_first.split(',') {
        if let Some(value) = part.strip_prefix("r=") {
            nonce = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("s=") {
            salt_b64 = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("i=") {
            iterations = value.parse::<u32>().ok();
        }
    }

    let salt = BASE64_STANDARD
        .decode(salt_b64.ok_or_else(|| std::io::Error::other("SCRAM salt missing"))?)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    Ok((
        nonce.ok_or_else(|| std::io::Error::other("SCRAM nonce missing"))?,
        salt,
        iterations.ok_or_else(|| std::io::Error::other("SCRAM iteration count missing"))?,
    ))
}

fn build_scram_client_final(
    password: &str,
    client_first_bare: &str,
    server_first: &str,
    combined_nonce: &str,
    salt: &[u8],
    iterations: u32,
) -> IoResult<String> {
    let client_final_without_proof = format!("c=biws,r={}", combined_nonce);
    let auth_message = format!(
        "{},{},{}",
        client_first_bare, server_first, client_final_without_proof
    );
    let salted_password = scram::derive_scram_salted_password(password, salt, iterations);
    let client_key = scram::hmac_sha256(&salted_password, b"Client Key");
    let stored_key = scram::sha256(&client_key);
    let client_signature = scram::hmac_sha256(&stored_key, auth_message.as_bytes());
    let proof: Vec<u8> = client_key
        .iter()
        .zip(client_signature.iter())
        .map(|(key_byte, sig_byte)| key_byte ^ sig_byte)
        .collect();
    let proof_b64 = BASE64_STANDARD.encode(proof);
    Ok(format!("{},p={}", client_final_without_proof, proof_b64))
}

fn write_len_prefixed_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.put_u16(bytes.len() as u16);
    buf.extend_from_slice(bytes);
}

/// Smart client supporting cluster metadata discovery, partition leadership resolution, and connection pooling
#[derive(Debug)]
pub struct RoutedClient {
    bootstrap_addr: SocketAddr,
    auth_token: Option<String>,
    connections: std::collections::HashMap<u32, TestClient>,
    broker_addrs: std::collections::HashMap<u32, SocketAddr>,
    topic_metadata:
        std::collections::HashMap<String, Vec<crate::protocol::wire::DescribedPartition>>,
}

impl RoutedClient {
    pub async fn connect(bootstrap_addr: SocketAddr) -> IoResult<Self> {
        let bootstrap_client = TestClient::connect(bootstrap_addr).await?;
        let mut connections = std::collections::HashMap::new();
        connections.insert(1, bootstrap_client);

        let mut client = Self {
            bootstrap_addr,
            auth_token: None,
            connections,
            broker_addrs: std::collections::HashMap::new(),
            topic_metadata: std::collections::HashMap::new(),
        };

        client.broker_addrs.insert(1, bootstrap_addr);
        Ok(client)
    }

    pub fn with_auth(mut self, auth_token: String) -> Self {
        self.auth_token = Some(auth_token);
        self
    }

    /// Refresh metadata for target topic via DescribeTopic
    pub async fn refresh_metadata(&mut self, topic: &str) -> IoResult<()> {
        let client = self.connections.get_mut(&1).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Bootstrap client not connected",
            )
        })?;

        if let Ok(cluster) = client.describe_cluster().await {
            for (node_id, addr) in cluster.brokers {
                if let Ok(mut addrs) = addr.to_socket_addrs() {
                    if let Some(sock) = addrs.next() {
                        self.broker_addrs.insert(node_id, sock);
                    }
                }
            }
        }

        if let Ok((res_topic, partitions)) = client.describe_topic(topic).await {
            self.topic_metadata.insert(res_topic, partitions);
        }
        Ok(())
    }

    /// Register a broker node address for client routing
    pub fn register_broker(&mut self, node_id: u32, addr: SocketAddr) {
        self.broker_addrs.insert(node_id, addr);
    }

    /// Ensures connection to target broker node ID
    pub async fn ensure_connection(&mut self, node_id: u32) -> IoResult<&mut TestClient> {
        if !self.connections.contains_key(&node_id) {
            let addr = self
                .broker_addrs
                .get(&node_id)
                .copied()
                .unwrap_or(self.bootstrap_addr);
            let client = TestClient::connect(addr).await?;
            self.connections.insert(node_id, client);
        }
        Ok(self.connections.get_mut(&node_id).unwrap())
    }

    /// Smart produce: resolves target partition leader and sends ProduceBatch directly to leader IP
    pub async fn produce_smart(
        &mut self,
        topic: &str,
        key: &str,
        transaction_id: Option<&str>,
        num_partitions: u32,
        records: &[bytes::Bytes],
    ) -> IoResult<ProduceResult> {
        self.refresh_metadata(topic).await?;

        let target_partition = if !key.is_empty() && num_partitions > 0 {
            crate::server::hash_key(key.as_bytes(), num_partitions as usize)
        } else {
            0
        };

        let leader_id = self
            .topic_metadata
            .get(topic)
            .and_then(|parts| parts.iter().find(|p| p.partition_id == target_partition))
            .map(|p| p.leader_id)
            .unwrap_or(1);

        let client = self.ensure_connection(leader_id).await?;
        client
            .produce_batch(topic, key, transaction_id, num_partitions, records)
            .await
    }

    /// Smart fetch: resolves partition leader and fetches directly
    pub async fn fetch_smart(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        if !self.topic_metadata.contains_key(topic) {
            let _ = self.refresh_metadata(topic).await;
        }

        let leader_id = self
            .topic_metadata
            .get(topic)
            .and_then(|parts| parts.iter().find(|p| p.partition_id == partition))
            .map(|p| p.leader_id)
            .unwrap_or(1);

        let client = self.ensure_connection(leader_id).await?;
        client.fetch(topic, partition, offset, max_bytes).await
    }
}
