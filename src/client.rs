use crate::protocol::{
    CommandCode, FrameError, RecordFrame, RequestPayload, WireError, WireRequest, WireResponse,
};
use bytes::{Buf, Bytes};
use std::fmt::Write as FmtWrite;
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Wire protocol error: {0}")]
    Wire(#[from] WireError),

    #[error("Frame format error: {0}")]
    Frame(#[from] FrameError),

    #[error("Server returned error (status=1): {0}")]
    ServerError(String),

    #[error("Not connected to server")]
    NotConnected,

    #[error("Timed out waiting for response")]
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponse {
    pub assigned_partition: u32,
    pub first_offset: u64,
    pub last_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeekResult {
    pub base_offset: u64,
    pub physical_pos: u64,
}

/// High-performance, modular test client for the Kafka-like event storage engine
pub struct TestClient {
    addr: SocketAddr,
    stream: Option<TcpStream>,
    debug_logging: bool,
    read_timeout: Duration,
}

impl TestClient {
    /// Connects to a target server address
    pub async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        Self::connect_with_timeout(addr, Duration::from_secs(5)).await
    }

    /// Connects with a custom connection timeout
    pub async fn connect_with_timeout(
        addr: SocketAddr,
        connect_timeout: Duration,
    ) -> Result<Self, ClientError> {
        let stream = timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Timeout)??;

        tracing::debug!("TestClient connected successfully to {}", addr);

        Ok(Self {
            addr,
            stream: Some(stream),
            debug_logging: false,
            read_timeout: Duration::from_secs(5),
        })
    }

    /// Set whether hex dump byte logging is enabled for request/response frames
    pub fn set_debug_logging(&mut self, enabled: bool) {
        self.debug_logging = enabled;
    }

    /// Set read timeout for request responses
    pub fn set_read_timeout(&mut self, timeout: Duration) {
        self.read_timeout = timeout;
    }

    /// Target server socket address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Checks if active TCP connection stream is established
    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// Explicitly closes the existing connection
    pub fn disconnect(&mut self) {
        self.stream = None;
        tracing::debug!("TestClient disconnected from {}", self.addr);
    }

    /// Reconnects to the server
    pub async fn reconnect(&mut self) -> Result<(), ClientError> {
        self.disconnect();
        let new_client = Self::connect(self.addr).await?;
        self.stream = new_client.stream;
        Ok(())
    }

    /// Formats bytes as a clean hex dump for debugging wire frames
    pub fn hex_dump(data: &[u8]) -> String {
        let mut out = String::new();
        for (i, chunk) in data.chunks(16).enumerate() {
            let _ = write!(out, "\n{:04x}  ", i * 16);
            for b in chunk {
                let _ = write!(out, "{:02X} ", b);
            }
            if chunk.len() < 16 {
                for _ in 0..(16 - chunk.len()) {
                    out.push_str("   ");
                }
            }
            out.push_str(" |");
            for b in chunk {
                if b.is_ascii_graphic() || *b == b' ' {
                    out.push(*b as char);
                } else {
                    out.push('.');
                }
            }
            out.push('|');
        }
        out
    }

    /// Low-level method to send a structured WireRequest and await a WireResponse
    pub async fn send_request(&mut self, req: &WireRequest) -> Result<WireResponse, ClientError> {
        let req_bytes = req.encode();
        self.send_bytes_and_receive(&req_bytes).await
    }

    /// Send raw bytes directly (used for malformed frame and edge-case testing)
    pub async fn send_raw_bytes(&mut self, raw: &[u8]) -> Result<WireResponse, ClientError> {
        self.send_bytes_and_receive(raw).await
    }

    /// Internal helper to write payload bytes and parse standard 5-byte header + payload WireResponse
    async fn send_bytes_and_receive(
        &mut self,
        bytes_to_send: &[u8],
    ) -> Result<WireResponse, ClientError> {
        let stream = self.stream.as_mut().ok_or(ClientError::NotConnected)?;

        if self.debug_logging {
            tracing::trace!(
                "TestClient OUTBOUND {} bytes:{}",
                bytes_to_send.len(),
                Self::hex_dump(bytes_to_send)
            );
        }

        stream.write_all(bytes_to_send).await?;
        stream.flush().await?;

        let mut header_buf = [0u8; 5];
        let read_res = timeout(self.read_timeout, stream.read_exact(&mut header_buf)).await;

        match read_res {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(ClientError::Io(e)),
            Err(_) => return Err(ClientError::Timeout),
        };

        let status = header_buf[0];
        let payload_len = u32::from_be_bytes(header_buf[1..5].try_into().unwrap()) as usize;

        let mut payload_buf = vec![0u8; payload_len];
        if payload_len > 0 {
            timeout(self.read_timeout, stream.read_exact(&mut payload_buf))
                .await
                .map_err(|_| ClientError::Timeout)??;
        }

        if self.debug_logging {
            tracing::trace!(
                "TestClient INBOUND status={} len={}:{}",
                status,
                payload_len,
                Self::hex_dump(&payload_buf)
            );
        }

        Ok(WireResponse {
            status,
            payload: payload_buf,
        })
    }

    /// Scenario 1: Connection & Protocol Handshake Verification
    /// Performs a metadata roundtrip (fetching latest offset of system ping topic) to verify protocol reactivity
    pub async fn ping_handshake(&mut self) -> Result<bool, ClientError> {
        let req = WireRequest {
            cmd: CommandCode::LatestOffset,
            payload: RequestPayload::LatestOffset {
                topic: "__ping__".to_string(),
                partition: 0,
            },
        };

        let resp = self.send_request(&req).await?;
        if resp.status == 0 && resp.payload.len() == 8 {
            Ok(true)
        } else if resp.status == 1 {
            Err(ClientError::ServerError(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        } else {
            Ok(false)
        }
    }

    /// Scenario 2: Produce Flow (Single or Batched)
    pub async fn produce_batch(
        &mut self,
        topic: &str,
        key: &str,
        num_partitions: u32,
        records: &[impl AsRef<[u8]>],
    ) -> Result<ProduceResponse, ClientError> {
        let rec_bytes: Vec<Bytes> = records
            .iter()
            .map(|r| Bytes::copy_from_slice(r.as_ref()))
            .collect();
        let req = WireRequest {
            cmd: CommandCode::ProduceBatch,
            payload: RequestPayload::ProduceBatch {
                topic: topic.to_string(),
                key: key.to_string(),
                num_partitions,
                records: rec_bytes,
            },
        };

        let resp = self.send_request(&req).await?;
        if resp.status != 0 {
            return Err(ClientError::ServerError(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ));
        }

        if resp.payload.len() < 20 {
            return Err(ClientError::Wire(WireError::Incomplete {
                needed: 20,
                available: resp.payload.len(),
            }));
        }

        let mut buf = &resp.payload[..];
        let assigned_partition = buf.get_u32();
        let first_offset = buf.get_u64();
        let last_offset = buf.get_u64();

        Ok(ProduceResponse {
            assigned_partition,
            first_offset,
            last_offset,
        })
    }

    pub async fn produce_single(
        &mut self,
        topic: &str,
        key: &str,
        num_partitions: u32,
        payload: impl AsRef<[u8]>,
    ) -> Result<ProduceResponse, ClientError> {
        self.produce_batch(topic, key, num_partitions, &[payload])
            .await
    }

    /// Scenario 3: Consume / Fetch Flow
    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> Result<Vec<RecordFrame>, ClientError> {
        let req = WireRequest {
            cmd: CommandCode::Fetch,
            payload: RequestPayload::Fetch {
                topic: topic.to_string(),
                partition,
                offset,
                max_bytes,
            },
        };

        let resp = self.send_request(&req).await?;
        if resp.status != 0 {
            return Err(ClientError::ServerError(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ));
        }

        if resp.payload.len() < 4 {
            return Err(ClientError::Wire(WireError::Incomplete {
                needed: 4,
                available: resp.payload.len(),
            }));
        }

        let mut slice = &resp.payload[..];
        let record_count = slice.get_u32() as usize;
        let mut frames = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            let (frame, consumed) = RecordFrame::decode(slice)?;
            slice = &slice[consumed..];
            frames.push(frame);
        }

        Ok(frames)
    }

    /// Scenario 4: Metadata & Management - Commit Offset
    pub async fn commit_offset(
        &mut self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), ClientError> {
        let req = WireRequest {
            cmd: CommandCode::CommitOffset,
            payload: RequestPayload::CommitOffset {
                group_id: group_id.to_string(),
                topic: topic.to_string(),
                partition,
                offset,
            },
        };

        let resp = self.send_request(&req).await?;
        if resp.status != 0 {
            Err(ClientError::ServerError(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Scenario 4: Metadata & Management - Fetch Committed Offset
    pub async fn fetch_offset(
        &mut self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Result<u64, ClientError> {
        let req = WireRequest {
            cmd: CommandCode::FetchOffset,
            payload: RequestPayload::FetchOffset {
                group_id: group_id.to_string(),
                topic: topic.to_string(),
                partition,
            },
        };

        let resp = self.send_request(&req).await?;
        if resp.status != 0 {
            return Err(ClientError::ServerError(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ));
        }

        if resp.payload.len() < 8 {
            return Err(ClientError::Wire(WireError::Incomplete {
                needed: 8,
                available: resp.payload.len(),
            }));
        }

        let mut buf = &resp.payload[..];
        Ok(buf.get_u64())
    }

    /// Scenario 4: Metadata & Management - Seek Index Offset Position
    pub async fn seek(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<SeekResult, ClientError> {
        let req = WireRequest {
            cmd: CommandCode::Seek,
            payload: RequestPayload::Seek {
                topic: topic.to_string(),
                partition,
                offset,
            },
        };

        let resp = self.send_request(&req).await?;
        if resp.status != 0 {
            return Err(ClientError::ServerError(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ));
        }

        if resp.payload.len() < 16 {
            return Err(ClientError::Wire(WireError::Incomplete {
                needed: 16,
                available: resp.payload.len(),
            }));
        }

        let mut buf = &resp.payload[..];
        let base_offset = buf.get_u64();
        let physical_pos = buf.get_u64();

        Ok(SeekResult {
            base_offset,
            physical_pos,
        })
    }

    /// Scenario 4: Metadata & Management - Latest Watermark Offset
    pub async fn latest_offset(
        &mut self,
        topic: &str,
        partition: u32,
    ) -> Result<u64, ClientError> {
        let req = WireRequest {
            cmd: CommandCode::LatestOffset,
            payload: RequestPayload::LatestOffset {
                topic: topic.to_string(),
                partition,
            },
        };

        let resp = self.send_request(&req).await?;
        if resp.status != 0 {
            return Err(ClientError::ServerError(
                String::from_utf8_lossy(&resp.payload).to_string(),
            ));
        }

        if resp.payload.len() < 8 {
            return Err(ClientError::Wire(WireError::Incomplete {
                needed: 8,
                available: resp.payload.len(),
            }));
        }

        let mut buf = &resp.payload[..];
        Ok(buf.get_u64())
    }
}
