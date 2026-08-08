use crate::protocol::{CommandCode, RecordFrame, WireResponse};
use bytes::{Buf, BufMut};
use std::io::Result as IoResult;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProduceResult {
    pub assigned_partition: u32,
    pub first_offset: u64,
    pub last_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeekResult {
    pub base_offset: u64,
    pub physical_position: u64,
}

/// Helper client for testing protocol commands over TCP
#[derive(Debug)]
pub struct TestClient {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl TestClient {
    pub async fn connect(addr: SocketAddr) -> IoResult<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            addr,
            stream: Some(stream),
        })
    }

    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    pub fn disconnect(&mut self) {
        self.stream = None;
    }

    pub async fn reconnect(&mut self) -> IoResult<()> {
        let stream = TcpStream::connect(self.addr).await?;
        self.stream = Some(stream);
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
        self.produce_batch(topic, key, transaction_id, num_partitions, &[payload.as_ref()])
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
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::ProduceBatch as u8);

        let mut inner = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut inner, topic);
        crate::protocol::wire::write_pascal_string(&mut inner, key);
        crate::protocol::wire::write_pascal_string(&mut inner, transaction_id.unwrap_or(""));
        inner.put_u32(num_partitions);
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            let assigned_partition = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap());
            let first_offset = u64::from_be_bytes(resp_payload[4..12].try_into().unwrap());
            let last_offset = u64::from_be_bytes(resp_payload[12..20].try_into().unwrap());
            Ok(ProduceResult {
                assigned_partition,
                first_offset,
                last_offset,
            })
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            let count = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap()) as usize;
            let mut frames = Vec::with_capacity(count);
            let mut cursor = 4usize;
            for _ in 0..count {
                if cursor >= resp_payload.len() {
                    break;
                }
                match RecordFrame::decode(&resp_payload[cursor..]) {
                    Ok((frame, consumed)) => {
                        cursor += consumed;
                        frames.push(frame);
                    }
                    Err(_) => break,
                }
            }
            Ok(frames)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            let offset = u64::from_be_bytes(resp_payload[0..8].try_into().unwrap());
            Ok(offset)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            let watermark = u64::from_be_bytes(resp_payload[0..8].try_into().unwrap());
            Ok(watermark)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            let base_offset = u64::from_be_bytes(resp_payload[0..8].try_into().unwrap());
            let physical_position = u64::from_be_bytes(resp_payload[8..16].try_into().unwrap());
            Ok(SeekResult {
                base_offset,
                physical_position,
            })
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn begin_transaction(&mut self, transaction_id: &str, producer_id: u64) -> IoResult<()> {
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            let count = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap()) as usize;
            let mut frames = Vec::with_capacity(count);
            let mut cursor = 4usize;
            for _ in 0..count {
                if cursor >= resp_payload.len() {
                    break;
                }
                match RecordFrame::decode(&resp_payload[cursor..]) {
                    Ok((frame, consumed)) => {
                        cursor += consumed;
                        frames.push(frame);
                    }
                    Err(_) => break,
                }
            }
            Ok(frames)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            let count = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap()) as usize;
            let mut frames = Vec::with_capacity(count);
            let mut cursor = 4usize;
            for _ in 0..count {
                if cursor >= resp_payload.len() {
                    break;
                }
                match RecordFrame::decode(&resp_payload[cursor..]) {
                    Ok((frame, consumed)) => {
                        cursor += consumed;
                        frames.push(frame);
                    }
                    Err(_) => break,
                }
            }
            Ok(frames)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        Ok(WireResponse {
            status,
            payload: resp_payload,
        })
    }

    pub async fn ping(&mut self) -> IoResult<bool> {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::Ping as u8);
        req_buf.put_u32(0);
        let resp = self.send_raw_bytes(&req_buf).await?;
        Ok(resp.status == 0 && resp.payload == b"PONG")
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
                if src.len() < 2 { break; }
                let len = src.get_u16() as usize;
                if src.len() < len { break; }
                let t = String::from_utf8_lossy(&src[..len]).to_string();
                src = &src[len..];
                topics.push(t);
            }
            Ok(topics)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::Other, "ListTopics failed"))
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
            Err(std::io::Error::new(std::io::ErrorKind::Other, "DeleteTopic failed"))
        }
    }
}
