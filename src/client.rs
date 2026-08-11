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

#[derive(Debug)]
pub struct ConsumerCoordinator {
    pub group_id: String,
    pub member_id: String,
}

impl ConsumerCoordinator {
    pub fn new(group_id: String, member_id: String) -> Self {
        Self { group_id, member_id }
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
        self.produce_batch_eos(topic, key, transaction_id, num_partitions, 0, 0, 0, records).await
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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
                    Ok((frame, consumed)) => {
                        cursor += consumed;
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

    pub async fn describe_topic(&mut self, topic: &str) -> IoResult<(String, Vec<(u32, u64)>)> {
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            use bytes::Buf;
            let mut cursor = &resp_payload[..];
            let t_len = cursor.get_u16() as usize;
            let res_topic = String::from_utf8_lossy(&cursor[..t_len]).to_string();
            cursor = &cursor[t_len..];
            let count = cursor.get_u32() as usize;
            let mut partitions = Vec::with_capacity(count);
            for _ in 0..count {
                let p_id = cursor.get_u32();
                let hw = cursor.get_u64();
                partitions.push((p_id, hw));
            }
            Ok((res_topic, partitions))
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn describe_group(&mut self, group_id: &str) -> IoResult<(String, Vec<crate::protocol::wire::DescribedGroupMember>)> {
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            if resp_payload.len() < 10 {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Payload too short"));
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

    pub async fn add_partitions_to_txn(&mut self, transactional_id: &str, producer_id: u64, producer_epoch: i16, topics: &[(&str, &[u32])]) -> IoResult<()> {
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            Ok(())
        } else {
            Err(std::io::Error::other(
                String::from_utf8_lossy(&resp_payload).to_string(),
            ))
        }
    }

    pub async fn end_txn(&mut self, transactional_id: &str, producer_id: u64, producer_epoch: i16, committed: bool) -> IoResult<()> {
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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
                    Ok((frame, consumed)) => {
                        cursor += consumed;
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

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

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
                    Ok((frame, consumed)) => {
                        cursor += consumed;
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

    pub async fn join_group(&mut self, group_id: &str, member_id: &str, protocols: &[&str]) -> IoResult<String> {
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

    pub async fn sync_group(&mut self, group_id: &str, generation_id: u32, member_id: &str, assignments: &[crate::protocol::wire::MemberAssignment]) -> IoResult<()> {
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

    pub async fn heartbeat(&mut self, group_id: &str, generation_id: u32, member_id: &str) -> IoResult<()> {
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

    pub async fn offset_commit(&mut self, group_id: &str, topic: &str, partition: u32, offset: u64, metadata: &str) -> IoResult<()> {
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

    pub async fn offset_fetch(&mut self, group_id: &str, topic: &str, partition: u32) -> IoResult<(u64, String)> {
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
                return Err(std::io::Error::other("Incomplete OffsetFetch response payload"));
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
