use crate::protocol::{RecordFrame, HEADER_SIZE};
use bytes::{Buf, BufMut};
use std::io::Result as IoResult;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const GRPC_REPLICATION_MAGIC: u8 = 0xBB; // gRPC HTTP/2 Streaming Magic

/// gRPC Replication Pull Request sent by Followers to Leader node
#[derive(Debug, Clone)]
pub struct ReplicationFetchRequest {
    pub follower_node_id: u32,
    pub topic: String,
    pub partition: u32,
    pub fetch_offset: u64,
    pub max_bytes: u32,
}

impl ReplicationFetchRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u8(GRPC_REPLICATION_MAGIC);
        buf.put_u32(self.follower_node_id);
        crate::protocol::wire::write_pascal_string(&mut buf, &self.topic);
        buf.put_u32(self.partition);
        buf.put_u64(self.fetch_offset);
        buf.put_u32(self.max_bytes);
        buf
    }

    pub fn decode(mut src: &[u8]) -> Result<(Self, usize), ()> {
        if src.len() < 1 + 4 + 2 {
            return Err(());
        }
        let original_len = src.len();
        let magic = src.get_u8();
        if magic != GRPC_REPLICATION_MAGIC {
            return Err(());
        }

        let follower_node_id = src.get_u32();
        let topic_len = src.get_u16() as usize;
        if src.len() < topic_len + 16 { // Topic + Partition(4) + Offset(8) + MaxBytes(4)
            return Err(());
        }

        let topic_bytes = &src[..topic_len];
        src = &src[topic_len..];
        let topic = String::from_utf8_lossy(topic_bytes).to_string();

        let partition = src.get_u32();
        let fetch_offset = src.get_u64();
        let max_bytes = src.get_u32();

        let consumed = original_len - src.len();
        Ok((
            Self {
                follower_node_id,
                topic,
                partition,
                fetch_offset,
                max_bytes,
            },
            consumed,
        ))
    }
}

/// gRPC Replication Pull Response sent by Leader node back to Follower
#[derive(Debug, Clone)]
pub struct ReplicationFetchResponse {
    pub leader_watermark: u64,
    pub isr_count: u32,
    pub frames: Vec<RecordFrame>,
}

impl ReplicationFetchResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u64(self.leader_watermark);
        buf.put_u32(self.isr_count);
        buf.put_u32(self.frames.len() as u32);
        for frame in &self.frames {
            frame.encode_into(&mut buf);
        }
        buf
    }

    pub fn decode(mut src: &[u8]) -> Result<Self, ()> {
        if src.len() < 16 {
            return Err(());
        }
        let leader_watermark = src.get_u64();
        let isr_count = src.get_u32();
        let frame_count = src.get_u32() as usize;

        let mut frames = Vec::with_capacity(frame_count);
        for _ in 0..frame_count {
            if src.len() < HEADER_SIZE {
                break;
            }
            match RecordFrame::decode(src) {
                Ok((frame, consumed)) => {
                    src = &src[consumed..];
                    frames.push(frame);
                }
                Err(_) => break,
            }
        }

        Ok(Self {
            leader_watermark,
            isr_count,
            frames,
        })
    }
}

/// gRPC Replication Stream Client used by Follower nodes to pull streams from Leader
pub async fn send_grpc_replication_fetch(
    leader_addr: &str,
    req: &ReplicationFetchRequest,
) -> IoResult<ReplicationFetchResponse> {
    let mut stream = TcpStream::connect(leader_addr).await?;
    let payload = req.encode();

    let mut frame_buf = Vec::with_capacity(4 + payload.len());
    frame_buf.put_u32(payload.len() as u32);
    frame_buf.extend_from_slice(&payload);

    stream.write_all(&frame_buf).await?;

    let mut resp_header = [0u8; 4];
    stream.read_exact(&mut resp_header).await?;
    let resp_len = u32::from_be_bytes(resp_header) as usize;

    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await?;

    ReplicationFetchResponse::decode(&resp_buf)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid gRPC replication response"))
}
