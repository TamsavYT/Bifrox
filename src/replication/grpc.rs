use bytes::{Buf, BufMut, Bytes};
use std::io::Result as IoResult;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

/// Connect timeout for gRPC replication fetch — mirrors PEER_CONNECT_TIMEOUT in mod.rs
const GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

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
    /// Encodes the payload of a [`FrameType::ReplicationFetch`] frame.
    ///
    /// No magic byte and no length prefix of its own: both are the envelope's job now (see
    /// `replication::envelope`). This used to carry `0xBB` plus a `[u32 len]` the
    /// connection loop had to peek past *before* dispatching, which is why the fetch was
    /// special-cased ahead of every other inter-node packet type.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u32(self.follower_node_id);
        crate::protocol::wire::write_pascal_string(&mut buf, &self.topic);
        buf.put_u32(self.partition);
        buf.put_u64(self.fetch_offset);
        buf.put_u32(self.max_bytes);
        crate::replication::envelope::write_no_extensions(&mut buf);
        buf
    }

    /// Wraps [`Self::encode`] in a [`FrameType::ReplicationFetch`] envelope.
    pub fn encode_frame(&self) -> Vec<u8> {
        crate::replication::envelope::encode_frame(
            crate::replication::envelope::FrameType::ReplicationFetch,
            &self.encode(),
        )
    }

    #[allow(clippy::result_unit_err)]
    pub fn decode(mut src: &[u8]) -> Result<(Self, usize), ()> {
        if src.len() < 4 + 2 {
            return Err(());
        }
        let original_len = src.len();

        let follower_node_id = src.get_u32();
        let topic_len = src.get_u16() as usize;
        if src.len() < topic_len + 16 {
            // Topic + Partition(4) + Offset(8) + MaxBytes(4)
            return Err(());
        }

        // Strict UTF-8, not lossy: replacing invalid bytes with U+FFFD would let two
        // different byte sequences name the same topic.
        let topic = String::from_utf8(src[..topic_len].to_vec()).map_err(|_| ())?;
        src = &src[topic_len..];

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
    /// The leader's stored bytes for the requested range: whole log entries exactly as
    /// they sit in its log, never decoded into records and never decompressed. The
    /// follower appends them verbatim, so its log ends up byte-identical to the leader's
    /// — still batched, still compressed in the producer's codec.
    pub entries: Bytes,
}

impl ReplicationFetchResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20 + self.entries.len());
        buf.put_u64(self.leader_watermark);
        buf.put_u32(self.isr_count);
        buf.put_u32(self.entries.len() as u32);
        buf.put_slice(&self.entries);
        crate::replication::envelope::write_no_extensions(&mut buf);
        buf
    }

    /// Wraps [`Self::encode`] in a [`FrameType::ReplicationFetchResponse`] envelope.
    pub fn encode_frame(&self) -> Vec<u8> {
        crate::replication::envelope::encode_frame(
            crate::replication::envelope::FrameType::ReplicationFetchResponse,
            &self.encode(),
        )
    }

    #[allow(clippy::result_unit_err)]
    pub fn decode(mut src: &[u8]) -> Result<Self, ()> {
        if src.len() < 16 {
            return Err(());
        }
        let leader_watermark = src.get_u64();
        let isr_count = src.get_u32();
        let entries_len = src.get_u32() as usize;
        if src.len() < entries_len {
            return Err(());
        }
        let entries = Bytes::copy_from_slice(&src[..entries_len]);

        Ok(Self {
            leader_watermark,
            isr_count,
            entries,
        })
    }
}

/// gRPC Replication Stream Client used by Follower nodes to pull streams from Leader
pub async fn send_grpc_replication_fetch(
    leader_addr: &str,
    req: &ReplicationFetchRequest,
) -> IoResult<ReplicationFetchResponse> {
    // H11: The push path (send_replication_push) already wraps connect in a timeout;
    // the pull path was missing this, blocking the Tokio task for the OS TCP SYN
    // timeout (~127s on Linux) whenever the leader is unreachable.
    let mut stream = match timeout(GRPC_CONNECT_TIMEOUT, TcpStream::connect(leader_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("gRPC replication connection to {} timed out", leader_addr),
            ))
        }
    };
    let frame = req.encode_frame();
    let payload = crate::replication::envelope::exchange_frame(
        &mut stream,
        &frame,
        crate::replication::envelope::FrameType::ReplicationFetchResponse,
    )
    .await?;

    ReplicationFetchResponse::decode(&payload).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid gRPC replication response",
        )
    })
}
