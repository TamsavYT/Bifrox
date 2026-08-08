use crate::protocol::{RecordFrame, RequestPayload, WireError, WireRequest, WireResponse, HEADER_SIZE};
use crate::server::engine::StorageEngine;
use bytes::{Buf, BufMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

/// Timeout for forwarding produce requests to the leader node
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

/// Handles incoming TCP client connections and inter-node replication/heartbeat streams.
///
/// Protocol dispatch by first byte:
/// - `0xAA` — Inter-node replication batch (Leader -> Follower)
/// - `0xAC` — Inter-node heartbeat PING (Leader -> Follower)  
/// - `0x01..0x0A` — Client wire protocol commands
///
/// **Produce Forwarding**: If this node is a Follower and receives a ProduceBatch (0x01),
/// it transparently proxies the raw request bytes to the Leader and relays the response.
pub async fn handle_connection(mut socket: TcpStream, engine: StorageEngine) {
    let peer_addr = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());

    let mut buffer = vec![0u8; 64 * 1024];
    let mut filled = 0usize;

    loop {
        let n = match socket.read(&mut buffer[filled..]).await {
            Ok(0) => {
                tracing::debug!("Connection closed by {}", peer_addr);
                break;
            }
            Ok(n) => n,
            Err(e) => {
                tracing::debug!("Read error from {}: {}", peer_addr, e);
                break;
            }
        };

        filled += n;
        let mut consumed = 0usize;

        while consumed < filled {
            let slice = &buffer[consumed..filled];
            if slice.is_empty() {
                break;
            }

            let first_byte = slice[0];

            match first_byte {
                // Inter-node replication batch (0xAA)
                0xAA => {
                    match decode_replication_packet(&engine, slice) {
                        Ok((bytes_used, response)) => {
                            consumed += bytes_used;
                            if let Err(e) = socket.write_all(&response).await {
                                tracing::error!("Failed to send replication ACK to {}: {}", peer_addr, e);
                                return;
                            }
                        }
                        Err(PacketError::NeedMoreData) => break,
                        Err(PacketError::Fatal(msg)) => {
                            tracing::warn!("Malformed replication packet from {}: {}", peer_addr, msg);
                            return;
                        }
                    }
                }

                // Inter-node heartbeat PING (0xAC)
                0xAC => {
                    match decode_heartbeat_packet(&engine, slice) {
                        Ok((bytes_used, response)) => {
                            consumed += bytes_used;
                            let _ = socket.write_all(&response).await;
                        }
                        Err(PacketError::NeedMoreData) => break,
                        Err(PacketError::Fatal(msg)) => {
                            tracing::warn!("Malformed heartbeat from {}: {}", peer_addr, msg);
                            return;
                        }
                    }
                }

                // Inter-node VoteRequest RPC (0xAE) — Raft leader election
                0xAE => {
                    match decode_vote_request_packet(&engine, slice) {
                        Ok((bytes_used, response)) => {
                            consumed += bytes_used;
                            let _ = socket.write_all(&response).await;
                        }
                        Err(PacketError::NeedMoreData) => break,
                        Err(PacketError::Fatal(msg)) => {
                            tracing::warn!("Malformed VoteRequest from {}: {}", peer_addr, msg);
                            return;
                        }
                    }
                }

                // Client wire protocol commands (0x01..0x0A)
                _ => {
                    match WireRequest::decode(slice) {
                        Ok((req, bytes_used)) => {
                            // Check if this is a ProduceBatch and we're NOT the leader
                            let is_produce = matches!(req.payload, RequestPayload::ProduceBatch { .. });
                            let is_leader = engine.is_leader();

                            if is_produce && !is_leader {
                                // Forward to leader: capture raw request bytes before advancing
                                let raw_request = slice[..bytes_used].to_vec();
                                consumed += bytes_used;

                                let response_bytes = match engine.leader_addr() {
                                    Some(leader) => {
                                        tracing::info!(
                                            "Produce Forwarding: Proxying produce from {} to leader at {}",
                                            peer_addr,
                                            leader
                                        );
                                        match forward_to_leader(&leader, &raw_request).await {
                                            Ok(bytes) => bytes,
                                            Err(e) => {
                                                tracing::error!(
                                                    "Produce Forwarding: Failed to forward to leader {}: {}",
                                                    leader,
                                                    e
                                                );
                                                WireResponse::error(
                                                    &format!("Failed to forward produce to leader: {}", e)
                                                ).encode()
                                            }
                                        }
                                    }
                                    None => {
                                        tracing::warn!(
                                            "Produce Forwarding: No leader known yet, rejecting produce from {}",
                                            peer_addr
                                        );
                                        WireResponse::error(
                                            "NOT_LEADER: No leader elected for this cluster. Retry later."
                                        ).encode()
                                    }
                                };

                                if let Err(e) = socket.write_all(&response_bytes).await {
                                    tracing::error!("Failed to relay leader response to {}: {}", peer_addr, e);
                                    return;
                                }
                            } else {
                                // Process locally (leader for produce, or any node for fetch/offset/etc)
                                consumed += bytes_used;
                                let response = process_request(&engine, req);
                                if let Err(e) = socket.write_all(&response.encode()).await {
                                    tracing::error!("Failed to send response to {}: {}", peer_addr, e);
                                    return;
                                }
                            }
                        }
                        Err(WireError::Incomplete { .. }) => break,
                        Err(err) => {
                            tracing::warn!("Protocol error from {}: {}", peer_addr, err);
                            let resp = WireResponse::error(&format!("Protocol Error: {}", err));
                            let _ = socket.write_all(&resp.encode()).await;
                            return;
                        }
                    }
                }
            }
        }

        if consumed > 0 {
            buffer.copy_within(consumed..filled, 0);
            filled -= consumed;
        }

        if filled == buffer.len() {
            const MAX_CONNECTION_BUFFER: usize = 128 * 1024 * 1024;
            if buffer.len() >= MAX_CONNECTION_BUFFER {
                tracing::error!("Connection buffer max limit reached (128MB) for {}. Closing.", peer_addr);
                return;
            }
            let new_size = std::cmp::min(buffer.len() * 2, MAX_CONNECTION_BUFFER);
            buffer.resize(new_size, 0);
        }
    }
}

/// Forwards raw produce request bytes to the leader node and returns the raw response bytes.
async fn forward_to_leader(leader_addr: &str, raw_request: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut stream = match timeout(
        FORWARD_TIMEOUT,
        TcpStream::connect(leader_addr),
    ).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Forward connection to leader {} timed out", leader_addr),
            ))
        }
    };

    // Send the raw WireRequest to leader
    stream.write_all(raw_request).await?;

    // Read the WireResponse: [status: 1b] [payload_len: 4b] [payload_bytes]
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let payload_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

    let mut response = Vec::with_capacity(5 + payload_len);
    response.extend_from_slice(&header);
    if payload_len > 0 {
        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await?;
        response.extend_from_slice(&payload);
    }

    Ok(response)
}

/// Internal error type for packet decoding
enum PacketError {
    NeedMoreData,
    Fatal(String),
}

/// Decodes a Raft VoteRequest RPC packet (0xAE) from a candidate node.
///
/// Wire format: `[0xAE] [cluster_id: pascal] [candidate_id: 4b] [term: 8b]`
/// Response:    `[0x01]` = vote granted, `[0x00]` = vote denied.
///
/// Grant rules (simplified Raft):
///   - The incoming cluster_id must match ours.
///   - The candidate's term must be >= our current epoch.
///   - We haven't already voted for someone else in this term.
fn decode_vote_request_packet(
    engine: &StorageEngine,
    mut src: &[u8],
) -> Result<(usize, Vec<u8>), PacketError> {
    let original_len = src.len();

    // Minimum: magic(1) + cid_len(2)
    if src.len() < 3 {
        return Err(PacketError::NeedMoreData);
    }
    src.get_u8(); // 0xAE

    let cid_len = src.get_u16() as usize;
    if src.len() < cid_len + 4 + 8 {
        return Err(PacketError::NeedMoreData);
    }

    let incoming_cluster_id = String::from_utf8_lossy(&src[..cid_len]).to_string();
    src = &src[cid_len..];
    let candidate_id = src.get_u32();
    let term = src.get_u64();

    let bytes_consumed = original_len - src.len();
    let local_cluster = &engine.config().cluster_id;
    let our_epoch = engine.replication().get_epoch();

    if &incoming_cluster_id != local_cluster {
        tracing::warn!(
            "VoteRequest: Rejected — cluster mismatch (got '{}', expected '{}')",
            incoming_cluster_id, local_cluster
        );
        return Ok((bytes_consumed, vec![0x00]));
    }

    if term >= our_epoch && engine.replication().can_vote_for(candidate_id, term) {
        // Grant vote and adopt the new term epoch
        engine.replication().set_epoch(term);
        engine.replication().record_vote(candidate_id, term);
        tracing::info!(
            "VoteRequest: GRANTED vote to candidate {} for term {} (our epoch was {})",
            candidate_id, term, our_epoch
        );
        Ok((bytes_consumed, vec![0x01]))
    } else {
        tracing::info!(
            "VoteRequest: DENIED — candidate {} term {} (epoch: {}, can_vote: {})",
            candidate_id, term, our_epoch, engine.replication().can_vote_for(candidate_id, term)
        );
        Ok((bytes_consumed, vec![0x00]))
    }
}

/// Decodes and processes an inter-node replication packet (0xAA).
fn decode_replication_packet(
    engine: &StorageEngine,
    mut src: &[u8],
) -> Result<(usize, Vec<u8>), PacketError> {
    let original_len = src.len();

    // Minimum header: magic (1) + topic_len (2)
    if src.len() < 3 {
        return Err(PacketError::NeedMoreData);
    }

    src.get_u8(); // 0xAA

    // Topic length and name
    let topic_len = src.get_u16() as usize;
    if src.len() < topic_len + 8 + 8 { // need topic + partition(4) + epoch(8) + count(4) minimum
        return Err(PacketError::NeedMoreData);
    }
    let topic = String::from_utf8_lossy(&src[..topic_len]).to_string();
    src = &src[topic_len..];

    // Partition ID
    let partition = src.get_u32();
    // Epoch (term) from leader
    let incoming_epoch = src.get_u64();
    // Record count
    let count = src.get_u32() as usize;

    // Epoch fencing: reject stale leader writes and signal STALE_EPOCH so leader steps down
    let current_epoch = engine.replication().get_epoch();
    if incoming_epoch < current_epoch {
        tracing::warn!(
            "HA Replication: Stale epoch {} (current {}) from leader for topic '{}' partition {} – rejecting",
            incoming_epoch, current_epoch, topic, partition
        );
        // Return STALE_EPOCH as error bytes so the sending leader can call step_down_to_follower
        return Ok((original_len - src.len(), b"STALE_EPOCH".to_vec()));
    }
    // If epoch is newer, update our local epoch state
    if incoming_epoch > current_epoch {
        engine.replication().set_epoch(incoming_epoch);
        tracing::info!(
            "HA Replication: Updated epoch to {} from leader for topic '{}' partition {}",
            incoming_epoch, topic, partition
        );
    }

    let pm = engine
        .get_or_create_partition(&topic, partition)
        .map_err(|e| PacketError::Fatal(format!("Partition create error: {}", e)))?;

    for i in 0..count {
        if src.len() < HEADER_SIZE {
            return Err(PacketError::NeedMoreData);
        }
        match RecordFrame::decode(src) {
            Ok((frame, frame_bytes)) => {
                src = &src[frame_bytes..];
                if let Err(e) = pm.produce_frame(&frame.payload) {
                    tracing::error!(
                        "HA Replication: Failed to persist record {}/{} on '{}' P{}: {}",
                        i + 1, count, topic, partition, e
                    );
                }

                // If this node is a Follower and receives a __cluster_metadata replication,
                // decode it and dynamically initialize partitions locally.
                if topic == "__cluster_metadata" {
                    if let Ok(meta_rec) = crate::replication::MetadataRecord::decode(&frame.payload) {
                        match meta_rec {
                            crate::replication::MetadataRecord::TopicPartition { topic: ref t, partition: p, .. } => {
                                tracing::info!(
                                    "HA Replication: Received dynamic partition metadata. Initializing partition {}-{}",
                                    t, p
                                );
                                if let Err(e) = engine.get_or_create_partition(t, p) {
                                    tracing::error!(
                                        "HA Replication: Failed to dynamically create partition {}-{}: {}",
                                        t, p, e
                                    );
                                }
                            }
                            crate::replication::MetadataRecord::BrokerRegister { node_id, ref bind_addr } => {
                                tracing::info!(
                                    "HA Replication: Broker register metadata record parsed. Node {} is at {}",
                                    node_id, bind_addr
                                );
                            }
                        }
                    }
                }
            }
            Err(crate::protocol::FrameError::BufferTooShort { .. }) => {
                return Err(PacketError::NeedMoreData);
            }
            Err(e) => {
                return Err(PacketError::Fatal(format!("Frame decode error: {}", e)));
            }
        }
    }

    let bytes_consumed = original_len - src.len();
    tracing::info!(
        "HA Replication: Follower persisted {} replicated record(s) on Topic '{}' Partition {}",
        count, topic, partition
    );

    Ok((bytes_consumed, vec![0u8]))
}

/// Decodes and validates an inter-node heartbeat PING packet (0xAC).
///
/// P4 Wire format: `[0xAC] [cluster_id: pascal] [node_id: 4b] [term: 8b] [leader_bind_addr: pascal]`
///
/// Followers only reset the election timer if the heartbeat's term >= our current epoch.
/// This prevents ghost heartbeats from old leaders blocking correct new-leader elections.
fn decode_heartbeat_packet(
    engine: &StorageEngine,
    mut src: &[u8],
) -> Result<(usize, Vec<u8>), PacketError> {
    let original_len = src.len();

    if src.len() < 3 {
        return Err(PacketError::NeedMoreData);
    }

    src.get_u8(); // 0xAC

    let cid_len = src.get_u16() as usize;
    if src.len() < cid_len + 4 + 8 { // node_id(4) + term(8)
        return Err(PacketError::NeedMoreData);
    }

    let incoming_cluster_id = String::from_utf8_lossy(&src[..cid_len]).to_string();
    src = &src[cid_len..];
    let peer_node_id = src.get_u32();
    let incoming_term = src.get_u64(); // P4: read leader term

    // Parse leader bind address
    if src.len() < 2 {
        return Err(PacketError::NeedMoreData);
    }
    let addr_len = src.get_u16() as usize;
    if src.len() < addr_len {
        return Err(PacketError::NeedMoreData);
    }
    let leader_bind_addr = String::from_utf8_lossy(&src[..addr_len]).to_string();
    src = &src[addr_len..];

    let local_cluster_id = &engine.config().cluster_id;
    let bytes_consumed = original_len - src.len();

    if incoming_cluster_id == *local_cluster_id {
        let our_epoch = engine.replication().get_epoch();
        if incoming_term >= our_epoch {
            // Valid heartbeat from current or newer leader — update state
            engine.replication().set_epoch(incoming_term);
            engine.set_leader_addr(leader_bind_addr.clone());
            tracing::info!(
                "HA Heartbeat: Leader is Node {} at {} term {} (Cluster '{}')",
                peer_node_id, leader_bind_addr, incoming_term, incoming_cluster_id
            );
        } else {
            // Stale heartbeat from ghost leader — ignore, don't reset election timer
            tracing::warn!(
                "HA Heartbeat: Ignoring ghost heartbeat from Node {} — term {} < our epoch {}",
                peer_node_id, incoming_term, our_epoch
            );
        }
        Ok((bytes_consumed, vec![0u8]))
    } else {
        tracing::warn!(
            "HA Heartbeat: REJECTED Node {}! Expected cluster '{}', got '{}'",
            peer_node_id, local_cluster_id, incoming_cluster_id
        );
        Ok((bytes_consumed, vec![1u8]))
    }
}

/// Routes a decoded client WireRequest to the appropriate StorageEngine method.
fn process_request(engine: &StorageEngine, req: WireRequest) -> WireResponse {
    match req.payload {
        RequestPayload::ProduceBatch {
            topic,
            key,
            transaction_id,
            num_partitions,
            records,
        } => match engine.produce_batch(
            &topic,
            &key,
            if transaction_id.is_empty() { None } else { Some(&transaction_id) },
            num_partitions,
            &records
        ) {
            Ok((assigned_partition, first_offset, last_offset)) => {
                let mut buf = Vec::with_capacity(20);
                buf.put_u32(assigned_partition);
                buf.put_u64(first_offset);
                buf.put_u64(last_offset);
                WireResponse::ok(buf)
            }
            Err(e) => WireResponse::error(&format!("ProduceBatch failed: {}", e)),
        },
        RequestPayload::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
        } => match engine.fetch(&topic, partition, offset, max_bytes) {
            Ok(frames) => {
                let mut buf = Vec::new();
                buf.put_u32(frames.len() as u32);
                for frame in frames {
                    frame.encode_into(&mut buf);
                }
                WireResponse::ok(buf)
            }
            Err(e) => WireResponse::error(&format!("Fetch failed: {}", e)),
        },
        RequestPayload::CommitOffset {
            group_id,
            topic,
            partition,
            offset,
        } => match engine.commit_offset(&group_id, &topic, partition, offset) {
            Ok(()) => WireResponse::ok(Vec::new()),
            Err(e) => WireResponse::error(&format!("CommitOffset failed: {}", e)),
        },
        RequestPayload::FetchOffset {
            group_id,
            topic,
            partition,
        } => {
            let offset = engine
                .fetch_offset(&group_id, &topic, partition)
                .unwrap_or(u64::MAX);
            let mut buf = Vec::with_capacity(8);
            buf.put_u64(offset);
            WireResponse::ok(buf)
        }
        RequestPayload::Seek {
            topic,
            partition,
            offset,
        } => match engine.seek(&topic, partition, offset) {
            Ok(Some((base_offset, physical_pos))) => {
                let mut buf = Vec::with_capacity(16);
                buf.put_u64(base_offset);
                buf.put_u64(physical_pos);
                WireResponse::ok(buf)
            }
            Ok(None) => WireResponse::error("Offset not found in index"),
            Err(e) => WireResponse::error(&format!("Seek failed: {}", e)),
        },
        RequestPayload::LatestOffset { topic, partition } => {
            match engine.latest_offset(&topic, partition) {
                Ok(watermark) => {
                    let mut buf = Vec::with_capacity(8);
                    buf.put_u64(watermark);
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("LatestOffset failed: {}", e)),
            }
        }
        RequestPayload::BeginTx {
            transaction_id,
            producer_id,
        } => match engine.begin_transaction(&transaction_id, producer_id) {
            Ok(()) => WireResponse::ok(Vec::new()),
            Err(e) => WireResponse::error(&format!("BeginTx failed: {}", e)),
        },
        RequestPayload::CommitTx { transaction_id } => {
            match engine.commit_transaction(&transaction_id) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("CommitTx failed: {}", e)),
            }
        }
        RequestPayload::AbortTx { transaction_id } => {
            match engine.abort_transaction(&transaction_id) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("AbortTx failed: {}", e)),
            }
        }
        RequestPayload::FetchByTimestamp {
            topic,
            partition,
            target_timestamp,
            max_bytes,
        } => match engine.fetch_by_timestamp(&topic, partition, target_timestamp, max_bytes) {
            Ok(frames) => {
                let mut buf = Vec::new();
                buf.put_u32(frames.len() as u32);
                for frame in frames {
                    frame.encode_into(&mut buf);
                }
                WireResponse::ok(buf)
            }
            Err(e) => WireResponse::error(&format!("FetchByTimestamp failed: {}", e)),
        },
        // P1: Read-committed fetch — hides uncommitted and aborted records via LSO
        RequestPayload::FetchCommitted {
            topic,
            partition,
            offset,
            max_bytes,
        } => match engine.fetch_committed(&topic, partition, offset, max_bytes) {
            Ok(frames) => {
                let mut buf = Vec::new();
                buf.put_u32(frames.len() as u32);
                for frame in frames {
                    frame.encode_into(&mut buf);
                }
                WireResponse::ok(buf)
            }
            Err(e) => WireResponse::error(&format!("FetchCommitted failed: {}", e)),
        },
    }
}
