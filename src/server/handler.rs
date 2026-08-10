use crate::protocol::{RecordFrame, RequestPayload, WireError, WireRequest, WireResponse, HEADER_SIZE};
use crate::server::engine::StorageEngine;
use bytes::{Buf, BufMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

/// Timeout for forwarding produce requests to the leader node
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum allowed size for a forwarded leader response (CRIT-05)
const MAX_FORWARD_RESPONSE_BYTES: usize = 64 * 1024 * 1024; // 64MB

/// Maximum allowed records per replication batch (CRIT-01 / SEC-MED-05)
const MAX_REPLICATION_BATCH_COUNT: usize = 100_000;

/// Maximum cluster-ID / peer string length accepted in inter-node packets (SEC-MED-06)
const MAX_CLUSTER_ID_LEN: usize = 256;

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

    // C4: Shared-secret authentication for client connections.
    // Inter-node peers (addresses in peer_addrs) are exempt; they authenticate
    // via cluster_id in every packet.  If auth_token is not configured the check
    // is skipped entirely (backward-compatible default).
    if let Some(ref token) = engine.config().auth_token {
        let peer_ip = peer_addr.split(':').next().unwrap_or("");
        let is_known_peer = engine.config().peer_addrs.iter().any(|p| {
            p.split(':').next().unwrap_or("") == peer_ip
        });
        if !is_known_peer {
            // Client must send: 4-byte magic (0xCA 0xFE 0xBA 0xBE) + token bytes
            const AUTH_MAGIC: &[u8] = b"\xCA\xFE\xBA\xBE";
            let token_bytes = token.as_bytes();
            let mut auth_buf = vec![0u8; AUTH_MAGIC.len() + token_bytes.len()];
            let ok = socket.read_exact(&mut auth_buf).await.is_ok()
                && auth_buf.starts_with(AUTH_MAGIC)
                && &auth_buf[AUTH_MAGIC.len()..] == token_bytes;
            if !ok {
                tracing::warn!("Authentication failed from {} — closing connection", peer_addr);
                let _ = socket.write_all(b"AUTH_FAILED\n").await;
                return;
            }
        }
    }

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
                                        match forward_to_leader(&leader, &raw_request, engine.config().auth_token.as_deref()).await {
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
                                let response = process_request(&engine, req).await;
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
async fn forward_to_leader(leader_addr: &str, raw_request: &[u8], auth_token: Option<&str>) -> std::io::Result<Vec<u8>> {
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

    // N8: Send auth handshake before the request when auth_token is configured.
    // The leader's handle_connection expects the magic + token prefix from every
    // non-peer client; followers are exempt by IP but must still authenticate when
    // the exempt-by-IP check doesn't cover the loopback/forwarded-IP case.
    if let Some(token) = auth_token {
        const AUTH_MAGIC: &[u8] = b"\xCA\xFE\xBA\xBE";
        stream.write_all(AUTH_MAGIC).await?;
        stream.write_all(token.as_bytes()).await?;
    }

    // Send the raw WireRequest to leader
    stream.write_all(raw_request).await?;

    // H7: Wrap both reads in FORWARD_TIMEOUT so a stalled leader cannot pin this
    // Tokio task indefinitely.  Previously only the connect() was wrapped.
    let mut header = [0u8; 5];
    match timeout(FORWARD_TIMEOUT, stream.read_exact(&mut header)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("Timed out reading response header from leader {}", leader_addr),
        )),
    }
    let payload_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

    // CRIT-05: Reject oversized response payloads to prevent OOM allocation.
    if payload_len > MAX_FORWARD_RESPONSE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Leader forward response payload {} exceeds maximum {} bytes", payload_len, MAX_FORWARD_RESPONSE_BYTES),
        ));
    }

    let mut response = Vec::with_capacity(5 + payload_len);
    response.extend_from_slice(&header);
    if payload_len > 0 {
        let mut payload = vec![0u8; payload_len];
        match timeout(FORWARD_TIMEOUT, stream.read_exact(&mut payload)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Timed out reading response payload from leader {}", leader_addr),
            )),
        }
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
///   - The incoming cluster_id must match ours (CRIT-02: prevents external term manipulation).
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

    // CRIT-02: Cap cluster_id length to prevent heap pressure from malicious packets.
    if cid_len > MAX_CLUSTER_ID_LEN {
        tracing::warn!("VoteRequest: Rejected — cluster_id length {} exceeds maximum {}", cid_len, MAX_CLUSTER_ID_LEN);
        return Err(PacketError::Fatal("cluster_id too long in VoteRequest".to_string()));
    }

    if src.len() < cid_len + 4 + 8 {
        return Err(PacketError::NeedMoreData);
    }

    // CRIT-02: Use from_utf8 (not lossy) so crafted invalid-UTF-8 cannot spoof a valid cluster_id.
    let incoming_cluster_id = match String::from_utf8(src[..cid_len].to_vec()) {
        Ok(s) => s,
        Err(_) => {
            tracing::warn!("VoteRequest: Rejected — cluster_id contains invalid UTF-8");
            return Err(PacketError::Fatal("Invalid UTF-8 in cluster_id".to_string()));
        }
    };
    src = &src[cid_len..];
    let candidate_id = src.get_u32();
    let term = src.get_u64();

    let bytes_consumed = original_len - src.len();
    let local_cluster = &engine.config().cluster_id;
    let our_epoch = engine.replication().get_epoch();

    // CRIT-02: Cluster-ID mismatch — reject to prevent external nodes from manipulating Raft term.
    if &incoming_cluster_id != local_cluster {
        tracing::warn!(
            "VoteRequest: Rejected — cluster mismatch (got '{}', expected '{}')",
            incoming_cluster_id, local_cluster
        );
        return Ok((bytes_consumed, vec![0x00]));
    }

    // C2: In standalone mode (no peers configured) this node is not part of a multi-node
    // cluster and should never grant Raft votes to external candidates.
    if engine.config().peer_addrs.is_empty() {
        tracing::warn!(
            "VoteRequest: Rejected — standalone mode, candidate {} denied",
            candidate_id
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
///
/// CRIT-01: Verifies cluster_id and epoch before accepting any replicated data.
/// SEC-MED-05: Caps record count to prevent CPU-exhaustion loops.
fn decode_replication_packet(
    engine: &StorageEngine,
    mut src: &[u8],
) -> Result<(usize, Vec<u8>), PacketError> {
    let original_len = src.len();

    // Minimum header: magic(1) + cluster_id_len(2)
    if src.len() < 3 {
        return Err(PacketError::NeedMoreData);
    }

    src.get_u8(); // 0xAA

    // CRIT-01: Read cluster_id prefix for authentication before touching any partition state.
    let cid_len = src.get_u16() as usize;
    if cid_len > MAX_CLUSTER_ID_LEN {
        tracing::warn!("HA Replication: Rejected — cluster_id length {} exceeds maximum", cid_len);
        return Err(PacketError::Fatal("cluster_id too long in replication packet".to_string()));
    }
    if src.len() < cid_len {
        return Err(PacketError::NeedMoreData);
    }
    // CRIT-01: Use from_utf8 (strict) so invalid UTF-8 cannot spoof a cluster_id.
    let incoming_cluster_id = match String::from_utf8(src[..cid_len].to_vec()) {
        Ok(s) => s,
        Err(_) => return Err(PacketError::Fatal("Invalid UTF-8 in replication cluster_id".to_string())),
    };
    src = &src[cid_len..];

    // CRIT-01: Reject replication from any node whose cluster_id does not match ours.
    let local_cluster = &engine.config().cluster_id;
    if &incoming_cluster_id != local_cluster {
        tracing::warn!(
            "HA Replication: REJECTED — cluster_id mismatch (got '{}', expected '{}').",
            incoming_cluster_id, local_cluster
        );
        return Err(PacketError::Fatal("Replication cluster_id mismatch".to_string()));
    }

    // Topic length and name
    if src.len() < 2 {
        return Err(PacketError::NeedMoreData);
    }
    let topic_len = src.get_u16() as usize;
    if src.len() < topic_len + 4 + 8 + 4 { // topic + partition(4) + epoch(8) + count(4)
        return Err(PacketError::NeedMoreData);
    }
    // CRIT-01: Use strict UTF-8 for topic name too.
    let topic = match String::from_utf8(src[..topic_len].to_vec()) {
        Ok(s) => s,
        Err(_) => return Err(PacketError::Fatal("Invalid UTF-8 in replicated topic name".to_string())),
    };
    src = &src[topic_len..];

    // Partition ID
    let partition = src.get_u32();
    // Epoch (term) from leader
    let incoming_epoch = src.get_u64();
    // Record count
    let count = src.get_u32() as usize;

    // SEC-MED-05: Cap count to prevent CPU-exhaustion via malicious large count with NeedMoreData drip.
    if count > MAX_REPLICATION_BATCH_COUNT {
        tracing::warn!(
            "HA Replication: Rejected — record count {} exceeds maximum {}",
            count, MAX_REPLICATION_BATCH_COUNT
        );
        return Err(PacketError::Fatal(format!("Replication batch count {} too large", count)));
    }

    // Epoch fencing: reject stale leader writes and signal STALE_EPOCH so leader steps down
    let current_epoch = engine.replication().get_epoch();
    if incoming_epoch < current_epoch {
        tracing::warn!(
            "HA Replication: Stale epoch {} (current {}) from leader for topic '{}' partition {} – rejecting",
            incoming_epoch, current_epoch, topic, partition
        );
        // H5: Return a single-byte STALE_EPOCH sentinel (0x02) so the leader reads
        // exactly 1 byte (matching its read_exact(&mut [0u8;1])) and can distinguish
        // this from a generic NACK (0x01).  Previously returned 11-byte "STALE_EPOCH"
        // literal which the leader read as 0x53 ('S') — a plain error, never triggering
        // step-down.
        return Ok((original_len - src.len(), vec![0x02]));
    }
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

    // C3: Two-pass replication decode — parse ALL frames before writing any.
    // Previously, frames were written inside the parse loop.  If the TCP stream
    // delivered the packet in multiple segments, NeedMoreData was returned mid-batch
    // without advancing `consumed`, causing already-written frames to be replayed on
    // the next call and permanently duplicated in the partition log.
    //
    // Now: first pass collects payloads (no writes); only after the entire batch is
    // confirmed present do we commit writes.  NeedMoreData can therefore only be
    // returned when zero bytes have been written.
    let is_cluster_meta = topic == "__cluster_metadata";
    let mut parsed_payloads: Vec<bytes::Bytes> = Vec::with_capacity(count);

    for _ in 0..count {
        if src.len() < HEADER_SIZE {
            return Err(PacketError::NeedMoreData);
        }
        match RecordFrame::decode(src) {
            Ok((frame, frame_bytes)) => {
                src = &src[frame_bytes..];
                parsed_payloads.push(frame.payload);
            }
            Err(crate::protocol::FrameError::BufferTooShort { .. }) => {
                return Err(PacketError::NeedMoreData);
            }
            Err(e) => {
                return Err(PacketError::Fatal(format!("Frame decode error: {}", e)));
            }
        }
    }

    // All frames parsed — compute consumed bytes before the write pass.
    let bytes_consumed = original_len - src.len();

    // C1: Track write failures and NACK the leader so it can remove this node from ISR.
    // Previously, disk errors were logged and silently swallowed; the function returned
    // a success ACK regardless, causing the leader to falsely count this follower as
    // in-sync for data it never persisted.
    let mut write_failed = false;
    for (i, payload) in parsed_payloads.iter().enumerate() {
        if let Err(e) = pm.produce_frame(payload) {
            tracing::error!(
                "HA Replication: Failed to persist record {}/{} on '{}' P{}: {}",
                i + 1, count, topic, partition, e
            );
            write_failed = true;
        }

        // If this node is a Follower and receives a __cluster_metadata replication,
        // decode it and dynamically initialize partitions locally.
        if is_cluster_meta {
            if let Ok(meta_rec) = crate::replication::MetadataRecord::decode(payload) {
                match meta_rec {
                    crate::replication::MetadataRecord::TopicPartition { topic: ref t, partition: p, .. } => {
                        // SEC-MED-07: Validate topic name before creating partitions from metadata.
                        if crate::server::engine::validate_topic_name(t).is_ok() {
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
                        } else {
                            tracing::warn!("HA Replication: Skipping invalid topic name '{}' in metadata record", t);
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

    tracing::info!(
        "HA Replication: Follower persisted {} replicated record(s) on Topic '{}' Partition {}",
        count, topic, partition
    );

    if write_failed {
        // Signal NACK so the leader retries or removes this follower from ISR.
        return Ok((bytes_consumed, vec![1u8]));
    }
    Ok((bytes_consumed, vec![0u8]))
}

/// Decodes and validates an inter-node heartbeat PING packet (0xAC).
///
/// P4 Wire format: `[0xAC] [cluster_id: pascal] [node_id: 4b] [term: 8b] [leader_bind_addr: pascal]`
///
/// Followers only reset the election timer if the heartbeat's term >= our current epoch.
/// CRIT-03: leader_bind_addr is validated against the configured peer_addrs whitelist before use.
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

    // CRIT-02/03: Cap cluster_id length.
    if cid_len > MAX_CLUSTER_ID_LEN {
        return Err(PacketError::Fatal("cluster_id too long in heartbeat".to_string()));
    }
    if src.len() < cid_len + 4 + 8 { // node_id(4) + term(8)
        return Err(PacketError::NeedMoreData);
    }

    // CRIT-03: Use from_utf8 (strict) so crafted invalid-UTF-8 cannot spoof a cluster_id.
    let incoming_cluster_id = match String::from_utf8(src[..cid_len].to_vec()) {
        Ok(s) => s,
        Err(_) => return Err(PacketError::Fatal("Invalid UTF-8 in heartbeat cluster_id".to_string())),
    };
    src = &src[cid_len..];
    let peer_node_id = src.get_u32();
    let incoming_term = src.get_u64();

    // Parse leader bind address
    if src.len() < 2 {
        return Err(PacketError::NeedMoreData);
    }
    let addr_len = src.get_u16() as usize;
    if addr_len > 256 {
        return Err(PacketError::Fatal("leader_bind_addr too long in heartbeat".to_string()));
    }
    if src.len() < addr_len {
        return Err(PacketError::NeedMoreData);
    }
    let leader_bind_addr = match String::from_utf8(src[..addr_len].to_vec()) {
        Ok(s) => s,
        Err(_) => return Err(PacketError::Fatal("Invalid UTF-8 in leader_bind_addr".to_string())),
    };
    src = &src[addr_len..];

    let local_cluster_id = &engine.config().cluster_id;
    let bytes_consumed = original_len - src.len();

    if incoming_cluster_id != *local_cluster_id {
        tracing::warn!(
            "HA Heartbeat: REJECTED Node {}! Expected cluster '{}', got '{}'",
            peer_node_id, local_cluster_id, incoming_cluster_id
        );
        return Ok((bytes_consumed, vec![1u8]));
    }

    // CRIT-03 / H6: Validate leader_bind_addr against peer_addrs only.
    // Previously also matched against this node's own bind_addr, which allowed two
    // exploits when all nodes share "0.0.0.0:port": (1) any peer could advertise our
    // own address as the leader, causing forwarded produces to loop back; (2) the
    // wildcard match made the whitelist entirely ineffective.  The leader's address
    // must be a known peer, never this node's own address.
    let peer_addrs = &engine.config().peer_addrs;
    let is_whitelisted = peer_addrs.iter().any(|p| *p == leader_bind_addr);
    if !is_whitelisted {
        tracing::warn!(
            "HA Heartbeat: REJECTED — leader_bind_addr '{}' not in configured peer whitelist (Node {})",
            leader_bind_addr, peer_node_id
        );
        return Ok((bytes_consumed, vec![1u8]));
    }

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
}

/// Routes a decoded client WireRequest to the appropriate StorageEngine method.
async fn process_request(engine: &StorageEngine, req: WireRequest) -> WireResponse {
    match req.payload {
        RequestPayload::ProduceBatch {
            topic,
            key,
            transaction_id,
            num_partitions,
            records,
        } => {
            if let Err(e) = crate::server::engine::validate_topic_name(&topic) {
                return WireResponse::error(&format!("Invalid topic name: {}", e));
            }
            match engine
                .produce_batch(
                    &topic,
                    &key,
                    if transaction_id.is_empty() {
                        None
                    } else {
                        Some(&transaction_id)
                    },
                    num_partitions,
                    &records,
                )
                .await
            {
                Ok((assigned_partition, first_offset, last_offset)) => {
                    let mut buf = Vec::with_capacity(20);
                    buf.put_u32(assigned_partition);
                    buf.put_u64(first_offset);
                    buf.put_u64(last_offset);
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("ProduceBatch failed: {}", e)),
            }
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
        RequestPayload::Ping => WireResponse::ok(b"PONG".to_vec()),
        RequestPayload::ListTopics => {
            let topics = engine.list_topics();
            let mut buf = Vec::new();
            buf.put_u32(topics.len() as u32);
            for t in topics {
                crate::protocol::wire::write_pascal_string(&mut buf, &t);
            }
            WireResponse::ok(buf)
        }
        RequestPayload::DescribeCluster => {
            let config = engine.config();
            let mut buf = Vec::new();
            crate::protocol::wire::write_pascal_string(&mut buf, &config.cluster_id);
            buf.put_u32(config.node_id);
            let role_byte = if engine.is_leader() { 1u8 } else { 0u8 };
            buf.put_u8(role_byte);
            WireResponse::ok(buf)
        }
        RequestPayload::DeleteTopic { topic } => {
            // H8: Validate topic name before deletion — ProduceBatch already does this
            // but DeleteTopic previously skipped it, allowing anonymous clients to
            // target system partitions like __transaction_state via remove_dir_all.
            if let Err(e) = crate::server::engine::validate_topic_name(&topic) {
                return WireResponse::error(&format!("Invalid topic name: {}", e));
            }
            match engine.delete_topic(&topic) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("DeleteTopic failed: {}", e)),
            }
        }
    }
}
