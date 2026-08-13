use crate::protocol::{
    RecordFrame, RequestPayload, WireError, WireRequest, WireResponse, HEADER_SIZE,
};
use crate::scram::{self, ScramCredential};
use crate::server::engine::StorageEngine;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bytes::{Buf, BufMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

/// Maximum allowed records per replication batch (CRIT-01 / SEC-MED-05)
const MAX_REPLICATION_BATCH_COUNT: usize = 100_000;

/// Maximum cluster-ID / peer string length accepted in inter-node packets (SEC-MED-06)
const MAX_CLUSTER_ID_LEN: usize = 256;
/// Timeout for reading client auth handshake bytes.
const AUTH_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for forwarding produce requests to the leader node.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum allowed size for a forwarded leader response (prevents OOM from a malicious/buggy leader).
const MAX_FORWARD_RESPONSE_BYTES: usize = 64 * 1024 * 1024; // 64MB
const MAX_CLIENT_ID_LEN: usize = 256;

const SCRAM_SERVER_NONCE_LEN: usize = 18;

#[derive(Debug, Clone)]
struct ScramSession {
    username: String,
    client_first_bare: String,
    server_first_message: String,
    combined_nonce: String,
}

/// Handles incoming TCP client connections and inter-node replication/heartbeat streams.
///
/// Protocol dispatch by first byte:
/// - `0xAA` — Inter-node replication batch (Leader -> Follower)
/// - `0xAC` — Inter-node heartbeat PING (Leader -> Follower)  
/// - `0x01..0x0A` — Client wire protocol commands
///
/// **Produce Forwarding**: If this node is a Follower and receives a ProduceBatch (0x01),
/// it transparently proxies the raw request bytes to the Leader and relays the response.
pub async fn handle_connection(socket: TcpStream, engine: StorageEngine) {
    let peer_addr = socket
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    handle_connection_stream(socket, engine, peer_addr).await;
}

struct ConnectionGuard<'a>(&'a StorageEngine);

impl<'a> Drop for ConnectionGuard<'a> {
    fn drop(&mut self) {
        self.0.metrics().record_connection_close();
    }
}

pub async fn handle_connection_stream<S>(mut socket: S, engine: StorageEngine, peer_addr: String)
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    engine.metrics().record_connection_open();
    let _conn_guard = ConnectionGuard(&engine);

    // Connection fallback identity for quotas when neither an authenticated principal
    // nor a logical client_id has been established on the socket.
    let client_key = peer_addr
        .split(':')
        .next()
        .unwrap_or(&peer_addr)
        .to_string();
    let sasl_required = matches!(
        engine.config().security_protocol,
        crate::config::SecurityProtocol::SaslPlaintext | crate::config::SecurityProtocol::SaslSsl
    );

    // C4: Shared-secret authentication for client connections.
    // Inter-node peers (addresses in peer_addrs) are exempt; they authenticate
    // via cluster_id in every packet.  If auth_token is not configured the check
    // is skipped entirely (backward-compatible default).
    if let Some(ref token) = engine.config().auth_token {
        // In SASL_* modes, enforce SASL as the sole client-auth handshake path.
        // Token auth remains available for legacy non-SASL deployments.
        if !sasl_required {
            let peer_ip = peer_addr.split(':').next().unwrap_or("");
            let is_known_peer = engine
                .config()
                .peer_addrs
                .iter()
                .any(|p| p.split(':').next().unwrap_or("") == peer_ip);
            if !is_known_peer {
                // Client must send: 4-byte magic (0xCA 0xFE 0xBA 0xBE) + token bytes
                const AUTH_MAGIC: &[u8] = b"\xCA\xFE\xBA\xBE";
                let token_bytes = token.as_bytes();
                let mut auth_buf = vec![0u8; AUTH_MAGIC.len() + token_bytes.len()];
                let ok = match timeout(AUTH_READ_TIMEOUT, socket.read_exact(&mut auth_buf)).await {
                    Ok(Ok(_)) => {
                        auth_buf.starts_with(AUTH_MAGIC)
                            && &auth_buf[AUTH_MAGIC.len()..] == token_bytes
                    }
                    Ok(Err(_)) => false,
                    Err(_) => false,
                };
                if !ok {
                    tracing::warn!(
                        "Authentication failed from {} — closing connection",
                        peer_addr
                    );
                    let _ = socket.write_all(b"AUTH_FAILED\n").await;
                    return;
                }
            }
        }
    }

    let mut client_principal = if engine.config().auth_token.is_some() && !sasl_required {
        "User:token_user".to_string()
    } else {
        "User:ANONYMOUS".to_string()
    };
    let mut logical_client_id: Option<String> = None;
    let mut scram_session: Option<ScramSession> = None;

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
                0xAA => match decode_replication_packet(&engine, slice) {
                    Ok((bytes_used, response)) => {
                        consumed += bytes_used;
                        if let Err(e) = socket.write_all(&response).await {
                            tracing::error!(
                                "Failed to send replication ACK to {}: {}",
                                peer_addr,
                                e
                            );
                            return;
                        }
                    }
                    Err(PacketError::NeedMoreData) => break,
                    Err(PacketError::Fatal(msg)) => {
                        tracing::warn!("Malformed replication packet from {}: {}", peer_addr, msg);
                        return;
                    }
                },

                // Inter-node heartbeat PING (0xAC)
                0xAC => match decode_heartbeat_packet(&engine, slice) {
                    Ok((bytes_used, response)) => {
                        consumed += bytes_used;
                        let _ = socket.write_all(&response).await;
                    }
                    Err(PacketError::NeedMoreData) => break,
                    Err(PacketError::Fatal(msg)) => {
                        tracing::warn!("Malformed heartbeat from {}: {}", peer_addr, msg);
                        return;
                    }
                },

                // Inter-node VoteRequest RPC (0xAE) — Raft leader election
                0xAE => match decode_vote_request_packet(&engine, slice) {
                    Ok((bytes_used, response)) => {
                        consumed += bytes_used;
                        let _ = socket.write_all(&response).await;
                    }
                    Err(PacketError::NeedMoreData) => break,
                    Err(PacketError::Fatal(msg)) => {
                        tracing::warn!("Malformed VoteRequest from {}: {}", peer_addr, msg);
                        return;
                    }
                },

                // Client wire protocol commands (0x01..0x0A)
                _ => {
                    match WireRequest::decode(slice) {
                        Ok((req, bytes_used)) => {
                            // Produce Forwarding: if this node is not the leader for the target
                            // partition, transparently proxy the raw request bytes to the actual
                            // partition leader broker and relay its response, rather than making
                            // the client discover the leader itself.  This restores Kafka's
                            // "connect to any broker" client experience for non-leader brokers.
                            //
                            // IMPORTANT: this must check *partition*-level leadership
                            // (`is_partition_leader`), not cluster-level Raft leadership
                            // (`engine.is_leader()`).  A node can be a Raft Follower yet still be
                            // the assigned leader for a specific partition under KIP-392-style
                            // per-partition leader assignment; forwarding based on Raft role alone
                            // would incorrectly proxy those produces away from the correct broker.
                            let target_partition = match &req.payload {
                                RequestPayload::ProduceBatch {
                                    topic,
                                    key,
                                    num_partitions,
                                    ..
                                } => Some((
                                    topic.clone(),
                                    if !key.is_empty() && *num_partitions > 0 {
                                        crate::server::hash_key(
                                            key.as_bytes(),
                                            *num_partitions as usize,
                                        )
                                    } else {
                                        0
                                    },
                                )),
                                _ => None,
                            };

                            if let Some((topic, partition)) = target_partition {
                                if !engine.is_partition_leader(&topic, partition) {
                                    let raw_request = slice[..bytes_used].to_vec();
                                    consumed += bytes_used;

                                    // Prefer the actual assigned leader for this partition; fall
                                    // back to the cluster Raft leader address if the partition
                                    // leader's broker address isn't known (e.g. not yet announced).
                                    let target_addr = engine
                                        .partition_leader_id(&topic, partition)
                                        .and_then(|leader_id| engine.get_broker_address(leader_id))
                                        .or_else(|| engine.leader_addr());

                                    let response_bytes = match target_addr {
                                        Some(leader) => {
                                            tracing::info!(
                                                "Produce Forwarding: Proxying produce from {} to partition leader at {}",
                                                peer_addr,
                                                leader
                                            );
                                            match forward_to_leader(
                                                &leader,
                                                &raw_request,
                                                engine.config().auth_token.as_deref(),
                                            )
                                            .await
                                            {
                                                Ok(bytes) => bytes,
                                                Err(e) => {
                                                    tracing::error!(
                                                        "Produce Forwarding: Failed to forward to leader {}: {}",
                                                        leader,
                                                        e
                                                    );
                                                    WireResponse::error(&format!(
                                                        "Failed to forward produce to leader: {}",
                                                        e
                                                    ))
                                                    .encode()
                                                }
                                            }
                                        }
                                        None => {
                                            tracing::warn!(
                                                "Produce Forwarding: No leader known yet, rejecting produce from {}",
                                                peer_addr
                                            );
                                            WireResponse::error(
                                                "NOT_LEADER: No leader elected for this partition. Retry later."
                                            ).encode()
                                        }
                                    };

                                    if let Err(e) = socket.write_all(&response_bytes).await {
                                        tracing::error!(
                                            "Failed to relay leader response to {}: {}",
                                            peer_addr,
                                            e
                                        );
                                        return;
                                    }
                                } else {
                                    consumed += bytes_used;
                                    let response = process_request(
                                        &engine,
                                        req,
                                        &client_key,
                                        &mut client_principal,
                                        &client_key,
                                        &mut logical_client_id,
                                        &mut scram_session,
                                    )
                                    .await;
                                    if let Err(e) = socket.write_all(&response.encode()).await {
                                        tracing::error!(
                                            "Failed to send response to {}: {}",
                                            peer_addr,
                                            e
                                        );
                                        return;
                                    }
                                }
                            } else {
                                consumed += bytes_used;
                                let response = process_request(
                                    &engine,
                                    req,
                                    &client_key,
                                    &mut client_principal,
                                    &client_key,
                                    &mut logical_client_id,
                                    &mut scram_session,
                                )
                                .await;
                                if let Err(e) = socket.write_all(&response.encode()).await {
                                    tracing::error!(
                                        "Failed to send response to {}: {}",
                                        peer_addr,
                                        e
                                    );
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
                tracing::error!(
                    "Connection buffer max limit reached (128MB) for {}. Closing.",
                    peer_addr
                );
                return;
            }
            let new_size = std::cmp::min(buffer.len() * 2, MAX_CONNECTION_BUFFER);
            buffer.resize(new_size, 0);
        }
    }
}

/// Forwards raw produce request bytes to the leader node and returns the raw response bytes.
///
/// Used by follower brokers to transparently proxy `ProduceBatch` requests to the current
/// cluster leader, so Kafka-style clients can "connect to any broker" without needing to
/// discover partition leadership themselves.
async fn forward_to_leader(
    leader_addr: &str,
    raw_request: &[u8],
    auth_token: Option<&str>,
) -> std::io::Result<Vec<u8>> {
    let mut stream = match timeout(FORWARD_TIMEOUT, TcpStream::connect(leader_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Forward connection to leader {} timed out", leader_addr),
            ))
        }
    };

    // If auth_token is configured, send the auth handshake before the request so the
    // leader's handle_connection accepts this as an authenticated client rather than
    // rejecting it (the forwarding follower is not necessarily in the leader's peer_addrs
    // exemption list, e.g. when NAT/port-forwarding changes the observed source IP).
    if let Some(token) = auth_token {
        const AUTH_MAGIC: &[u8] = b"\xCA\xFE\xBA\xBE";
        stream.write_all(AUTH_MAGIC).await?;
        stream.write_all(token.as_bytes()).await?;
    }

    stream.write_all(raw_request).await?;

    let mut header = [0u8; 5];
    match timeout(FORWARD_TIMEOUT, stream.read_exact(&mut header)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "Timed out reading response header from leader {}",
                    leader_addr
                ),
            ))
        }
    }
    let payload_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

    if payload_len > MAX_FORWARD_RESPONSE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Leader forward response payload {} exceeds maximum {} bytes",
                payload_len, MAX_FORWARD_RESPONSE_BYTES
            ),
        ));
    }

    let mut response = Vec::with_capacity(5 + payload_len);
    response.extend_from_slice(&header);
    if payload_len > 0 {
        let mut payload = vec![0u8; payload_len];
        match timeout(FORWARD_TIMEOUT, stream.read_exact(&mut payload)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Timed out reading response payload from leader {}",
                        leader_addr
                    ),
                ))
            }
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
        tracing::warn!(
            "VoteRequest: Rejected — cluster_id length {} exceeds maximum {}",
            cid_len,
            MAX_CLUSTER_ID_LEN
        );
        return Err(PacketError::Fatal(
            "cluster_id too long in VoteRequest".to_string(),
        ));
    }

    if src.len() < cid_len + 4 + 8 {
        return Err(PacketError::NeedMoreData);
    }

    // CRIT-02: Use from_utf8 (not lossy) so crafted invalid-UTF-8 cannot spoof a valid cluster_id.
    let incoming_cluster_id = match String::from_utf8(src[..cid_len].to_vec()) {
        Ok(s) => s,
        Err(_) => {
            tracing::warn!("VoteRequest: Rejected — cluster_id contains invalid UTF-8");
            return Err(PacketError::Fatal(
                "Invalid UTF-8 in cluster_id".to_string(),
            ));
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
            incoming_cluster_id,
            local_cluster
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
            candidate_id,
            term,
            our_epoch
        );
        Ok((bytes_consumed, vec![0x01]))
    } else {
        tracing::info!(
            "VoteRequest: DENIED — candidate {} term {} (epoch: {}, can_vote: {})",
            candidate_id,
            term,
            our_epoch,
            engine.replication().can_vote_for(candidate_id, term)
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
        tracing::warn!(
            "HA Replication: Rejected — cluster_id length {} exceeds maximum",
            cid_len
        );
        return Err(PacketError::Fatal(
            "cluster_id too long in replication packet".to_string(),
        ));
    }
    if src.len() < cid_len {
        return Err(PacketError::NeedMoreData);
    }
    // CRIT-01: Use from_utf8 (strict) so invalid UTF-8 cannot spoof a cluster_id.
    let incoming_cluster_id = match String::from_utf8(src[..cid_len].to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return Err(PacketError::Fatal(
                "Invalid UTF-8 in replication cluster_id".to_string(),
            ))
        }
    };
    src = &src[cid_len..];

    // CRIT-01: Reject replication from any node whose cluster_id does not match ours.
    let local_cluster = &engine.config().cluster_id;
    if &incoming_cluster_id != local_cluster {
        tracing::warn!(
            "HA Replication: REJECTED — cluster_id mismatch (got '{}', expected '{}').",
            incoming_cluster_id,
            local_cluster
        );
        return Err(PacketError::Fatal(
            "Replication cluster_id mismatch".to_string(),
        ));
    }

    // Topic length and name
    if src.len() < 2 {
        return Err(PacketError::NeedMoreData);
    }
    let topic_len = src.get_u16() as usize;
    if src.len() < topic_len + 4 + 8 + 4 {
        // topic + partition(4) + epoch(8) + count(4)
        return Err(PacketError::NeedMoreData);
    }
    // CRIT-01: Use strict UTF-8 for topic name too.
    let topic = match String::from_utf8(src[..topic_len].to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return Err(PacketError::Fatal(
                "Invalid UTF-8 in replicated topic name".to_string(),
            ))
        }
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
            count,
            MAX_REPLICATION_BATCH_COUNT
        );
        return Err(PacketError::Fatal(format!(
            "Replication batch count {} too large",
            count
        )));
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
            incoming_epoch,
            topic,
            partition
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
                i + 1,
                count,
                topic,
                partition,
                e
            );
            write_failed = true;
        }

        // If this node is a Follower and receives a __cluster_metadata replication,
        // decode it and dynamically initialize partitions locally.
        if is_cluster_meta {
            if let Ok(meta_rec) = crate::replication::MetadataRecord::decode(payload) {
                match meta_rec {
                    crate::replication::MetadataRecord::TopicPartition {
                        topic: ref t,
                        partition: p,
                        ..
                    } => {
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
                    crate::replication::MetadataRecord::BrokerRegister {
                        node_id,
                        ref bind_addr,
                    } => {
                        engine.register_broker_address(node_id, bind_addr.clone());
                        tracing::info!(
                            "HA Replication: Broker register metadata record parsed. Node {} is at {}",
                            node_id, bind_addr
                        );
                    }
                    crate::replication::MetadataRecord::TopicCreated {
                        topic: ref t,
                        num_partitions,
                        ..
                    } => {
                        let _ = engine.create_topic(t, num_partitions);
                    }
                    crate::replication::MetadataRecord::TopicDeleted { topic: ref t } => {
                        let _ = engine.delete_topic(t);
                    }
                    crate::replication::MetadataRecord::PartitionLeadershipChange {
                        topic: ref t,
                        partition,
                        leader_id,
                        leader_epoch,
                        isr,
                    } => {
                        let replicas = isr.clone();
                        if let Ok(pm) = engine.get_or_create_partition(t, partition) {
                            pm.update_leadership(leader_id, leader_epoch, replicas, isr);
                        }
                    }
                    crate::replication::MetadataRecord::AclCreated { binding } => {
                        engine.acl().add_acl(binding);
                    }
                    crate::replication::MetadataRecord::AclDeleted { binding } => {
                        engine.acl().remove_acl(&binding);
                    }
                    crate::replication::MetadataRecord::BrokerUnregister { node_id } => {
                        engine.unregister_broker_address(node_id);
                        tracing::info!(
                            "HA Replication: Broker unregister metadata record parsed. Node {} removed",
                            node_id
                        );
                    }
                    crate::replication::MetadataRecord::ScramCredentialUpsert {
                        username,
                        iterations,
                        salt,
                        stored_key,
                        server_key,
                    } => {
                        engine.apply_scram_credential_state(
                            username, iterations, salt, stored_key, server_key,
                        );
                    }
                    crate::replication::MetadataRecord::ScramCredentialDelete { username } => {
                        engine.remove_scram_credential_state(&username);
                    }
                    crate::replication::MetadataRecord::TransactionalProducerRegistration {
                        transactional_id,
                        producer_id,
                        producer_epoch,
                    } => {
                        engine.transactions().restore_transactional_producer(
                            &transactional_id,
                            producer_id,
                            producer_epoch,
                        );
                    }
                }
            }
        }
    }

    tracing::info!(
        "HA Replication: Follower persisted {} replicated record(s) on Topic '{}' Partition {}",
        count,
        topic,
        partition
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
        return Err(PacketError::Fatal(
            "cluster_id too long in heartbeat".to_string(),
        ));
    }
    if src.len() < cid_len + 4 + 8 {
        // node_id(4) + term(8)
        return Err(PacketError::NeedMoreData);
    }

    // CRIT-03: Use from_utf8 (strict) so crafted invalid-UTF-8 cannot spoof a cluster_id.
    let incoming_cluster_id = match String::from_utf8(src[..cid_len].to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return Err(PacketError::Fatal(
                "Invalid UTF-8 in heartbeat cluster_id".to_string(),
            ))
        }
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
        return Err(PacketError::Fatal(
            "leader_bind_addr too long in heartbeat".to_string(),
        ));
    }
    if src.len() < addr_len {
        return Err(PacketError::NeedMoreData);
    }
    let leader_bind_addr = match String::from_utf8(src[..addr_len].to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return Err(PacketError::Fatal(
                "Invalid UTF-8 in leader_bind_addr".to_string(),
            ))
        }
    };
    src = &src[addr_len..];

    let local_cluster_id = &engine.config().cluster_id;
    let bytes_consumed = original_len - src.len();

    if incoming_cluster_id != *local_cluster_id {
        tracing::warn!(
            "HA Heartbeat: REJECTED Node {}! Expected cluster '{}', got '{}'",
            peer_node_id,
            local_cluster_id,
            incoming_cluster_id
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
    let is_whitelisted = peer_addrs.contains(&leader_bind_addr);
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
            peer_node_id,
            leader_bind_addr,
            incoming_term,
            incoming_cluster_id
        );
    } else {
        // Stale heartbeat from ghost leader — ignore, don't reset election timer
        tracing::warn!(
            "HA Heartbeat: Ignoring ghost heartbeat from Node {} — term {} < our epoch {}",
            peer_node_id,
            incoming_term,
            our_epoch
        );
    }

    // Reply with this node's own identity (node_id + bind_addr) so the Leader can learn
    // this follower's broker address purely from the heartbeat round-trip.  Followers
    // otherwise have no way to publish their address, since only the partition leader
    // for `__cluster_metadata` may write BrokerRegister records.
    let self_bind_addr = &engine.config().bind_addr;
    let mut ack = Vec::with_capacity(1 + 4 + 2 + self_bind_addr.len());
    ack.put_u8(0u8);
    ack.put_u32(engine.config().node_id);
    crate::protocol::wire::write_pascal_string(&mut ack, self_bind_addr);
    Ok((bytes_consumed, ack))
}

/// Routes a decoded client WireRequest to the appropriate StorageEngine method.
async fn process_request(
    engine: &StorageEngine,
    req: WireRequest,
    client_key: &str,
    principal: &mut String,
    client_host: &str,
    logical_client_id: &mut Option<String>,
    scram_session: &mut Option<ScramSession>,
) -> WireResponse {
    let quota_key = resolve_quota_key(principal.as_str(), logical_client_id.as_deref(), client_key);

    let sec_proto = engine.config().security_protocol;
    let sasl_required = matches!(
        sec_proto,
        crate::config::SecurityProtocol::SaslPlaintext | crate::config::SecurityProtocol::SaslSsl
    );

    if sasl_required && principal == "User:ANONYMOUS" {
        let is_sasl_payload = matches!(
            req.payload,
            RequestPayload::SaslHandshake { .. }
                | RequestPayload::SaslAuthenticate { .. }
                | RequestPayload::SetClientId { .. }
                | RequestPayload::Ping
        );
        if !is_sasl_payload {
            tracing::warn!(
                "Rejecting unauthenticated request on SASL-enabled socket from {}: {:?}",
                client_host,
                req.cmd
            );
            return WireResponse::error("SaslAuthenticationRequired");
        }
    }

    match req.payload {
        RequestPayload::SaslHandshake { mechanism } => {
            let mechs = &engine.config().sasl_mechanisms;
            let supported = mechs.iter().any(|m| m.eq_ignore_ascii_case(&mechanism));
            let error_code: i16 = if supported { 0 } else { 33 };
            let mut buf = Vec::new();
            buf.put_i16(error_code);
            buf.put_u32(mechs.len() as u32);
            for m in mechs {
                crate::protocol::wire::write_pascal_string(&mut buf, m);
            }
            WireResponse::ok(buf)
        }
        RequestPayload::SaslAuthenticate { auth_bytes } => {
            let auth_text = match std::str::from_utf8(&auth_bytes) {
                Ok(s) => s,
                Err(_) => {
                    scram_session.take();
                    return build_sasl_auth_response(
                        58,
                        Some("SASL Authentication Failed"),
                        &[],
                        0,
                    );
                }
            };

            if is_scram_client_first(auth_text) {
                let (username, client_nonce, client_first_bare) =
                    match parse_scram_client_first(auth_text) {
                        Some(parts) => parts,
                        None => {
                            scram_session.take();
                            return build_sasl_auth_response(
                                58,
                                Some("SASL Authentication Failed"),
                                &[],
                                0,
                            );
                        }
                    };

                let credential = match engine.lookup_scram_credential(&username) {
                    Some(credential) => credential,
                    None => {
                        scram_session.take();
                        return build_sasl_auth_response(
                            58,
                            Some("SASL Authentication Failed"),
                            &[],
                            0,
                        );
                    }
                };

                let server_nonce = match generate_scram_server_nonce() {
                    Ok(nonce) => nonce,
                    Err(_) => {
                        scram_session.take();
                        return build_sasl_auth_response(
                            58,
                            Some("SASL Authentication Failed"),
                            &[],
                            0,
                        );
                    }
                };
                let combined_nonce = format!("{}{}", client_nonce, server_nonce);
                let salt_b64 = BASE64_STANDARD.encode(&credential.salt);
                let server_first_message = format!(
                    "r={},s={},i={}",
                    combined_nonce, salt_b64, credential.iterations
                );

                *scram_session = Some(ScramSession {
                    username,
                    client_first_bare,
                    server_first_message: server_first_message.clone(),
                    combined_nonce,
                });

                return build_sasl_auth_response(0, None, server_first_message.as_bytes(), 0);
            }

            if is_scram_client_final(auth_text) {
                let session = match scram_session.as_ref() {
                    Some(session) => session,
                    None => {
                        return build_sasl_auth_response(
                            58,
                            Some("SASL Authentication Failed"),
                            &[],
                            0,
                        );
                    }
                };

                let credential = match engine.lookup_scram_credential(&session.username) {
                    Some(credential) => credential,
                    None => {
                        scram_session.take();
                        return build_sasl_auth_response(
                            58,
                            Some("SASL Authentication Failed"),
                            &[],
                            0,
                        );
                    }
                };

                let client_final = match parse_scram_client_final(auth_text) {
                    Some(msg) => msg,
                    None => {
                        scram_session.take();
                        return build_sasl_auth_response(
                            58,
                            Some("SASL Authentication Failed"),
                            &[],
                            0,
                        );
                    }
                };

                if client_final.nonce != session.combined_nonce
                    || !verify_scram_proof(&client_final, session, &credential)
                {
                    scram_session.take();
                    return build_sasl_auth_response(
                        58,
                        Some("SASL Authentication Failed"),
                        &[],
                        0,
                    );
                }

                let server_final = build_scram_server_final(session, &credential, &client_final);

                *principal = format!("User:{}", session.username);
                scram_session.take();
                return build_sasl_auth_response(0, None, server_final.as_bytes(), 0);
            }

            let (username, password) = parse_plain_auth(auth_text);
            let auth_ok = if let Some(credential) = engine.lookup_scram_credential(&username) {
                credential.verify_password(&password)
            } else if let Some(ref tok) = engine.config().auth_token {
                tok == &password || tok == &username
            } else {
                false
            };

            if auth_ok {
                scram_session.take();
                *principal = format!("User:{}", username);
                build_sasl_auth_response(0, None, &[], 0)
            } else {
                scram_session.take();
                build_sasl_auth_response(58, Some("SASL Authentication Failed"), &[], 0)
            }
        }
        RequestPayload::SetClientId { client_id } => {
            if client_id.is_empty() || client_id.len() > MAX_CLIENT_ID_LEN {
                return WireResponse::error("Invalid client_id");
            }
            *logical_client_id = Some(client_id);
            WireResponse::ok(Vec::new())
        }
        RequestPayload::UpsertScramUser {
            username,
            iterations,
            salt,
            stored_key,
            server_key,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Alter as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            if username.is_empty() {
                return WireResponse::error("SCRAM username cannot be empty");
            }
            match engine
                .upsert_scram_credential(&username, iterations, salt, stored_key, server_key)
            {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(err) => WireResponse::error(&format!("UpsertScramUser failed: {}", err)),
            }
        }
        RequestPayload::DeleteScramUser { username } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Alter as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            match engine.delete_scram_user(&username) {
                Ok(_) => WireResponse::ok(Vec::new()),
                Err(err) => WireResponse::error(&format!("DeleteScramUser failed: {}", err)),
            }
        }
        RequestPayload::DescribeAcls {
            resource_type,
            resource_name,
            pattern_type,
            principal: p,
            host,
            operation,
            permission_type,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            let filter = crate::server::acl::AclBinding {
                resource_type,
                resource_name,
                pattern_type,
                principal: p,
                host,
                operation,
                permission_type,
            };
            let bindings = engine.list_acls(filter);
            let mut buf = Vec::new();
            buf.put_i16(0);
            buf.put_u32(bindings.len() as u32);
            for b in bindings {
                buf.put_u8(b.resource_type);
                crate::protocol::wire::write_pascal_string(&mut buf, &b.resource_name);
                buf.put_u8(b.pattern_type);
                crate::protocol::wire::write_pascal_string(&mut buf, &b.principal);
                crate::protocol::wire::write_pascal_string(&mut buf, &b.host);
                buf.put_u8(b.operation);
                buf.put_u8(b.permission_type);
            }
            WireResponse::ok(buf)
        }
        RequestPayload::CreateAcls {
            resource_type,
            resource_name,
            pattern_type,
            principal: p,
            host,
            operation,
            permission_type,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Alter as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            let binding = crate::server::acl::AclBinding {
                resource_type,
                resource_name,
                pattern_type,
                principal: p,
                host,
                operation,
                permission_type,
            };
            match engine.create_acl(binding) {
                Ok(_) => {
                    let mut buf = Vec::new();
                    buf.put_i16(0);
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("CreateAcls failed: {}", e)),
            }
        }
        RequestPayload::DeleteAcls {
            resource_type,
            resource_name,
            pattern_type,
            principal: p,
            host,
            operation,
            permission_type,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Alter as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            let binding = crate::server::acl::AclBinding {
                resource_type,
                resource_name,
                pattern_type,
                principal: p,
                host,
                operation,
                permission_type,
            };
            match engine.delete_acl(binding) {
                Ok(_) => {
                    let mut buf = Vec::new();
                    buf.put_i16(0);
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("DeleteAcls failed: {}", e)),
            }
        }
        RequestPayload::ProduceBatch {
            topic,
            key,
            transaction_id,
            num_partitions,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Write as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            if let Err(e) = crate::server::engine::validate_topic_name(&topic) {
                return WireResponse::error(&format!("Invalid topic name: {}", e));
            }
            let target_partition = if !key.is_empty() && num_partitions > 0 {
                crate::server::hash_key(key.as_bytes(), num_partitions as usize)
            } else {
                0
            };
            if !engine.is_partition_leader(&topic, target_partition) {
                return WireResponse::error("NotLeaderForPartition");
            }
            // Quota: account for produced bytes before executing the write so we know
            // exactly how many bytes this request will use, then apply the throttle
            // delay (if any) after the write completes — matching Kafka's model of
            // "process the request, delay the response" rather than rejecting outright.
            let produced_bytes: u64 = records.iter().map(|r| r.len() as u64).sum();
            match engine
                .produce_batch(crate::server::engine::ProduceBatchParams {
                    topic: &topic,
                    key: &key,
                    transaction_id: if transaction_id.is_empty() {
                        None
                    } else {
                        Some(&transaction_id)
                    },
                    num_partitions,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                    records: &records,
                })
                .await
            {
                Ok((assigned_partition, first_offset, last_offset)) => {
                    engine
                        .throttle_produce(&quota_key, produced_bytes, records.len() as u64)
                        .await;
                    let mut buf = Vec::with_capacity(20);
                    buf.put_u32(assigned_partition);
                    buf.put_u64(first_offset);
                    buf.put_u64(last_offset);
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("ProduceBatch failed: {}", e)),
            }
        }
        RequestPayload::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            if !engine.is_partition_replica(&topic, partition) {
                return WireResponse::error("NotLeaderForPartition");
            }
            match engine.fetch(&topic, partition, offset, max_bytes) {
                Ok(frames) => {
                    let mut buf = Vec::new();
                    buf.put_u32(frames.len() as u32);
                    let mut fetched_bytes: u64 = 0;
                    for frame in frames {
                        fetched_bytes += frame.encoded_size() as u64;
                        frame.encode_into(&mut buf);
                    }
                    engine.throttle_fetch(&quota_key, fetched_bytes).await;
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("Fetch failed: {}", e)),
            }
        }
        RequestPayload::FetchCommitted {
            topic,
            partition,
            offset,
            max_bytes,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            if !engine.is_partition_replica(&topic, partition) {
                return WireResponse::error("NotLeaderForPartition");
            }
            match engine.fetch_committed(&topic, partition, offset, max_bytes) {
                Ok(frames) => {
                    let mut buf = Vec::new();
                    buf.put_u32(frames.len() as u32);
                    let mut fetched_bytes: u64 = 0;
                    for frame in frames {
                        fetched_bytes += frame.encoded_size() as u64;
                        frame.encode_into(&mut buf);
                    }
                    engine.throttle_fetch(&quota_key, fetched_bytes).await;
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("FetchCommitted failed: {}", e)),
            }
        }
        RequestPayload::CommitOffset {
            group_id,
            topic,
            partition,
            offset,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            match engine.commit_offset(&group_id, &topic, partition, offset) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("CommitOffset failed: {}", e)),
            }
        }
        RequestPayload::FetchOffset {
            group_id,
            topic,
            partition,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
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
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            match engine.seek(&topic, partition, offset) {
                Ok(Some((base_offset, physical_pos))) => {
                    let mut buf = Vec::with_capacity(16);
                    buf.put_u64(base_offset);
                    buf.put_u64(physical_pos);
                    WireResponse::ok(buf)
                }
                Ok(None) => WireResponse::error("Offset not found in index"),
                Err(e) => WireResponse::error(&format!("Seek failed: {}", e)),
            }
        }
        RequestPayload::LatestOffset { topic, partition } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
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
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Write as u8,
                crate::server::acl::ResourceType::TransactionalId as u8,
                &transaction_id,
            ) {
                return WireResponse::error("TransactionalIdAuthorizationFailed");
            }
            match engine.begin_transaction(&transaction_id, producer_id) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("BeginTx failed: {}", e)),
            }
        }
        RequestPayload::CommitTx { transaction_id } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Write as u8,
                crate::server::acl::ResourceType::TransactionalId as u8,
                &transaction_id,
            ) {
                return WireResponse::error("TransactionalIdAuthorizationFailed");
            }
            match engine.commit_transaction(&transaction_id) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("CommitTx failed: {}", e)),
            }
        }
        RequestPayload::AbortTx { transaction_id } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Write as u8,
                crate::server::acl::ResourceType::TransactionalId as u8,
                &transaction_id,
            ) {
                return WireResponse::error("TransactionalIdAuthorizationFailed");
            }
            match engine.abort_transaction(&transaction_id) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("AbortTx failed: {}", e)),
            }
        }
        RequestPayload::InitProducerId { transactional_id } => {
            if !transactional_id.is_empty()
                && !engine.authorize(
                    principal,
                    client_host,
                    crate::server::acl::AclOperation::Write as u8,
                    crate::server::acl::ResourceType::TransactionalId as u8,
                    &transactional_id,
                )
            {
                return WireResponse::error("TransactionalIdAuthorizationFailed");
            }
            match engine.init_producer_id(&transactional_id) {
                Ok((pid, epoch)) => {
                    let mut buf = Vec::with_capacity(10);
                    buf.put_u64(pid);
                    buf.put_i16(epoch);
                    WireResponse::ok(buf)
                }
                Err(err) => WireResponse::error(&format!("InitProducerId failed: {}", err)),
            }
        }
        RequestPayload::AddPartitionsToTxn {
            transactional_id,
            producer_id,
            producer_epoch,
            topics,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Write as u8,
                crate::server::acl::ResourceType::TransactionalId as u8,
                &transactional_id,
            ) {
                return WireResponse::error("TransactionalIdAuthorizationFailed");
            }
            let result = engine.add_partitions_to_txn(
                &transactional_id,
                producer_id,
                producer_epoch,
                &topics,
            );
            if result.is_ok() {
                WireResponse::ok(Vec::new())
            } else {
                WireResponse::error("AddPartitionsToTxn failed")
            }
        }
        RequestPayload::EndTxn {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Write as u8,
                crate::server::acl::ResourceType::TransactionalId as u8,
                &transactional_id,
            ) {
                return WireResponse::error("TransactionalIdAuthorizationFailed");
            }
            let result =
                engine.end_transaction(&transactional_id, producer_id, producer_epoch, committed);
            if result.is_ok() {
                WireResponse::ok(Vec::new())
            } else {
                WireResponse::error("EndTxn failed")
            }
        }
        RequestPayload::FetchByTimestamp {
            topic,
            partition,
            target_timestamp,
            max_bytes,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            match engine.fetch_by_timestamp(&topic, partition, target_timestamp, max_bytes) {
                Ok(frames) => {
                    let mut buf = Vec::new();
                    buf.put_u32(frames.len() as u32);
                    let mut fetched_bytes: u64 = 0;
                    for frame in frames {
                        fetched_bytes += frame.encoded_size() as u64;
                        frame.encode_into(&mut buf);
                    }
                    engine.throttle_fetch(&quota_key, fetched_bytes).await;
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("FetchByTimestamp failed: {}", e)),
            }
        }
        RequestPayload::Ping => WireResponse::ok(b"PONG".to_vec()),
        RequestPayload::ListTopics => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            let topics = engine.list_topics();
            let mut buf = Vec::new();
            buf.put_u32(topics.len() as u32);
            for t in topics {
                crate::protocol::wire::write_pascal_string(&mut buf, &t);
            }
            WireResponse::ok(buf)
        }
        RequestPayload::DescribeCluster => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            let config = engine.config();
            let mut buf = Vec::new();
            crate::protocol::wire::write_pascal_string(&mut buf, &config.cluster_id);
            buf.put_u32(config.node_id);
            let role_byte = if engine.is_leader() { 1u8 } else { 0u8 };
            buf.put_u8(role_byte);
            let brokers = engine.broker_endpoints();
            buf.put_u32(brokers.len() as u32);
            for (node_id, addr) in brokers {
                buf.put_u32(node_id);
                crate::protocol::wire::write_pascal_string(&mut buf, &addr);
            }
            WireResponse::ok(buf)
        }
        RequestPayload::DeleteTopic { topic } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Delete as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
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
        RequestPayload::JoinGroup {
            group_id,
            member_id,
            protocols,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            match engine
                .group_coordinator()
                .join_group(&group_id, &member_id, protocols)
            {
                Ok(m_id) => {
                    let mut buf = Vec::new();
                    crate::protocol::wire::write_pascal_string(&mut buf, &m_id);
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&e),
            }
        }
        RequestPayload::SyncGroup {
            group_id,
            generation_id,
            member_id,
            assignments,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            match engine.group_coordinator().sync_group(
                &group_id,
                generation_id,
                &member_id,
                assignments,
            ) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&e),
            }
        }
        RequestPayload::Heartbeat {
            group_id,
            generation_id,
            member_id,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            match engine
                .group_coordinator()
                .heartbeat(&group_id, generation_id, &member_id)
            {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&e),
            }
        }
        RequestPayload::LeaveGroup {
            group_id,
            member_id,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            match engine
                .group_coordinator()
                .leave_group(&group_id, &member_id)
            {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&e),
            }
        }
        RequestPayload::CreateTopic { topic, partitions } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Create as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            match engine.create_topic(&topic, partitions) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(err) => WireResponse::error(&format!("CreateTopic failed: {}", err)),
            }
        }
        RequestPayload::DescribeTopic { topic } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            if let Some(partitions) = engine.describe_topic(&topic) {
                let payload =
                    crate::protocol::wire::encode_describe_topic_response(&topic, &partitions);
                WireResponse::ok(payload)
            } else {
                WireResponse::error("Topic not found")
            }
        }
        RequestPayload::ListGroups => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            let groups = engine.group_coordinator().list_groups();
            let mut buf = Vec::new();
            buf.put_u32(groups.len() as u32);
            for g in groups {
                crate::protocol::wire::write_pascal_string(&mut buf, &g);
            }
            WireResponse::ok(buf)
        }
        RequestPayload::DescribeGroup { group_id } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            if let Some(desc) = engine.group_coordinator().describe_group(&group_id) {
                let payload = crate::protocol::wire::encode_describe_group_response(
                    &desc.state_str,
                    &desc.members,
                );
                WireResponse::ok(payload)
            } else {
                WireResponse::error("Group not found")
            }
        }
        RequestPayload::OffsetCommit {
            group_id,
            topic,
            partition,
            offset,
            metadata,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            match engine
                .commit_offset_with_metadata(&group_id, &topic, partition, offset, &metadata)
            {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("OffsetCommit failed: {}", e)),
            }
        }
        RequestPayload::OffsetFetch {
            group_id,
            topic,
            partition,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            if let Some(entry) = engine.fetch_offset_with_metadata(&group_id, &topic, partition) {
                let payload = crate::protocol::wire::encode_offset_fetch_response(
                    entry.offset,
                    &entry.metadata,
                );
                WireResponse::ok(payload)
            } else {
                WireResponse::error("Offset not found")
            }
        }
        RequestPayload::RegisterBroker { node_id, endpoint } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Alter as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            match engine.register_broker(node_id, endpoint) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("RegisterBroker failed: {}", e)),
            }
        }
        RequestPayload::UnregisterBroker { node_id } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Alter as u8,
                crate::server::acl::ResourceType::Cluster as u8,
                "",
            ) {
                return WireResponse::error("ClusterAuthorizationFailed");
            }
            match engine.unregister_broker(node_id) {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("UnregisterBroker failed: {}", e)),
            }
        }
    }
}

#[derive(Debug)]
struct ScramClientFinal {
    nonce: String,
    proof: Vec<u8>,
    without_proof: String,
}

fn build_sasl_auth_response(
    error_code: i16,
    error_message: Option<&str>,
    auth_bytes: &[u8],
    session_lifetime_ms: i64,
) -> WireResponse {
    let mut buf = Vec::new();
    buf.put_i16(error_code);
    crate::protocol::wire::write_pascal_string(&mut buf, error_message.unwrap_or(""));
    buf.put_u32(auth_bytes.len() as u32);
    buf.extend_from_slice(auth_bytes);
    buf.put_i64(session_lifetime_ms);
    WireResponse::ok(buf)
}

fn resolve_quota_key<'a>(
    principal: &'a str,
    logical_client_id: Option<&'a str>,
    fallback_client_key: &'a str,
) -> String {
    match (principal != "User:ANONYMOUS", logical_client_id) {
        (true, Some(client_id)) => format!("{}|client:{}", principal, client_id),
        (true, None) => principal.to_string(),
        (false, Some(client_id)) => format!("client:{}", client_id),
        (false, None) => fallback_client_key.to_string(),
    }
}

fn parse_plain_auth(auth_text: &str) -> (String, String) {
    let parts: Vec<&str> = auth_text.split('\0').filter(|p| !p.is_empty()).collect();
    if parts.len() >= 2 {
        (parts[0].to_string(), parts[1].to_string())
    } else if let Some((username, password)) = auth_text.split_once(':') {
        (username.to_string(), password.to_string())
    } else {
        (String::new(), String::new())
    }
}

fn is_scram_client_first(auth_text: &str) -> bool {
    auth_text.contains("n=") && auth_text.contains("r=") && !auth_text.contains("p=")
}

fn is_scram_client_final(auth_text: &str) -> bool {
    auth_text.contains("c=") && auth_text.contains("r=") && auth_text.contains("p=")
}

fn parse_scram_client_first(auth_text: &str) -> Option<(String, String, String)> {
    let bare_start = auth_text.find("n=")?;
    let client_first_bare = auth_text[bare_start..].to_string();
    let mut username = None;
    let mut nonce = None;
    for part in client_first_bare.split(',') {
        if let Some(value) = part.strip_prefix("n=") {
            username = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("r=") {
            nonce = Some(value.to_string());
        }
    }
    Some((username?, nonce?, client_first_bare))
}

fn parse_scram_client_final(auth_text: &str) -> Option<ScramClientFinal> {
    let mut channel_binding = None;
    let mut nonce = None;
    let mut proof_b64 = None;
    let mut without_proof_parts = Vec::new();

    for part in auth_text.split(',') {
        if let Some(value) = part.strip_prefix("c=") {
            channel_binding = Some(value.to_string());
            without_proof_parts.push(part.to_string());
        } else if let Some(value) = part.strip_prefix("r=") {
            nonce = Some(value.to_string());
            without_proof_parts.push(part.to_string());
        } else if let Some(value) = part.strip_prefix("p=") {
            proof_b64 = Some(value.to_string());
        }
    }

    channel_binding?;
    let proof = BASE64_STANDARD.decode(proof_b64?).ok()?;
    Some(ScramClientFinal {
        nonce: nonce?,
        proof,
        without_proof: without_proof_parts.join(","),
    })
}

fn generate_scram_server_nonce() -> Result<String, ring::error::Unspecified> {
    let rng = ring::rand::SystemRandom::new();
    let mut nonce = [0u8; SCRAM_SERVER_NONCE_LEN];
    ring::rand::SecureRandom::fill(&rng, &mut nonce)?;
    Ok(scram::hex_encode(&nonce))
}

fn verify_scram_proof(
    client_final: &ScramClientFinal,
    session: &ScramSession,
    credential: &ScramCredential,
) -> bool {
    let auth_message = format!(
        "{},{},{}",
        session.client_first_bare, session.server_first_message, client_final.without_proof
    );
    credential.verify_client_proof(&auth_message, &client_final.proof)
}

fn build_scram_server_final(
    session: &ScramSession,
    credential: &ScramCredential,
    client_final: &ScramClientFinal,
) -> String {
    let auth_message = format!(
        "{},{},{}",
        session.client_first_bare, session.server_first_message, client_final.without_proof
    );
    credential.build_server_final(&auth_message)
}
