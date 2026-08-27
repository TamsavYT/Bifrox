use crate::protocol::{RequestPayload, WireError, WireRequest, WireResponse};
use crate::scram::{self, ScramCredential};
use crate::server::engine::StorageEngine;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bytes::BufMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

/// Maximum allowed records per replication batch (CRIT-01 / SEC-MED-05)
const MAX_REPLICATION_BATCH_COUNT: usize = 100_000;

/// Maximum cluster-ID / peer string length accepted in inter-node packets (SEC-MED-06)
const MAX_CLUSTER_ID_LEN: usize = 256;
/// Cap on a `host:port` a peer advertises for itself, matching what the heartbeat decoder
/// has always enforced inline.
const MAX_BIND_ADDR_LEN: usize = 256;
/// Cap on a topic name arriving from a peer, checked before the name is allocated.
const MAX_TOPIC_NAME_LEN: usize = 512;
/// Timeout for reading client auth handshake bytes.
const AUTH_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for forwarding produce requests to the leader node.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum allowed size for a forwarded leader response (prevents OOM from a malicious/buggy leader).
const MAX_FORWARD_RESPONSE_BYTES: usize = 64 * 1024 * 1024; // 64MB
const MAX_CLIENT_ID_LEN: usize = 256;

const SCRAM_SERVER_NONCE_LEN: usize = 18;

/// Exposes the underlying plain `TcpStream` for zero-copy transmit (`TransmitFile`/
/// `sendfile`), when there is one. TLS connections are never eligible — the kernel copy
/// primitives stream raw bytes straight from disk to the socket, bypassing user space
/// entirely, which would skip encryption. `handle_connection_stream<S>` is generic over
/// both plain and TLS-wrapped sockets, so this trait lets the zero-copy fetch path opt in
/// only for the plain case without duplicating the whole connection loop.
pub trait AsPlainTcpStream {
    fn as_plain_tcp_stream(&mut self) -> Option<&mut TcpStream>;
}

impl AsPlainTcpStream for TcpStream {
    fn as_plain_tcp_stream(&mut self) -> Option<&mut TcpStream> {
        Some(self)
    }
}

impl AsPlainTcpStream for tokio_rustls::server::TlsStream<TcpStream> {
    fn as_plain_tcp_stream(&mut self) -> Option<&mut TcpStream> {
        None
    }
}

#[derive(Debug, Clone)]
struct ScramSession {
    /// True when the negotiated mechanism was a `-PLUS` variant, so the client's `c=` must
    /// carry this server's binding rather than a non-binding header.
    channel_bound: bool,
    username: String,
    /// Mechanism negotiated in `SaslHandshake`. The credential store holds one entry per
    /// `(user, mechanism)`, so this selects which credential the exchange validates against.
    mechanism: crate::scram::ScramMechanism,
    client_first_bare: String,
    server_first_message: String,
    combined_nonce: String,
}

/// Handles incoming TCP client connections and inter-node replication/heartbeat streams.
///
/// Protocol dispatch by first byte:
/// - `0xB0` — Any inter-node frame, in the versioned envelope that carries its type and
///   length (`replication::envelope`)
/// - `0x01..0x0A` / `0xF1` — Client wire protocol commands, legacy- or versioned-framed
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
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send + AsPlainTcpStream,
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
    // Set by `SaslHandshake`; selects which of a user's credentials the SCRAM exchange
    // validates against (see `ScramSession::mechanism`).
    let mut negotiated_scram_mechanism: Option<crate::scram::ScramMechanism> = None;
    // Whether the negotiated mechanism was a `-PLUS` variant, which is what requires the
    // client's `c=` to carry this server's channel binding.
    let mut negotiated_scram_is_plus = false;

    let mut buffer = vec![0u8; 64 * 1024];
    let mut filled = 0usize;
    // Reused across every request/response round trip on this connection instead of
    // allocating a fresh Vec per response (see `WireResponse::encode_into`).
    let mut response_scratch = bytes::BytesMut::new();

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

            // Every inter-node frame — replication push, heartbeat, vote request, and
            // the follower pull fetch — arrives under one versioned, length-delimited
            // envelope (issue #48, see `replication::envelope`). Five hand-rolled
            // magic-byte formats used to be dispatched here, each parsed by reading fields
            // until the decoder ran out of ones it knew about; a length now says where the
            // frame ends, so this loop can advance past a frame it could not act on
            // instead of losing sync with the peer.
            if slice[0] == crate::replication::INTER_NODE_MAGIC {
                match crate::replication::decode_frame(slice) {
                    Ok(frame) => {
                        let response = handle_inter_node_frame(&engine, &frame);
                        // The cursor advances by the frame's own declared length, whatever
                        // the outcome — including a frame this build could not serve.
                        consumed += frame.total_len;
                        match response {
                            Ok(bytes) => {
                                if let Err(e) = socket.write_all(&bytes).await {
                                    tracing::error!(
                                        "Failed to send {:?} reply to {}: {}",
                                        frame.frame_type,
                                        peer_addr,
                                        e
                                    );
                                    return;
                                }
                            }
                            Err(PacketError(msg)) => {
                                tracing::warn!(
                                    "Rejected {:?} frame from {}: {}",
                                    frame.frame_type,
                                    peer_addr,
                                    msg
                                );
                                return;
                            }
                        }
                        continue;
                    }
                    Err(crate::replication::EnvelopeError::Incomplete { .. }) => break,
                    Err(e) => {
                        tracing::warn!("Malformed inter-node frame from {}: {}", peer_addr, e);
                        return;
                    }
                }
            }

            // Everything else is the client wire protocol (`protocol::wire`), which
            // carries its own framing and its own version envelope.
            match WireRequest::decode_framed(slice) {
                Ok((req, framing, bytes_used)) => {
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
                                crate::server::hash_key(key.as_bytes(), *num_partitions as usize)
                            } else {
                                0
                            },
                        )),
                        _ => None,
                    };

                    // Controller-mutation Forwarding: these requests write to
                    // `__cluster_metadata` via `propose_metadata`, which only the
                    // cluster (Raft) leader may do. Forward to it transparently —
                    // same "connect to any broker" reasoning as produce forwarding
                    // above, but keyed on cluster leadership rather than partition
                    // leadership since there's a single controller, not one per
                    // partition.
                    let is_controller_mutation = matches!(
                        &req.payload,
                        RequestPayload::CreateTopic { .. }
                            | RequestPayload::DeleteTopic { .. }
                            | RequestPayload::RegisterBroker { .. }
                            | RequestPayload::UnregisterBroker { .. }
                            | RequestPayload::CreateAcls { .. }
                            | RequestPayload::DeleteAcls { .. }
                            | RequestPayload::UpsertScramUser { .. }
                            | RequestPayload::DeleteScramUser { .. }
                            | RequestPayload::AlterConfigs { .. }
                            | RequestPayload::IncrementalAlterConfigs { .. }
                    );

                    // Controller-mediated topic creation, deliberately *before* the
                    // leadership check below.
                    //
                    // A produce naming an unregistered topic used to create a local
                    // partition directory on whichever broker caught the request,
                    // with no metadata record — so cluster metadata never learned
                    // the partition existed, no follower knew to replicate it, and
                    // neither ISR management nor failover had anything to work with.
                    //
                    // Creating it here means replicas are assigned before the
                    // partition holds a single byte, and the existing forwarding
                    // logic below then routes the produce to the assigned leader
                    // like any other partition. Non-controllers skip this and
                    // forward to the controller instead (see `is_partition_leader`).
                    if let Some((topic, _)) = &target_partition {
                        if engine.is_leader() && !engine.topic_is_registered(topic) {
                            let partitions = match &req.payload {
                                RequestPayload::ProduceBatch { num_partitions, .. } => {
                                    *num_partitions
                                }
                                _ => 1,
                            };
                            if let Err(e) = engine.ensure_topic_created(topic, partitions).await {
                                tracing::debug!(
                                    "Auto-create: could not create topic '{}': {}",
                                    topic,
                                    e
                                );
                            }
                        }
                    }

                    // A request another broker relayed to us is served here, never
                    // relayed again. The sender already decided we are the right
                    // destination; bouncing it back is how the two brokers end up
                    // ping-ponging a request that neither will serve — which is
                    // exactly what happens when the assigned leader has not yet
                    // received its assignment through the metadata log.
                    if let Some((topic, partition)) = target_partition {
                        if !framing.is_forwarded() && !engine.is_partition_leader(&topic, partition)
                        {
                            // Marked as forwarded so the receiving broker serves it rather than
                            // relaying it onward — see `tags::FORWARDED`. Falls back to
                            // the raw bytes if the request cannot be rewrapped, which
                            // preserves the previous behavior rather than dropping it.
                            let raw_request =
                                crate::protocol::wire::wrap_forwarded_request(&slice[..bytes_used])
                                    .unwrap_or_else(|_| slice[..bytes_used].to_vec());
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
                                        Ok(bytes) => {
                                            // The relay hop is always framed (it
                                            // carries the forwarded marker); the
                                            // client may not be.
                                            crate::protocol::wire::relay_response(&bytes, &framing)
                                        }
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
                                            .encode_framed(&framing)
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
                                    ).encode_framed(&framing)
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
                                &mut negotiated_scram_mechanism,
                                &mut negotiated_scram_is_plus,
                                &framing,
                            )
                            .await;
                            response_scratch.clear();
                            response.encode_framed_into(&framing, &mut response_scratch);
                            if let Err(e) = socket.write_all(&response_scratch).await {
                                tracing::error!("Failed to send response to {}: {}", peer_addr, e);
                                return;
                            }
                        }
                    } else if is_controller_mutation
                        && !engine.is_leader()
                        && !framing.is_forwarded()
                    {
                        // Marked as forwarded so the receiving broker serves it rather than
                        // relaying it onward — see `tags::FORWARDED`. Falls back to
                        // the raw bytes if the request cannot be rewrapped, which
                        // preserves the previous behavior rather than dropping it.
                        let raw_request =
                            crate::protocol::wire::wrap_forwarded_request(&slice[..bytes_used])
                                .unwrap_or_else(|_| slice[..bytes_used].to_vec());
                        consumed += bytes_used;

                        let response_bytes = match engine.leader_addr() {
                            Some(leader) => {
                                tracing::info!(
                                    "Controller Forwarding: Proxying request from {} to cluster leader at {}",
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
                                    Ok(bytes) => {
                                        crate::protocol::wire::relay_response(&bytes, &framing)
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Controller Forwarding: Failed to forward to leader {}: {}",
                                            leader,
                                            e
                                        );
                                        WireResponse::error(&format!(
                                            "Failed to forward request to cluster leader: {}",
                                            e
                                        ))
                                        .encode_framed(&framing)
                                    }
                                }
                            }
                            None => {
                                tracing::warn!(
                                    "Controller Forwarding: No cluster leader known yet, rejecting request from {}",
                                    peer_addr
                                );
                                WireResponse::error(
                                    "NOT_CONTROLLER: No cluster leader elected. Retry later.",
                                )
                                .encode_framed(&framing)
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

                        // Zero-copy fast path: a `Fetch` on a plain (non-TLS) TCP
                        // connection can be served by streaming the entry bytes
                        // straight from the segment file to the socket via the
                        // kernel (`sendfile`/`TransmitFile`), instead of
                        // `process_request` reading them into a Vec and copying
                        // them through the response buffer. The response bytes are
                        // identical either way (see `try_zero_copy_fetch`).
                        //
                        // Any ineligible case — TLS, a request that needs to wait
                        // for data, an unsupported OS, nothing to send — falls
                        // straight through to the buffered handling below. This is
                        // purely an optimization, never a behavior change.
                        let mut zero_copy_handled = false;
                        #[cfg(any(windows, target_os = "linux"))]
                        if let RequestPayload::Fetch {
                            topic,
                            partition,
                            offset,
                            max_bytes,
                        } = &req.payload
                        {
                            if let Some(raw_socket) = socket.as_plain_tcp_stream() {
                                match try_zero_copy_fetch(
                                    &engine,
                                    raw_socket,
                                    topic,
                                    *partition,
                                    *offset,
                                    *max_bytes,
                                    &client_principal,
                                    &client_key,
                                    &logical_client_id,
                                    &framing,
                                )
                                .await
                                {
                                    Ok(true) => zero_copy_handled = true,
                                    Ok(false) => {}
                                    Err(e) => {
                                        tracing::error!(
                                            "Zero-copy fetch transmit failed for {}: {}",
                                            peer_addr,
                                            e
                                        );
                                        return;
                                    }
                                }
                            }
                        }

                        if !zero_copy_handled {
                            let response = process_request(
                                &engine,
                                req,
                                &client_key,
                                &mut client_principal,
                                &client_key,
                                &mut logical_client_id,
                                &mut scram_session,
                                &mut negotiated_scram_mechanism,
                                &mut negotiated_scram_is_plus,
                                &framing,
                            )
                            .await;
                            response_scratch.clear();
                            response.encode_framed_into(&framing, &mut response_scratch);
                            if let Err(e) = socket.write_all(&response_scratch).await {
                                tracing::error!("Failed to send response to {}: {}", peer_addr, e);
                                return;
                            }
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

    // The relay hop always uses the versioned envelope (it carries the forwarded marker),
    // so the leader answers framed: `[0xF1][correlation: 4b]` precedes the usual
    // `[status][len: 4b][payload]`. That prefix has to be consumed here, or its bytes get
    // read as the response's status and length — turning the correlation id into a
    // multi-gigabyte payload length and hanging the relay until it times out.
    //
    // Still tolerates an unframed reply, so a peer that answers without the envelope is
    // handled rather than misread.
    let mut first = [0u8; 1];
    match timeout(FORWARD_TIMEOUT, stream.read_exact(&mut first)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Timed out reading response from leader {}", leader_addr),
            ))
        }
    }

    let mut header = [0u8; 5];
    if first[0] == crate::protocol::wire::VERSIONED_ENVELOPE_MAGIC {
        let mut correlation = [0u8; 4];
        match timeout(FORWARD_TIMEOUT, stream.read_exact(&mut correlation)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Timed out reading correlation id from {}", leader_addr),
                ))
            }
        }
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
    } else {
        header[0] = first[0];
        match timeout(FORWARD_TIMEOUT, stream.read_exact(&mut header[1..5])).await {
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
/// A frame this node will not serve, with the reason.
///
/// Always terminal. There is deliberately no "need more data" outcome any more: the
/// envelope guarantees a whole frame arrived before a payload is ever looked at, so a
/// payload that runs short is malformed rather than partial. That distinction used to be
/// every inter-node decoder's hardest job — misjudging it either dropped a healthy peer or
/// re-applied a batch that had already been written — and it is now unrepresentable.
#[derive(Debug)]
struct PacketError(String);

impl PacketError {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

/// Routes one decoded inter-node frame to its handler and returns the reply frame to write
/// back.
///
/// Response frame types never appear here — a peer that sends one on a connection where a
/// request is expected is confused, and saying so is better than parsing it as whatever
/// request happened to share its shape.
fn handle_inter_node_frame(
    engine: &StorageEngine,
    frame: &crate::replication::InterNodeFrame<'_>,
) -> Result<Vec<u8>, PacketError> {
    use crate::replication::FrameType;
    match frame.frame_type {
        FrameType::ReplicationPush => handle_replication_push(engine, frame.payload),
        FrameType::Heartbeat => handle_heartbeat(engine, frame.payload),
        FrameType::VoteRequest => handle_vote_request(engine, frame.payload),
        FrameType::ReplicationFetch => handle_replication_fetch(engine, frame.payload),
        FrameType::ReplicationPushAck
        | FrameType::HeartbeatAck
        | FrameType::VoteResponse
        | FrameType::ReplicationFetchResponse => Err(PacketError::new(format!(
            "received {:?}, which is a reply and never a request",
            frame.frame_type
        ))),
    }
}

/// Reads fields out of an inter-node frame payload, checking each has actually arrived
/// before reading it.
///
/// Every field a peer sends is length- or bounds-checked here rather than trusted: a
/// truncated, corrupt, or hostile payload must produce a clean rejection, never a panic or
/// an unbounded allocation. This exists as one reader because the alternative — each frame
/// decoder open-coding its own `if src.len() < n` ladder — is where a missed check hides.
///
/// Unlike the pre-envelope decoders, running short here is always fatal and never "read
/// more": the envelope already guaranteed the whole frame arrived, so a payload that ends
/// early is a malformed frame, not a partial one.
struct PayloadReader<'a> {
    cursor: &'a [u8],
    frame: &'static str,
}

impl<'a> PayloadReader<'a> {
    fn new(payload: &'a [u8], frame: &'static str) -> Self {
        Self {
            cursor: payload,
            frame,
        }
    }

    fn short(&self, field: &str) -> PacketError {
        PacketError::new(format!(
            "{} frame ended before its {} field",
            self.frame, field
        ))
    }

    fn take(&mut self, n: usize, field: &str) -> Result<&'a [u8], PacketError> {
        if self.cursor.len() < n {
            return Err(self.short(field));
        }
        let (head, tail) = self.cursor.split_at(n);
        self.cursor = tail;
        Ok(head)
    }

    fn u16(&mut self, field: &str) -> Result<u16, PacketError> {
        Ok(u16::from_be_bytes(self.take(2, field)?.try_into().unwrap()))
    }

    fn u32(&mut self, field: &str) -> Result<u32, PacketError> {
        Ok(u32::from_be_bytes(self.take(4, field)?.try_into().unwrap()))
    }

    fn u64(&mut self, field: &str) -> Result<u64, PacketError> {
        Ok(u64::from_be_bytes(self.take(8, field)?.try_into().unwrap()))
    }

    /// A `[len: 2b][bytes]` string, capped and strictly UTF-8 validated.
    ///
    /// Strict rather than lossy on purpose: `from_utf8_lossy` would replace invalid bytes
    /// with U+FFFD, which means two different byte sequences can produce the same string —
    /// so a crafted cluster_id could be made to compare equal to the real one. The cap is
    /// checked before any allocation, so a claimed length cannot itself be the attack.
    fn pascal_string(&mut self, max_len: usize, field: &str) -> Result<String, PacketError> {
        let len = self.u16(field)? as usize;
        if len > max_len {
            return Err(PacketError::new(format!(
                "{} frame's {} length {} exceeds the {} byte maximum",
                self.frame, field, len, max_len
            )));
        }
        let bytes = self.take(len, field)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| {
            PacketError::new(format!("Invalid UTF-8 in {} frame's {}", self.frame, field))
        })
    }
}

/// Serves a [`FrameType::VoteRequest`] from a Raft candidate, returning the encoded
/// [`FrameType::VoteResponse`].
///
/// Grant rules (Raft):
///   - The incoming cluster_id must match ours (CRIT-02: prevents external term manipulation).
///   - The candidate's term must be >= our current epoch.
///   - The candidate's `__cluster_metadata` log must be at least as up to date as ours
///     (§5.4.1 log-completeness / leader-completeness safety) — a candidate that hasn't
///     seen as much committed metadata as we have must never win, or an election could
///     silently roll back topics/ACLs/broker registrations that only we (and possibly
///     other followers) know about.
///   - We haven't already voted for someone else in this term.
fn handle_vote_request(engine: &StorageEngine, payload: &[u8]) -> Result<Vec<u8>, PacketError> {
    let mut reader = PayloadReader::new(payload, "VoteRequest");
    let incoming_cluster_id = reader.pascal_string(MAX_CLUSTER_ID_LEN, "cluster_id")?;
    let candidate_id = reader.u32("candidate_id")?;
    let term = reader.u64("term")?;
    let candidate_last_log_index = reader.u64("candidate_last_log_index")?;

    let deny = || Ok(crate::replication::encode_vote_response(false));

    let local_cluster = &engine.config().cluster_id;
    let our_epoch = engine.replication().get_epoch();

    // CRIT-02: Cluster-ID mismatch — reject to prevent external nodes from manipulating Raft term.
    if &incoming_cluster_id != local_cluster {
        tracing::warn!(
            "VoteRequest: Rejected — cluster mismatch (got '{}', expected '{}')",
            incoming_cluster_id,
            local_cluster
        );
        return deny();
    }

    // C2: In standalone mode (no peers configured) this node is not part of a multi-node
    // cluster and should never grant Raft votes to external candidates.
    if engine.config().peer_addrs.is_empty() {
        tracing::warn!(
            "VoteRequest: Rejected — standalone mode, candidate {} denied",
            candidate_id
        );
        return deny();
    }

    // A broker-only node is never part of the metadata Raft quorum — it must not grant
    // votes even if asked, both because it isn't counted in any controller's quorum math
    // (see `ClusterConfig::effective_controller_peer_addrs`) and as defense in depth
    // against a misconfigured/malicious peer trying to inflate a candidate's vote count
    // with a voter that was never supposed to be eligible.
    if !engine.config().is_controller_role() {
        tracing::warn!(
            "VoteRequest: Rejected — this node has no Controller role, candidate {} denied",
            candidate_id
        );
        return deny();
    }

    // §5.4.1: our own last-applied `__cluster_metadata` index is the log-completeness
    // yardstick. get_or_create_partition never fails for a valid static topic name, but
    // fall back to "empty log" (0) rather than propagating an I/O error into vote denial.
    let our_last_log_index = engine
        .get_or_create_partition("__cluster_metadata", 0)
        .map(|pm| pm.latest_offset())
        .unwrap_or(0);
    let log_ok = candidate_last_log_index >= our_last_log_index;

    if term >= our_epoch
        && log_ok
        && engine
            .replication()
            .consensus()
            .try_record_vote(candidate_id, term)
    {
        // Grant vote and adopt the new term epoch
        engine.replication().set_epoch(term);
        tracing::info!(
            "VoteRequest: GRANTED vote to candidate {} for term {} (our epoch was {})",
            candidate_id,
            term,
            our_epoch
        );
        Ok(crate::replication::encode_vote_response(true))
    } else {
        tracing::info!(
            "VoteRequest: DENIED — candidate {} term {} (our epoch: {}, our last_log_index: {}, candidate last_log_index: {}, log_ok: {})",
            candidate_id,
            term,
            our_epoch,
            our_last_log_index,
            candidate_last_log_index,
            log_ok
        );
        deny()
    }
}

/// Serves a [`FrameType::ReplicationFetch`] — a follower pulling from its leader,
/// Kafka-style — returning the encoded [`FrameType::ReplicationFetchResponse`].
fn handle_replication_fetch(
    engine: &StorageEngine,
    payload: &[u8],
) -> Result<Vec<u8>, PacketError> {
    let (req, _) = crate::replication::ReplicationFetchRequest::decode(payload)
        .map_err(|_| PacketError::new("Invalid replication fetch request".to_string()))?;

    let response = if !engine.is_partition_leader(&req.topic, req.partition) {
        // Not this node's partition to serve — respond empty rather than erroring; the
        // follower will simply see no progress this round and re-resolve the leader on
        // its next iteration (see the fetcher loop's per-iteration leader lookup).
        crate::replication::ReplicationFetchResponse {
            leader_watermark: 0,
            isr_count: 0,
            entries: bytes::Bytes::new(),
        }
    } else {
        match engine.get_or_create_partition(&req.topic, req.partition) {
            Ok(pm) => {
                // Served as the leader's stored bytes, not decoded records: the follower
                // appends them verbatim so its log stays byte-identical to this one —
                // still batched, still compressed in the producer's codec.
                let entries = pm
                    .fetch_entries_for_replication(req.fetch_offset, req.max_bytes)
                    .unwrap_or_default();

                // A Fetch request at offset X is itself proof the follower already durably
                // has everything up to X-1; record that as this follower's confirmed
                // watermark. (X == 0 means "nothing yet" — nothing to record.)
                if req.fetch_offset > 0 {
                    if let Some(follower_addr) = engine.get_broker_address(req.follower_node_id) {
                        engine.replication().update_replica_watermark(
                            &req.topic,
                            req.partition,
                            &follower_addr,
                            req.fetch_offset - 1,
                        );
                    }
                }

                crate::replication::ReplicationFetchResponse {
                    leader_watermark: pm.high_watermark(),
                    isr_count: 1,
                    entries,
                }
            }
            Err(_) => crate::replication::ReplicationFetchResponse {
                leader_watermark: 0,
                isr_count: 0,
                entries: bytes::Bytes::new(),
            },
        }
    };

    Ok(response.encode_frame())
}

/// Serves a [`FrameType::ReplicationPush`] from a leader, returning the encoded
/// [`FrameType::ReplicationPushAck`].
///
/// CRIT-01: verifies cluster_id and epoch before accepting any replicated data.
///
/// The envelope having already delimited the frame removes an entire class of hazard this
/// decoder used to carry. It previously parsed straight off the connection buffer and
/// could run out of bytes mid-batch, which meant it had to be careful never to write
/// anything before the last entry had parsed — otherwise a batch split across TCP segments
/// was replayed from the start on the next call and permanently duplicated in the log.
/// Here the whole frame is present by construction, so short data is malformed, never
/// partial. The parse-everything-then-write ordering is kept regardless: it is also what
/// makes a batch all-or-nothing against a decode error partway through.
fn handle_replication_push(engine: &StorageEngine, payload: &[u8]) -> Result<Vec<u8>, PacketError> {
    let mut reader = PayloadReader::new(payload, "ReplicationPush");
    // CRIT-01: cluster_id is the first field, so an unknown sender is rejected before any
    // partition state is touched.
    let incoming_cluster_id = reader.pascal_string(MAX_CLUSTER_ID_LEN, "cluster_id")?;
    let local_cluster = &engine.config().cluster_id;
    if &incoming_cluster_id != local_cluster {
        tracing::warn!(
            "HA Replication: REJECTED — cluster_id mismatch (got '{}', expected '{}').",
            incoming_cluster_id,
            local_cluster
        );
        return Err(PacketError::new(
            "Replication cluster_id mismatch".to_string(),
        ));
    }

    let topic = reader.pascal_string(MAX_TOPIC_NAME_LEN, "topic")?;
    let partition = reader.u32("partition")?;
    let incoming_epoch = reader.u64("epoch")?;
    // The leader's committed high watermark at push time (see `encode_replication_push`).
    let leader_hw = reader.u64("leader_hw")?;
    let entries_len = reader.u32("entries_len")? as usize;
    let mut entry_bytes = reader.take(entries_len, "entry bytes")?;

    let nack = || {
        Ok(crate::replication::encode_replication_push_ack(
            crate::replication::push_ack::NACK,
        ))
    };

    // Epoch fencing. Which epoch is authoritative depends on what is being replicated
    // (see `ReplicationManager::replicate_batch`): the metadata log is fenced by the
    // controller's Raft term, while a data partition is fenced by its own leader epoch.
    // Using the controller term for data partitions — as this used to — made every
    // controller election spuriously invalidate in-flight pushes for every partition,
    // while failing to fence a partition leader that had genuinely been superseded.
    let is_cluster_meta_topic = topic == "__cluster_metadata";
    let current_epoch = if is_cluster_meta_topic {
        engine.replication().get_epoch()
    } else {
        engine
            .get_partition(&topic, partition)
            .map(|pm| pm.leader_epoch() as u64)
            .unwrap_or(0)
    };
    if incoming_epoch < current_epoch {
        tracing::warn!(
            "HA Replication: Stale epoch {} (current {}) from leader for topic '{}' partition {} – rejecting",
            incoming_epoch, current_epoch, topic, partition
        );
        // A distinct status, not a generic NACK: it tells the sender it is no longer the
        // leader and must step down, rather than to retry.
        return Ok(crate::replication::encode_replication_push_ack(
            crate::replication::push_ack::STALE_EPOCH,
        ));
    }
    if incoming_epoch > current_epoch && is_cluster_meta_topic {
        // Only the controller term is adopted from the data path. A data partition's
        // leader epoch is owned by the replicated metadata log (`PartitionLeadershipChange`)
        // — letting a push mutate it here would make the fence self-certifying, since the
        // sender would be declaring the very epoch it is then validated against. A push
        // carrying a newer partition epoch is simply accepted; the authoritative leadership
        // record follows through the metadata log.
        engine.replication().set_epoch(incoming_epoch);
        tracing::info!(
            "HA Replication: Updated controller epoch to {} from leader for topic '{}' partition {}",
            incoming_epoch,
            topic,
            partition
        );
    }

    // Parse every entry before writing any, so a decode failure partway through leaves the
    // log untouched rather than half-applied.
    //
    // Entries are kept whole (not narrowed to their records) so the write pass can append
    // them verbatim — preserving the leader's original offsets, timestamps and CRC instead
    // of re-encoding them locally, which is what keeps a replica's log byte-identical to
    // its leader's.
    let mut parsed_entries: Vec<crate::segment::LogEntry> = Vec::new();
    while !entry_bytes.is_empty() {
        if parsed_entries.len() >= MAX_REPLICATION_BATCH_COUNT {
            tracing::warn!(
                "HA Replication: Rejected — entry count exceeds maximum {}",
                MAX_REPLICATION_BATCH_COUNT
            );
            return Err(PacketError::new(
                "Replication batch entry count too large".to_string(),
            ));
        }
        match crate::segment::decode_entry(entry_bytes) {
            Ok((entry, consumed)) => {
                entry_bytes = &entry_bytes[consumed..];
                parsed_entries.push(entry);
            }
            Err(e) => {
                return Err(PacketError::new(format!("Entry decode error: {}", e)));
            }
        }
    }
    let count = parsed_entries.len();

    let is_cluster_meta = is_cluster_meta_topic;
    let pm = engine
        .get_or_create_partition(&topic, partition)
        .map_err(|e| PacketError::new(format!("Partition create error: {}", e)))?;

    // C1: Track write failures and NACK the leader so it can remove this node from ISR.
    // Previously, disk errors were logged and silently swallowed; the function returned
    // a success ACK regardless, causing the leader to falsely count this follower as
    // in-sync for data it never persisted. A `Gap` (this follower is missing earlier
    // offsets, e.g. after a reconnect) is treated the same way: NACK so the leader keeps
    // this replica out of the ISR quorum count until it catches back up, rather than
    // silently dropping the out-of-order frame.
    let mut write_failed = false;
    // Metadata records decoded from this batch, applied only once the batch is durable.
    // Applying inline (as this used to) meant the in-memory topic registry, ACLs and
    // partition assignments could reflect records whose fsync then failed — leaving memory
    // ahead of disk with nothing to signal it, so the broker would serve authorization
    // decisions that silently revert on restart.
    let mut pending_metadata: Vec<(u64, crate::replication::MetadataRecord)> = Vec::new();
    for (i, entry) in parsed_entries.iter().enumerate() {
        // The offset an entry starts at, for the diagnostics below.
        let crate::segment::LogEntry::Batch(b) = entry;
        let entry_offset = b.base_offset;
        match pm.append_replica_entry_verbatim(entry) {
            Ok(crate::segment::VerbatimAppendResult::Appended) => {}
            Ok(crate::segment::VerbatimAppendResult::AlreadyApplied) => {
                tracing::debug!(
                    "HA Replication: Record {}/{} on '{}' P{} at offset {} already applied — skipping",
                    i + 1,
                    count,
                    topic,
                    partition,
                    entry_offset
                );
            }
            Ok(crate::segment::VerbatimAppendResult::Gap { expected }) => {
                tracing::warn!(
                    "HA Replication: Gap on '{}' P{} — got offset {} but expected {}. Rejecting batch.",
                    topic,
                    partition,
                    entry_offset,
                    expected
                );
                write_failed = true;
                break;
            }
            Err(e) => {
                tracing::error!(
                    "HA Replication: Failed to persist record {}/{} on '{}' P{}: {}",
                    i + 1,
                    count,
                    topic,
                    partition,
                    e
                );
                write_failed = true;
                // Don't apply this frame's metadata-record effects — it was never
                // durably persisted, so applying it would desync memory from disk.
                continue;
            }
        }

        // If this node is a Follower and receives a __cluster_metadata replication,
        // decode it now but hold the state effects until the batch is durable (see the
        // apply pass below the flush).
        if is_cluster_meta {
            for record in crate::segment::records_from_entries(std::slice::from_ref(entry)) {
                let payload = record.value.unwrap_or_default();
                if let Ok(meta_rec) = crate::replication::MetadataRecord::decode(&payload) {
                    pending_metadata.push((record.offset, meta_rec));
                }
            }
        }
    }

    // Adopt the leader's committed point, clamped to what this replica actually holds.
    // Never the local LEO: the records just appended are not committed until the ISR has
    // acknowledged them on the leader, so treating them as committed here would expose
    // uncommitted data to follower-fetch reads and would let this replica claim a
    // too-high committed offset if it were promoted.
    pm.advance_committed_hw(leader_hw.min(pm.latest_offset()));

    // Group commit: one fsync for the whole replicated batch instead of one per frame
    // (see `PartitionManager::flush_if_sync_policy`).
    //
    // `__cluster_metadata` is the exception: it always syncs, regardless of the configured
    // flush policy (see `PartitionManager::flush_durable`). Issue #24's remaining gap was
    // that under the default `AsyncPeriodic` policy this ACK meant "in page cache", not "on
    // disk" — so a majority acknowledgement did not actually guarantee durability of state
    // that drives authorization and partition placement. Data topics keep the configurable
    // policy; only the low-volume control-plane log pays the unconditional fsync.
    let flush_result = if is_cluster_meta {
        pm.flush_durable()
    } else {
        pm.flush_if_sync_policy()
    };
    if let Err(e) = flush_result {
        tracing::error!(
            "HA Replication: Failed to sync '{}' P{} after replicated batch: {}",
            topic,
            partition,
            e
        );
        write_failed = true;
    }

    // Metadata takes effect only now — after the batch is durable. Disk is the source of
    // truth and memory follows it, never the other way round.
    //
    // Applying inline during the append loop (as this used to) left a window where the
    // in-memory topic registry, ACLs and partition assignments reflected records whose
    // fsync then failed: memory ran ahead of disk with nothing signalling the divergence,
    // so the broker served authorization decisions that would silently revert on restart.
    // Leaving them unapplied and NACKing instead makes the leader re-deliver the batch.
    if !write_failed {
        for (offset, record) in pending_metadata {
            engine.apply_metadata_record(offset, record);
        }
    } else if !pending_metadata.is_empty() {
        tracing::error!(
            "HA Replication: '{}' P{} batch was not durable — {} metadata record(s) left \
             unapplied so in-memory state cannot run ahead of disk",
            topic,
            partition,
            pending_metadata.len()
        );
    }

    tracing::info!(
        "HA Replication: Follower persisted {} replicated record(s) on Topic '{}' Partition {}",
        count,
        topic,
        partition
    );

    if write_failed {
        // Signal NACK so the leader retries or removes this follower from ISR.
        return nack();
    }
    Ok(crate::replication::encode_replication_push_ack(
        crate::replication::push_ack::OK,
    ))
}

/// Serves a [`FrameType::Heartbeat`] from a peer, returning the encoded
/// [`FrameType::HeartbeatAck`]. See `replication::encode_heartbeat` for the payload.
///
/// Followers only reset the election timer if the heartbeat's term >= our current epoch.
///
/// CRIT-03: leader_bind_addr must never equal this node's own advertised address,
/// unconditionally. Beyond that, issue #62: a non-empty configured peer_addrs whitelist
/// still requires leader_bind_addr to be one of those peers; an *empty* peer_addrs means
/// no static allowlist was configured, so it's accepted (subject to the cluster_id and
/// CRIT-03 checks) rather than rejecting every sender — see the inline comments below.
fn handle_heartbeat(engine: &StorageEngine, payload: &[u8]) -> Result<Vec<u8>, PacketError> {
    let mut reader = PayloadReader::new(payload, "Heartbeat");
    let incoming_cluster_id = reader.pascal_string(MAX_CLUSTER_ID_LEN, "cluster_id")?;
    let peer_node_id = reader.u32("node_id")?;
    let incoming_term = reader.u64("term")?;
    let leader_bind_addr = reader.pascal_string(MAX_BIND_ADDR_LEN, "leader_bind_addr")?;

    let local_cluster_id = &engine.config().cluster_id;
    // A rejection carries no identity of ours — a peer we refuse learns nothing about this
    // node beyond the refusal itself.
    let reject = || Ok(crate::replication::encode_heartbeat_ack(1, 0, "", &[]));

    if incoming_cluster_id != *local_cluster_id {
        tracing::warn!(
            "HA Heartbeat: REJECTED Node {}! Expected cluster '{}', got '{}'",
            peer_node_id,
            local_cluster_id,
            incoming_cluster_id
        );
        return reject();
    }

    // CRIT-03 / H6: leader_bind_addr must never equal this node's own advertised
    // address, full stop — independent of whitelist state (checked below) and enforced
    // before it. This is the actual exploit: a peer advertising our own address as "the
    // leader" would make any produce we forward loop straight back to us. Previously
    // this was only ever enforced implicitly, by peer_addrs never containing our own
    // address by construction; issue #62 makes an empty peer_addrs valid (see below),
    // which removes that implicit protection, so it's now checked explicitly and
    // unconditionally.
    let self_advertised_addr = engine.replication().advertised_addr();
    if leader_bind_addr == self_advertised_addr {
        tracing::warn!(
            "HA Heartbeat: REJECTED — leader_bind_addr '{}' equals this node's own advertised \
             address (Node {}); a peer must never claim to be us",
            leader_bind_addr,
            peer_node_id
        );
        return reject();
    }

    // Issue #62: an empty `peer_addrs` means "no static peer allowlist configured", not
    // "reject every peer". Static configuration cannot always name a peer's address up
    // front — its port may be ephemeral, or simply not yet known — which is exactly what
    // made broker discovery deadlock: a follower with no way to list its leader's address
    // rejected every heartbeat forever, so the leader never learned it existed and no
    // replica assignment was ever published. Kafka's answer here is that cluster
    // membership is gated by authentication (Bifrox has SCRAM/ACLs for that), not by a
    // static address allowlist — so once the CRIT-03 self-address check above and the
    // cluster_id check further above both pass, an empty allowlist accepts. A non-empty
    // allowlist keeps exactly its previous behavior: the leader's address must be one of
    // the configured peers.
    let peer_addrs = &engine.config().peer_addrs;
    if !peer_addrs.is_empty() && !peer_addrs.contains(&leader_bind_addr) {
        tracing::warn!(
            "HA Heartbeat: REJECTED — leader_bind_addr '{}' not in configured peer whitelist (Node {})",
            leader_bind_addr, peer_node_id
        );
        return reject();
    }

    let our_epoch = engine.replication().get_epoch();
    if incoming_term >= our_epoch {
        // Valid heartbeat from current or newer leader — update state
        engine.replication().set_epoch(incoming_term);
        engine.set_leader_addr(leader_bind_addr.clone());
        // Also register the leader's address by node_id (not just as "the current
        // leader"), so a follower's pull-replication fetch loop
        // (`ReplicationManager::start_per_partition_fetcher_manager`) can resolve where
        // to send its Fetch requests via `get_broker_address(leader_id)` — previously
        // only `__cluster_metadata`'s BrokerRegister replay populated this, which a
        // freshly-started node hasn't necessarily received yet.
        engine.register_broker_address(peer_node_id, leader_bind_addr.clone());
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

    // Reply with this node's own identity (node_id + advertised address + roles) so the
    // Leader can learn this follower's broker address AND process role(s) purely from the
    // heartbeat round-trip. Followers otherwise have no way to publish this, since only
    // the controller may durably write `BrokerRegister` records — and a follower's own
    // bootstrap-time self-registration attempt would just fail with NOT_CONTROLLER.
    //
    // Uses the *advertised* address (issue #62), not `config().bind_addr` verbatim: the
    // latter is whatever was configured before the listener bound (often a wildcard host
    // or an ephemeral `:0` port), neither of which the Leader could ever dial back.
    let self_advertised_addr = engine.replication().advertised_addr();
    Ok(crate::replication::encode_heartbeat_ack(
        0,
        engine.config().node_id,
        &self_advertised_addr,
        &engine.config().roles,
    ))
}

/// Attempts to serve a `Fetch` straight from the segment file to `socket` via the kernel
/// (`sendfile(2)` on Linux, `TransmitFile` on Windows), so the entry bytes never pass
/// through a user-space buffer at all.
///
/// The response it writes is **byte-identical** to what the buffered path in
/// `process_request` would have written for the same request: the same framing envelope,
/// the same `WireResponse` status/length prefix, the same header from
/// `fetch_entries_response_header`, and the same entry bytes — which are the same bytes
/// because both paths derive them from `SegmentManager::plan_entries_range`. This is not
/// an incidental property; a fetch response whose shape depended on which internal path
/// happened to serve it is precisely the bug that got the previous zero-copy path deleted.
///
/// Returns `Ok(false)` for anything that simply isn't eligible — a failed authorization or
/// replica check (the caller falls through to `process_request`, which re-does them and
/// produces the correct error response), or nothing to send, which includes every case
/// where the request wants to *wait* for data. Long polling is deliberately left to the
/// buffered path: parking is not a fast path, and re-implementing the wait loop here would
/// be a second place for it to be wrong.
///
/// Once the response header has been written to the socket, any further error is an
/// unrecoverable mid-response I/O failure and is returned as `Err` — the caller must close
/// the connection, since a partial response would desynchronize the client's read stream.
#[cfg(any(windows, target_os = "linux"))]
// The parameters are the fetch request's fields plus the connection identity needed to
// authorize it; grouping them into a struct would just move the same list behind a name.
#[allow(clippy::too_many_arguments)]
async fn try_zero_copy_fetch(
    engine: &StorageEngine,
    socket: &mut TcpStream,
    topic: &str,
    partition: u32,
    offset: u64,
    max_bytes: u32,
    principal: &str,
    client_key: &str,
    logical_client_id: &Option<String>,
    framing: &crate::protocol::wire::RequestFraming,
) -> std::io::Result<bool> {
    if !engine.authorize(
        principal,
        client_key,
        crate::server::acl::AclOperation::Read as u8,
        crate::server::acl::ResourceType::Topic as u8,
        topic,
    ) {
        return Ok(false);
    }
    if !engine.is_partition_replica(topic, partition) {
        return Ok(false);
    }

    let fetch_start = std::time::Instant::now();
    let plan = match engine
        .plan_entries_fetch(topic, partition, offset, max_bytes)
        .await
    {
        Ok(Some(plan)) if plan.physical_len > 0 => plan,
        // Nothing to send. A request that asked to wait must reach the buffered path so it
        // can park on the high watermark instead of being answered empty here.
        Ok(_) => return Ok(false),
        Err(_) => return Ok(false), // let the buffered path surface the real error
    };

    // `min_bytes` is a floor on what the client considers worth returning; below it the
    // request is supposed to wait. Only the buffered path can wait, so hand it back.
    if plan.physical_len < framing.min_bytes() as u64 && framing.max_wait_ms() > 0 {
        return Ok(false);
    }

    // Attribute this fetch to the group member it was tagged for (issue #54). A sequential
    // consumer is served almost entirely from here, so without this, progress tracking
    // would silently never fire for the case it exists to cover.
    if let Some((group_id, member_id)) = framing.group_member() {
        engine
            .group_coordinator()
            .record_progress(&group_id, &member_id);
    }

    // The transactional metadata a read-committed consumer filters with. The broker does
    // not drop aborted records itself — it cannot, without decoding a compressed batch —
    // so this path serves transactional partitions exactly as the buffered one does:
    // identical entry bytes, identical `last_stable_offset` and aborted ranges, and the
    // consumer applies them.
    let (lso, aborted) = engine.read_committed_filter(topic, partition, framing.isolation_level());
    let header = fetch_entries_response_header(lso, &aborted, plan.physical_len as usize);

    let payload_len = header.len() as u64 + plan.physical_len;
    if payload_len > u32::MAX as u64 {
        return Ok(false); // not representable in the length prefix; let the buffered path cap it
    }

    // Recorded here as well as on the buffered arm — measuring only the buffered path
    // would leave the fetch-latency histogram near-empty on exactly the sequential-read
    // workload it matters most for. Timed before the throttle sleep and the socket write,
    // so it reflects broker-side read cost rather than deliberate quota delay or client
    // backpressure.
    engine
        .metrics()
        .fetch_latency_ms
        .record(fetch_start.elapsed());

    let quota_key = resolve_quota_key(principal, logical_client_id.as_deref(), client_key);
    engine
        .throttle_fetch(topic, &quota_key, plan.physical_len)
        .await;

    // This path writes straight to the socket rather than returning a `WireResponse`, so
    // it applies the request's framing itself. Without that, a versioned request would get
    // an unwrapped reply here and only here, and the client would read the status byte as
    // the envelope magic it was expecting.
    let mut prefix = Vec::with_capacity(10 + header.len());
    if let crate::protocol::wire::RequestFraming::Versioned { correlation_id, .. } = framing {
        prefix.put_u8(crate::protocol::wire::VERSIONED_ENVELOPE_MAGIC);
        prefix.put_u32(*correlation_id);
    }
    prefix.put_u8(0u8); // WireResponse status = OK
    prefix.put_u32(payload_len as u32);
    prefix.extend_from_slice(&header);
    socket.write_all(&prefix).await?;
    plan.transmit(socket).await?;
    Ok(true)
}

/// Encodes a record-bearing fetch response.
///
/// ```text
/// [last_stable_offset: 8b] [aborted_count: 4b] [(start: 8b, end: 8b) * aborted_count]
/// [entries_len: 4b] [entry bytes]
/// ```
///
/// `entry bytes` are the log's stored bytes, handed over exactly as written — the broker
/// does not decode records out of them and never decompresses a batch. Deciding what a
/// record means, and decompressing it, is the consumer's job.
///
/// The transactional metadata is what makes read-committed work without the broker
/// decoding anything: aborted records cannot be filtered out of a compressed batch
/// server-side, so the broker reports which offset ranges were aborted and how far the log
/// is stable, and the consumer drops them after decompressing. Kafka's fetch response
/// carries `last_stable_offset` and `aborted_transactions` for exactly this reason.
/// `u64::MAX` with an empty list means "nothing to filter" (read-uncommitted).
fn encode_fetch_entries_response(
    last_stable_offset: u64,
    aborted: &[(u64, u64)],
    entries: &bytes::Bytes,
) -> Vec<u8> {
    let mut buf = fetch_entries_response_header(last_stable_offset, aborted, entries.len());
    buf.reserve(entries.len());
    buf.put_slice(entries);
    buf
}

/// Everything in a fetch response up to but not including the entry bytes themselves.
///
/// Split out so the zero-copy path (`try_zero_copy_fetch`) can write exactly this header
/// and then have the kernel append the entry bytes, without a second implementation of the
/// response layout that could drift from this one. A divergence here is not a cosmetic bug
/// — the client parses the two as one format, so any difference corrupts its read stream.
fn fetch_entries_response_header(
    last_stable_offset: u64,
    aborted: &[(u64, u64)],
    entries_len: usize,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + aborted.len() * 16);
    buf.put_u64(last_stable_offset);
    buf.put_u32(aborted.len() as u32);
    for (start, end) in aborted {
        buf.put_u64(*start);
        buf.put_u64(*end);
    }
    buf.put_u32(entries_len as u32);
    buf
}

/// Routes a decoded client WireRequest to the appropriate StorageEngine method.
// The parameters are this connection's mutable state (principal, client id, SCRAM session
// and negotiated mechanism), not independent knobs — bundling them into a struct purely to
// satisfy the lint would obscure that they are borrowed mutably per request.
#[allow(clippy::too_many_arguments)]
async fn process_request(
    engine: &StorageEngine,
    req: WireRequest,
    client_key: &str,
    principal: &mut String,
    client_host: &str,
    logical_client_id: &mut Option<String>,
    scram_session: &mut Option<ScramSession>,
    negotiated_scram_mechanism: &mut Option<crate::scram::ScramMechanism>,
    negotiated_scram_is_plus: &mut bool,
    // How this request was framed, so per-request options carried as envelope tagged
    // fields (e.g. fetch isolation) are visible to the handlers that act on them.
    framing: &crate::protocol::wire::RequestFraming,
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
            // Includes the -PLUS variants when this broker has a certificate to bind to.
            let mechs = engine.advertised_sasl_mechanisms();
            let mechs = &mechs;
            let supported = mechs.iter().any(|m| m.eq_ignore_ascii_case(&mechanism));
            let error_code: i16 = if supported { 0 } else { 33 };
            // Remember which SCRAM mechanism was negotiated, so the subsequent
            // `SaslAuthenticate` validates against the credential derived under that same
            // hash — a SHA-256 credential cannot verify a SHA-512 exchange.
            if supported {
                *negotiated_scram_mechanism =
                    mechanism.parse::<crate::scram::ScramMechanism>().ok();
            }
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

                let credential =
                    match engine.lookup_scram_credential(&username, *negotiated_scram_mechanism) {
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
                    // A -PLUS mechanism was negotiated in the handshake, so this
                    // exchange must carry a real channel binding.
                    channel_bound: *negotiated_scram_is_plus,
                    // Pin the exchange to the credential actually selected above, so the
                    // client-final step verifies under the same hash the server-first
                    // salt/iterations came from.
                    mechanism: credential.mechanism,
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

                let credential = match engine
                    .lookup_scram_credential(&session.username, Some(session.mechanism))
                {
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

                // The `c=` value was previously only checked for *presence*, so a client
                // could claim any binding — or none — and be believed. Verifying it is what
                // makes the `-PLUS` mechanisms mean anything: a proof captured on one TLS
                // connection cannot be replayed on another, because it was computed against
                // this server's certificate.
                //
                // `expected` is `Some` only when a `-PLUS` mechanism was negotiated; for the
                // plain mechanisms the header must still be one of the non-binding forms
                // rather than an arbitrary value.
                let expected_binding = if session.channel_bound {
                    engine.tls_channel_binding()
                } else {
                    None
                };
                if !crate::scram::verify_channel_binding(
                    &client_final.channel_binding,
                    expected_binding,
                ) {
                    tracing::warn!(
                        "SASL: channel binding mismatch for user '{}' — rejecting",
                        session.username
                    );
                    scram_session.take();
                    return build_sasl_auth_response(
                        58,
                        Some("SASL Authentication Failed"),
                        &[],
                        0,
                    );
                }

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
            let auth_ok = if let Some(credential) =
                engine.lookup_scram_credential(&username, *negotiated_scram_mechanism)
            {
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
                .upsert_scram_credential(
                    &username,
                    // The wire request carries no mechanism field yet, so an
                    // externally-supplied credential is recorded under the default —
                    // which is what it was derived with. Mechanism selection is available
                    // through `upsert_scram_user_with_mechanism`.
                    crate::scram::ScramMechanism::default(),
                    iterations,
                    salt,
                    stored_key,
                    server_key,
                )
                .await
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
            match engine.delete_scram_user(&username).await {
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
            match engine.create_acl(binding).await {
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
            match engine.delete_acl(binding).await {
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
            batch,
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
            // A forwarded request was already routed here by the sending broker, which
            // resolved the leader from cluster metadata. Re-checking locally would reject
            // it whenever this node has not yet received that assignment through the
            // metadata log — the exact window in which the request was forwarded.
            if !framing.is_forwarded() && !engine.is_partition_leader(&topic, target_partition) {
                return WireResponse::error("NotLeaderForPartition");
            }
            // Quota: the request's byte cost is known up front, so charge it and serve
            // any resulting throttle delay *before* the write rather than after — a
            // post-write delay would let an over-quota burst hit the disk in full and
            // only then slow the client down. See `apply_produce_quota`.
            // The batch is CRC-checked here and nowhere else on the way in. Decoding the
            // 53-byte header also validates the stored bytes without decompressing them,
            // since the CRC covers `record_data` in its compressed form.
            let batch = match crate::protocol::RecordBatch::decode(&batch) {
                Ok((b, _)) => b,
                Err(e) => return WireResponse::error(&format!("Malformed record batch: {}", e)),
            };
            let record_count = batch.record_count as u64;
            // Quota is charged on the bytes actually stored, i.e. the compressed size.
            let produced_bytes: u64 = batch.encoded_size() as u64;
            engine.apply_produce_quota(&quota_key, produced_bytes).await;
            let produce_start = std::time::Instant::now();
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
                    batch,
                })
                .await
            {
                Ok((assigned_partition, first_offset, last_offset)) => {
                    engine
                        .metrics()
                        .produce_latency_ms
                        .record(produce_start.elapsed());
                    engine.record_produce_metrics(&topic, produced_bytes, record_count);
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
            if !framing.is_forwarded() && !engine.is_partition_replica(&topic, partition) {
                return WireResponse::error("NotLeaderForPartition");
            }
            // Attribute this fetch to the group member it was tagged for, if any (issue
            // #54): the coordinator otherwise has no visibility into consumption at all —
            // membership is only ever asserted through `Heartbeat` — so a member that
            // keeps heartbeating but has stopped actually consuming is indistinguishable
            // from a healthy one. An absent tag (a legacy client, or a fetch not made on
            // behalf of a group member) records nothing, same as today.
            //
            // Also duplicated on the zero-copy fast path (`try_zero_copy_fetch`), which
            // serves most plain-TCP fetches that find data waiting, instead of reaching
            // this arm at all.
            if let Some((group_id, member_id)) = framing.group_member() {
                engine
                    .group_coordinator()
                    .record_progress(&group_id, &member_id);
            }
            let fetch_start = std::time::Instant::now();
            // Isolation is a per-request property, expressed as a tagged field on the
            // request envelope. Committed-only reads previously required calling a
            // *different command* (`FetchCommitted`), so a client had to decide which
            // command to use up front and could not vary isolation per fetch. An absent
            // tag — including every legacy-framed request — means read-uncommitted, which
            // is exactly what `Fetch` has always done.
            let isolation = framing.isolation_level();
            match engine
                .fetch_entries_waiting(
                    &topic,
                    partition,
                    offset,
                    max_bytes,
                    framing.max_wait_ms(),
                    framing.min_bytes(),
                )
                .await
            {
                Ok(entries) => {
                    engine
                        .metrics()
                        .fetch_latency_ms
                        .record(fetch_start.elapsed());
                    let (lso, aborted) = engine.read_committed_filter(&topic, partition, isolation);
                    let fetched_bytes = entries.len() as u64;
                    let buf = encode_fetch_entries_response(lso, &aborted, &entries);
                    engine
                        .throttle_fetch(&topic, &quota_key, fetched_bytes)
                        .await;
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
            match engine
                .fetch_entries(&topic, partition, offset, max_bytes)
                .await
            {
                Ok(entries) => {
                    let (lso, aborted) = engine.read_committed_filter(
                        &topic,
                        partition,
                        crate::protocol::wire::IsolationLevel::ReadCommitted,
                    );
                    let fetched_bytes = entries.len() as u64;
                    let buf = encode_fetch_entries_response(lso, &aborted, &entries);
                    engine
                        .throttle_fetch(&topic, &quota_key, fetched_bytes)
                        .await;
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
            match engine
                .commit_offset(&group_id, &topic, partition, offset)
                .await
            {
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
            match engine
                .fetch_entries_by_timestamp(&topic, partition, target_timestamp, max_bytes)
                .await
            {
                Ok(entries) => {
                    let fetched_bytes = entries.len() as u64;
                    // The timestamp filter is the consumer's — the broker resolves the
                    // starting offset through the time index and hands over stored bytes.
                    let buf = encode_fetch_entries_response(u64::MAX, &[], &entries);
                    engine
                        .throttle_fetch(&topic, &quota_key, fetched_bytes)
                        .await;
                    WireResponse::ok(buf)
                }
                Err(e) => WireResponse::error(&format!("FetchByTimestamp failed: {}", e)),
            }
        }
        RequestPayload::Ping => WireResponse::ok(b"PONG".to_vec()),
        RequestPayload::NegotiateProtocol => {
            // Answered without authorization on purpose: a client has to learn what this
            // broker speaks before it can construct a request that would carry credentials,
            // and the reply exposes only protocol shape — never cluster or topic data.
            //
            // Encoded as [min: 2b][max: 2b][command_count: 2b][codes...] so a client can
            // both pick a version and avoid sending a command this broker would reject.
            let mut buf = Vec::new();
            buf.put_u16(crate::protocol::wire::PROTOCOL_VERSION_MIN);
            buf.put_u16(crate::protocol::wire::PROTOCOL_VERSION_MAX);
            let codes = crate::protocol::wire::supported_command_codes();
            buf.put_u16(codes.len() as u16);
            for code in codes {
                buf.put_u8(code);
            }
            WireResponse::ok(buf)
        }
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
            match engine.delete_topic(&topic).await {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("DeleteTopic failed: {}", e)),
            }
        }
        RequestPayload::JoinGroup {
            group_id,
            member_id,
            protocols,
            group_instance_id,
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
            // `join_group_awaited` holds the response until the group's join window
            // closes, so every member that joined the same window is handed the same
            // generation (see `GroupCoordinator::join_group`).
            //
            // `session_timeout_ms` — when the client sent the tag — lets the coordinator
            // use the timeout the client actually asked for instead of its historical
            // hardcoded default, clamped to a sane range (see
            // `GroupCoordinator::resolve_session_timeout`).
            match engine
                .join_group_awaited_with_options(
                    &group_id,
                    &member_id,
                    group_instance_id.as_deref(),
                    protocols,
                    crate::server::engine::JoinGroupOptions {
                        session_timeout_ms: framing.session_timeout_ms(),
                        cooperative_round_two: framing.is_cooperative_round_two(),
                    },
                )
                .await
            {
                Ok((m_id, generation_id, is_leader, protocol_name)) => {
                    let mut buf = Vec::new();
                    crate::protocol::wire::write_pascal_string(&mut buf, &m_id);
                    buf.put_u32(generation_id);
                    buf.put_u8(if is_leader { 1 } else { 0 });
                    crate::protocol::wire::write_pascal_string(&mut buf, &protocol_name);
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
                Ok(assignment) => {
                    let mut buf = Vec::new();
                    buf.put_u32(assignment.len() as u32);
                    for (topic, partitions) in assignment {
                        crate::protocol::wire::write_pascal_string(&mut buf, &topic);
                        buf.put_u32(partitions.len() as u32);
                        for p in partitions {
                            buf.put_u32(p);
                        }
                    }
                    WireResponse::ok(buf)
                }
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
            group_instance_id,
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
            match engine.group_coordinator().leave_group(
                &group_id,
                &member_id,
                group_instance_id.as_deref(),
            ) {
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
            match engine.create_topic(&topic, partitions).await {
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
                .await
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
            match engine.register_broker(node_id, endpoint).await {
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
            match engine.unregister_broker(node_id).await {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("UnregisterBroker failed: {}", e)),
            }
        }
        RequestPayload::ShareFetch {
            group_id,
            member_id,
            topic,
            partition,
            max_records,
            max_bytes: _,
            lock_timeout_ms,
            acknowledgements,
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
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Read as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            if !engine.is_partition_replica(&topic, partition) {
                return WireResponse::error("NotLeaderForPartition");
            }

            // If piggybacked acknowledgements are present, apply them first
            if !acknowledgements.is_empty() {
                if let Err(e) = engine.share_acknowledge(
                    &group_id,
                    &member_id,
                    &topic,
                    partition,
                    &acknowledgements,
                ) {
                    return WireResponse::error(&format!("ShareAcknowledge failed: {}", e));
                }
            }

            match engine.share_fetch(
                &group_id,
                &member_id,
                &topic,
                partition,
                max_records,
                lock_timeout_ms,
            ) {
                Ok(batches) => {
                    let mut fetched_bytes: u64 = 0;
                    for b in &batches {
                        for rec in &b.records {
                            // Key + value bytes plus the fixed per-record fields the encoder writes.
                            fetched_bytes += (24
                                + rec.key.as_ref().map_or(0, |k| k.len())
                                + rec.value.as_ref().map_or(0, |v| v.len()))
                                as u64;
                        }
                    }
                    let payload = crate::protocol::wire::encode_share_fetch_response(&batches);
                    engine
                        .throttle_fetch(&topic, &quota_key, fetched_bytes)
                        .await;
                    WireResponse::ok(payload)
                }
                Err(e) => WireResponse::error(&format!("ShareFetch failed: {}", e)),
            }
        }
        RequestPayload::ShareAcknowledge {
            group_id,
            member_id,
            topic,
            partition,
            acknowledgements,
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
            match engine.share_acknowledge(
                &group_id,
                &member_id,
                &topic,
                partition,
                &acknowledgements,
            ) {
                Ok(()) => {
                    let payload = crate::protocol::wire::encode_share_acknowledge_response(0, None);
                    WireResponse::ok(payload)
                }
                Err(e) => {
                    let payload =
                        crate::protocol::wire::encode_share_acknowledge_response(1, Some(&e));
                    WireResponse::ok(payload)
                }
            }
        }
        RequestPayload::ShareGroupHeartbeat {
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
            engine.share_group_heartbeat(&group_id, &member_id);
            WireResponse::ok(Vec::new())
        }
        RequestPayload::ShareGroupDescribe { group_id } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::Describe as u8,
                crate::server::acl::ResourceType::Group as u8,
                &group_id,
            ) {
                return WireResponse::error("GroupAuthorizationFailed");
            }
            let (state, members, inflight, start_offset) = engine.share_group_describe(&group_id);
            let payload = crate::protocol::wire::encode_share_group_describe_response(
                &state,
                &members,
                inflight,
                start_offset,
            );
            WireResponse::ok(payload)
        }
        RequestPayload::DescribeConfigs { topic } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::DescribeConfigs as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            let configs = engine.describe_configs(&topic);
            WireResponse::ok(crate::protocol::wire::encode_describe_configs_response(
                &configs,
            ))
        }
        RequestPayload::AlterConfigs { topic, configs } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::AlterConfigs as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            match engine.alter_configs(&topic, configs).await {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("AlterConfigs failed: {}", e)),
            }
        }
        RequestPayload::IncrementalAlterConfigs {
            topic,
            upserts,
            deletes,
        } => {
            if !engine.authorize(
                principal,
                client_host,
                crate::server::acl::AclOperation::AlterConfigs as u8,
                crate::server::acl::ResourceType::Topic as u8,
                &topic,
            ) {
                return WireResponse::error("TopicAuthorizationFailed");
            }
            match engine
                .incremental_alter_configs(&topic, upserts, deletes)
                .await
            {
                Ok(()) => WireResponse::ok(Vec::new()),
                Err(e) => WireResponse::error(&format!("IncrementalAlterConfigs failed: {}", e)),
            }
        }
    }
}

#[derive(Debug)]
struct ScramClientFinal {
    nonce: String,
    proof: Vec<u8>,
    without_proof: String,
    /// The raw `c=` value, retained so the server can verify it against the binding it
    /// expects rather than merely checking the field was present.
    channel_binding: String,
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

    let channel_binding = channel_binding?;
    let proof = BASE64_STANDARD.decode(proof_b64?).ok()?;
    Some(ScramClientFinal {
        nonce: nonce?,
        proof,
        without_proof: without_proof_parts.join(","),
        channel_binding,
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

#[cfg(test)]
mod grpc_replication_fetch_tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::replication::{ReplicationFetchRequest, ReplicationFetchResponse};
    use bytes::Bytes;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "bifrox_handler_grpc_fetch_test_{}_{}_{}",
                label,
                std::process::id(),
                unique
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn open_engine(dir: &TempDir) -> StorageEngine {
        StorageEngine::new(EngineConfig {
            data_dir: dir.0.clone(),
            bind_addr: "127.0.0.1:0".to_string(),
            ..EngineConfig::default()
        })
        .unwrap()
    }

    /// Encodes a `ReplicationFetchRequest` the exact way `send_grpc_replication_fetch`
    /// puts it on the wire.
    fn frame_request(req: &ReplicationFetchRequest) -> Vec<u8> {
        req.encode_frame()
    }

    /// Runs a framed request through the same dispatch a real connection would, so these
    /// tests exercise the envelope decode too rather than reaching past it.
    fn serve(engine: &StorageEngine, framed: &[u8]) -> (usize, Vec<u8>) {
        let frame = crate::replication::decode_frame(framed).expect("a well-formed frame");
        let response = handle_inter_node_frame(engine, &frame).expect("a servable frame");
        (frame.total_len, response)
    }

    /// Walks the raw entry bytes a replication fetch returns, flattening them into records
    /// for assertion purposes only — the follower itself appends the bytes verbatim.
    fn decode_entries_as_records(entries: &[u8]) -> Vec<crate::segment::Record> {
        let mut decoded = Vec::new();
        let mut cursor = 0usize;
        while cursor < entries.len() {
            let Ok((entry, consumed)) = crate::segment::decode_entry(&entries[cursor..]) else {
                break;
            };
            decoded.push(entry);
            cursor += consumed;
        }
        crate::segment::records_from_entries(&decoded)
    }

    fn decode_response(bytes: &[u8]) -> ReplicationFetchResponse {
        let frame = crate::replication::decode_frame(bytes).unwrap();
        assert_eq!(
            frame.frame_type,
            crate::replication::FrameType::ReplicationFetchResponse
        );
        assert_eq!(frame.total_len, bytes.len(), "no trailing bytes");
        ReplicationFetchResponse::decode(frame.payload).unwrap()
    }

    #[tokio::test]
    async fn serves_frames_from_the_requested_offset_when_this_node_is_partition_leader() {
        let dir = TempDir::new("leader_serves");
        let engine = open_engine(&dir);

        let pm = engine.get_or_create_partition("t", 0).unwrap();
        for payload in [b"rec0".to_vec(), b"rec1".to_vec(), b"rec2".to_vec()] {
            pm.produce_frame_eos(Bytes::from(payload), 0, 0, 0)
                .unwrap()
                .unwrap();
        }
        // A freshly auto-created partition already defaults leader_id to this node
        // (see `PartitionManager::open`), so no explicit `update_leadership` is needed
        // for this node to be considered the leader.

        let req = ReplicationFetchRequest {
            follower_node_id: 2,
            topic: "t".to_string(),
            partition: 0,
            fetch_offset: 1,
            max_bytes: 4096,
        };
        let framed = frame_request(&req);

        let (bytes_consumed, response_bytes) = serve(&engine, &framed);
        assert_eq!(bytes_consumed, framed.len());

        let resp = decode_response(&response_bytes);
        // The response carries the leader's stored bytes, so decode entries out of it the
        // way a follower does rather than expecting pre-decoded frames.
        let frames = decode_entries_as_records(&resp.entries);
        assert_eq!(frames.len(), 2, "expected offsets 1 and 2 (fetch_offset=1)");
        assert_eq!(frames[0].offset, 1);
        assert_eq!(frames[0].value.as_deref().unwrap_or_default(), b"rec1");
        assert_eq!(frames[1].offset, 2);
        assert_eq!(frames[1].value.as_deref().unwrap_or_default(), b"rec2");
    }

    #[tokio::test]
    async fn records_follower_progress_into_replica_watermarks_when_address_is_known() {
        let dir = TempDir::new("watermark_update");
        let engine = open_engine(&dir);
        engine.register_broker_address(2, "127.0.0.1:9999".to_string());

        let pm = engine.get_or_create_partition("t", 0).unwrap();
        pm.produce_frame_eos(Bytes::from_static(b"rec0"), 0, 0, 0)
            .unwrap()
            .unwrap();
        pm.produce_frame_eos(Bytes::from_static(b"rec1"), 0, 0, 0)
            .unwrap()
            .unwrap();

        let req = ReplicationFetchRequest {
            follower_node_id: 2,
            topic: "t".to_string(),
            partition: 0,
            fetch_offset: 2, // follower claims to already have offsets 0 and 1
            max_bytes: 4096,
        };
        let framed = frame_request(&req);
        serve(&engine, &framed);

        assert!(
            engine
                .replication()
                .replica_ack_age("t", 0, "127.0.0.1:9999")
                .is_some(),
            "a fetch request should record this follower's progress the same way a push ack does"
        );
    }

    #[tokio::test]
    async fn returns_empty_frames_without_error_when_this_node_is_not_partition_leader() {
        let dir = TempDir::new("not_leader");
        let engine = open_engine(&dir);

        let pm = engine.get_or_create_partition("t", 0).unwrap();
        pm.produce_frame_eos(Bytes::from_static(b"rec0"), 0, 0, 0)
            .unwrap()
            .unwrap();
        // Explicitly hand leadership to a different node — this node must now refuse to
        // serve fetches for it rather than handing out possibly-stale local data.
        pm.update_leadership(99, 1, vec![1, 99], vec![1, 99]);

        let req = ReplicationFetchRequest {
            follower_node_id: 2,
            topic: "t".to_string(),
            partition: 0,
            fetch_offset: 0,
            max_bytes: 4096,
        };
        let framed = frame_request(&req);

        let (_, response_bytes) = serve(&engine, &framed);
        let resp = decode_response(&response_bytes);
        assert!(resp.entries.is_empty());
    }

    /// A frame split across TCP segments must be recognised as *incomplete* and waited
    /// for, never mistaken for a malformed one — the connection loop closes on malformed,
    /// so confusing the two drops healthy peers under ordinary segmentation.
    #[tokio::test]
    async fn a_truncated_frame_is_reported_as_incomplete_at_every_prefix() {
        let req = ReplicationFetchRequest {
            follower_node_id: 2,
            topic: "t".to_string(),
            partition: 0,
            fetch_offset: 0,
            max_bytes: 4096,
        };
        let framed = frame_request(&req);

        for prefix in 0..framed.len() {
            match crate::replication::decode_frame(&framed[..prefix]) {
                Err(crate::replication::EnvelopeError::Incomplete { needed }) => {
                    assert_eq!(needed.min(framed.len()), needed.min(framed.len()));
                    assert!(needed > prefix);
                }
                other => panic!("expected Incomplete at prefix {}, got {:?}", prefix, other),
            }
        }
        // ...and the whole thing decodes.
        assert_eq!(
            crate::replication::decode_frame(&framed).unwrap().total_len,
            framed.len()
        );
    }
}

/// Issue #24's remaining item: a follower's ACK on the `__cluster_metadata` replication
/// path (`handle_replication_push`) must mean the record is actually
/// durable, not merely written to the page cache. `FlushPolicy::AsyncPeriodic` (the
/// default) makes `flush_if_sync_policy` a no-op until its interval/byte threshold trips,
/// so before this fix the ACK was issued regardless. These tests exercise the fix directly
/// against the push handler, the same way `grpc_replication_fetch_tests` above exercises
/// the fetch-side decoder.
#[cfg(test)]
mod cluster_metadata_durability_tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::replication::MetadataRecord;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "bifrox_handler_meta_durability_test_{}_{}_{}",
                label,
                std::process::id(),
                unique
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn open_engine(dir: &TempDir) -> StorageEngine {
        StorageEngine::new(EngineConfig {
            data_dir: dir.0.clone(),
            bind_addr: "127.0.0.1:0".to_string(),
            // Explicit for documentation, though this is already `EngineConfig::default()`:
            // AsyncPeriodic is the policy under which the durability gap existed.
            flush_policy: crate::config::FlushPolicy::default(),
            ..EngineConfig::default()
        })
        .unwrap()
    }

    /// Builds a replication-push frame exactly the way `send_replication_push_pooled`
    /// (`src/replication/mod.rs`) puts it on the wire — through the same encoder, so these
    /// tests cannot drift from what a real leader sends.
    fn build_push_packet(
        engine: &StorageEngine,
        topic: &str,
        partition: u32,
        epoch: u64,
        leader_hw: u64,
        batches: &[crate::protocol::RecordBatch],
    ) -> Vec<u8> {
        let entries: Vec<crate::replication::EncodedEntry> = batches
            .iter()
            .map(crate::replication::EncodedEntry::from_batch)
            .collect();
        crate::replication::encode_replication_push(
            &engine.config().cluster_id,
            topic,
            partition,
            epoch,
            leader_hw,
            &entries,
        )
    }

    /// The additive path, end to end: a push from a build that carries a field this one
    /// has never heard of must still be served, and its entries must still land.
    ///
    /// This is what the extension section buys. Without it the only way to add a field is
    /// to widen the fixed header, which means every broker in the cluster has to be
    /// upgraded in the same moment or they stop understanding each other's pushes — the
    /// lockstep upgrade this whole change exists to end.
    #[tokio::test]
    async fn a_push_carrying_an_unknown_extension_is_still_applied() {
        let dir = TempDir::new("future_extension");
        let engine = open_engine(&dir);

        let batch = broker_record(0, b"from a newer broker".to_vec());
        let entry = crate::replication::EncodedEntry::from_batch(&batch);

        // Rebuild the payload by hand so an extension can be planted in it — the encoder
        // has no field to put there yet, which is exactly the situation being modelled.
        let mut payload = Vec::new();
        crate::protocol::wire::write_pascal_string(&mut payload, &engine.config().cluster_id);
        crate::protocol::wire::write_pascal_string(&mut payload, "orders");
        payload.put_u32(0);
        payload.put_u64(0);
        payload.put_u64(1);
        payload.put_u32(entry.bytes.len() as u32);
        payload.extend_from_slice(&entry.bytes);
        crate::replication::write_extensions(
            &mut payload,
            &[(0x9001, b"a field this build has never heard of".to_vec())],
        );
        let packet = crate::replication::encode_frame(
            crate::replication::FrameType::ReplicationPush,
            &payload,
        );

        assert_eq!(
            serve_push(&engine, &packet),
            crate::replication::push_ack::OK,
            "an unknown extension must not make a push unservable"
        );

        let pm = engine.get_or_create_partition("orders", 0).unwrap();
        let records = pm.fetch(0, 4096).unwrap();
        assert_eq!(records.len(), 1, "the entry must have been appended");
        assert_eq!(
            records[0].value.as_deref(),
            Some(b"from a newer broker".as_slice())
        );
    }

    /// Serves a push frame through the real dispatch and returns the ACK's status byte.
    fn serve_push(engine: &StorageEngine, packet: &[u8]) -> u8 {
        let frame = crate::replication::decode_frame(packet).expect("a well-formed frame");
        let ack = handle_inter_node_frame(engine, &frame).expect("a servable frame");
        let ack_frame = crate::replication::decode_frame(&ack).expect("a well-formed ACK");
        assert_eq!(
            ack_frame.frame_type,
            crate::replication::FrameType::ReplicationPushAck
        );
        ack_frame.payload[0]
    }

    /// A single-record batch at `offset`, the shape everything the broker authors now takes.
    fn broker_record(offset: u64, payload: Vec<u8>) -> crate::protocol::RecordBatch {
        let mut batch = crate::protocol::RecordBatch::create(
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            crate::protocol::BatchCompression::None,
            &[(0, None, Some(bytes::Bytes::from(payload)))],
        );
        batch.assign_base_offset_and_leader_epoch(offset, 0);
        batch
    }

    #[tokio::test]
    async fn cluster_metadata_push_is_synced_even_under_the_default_async_periodic_policy() {
        let dir = TempDir::new("sync_proof");
        let engine = open_engine(&dir);

        let record = MetadataRecord::TopicCreated {
            topic: "sync_proof_topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        };
        let frame = broker_record(0, record.encode());
        let epoch = engine.replication().get_epoch();
        let packet = build_push_packet(&engine, "__cluster_metadata", 0, epoch, 1, &[frame]);

        assert_eq!(
            serve_push(&engine, &packet),
            crate::replication::push_ack::OK,
            "expected an ACK for a clean push"
        );
        assert!(
            engine.topic_is_registered("sync_proof_topic"),
            "the durable record must have been applied"
        );

        // The honest thing this proves: `unsynced_bytes` is reset to 0 only by a code path
        // that called `SegmentManager::sync()` (a real `fsync`/`fdatasync` syscall) and
        // succeeded — see `PartitionManager::flush_durable`/`flush_if_sync_policy`. It does
        // not prove the bytes reached the physical platter (nothing running inside the
        // process can prove that), but it does prove the sync syscall was actually issued
        // here, under `FlushPolicy::AsyncPeriodic` with its default 5ms/64KB thresholds
        // nowhere near tripped by this one small record — i.e. that the metadata push no
        // longer depends on the configured flush policy to become durable.
        let pm = engine
            .get_or_create_partition("__cluster_metadata", 0)
            .unwrap();
        assert_eq!(
            pm.unsynced_bytes_for_test(),
            0,
            "the metadata partition must be fully synced immediately after the ACK"
        );
    }

    /// Same push, but for an ordinary data topic — confirms the fix did not change data-topic
    /// behavior under the default policy: the write lands, but nothing forces a sync, so
    /// `unsynced_bytes` stays nonzero exactly as it always has.
    #[tokio::test]
    async fn data_topic_push_is_still_unsynced_under_the_default_async_periodic_policy() {
        let dir = TempDir::new("data_topic_unaffected");
        let engine = open_engine(&dir);

        let frame = broker_record(0, b"data record".to_vec());
        let packet = build_push_packet(&engine, "orders", 0, 0, 1, &[frame]);

        assert_eq!(
            serve_push(&engine, &packet),
            crate::replication::push_ack::OK,
            "expected an ACK for a clean push"
        );

        let pm = engine.get_or_create_partition("orders", 0).unwrap();
        assert!(
            pm.unsynced_bytes_for_test() > 0,
            "a data-topic push must NOT be forced durable under AsyncPeriodic — only \
             __cluster_metadata gets the unconditional sync"
        );
    }

    /// Sabotages the OS-level file descriptor backing `path` so the next `sync()` through
    /// any `std::fs::File` still pointing at it fails, without ever fully closing the fd.
    /// Used to force a real I/O failure in the sync step without depending on any
    /// fault-injection seam in production code.
    ///
    /// This finds the fd via `/proc/self/fd` and `dup2`s a pipe's write end onto it after
    /// closing the pipe's read end — a write end whose reader is gone still accepts
    /// `close()` normally, but `fsync`/`fdatasync` on a pipe always fails with `EINVAL`
    /// (verified: pipes aren't syncable objects). Redirecting instead of closing matters:
    /// an outright `libc::close` on a fd the standard library still owns is exactly the
    /// double-close pattern its IO-safety hardening watches for, and it aborts the whole
    /// process the moment the owning `File`'s `Drop` tries to close the same fd again.
    /// `dup2` keeps the fd number continuously open (just repointed), so that `Drop` closes
    /// a live fd like any other and nothing aborts.
    ///
    /// Linux-only: relies on `/proc/self/fd` and `libc`, which is a
    /// `cfg(target_os = "linux")`-only dependency (see `Cargo.toml`) and unavailable on the
    /// Windows CI target.
    #[cfg(target_os = "linux")]
    fn sabotage_fd_for(path: &std::path::Path) {
        let target = std::fs::canonicalize(path).expect("path must exist to steal its fd");
        let fd_dir = std::fs::read_dir("/proc/self/fd").expect("/proc/self/fd must be readable");
        let mut target_fd: Option<i32> = None;
        for entry in fd_dir.flatten() {
            let fd_path = entry.path();
            if std::fs::read_link(&fd_path).ok().as_deref() != Some(target.as_path()) {
                continue;
            }
            target_fd = fd_path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.parse().ok());
            break;
        }
        let target_fd = target_fd.unwrap_or_else(|| panic!("no open fd found for {:?}", target));

        unsafe {
            let mut pipe_fds = [0i32; 2];
            assert_eq!(libc::pipe(pipe_fds.as_mut_ptr()), 0, "pipe() failed");
            let [read_end, write_end] = pipe_fds;
            assert_eq!(libc::close(read_end), 0, "closing pipe read end failed");
            assert_eq!(
                libc::dup2(write_end, target_fd),
                target_fd,
                "dup2 onto the target fd failed"
            );
            assert_eq!(
                libc::close(write_end),
                0,
                "closing spare pipe write end failed"
            );
        }
    }

    /// Guards the NACK-on-failure logic (existing behavior) specifically against the sync
    /// step, isolated from the write step: this closes only the *index* file's fd, after an
    /// earlier append already forced its first (and only, for a while — see
    /// `index_interval_bytes`) index write, so the record under test appends its log bytes
    /// successfully — `append_replica_frame_verbatim` returns `Appended` — and only the
    /// follow-up `flush_durable()` sync fails. This is deliberately a different failure
    /// shape than the existing `Gap`-based write-failure path: it proves specifically that a
    /// failed *sync*, with the write itself having succeeded, still produces a NACK and still
    /// leaves the metadata record unapplied.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn failed_sync_still_nacks_and_leaves_metadata_unapplied() {
        let dir = TempDir::new("sync_failure_nacks");
        let engine = open_engine(&dir);

        let pm = engine
            .get_or_create_partition("__cluster_metadata", 0)
            .unwrap();
        // Prime the index: the very first append always writes an index entry (see
        // `SegmentManager::append_verbatim`), so this is out of the way before the fd gets
        // closed below.
        pm.append_replica_entry_verbatim(&crate::segment::LogEntry::Batch(broker_record(
            0,
            b"priming record".to_vec(),
        )))
        .unwrap();
        pm.flush_durable().unwrap();

        let index_path = dir.0.join("__cluster_metadata-0").join(format!(
            "{}.index",
            crate::segment::log::format_segment_filename(0)
        ));
        sabotage_fd_for(&index_path);

        let record = MetadataRecord::TopicCreated {
            topic: "should_not_apply".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        };
        // Offset 1, tiny payload: well under `index_interval_bytes` (4096 by default), so
        // this append writes only to the log — the index file (whose fd is now closed) is
        // untouched by the write itself, only by the sync that follows.
        let frame = broker_record(1, record.encode());
        let epoch = engine.replication().get_epoch();
        let packet = build_push_packet(&engine, "__cluster_metadata", 0, epoch, 2, &[frame]);

        assert_eq!(
            serve_push(&engine, &packet),
            crate::replication::push_ack::NACK,
            "a sync failure must still produce a NACK, not a silent ACK"
        );
        assert!(
            !engine.topic_is_registered("should_not_apply"),
            "metadata whose durability could not be confirmed must not be applied"
        );
    }
}

/// The one property that matters about the zero-copy fetch path: a client cannot tell
/// which path served it.
///
/// The previous zero-copy path was deleted because it had quietly drifted into writing a
/// *different response format* from the buffered one — a divergence that never fired only
/// because the path happened to always decline. So the test here is not "zero-copy returns
/// the right records", it is "the bytes on the socket are the same bytes, for the same
/// request, whichever path produced them". Anything less would not have caught that bug.
#[cfg(all(test, any(windows, target_os = "linux")))]
mod zero_copy_fetch_tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::protocol::wire::{RequestFraming, RequestTags};
    use crate::protocol::CommandCode;
    use tokio::net::{TcpListener, TcpStream};

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "bifrox_zero_copy_fetch_{}_{}_{}",
                label,
                std::process::id(),
                unique
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn open_engine(dir: &TempDir) -> StorageEngine {
        StorageEngine::new(EngineConfig {
            data_dir: dir.0.clone(),
            bind_addr: "127.0.0.1:0".to_string(),
            ..EngineConfig::default()
        })
        .unwrap()
    }

    /// Produces `payloads` into `topic`/0 and commits them, so a consumer-bound fetch can
    /// actually see them (entries above the high watermark are withheld).
    fn seed(engine: &StorageEngine, topic: &str, payloads: &[&[u8]]) {
        let pm = engine.get_or_create_partition(topic, 0).unwrap();
        for payload in payloads {
            pm.produce_frame(payload).unwrap();
        }
        pm.advance_committed_hw(pm.latest_offset());
    }

    /// A connected loopback pair, so the zero-copy path has a real `TcpStream` to
    /// `sendfile`/`TransmitFile` into and the test can read back exactly what landed.
    async fn socket_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (server, client)
    }

    /// Runs the fetch through `try_zero_copy_fetch` and returns the bytes it wrote to the
    /// socket, or `None` if it declined.
    async fn zero_copy_bytes(
        engine: &StorageEngine,
        topic: &str,
        offset: u64,
        max_bytes: u32,
        framing: &RequestFraming,
    ) -> Option<Vec<u8>> {
        let (mut server, mut client) = socket_pair().await;
        let served = try_zero_copy_fetch(
            engine,
            &mut server,
            topic,
            0,
            offset,
            max_bytes,
            "User:ANONYMOUS",
            "127.0.0.1",
            &None,
            framing,
        )
        .await
        .unwrap();
        // Dropping the server half closes the connection, so the read below sees a clean
        // EOF instead of blocking on a socket nobody will write to again.
        drop(server);
        if !served {
            return None;
        }
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        Some(buf)
    }

    /// The same fetch as `zero_copy_bytes`, served the buffered way, encoded exactly as
    /// the connection loop encodes a `WireResponse`.
    async fn buffered_bytes(
        engine: &StorageEngine,
        topic: &str,
        offset: u64,
        max_bytes: u32,
        framing: &RequestFraming,
    ) -> Vec<u8> {
        let req = WireRequest {
            cmd: CommandCode::Fetch,
            payload: RequestPayload::Fetch {
                topic: topic.to_string(),
                partition: 0,
                offset,
                max_bytes,
            },
        };
        let mut principal = "User:ANONYMOUS".to_string();
        let mut logical_client_id = None;
        let mut scram_session = None;
        let mut mechanism = None;
        let mut is_plus = false;
        let response = process_request(
            engine,
            req,
            "127.0.0.1",
            &mut principal,
            "127.0.0.1",
            &mut logical_client_id,
            &mut scram_session,
            &mut mechanism,
            &mut is_plus,
            framing,
        )
        .await;
        response.encode_framed(framing)
    }

    #[tokio::test]
    async fn zero_copy_response_is_byte_identical_to_the_buffered_one() {
        let dir = TempDir::new("identical");
        let engine = open_engine(&dir);
        seed(
            &engine,
            "t",
            &[b"alpha", b"bravo", b"charlie", b"delta", b"echo"],
        );

        for framing in [
            RequestFraming::Legacy,
            RequestFraming::Versioned {
                api_version: 1,
                correlation_id: 4242,
                tags: RequestTags::default(),
            },
        ] {
            // Every start offset, including one past the end, and a budget that cuts the
            // fetch short — the paths have to agree on where entries begin and end, not
            // just on "all of them".
            for offset in 0..=5u64 {
                for max_bytes in [64u32, 128, 4096] {
                    let zc = zero_copy_bytes(&engine, "t", offset, max_bytes, &framing).await;
                    let buffered = buffered_bytes(&engine, "t", offset, max_bytes, &framing).await;
                    match zc {
                        Some(zc) => assert_eq!(
                            zc, buffered,
                            "zero-copy and buffered responses diverged at offset {} \
                             with max_bytes {} ({:?})",
                            offset, max_bytes, framing
                        ),
                        None => {
                            // Declining is always allowed, but only when there is nothing
                            // to serve — never as a way to hide a disagreement.
                            let entries_len = u32::from_be_bytes(
                                buffered[buffered.len() - 4..].try_into().unwrap(),
                            );
                            assert_eq!(
                                entries_len, 0,
                                "zero-copy declined at offset {} with max_bytes {} even \
                                 though the buffered path had entries to serve",
                                offset, max_bytes
                            );
                        }
                    }
                }
            }
        }
    }

    /// A batch larger than the client's fetch budget must still be served whole, or that
    /// offset is unreachable forever. Both paths owe the same answer here.
    #[tokio::test]
    async fn an_oversized_first_batch_is_served_whole_by_both_paths() {
        let dir = TempDir::new("oversized");
        let engine = open_engine(&dir);
        let big = vec![b'x'; 8 * 1024];
        seed(&engine, "t", &[&big, b"after"]);

        let framing = RequestFraming::Legacy;
        let zc = zero_copy_bytes(&engine, "t", 0, 16, &framing)
            .await
            .expect("an oversized first batch is still servable");
        let buffered = buffered_bytes(&engine, "t", 0, 16, &framing).await;
        assert_eq!(zc, buffered);
        // ...and it really is the whole batch, not a 16-byte prefix of it.
        assert!(
            zc.len() > big.len(),
            "expected the whole oversized batch, got {} bytes",
            zc.len()
        );
    }

    /// Uncommitted entries are invisible on both paths: the zero-copy path reads the same
    /// high watermark, it does not get to skip the bound because it never decodes.
    #[tokio::test]
    async fn neither_path_serves_past_the_high_watermark() {
        let dir = TempDir::new("hw");
        let engine = open_engine(&dir);
        let pm = engine.get_or_create_partition("t", 0).unwrap();
        // `produce_frame_eos` appends without advancing the watermark (`produce_frame`
        // would advance it itself), which is what lets this test hold offsets 1 and 2
        // uncommitted — the state a leader is in while its ISR has not acknowledged yet.
        for payload in [b"one".as_slice(), b"two", b"three"] {
            pm.produce_frame_eos(bytes::Bytes::copy_from_slice(payload), 0, 0, 0)
                .unwrap()
                .unwrap();
        }
        pm.advance_committed_hw(1); // only offset 0 is committed
        assert_eq!(pm.high_watermark(), 1);
        assert_eq!(pm.latest_offset(), 3);

        let framing = RequestFraming::Legacy;
        let zc = zero_copy_bytes(&engine, "t", 0, 4096, &framing)
            .await
            .expect("the committed entry is servable");
        let buffered = buffered_bytes(&engine, "t", 0, 4096, &framing).await;
        assert_eq!(zc, buffered);

        // Offset 1 is produced but not committed — nothing to serve, both ways.
        assert!(zero_copy_bytes(&engine, "t", 1, 4096, &framing)
            .await
            .is_none());
        let buffered_uncommitted = buffered_bytes(&engine, "t", 1, 4096, &framing).await;
        let entries_len = u32::from_be_bytes(
            buffered_uncommitted[buffered_uncommitted.len() - 4..]
                .try_into()
                .unwrap(),
        );
        assert_eq!(entries_len, 0);
    }

    /// The old zero-copy path had to refuse any partition carrying transactional data: it
    /// streamed bytes with nothing inspecting them, so aborted records and control markers
    /// went out unfiltered while the buffered path filtered them — the same request
    /// answered differently depending on which path took it.
    ///
    /// That is no longer a conflict, because the *broker* no longer filters on either
    /// path. It reports `last_stable_offset` and the aborted ranges in the response header
    /// and the consumer drops what it must, which is the only thing that works once
    /// batches are compressed. So the fast path now serves transactional partitions too —
    /// and owes the identical answer, header included.
    #[tokio::test]
    async fn a_transactional_partition_is_served_identically_by_both_paths() {
        let dir = TempDir::new("txn");
        let engine = open_engine(&dir);
        let topic = "txn_t";

        let (tx_id, pid) = ("tx-zero-copy", 4242u64);
        engine.begin_transaction(tx_id, pid).unwrap();
        engine
            .add_partitions_to_txn(tx_id, pid, 0, &[(topic.to_string(), vec![0])])
            .unwrap();
        let batch = crate::protocol::RecordBatch::create(
            0,
            1,
            0,
            pid,
            0,
            0,
            true,
            crate::protocol::BatchCompression::None,
            &[
                (1, None, Some(bytes::Bytes::from_static(b"doomed-1"))),
                (1, None, Some(bytes::Bytes::from_static(b"doomed-2"))),
            ],
        );
        engine
            .produce_batch(crate::server::engine::ProduceBatchParams {
                topic,
                key: "",
                transaction_id: Some(tx_id),
                num_partitions: 1,
                batch,
            })
            .await
            .unwrap();
        engine.abort_transaction(tx_id).unwrap();

        let pm = engine.get_or_create_partition(topic, 0).unwrap();
        pm.produce_frame(b"after-the-abort").unwrap();
        pm.advance_committed_hw(pm.latest_offset());
        assert!(
            !pm.aborted_ranges().is_empty(),
            "the fixture must actually have an aborted range for this to test anything"
        );

        // Read-committed is what makes the header non-trivial: a real last stable offset
        // and a real aborted range, both of which the fast path has to reproduce exactly.
        let read_committed = RequestFraming::Versioned {
            api_version: 1,
            correlation_id: 11,
            tags: RequestTags {
                isolation_level: Some(crate::protocol::wire::IsolationLevel::ReadCommitted),
                ..RequestTags::default()
            },
        };
        for framing in [read_committed, RequestFraming::Legacy] {
            let zc = zero_copy_bytes(&engine, topic, 0, 4096, &framing)
                .await
                .expect("a transactional partition is servable from the fast path");
            let buffered = buffered_bytes(&engine, topic, 0, 4096, &framing).await;
            assert_eq!(
                zc, buffered,
                "zero-copy and buffered responses diverged on a transactional partition ({:?})",
                framing
            );
        }
    }

    /// A fetch that wants to wait must reach the buffered path, which is the only one that
    /// can park on the high watermark. Serving it immediately here would silently turn
    /// `fetch.min.bytes` into a no-op for every plain-TCP consumer.
    #[tokio::test]
    async fn a_waiting_fetch_below_min_bytes_falls_through_to_the_buffered_path() {
        let dir = TempDir::new("min_bytes");
        let engine = open_engine(&dir);
        seed(&engine, "t", &[b"small"]);

        let waiting = RequestFraming::Versioned {
            api_version: 1,
            correlation_id: 7,
            tags: RequestTags {
                max_wait_ms: Some(50),
                min_bytes: Some(1_000_000),
                ..RequestTags::default()
            },
        };
        assert!(
            zero_copy_bytes(&engine, "t", 0, 4096, &waiting)
                .await
                .is_none(),
            "a fetch asking to wait for more data must not be answered from the fast path"
        );

        // The same request without the wait tags is served zero-copy.
        assert!(
            zero_copy_bytes(&engine, "t", 0, 4096, &RequestFraming::Legacy)
                .await
                .is_some()
        );
    }
}
