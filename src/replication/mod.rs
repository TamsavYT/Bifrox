pub mod consensus;
pub mod grpc;
pub mod metadata;

pub use consensus::{ConsensusState, HermesConsensus};
pub use metadata::MetadataRecord;
pub use grpc::{
    send_grpc_replication_fetch, ReplicationFetchRequest, ReplicationFetchResponse,
    GRPC_REPLICATION_MAGIC,
};

use crate::protocol::RecordFrame;
use bytes::BufMut;
use dashmap::DashMap;
use std::io::Result as IoResult;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Duration};

/// Connection timeout for inter-node TCP replication and heartbeat connections
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Heartbeat interval for leader node to ping followers
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Election timeout base for followers (jitter is added per-node using node_id)
const ELECTION_TIMEOUT_BASE_SECS: u64 = 15;
const ELECTION_TIMEOUT_JITTER_SECS: u64 = 15;

/// Magic byte for vote-request RPC (Candidate -> Peers)
pub const VOTE_REQUEST_MAGIC: u8 = 0xAE;
/// Magic byte for vote-response RPC (Peer -> Candidate)
pub const VOTE_RESPONSE_MAGIC: u8 = 0xAF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub node_id: u32,
    pub role: NodeRole,
    pub peer_addrs: Vec<String>,
    pub min_insync_replicas: usize,
}

#[derive(Debug, Clone)]
pub struct ReplicationManager {
    config: ClusterConfig,
    /// Current epoch (term) for consensus (RACE-03 atomic)
    epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Bind address of this node's TCP listener (used in heartbeat announcements)
    bind_addr: String,
    /// Tracking partition high watermarks for replicas: (topic, partition, peer_addr) -> watermark_offset
    replica_watermarks: Arc<DashMap<(String, u32, String), u64>>,
    consensus: HermesConsensus,
    /// Dynamically tracked leader address (set by followers from heartbeat)
    leader_addr: Arc<RwLock<Option<String>>>,
    /// Last time a heartbeat was received (used for election timeout)
    last_heartbeat: Arc<RwLock<std::time::Instant>>,
    /// REP-02: Tracks (term, candidate_id) for which this node voted in the current term
    voted_for: Arc<RwLock<Option<(u64, u32)>>>,
}

impl ReplicationManager {
    pub fn new(config: ClusterConfig, bind_addr: String) -> Self {
        let cluster_size = config.peer_addrs.len() + 1;
        // Bootstrap consensus state from configured role so config-declared
        // leaders start immediately accepting writes without running an election.
        let initial_consensus_state = if config.role == NodeRole::Leader {
            ConsensusState::Leader
        } else {
            ConsensusState::Follower
        };
        let consensus = HermesConsensus::new_with_state(config.node_id, cluster_size, initial_consensus_state);

        let leader_addr = if config.role == NodeRole::Leader {
            // Leader knows its own address
            Arc::new(RwLock::new(Some(bind_addr.clone())))
        } else {
            Arc::new(RwLock::new(None))
        };

        let mgr = Self {
            config,
            epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            bind_addr,
            replica_watermarks: Arc::new(DashMap::new()),
            consensus,
            leader_addr,
            last_heartbeat: Arc::new(RwLock::new(std::time::Instant::now())),
            voted_for: Arc::new(RwLock::new(None)),
        };

        // Leader: start heartbeat broadcaster to all followers
        // Start appropriate background loops based on initial role.
        if mgr.config.role == NodeRole::Leader && !mgr.config.peer_addrs.is_empty() {
            mgr.start_leader_heartbeat_loop();
        } else {
            // Followers start election timeout monitoring.
            mgr.start_election_timeout_loop();
        }

        mgr
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Return current role based on consensus state.
    pub fn role(&self) -> NodeRole {
        match self.consensus.state() {
            ConsensusState::Leader => NodeRole::Leader,
            _ => NodeRole::Follower,
        }
    }

    /// Returns true if this node is currently the cluster leader.
    pub fn is_leader(&self) -> bool {
        self.consensus.state() == ConsensusState::Leader
    }

    pub fn consensus(&self) -> &HermesConsensus {
        &self.consensus
    }

    /// Returns the current cluster leader's bind address (for produce forwarding).
    /// Returns the known leader bind address (if any).
    pub fn get_leader_addr(&self) -> Option<String> {
        self.leader_addr.read().unwrap().clone()
    }

    /// Returns the current epoch (term) for this node (RACE-03 atomic).
    pub fn get_epoch(&self) -> u64 {
        self.epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Sets the current epoch (term). Used when this node becomes leader (RACE-03 atomic).
    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, std::sync::atomic::Ordering::Release);
    }

    /// Called by followers when they receive a heartbeat from the leader.
    pub fn set_leader_addr(&self, addr: String) {
        let mut guard = self.leader_addr.write().unwrap();
        *guard = Some(addr);
        self.record_heartbeat();
    }

    /// Record receipt of a valid heartbeat (BUG-05)
    pub fn record_heartbeat(&self) {
        let mut guard = self.last_heartbeat.write().unwrap();
        *guard = std::time::Instant::now();
    }

    /// Checks if a vote can be granted to candidate_id for term (REP-02)
    pub fn can_vote_for(&self, candidate_id: u32, term: u64) -> bool {
        let guard = self.voted_for.read().unwrap();
        match *guard {
            Some((voted_term, voted_candidate)) => {
                if voted_term < term {
                    true
                } else if voted_term == term {
                    voted_candidate == candidate_id
                } else {
                    false
                }
            }
            None => true,
        }
    }

    /// Records a vote for candidate_id in term (REP-02)
    pub fn record_vote(&self, candidate_id: u32, term: u64) {
        let mut guard = self.voted_for.write().unwrap();
        *guard = Some((term, candidate_id));
    }

    pub fn update_replica_watermark(&self, topic: &str, partition: u32, peer_addr: &str, offset: u64) {
        self.replica_watermarks
            .insert((topic.to_string(), partition, peer_addr.to_string()), offset);
    }

    /// Leader heartbeat broadcaster: sends periodic heartbeats to all follower peers,
    /// announcing this node as the cluster leader and providing its bind address.
    fn start_leader_heartbeat_loop(&self) {
        let peer_addrs = self.config.peer_addrs.clone();
        let cluster_id = self.config.cluster_id.clone();
        let node_id = self.config.node_id;
        let bind_addr = self.bind_addr.clone();
        let epoch = self.epoch.clone(); // share epoch for heartbeat term

        tokio::spawn(async move {
            tracing::info!(
                "HA Cluster [{}]: Leader Node {} starting heartbeat broadcaster to {} peer(s)",
                cluster_id,
                node_id,
                peer_addrs.len()
            );

            loop {
                let current_term = epoch.load(std::sync::atomic::Ordering::Acquire);
                for peer in &peer_addrs {
                    match send_leader_heartbeat(peer, &cluster_id, node_id, current_term, &bind_addr).await {
                        Ok(()) => {
                            tracing::info!(
                                "HA Cluster [{}]: Heartbeat OK — peer {} acknowledged leader",
                                cluster_id,
                                peer
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "HA Cluster [{}]: Heartbeat FAILED — peer {} offline: {}",
                                cluster_id,
                                peer,
                                e
                            );
                        }
                    }
                }
                sleep(HEARTBEAT_INTERVAL).await;
            }
        });
    }

    /// Election timeout task: monitors heartbeat arrivals and triggers elections.
    ///
    /// Uses node_id-derived jitter to avoid split-vote storms (no `rand` crate needed).
    fn start_election_timeout_loop(&self) {
        let last_heartbeat = self.last_heartbeat.clone();
        let consensus = self.consensus.clone();
        let peer_addrs = self.config.peer_addrs.clone();
        let cluster_id = self.config.cluster_id.clone();
        let node_id = self.config.node_id;
        let epoch = self.epoch.clone();
        let leader_addr = self.leader_addr.clone();
        let bind_addr = self.bind_addr.clone();

        tokio::spawn(async move {
            let mut tick: u64 = 0;
            loop {
                sleep(Duration::from_secs(1)).await;
                tick = tick.wrapping_add(1);

                // Derive jitter: mix node_id with tick using a simple multiplicative hash.
                // This ensures different nodes time out at different moments.
                let jitter = (node_id as u64).wrapping_mul(2654435761).wrapping_add(tick) % ELECTION_TIMEOUT_JITTER_SECS;
                let timeout_duration = Duration::from_secs(ELECTION_TIMEOUT_BASE_SECS + jitter);

                let last = *last_heartbeat.read().unwrap();
                if last.elapsed() < timeout_duration {
                    continue; // Leader is alive — no action needed.
                }

                // Election timeout expired — become a candidate and start election.
                if !consensus.check_election_timeout() {
                    continue; // already Leader or check returned false
                }

                let new_term = consensus.current_term();
                // Bump epoch to match new term
                epoch.fetch_max(new_term, std::sync::atomic::Ordering::AcqRel);

                tracing::info!(
                    "Hermes Election: Node {} became Candidate for term {}. Broadcasting VoteRequest to {} peer(s).",
                    node_id, new_term, peer_addrs.len()
                );

                // Broadcast VoteRequest to each peer and collect granted votes.
                let mut votes_granted = 1usize; // vote for self
                for peer in &peer_addrs {
                    match send_vote_request(peer, &cluster_id, node_id, new_term).await {
                        Ok(true) => {
                            votes_granted += 1;
                            tracing::info!(
                                "Hermes Election: Vote GRANTED by {} (term {})",
                                peer, new_term
                            );
                        }
                        Ok(false) => {
                            tracing::info!(
                                "Hermes Election: Vote DENIED by {} (term {})",
                                peer, new_term
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Hermes Election: No response from {} (term {}): {}",
                                peer, new_term, e
                            );
                        }
                    }
                }

                // Check quorum
                if consensus.tally_election_votes(votes_granted) {
                    // Promoted to Leader!
                    epoch.store(new_term, std::sync::atomic::Ordering::Release);
                    {
                        let mut la = leader_addr.write().unwrap();
                        *la = Some(bind_addr.clone());
                    }
                    tracing::info!(
                        "Hermes Election: Node {} is now LEADER for term {} with {}/{} votes.",
                        node_id, new_term, votes_granted, peer_addrs.len() + 1
                    );
                    // Reset heartbeat timestamp so we don't re-trigger election.
                    *last_heartbeat.write().unwrap() = std::time::Instant::now();

                    // REP-01: Newly elected leader starts heartbeat loop to peers
                    if !peer_addrs.is_empty() {
                        let peer_addrs_c = peer_addrs.clone();
                        let cluster_id_c = cluster_id.clone();
                        let bind_addr_c = bind_addr.clone();
                        let epoch_c = epoch.clone();
                        tokio::spawn(async move {
                            loop {
                                let current_term = epoch_c.load(std::sync::atomic::Ordering::Acquire);
                                for peer in &peer_addrs_c {
                                    let _ = send_leader_heartbeat(peer, &cluster_id_c, node_id, current_term, &bind_addr_c).await;
                                }
                                sleep(HEARTBEAT_INTERVAL).await;
                            }
                        });
                    }
                } else {
                    tracing::warn!(
                        "Hermes Election: Node {} failed to reach quorum ({} votes) for term {}.",
                        node_id, votes_granted, new_term
                    );
                }
            }
        });
    }

    /// In-Sync Replicas (ISR) Quorum Gating.
    /// Blocks client acknowledgment until min_insync_replicas report watermark >= target_offset.
    pub async fn await_isr_quorum(
        &self,
        topic: &str,
        partition: u32,
        target_offset: u64,
        quorum_timeout: Duration,
    ) -> bool {
        let start = std::time::Instant::now();
        let needed_replicas = self.config.min_insync_replicas;

        if needed_replicas <= 1 || self.config.peer_addrs.is_empty() {
            return true;
        }

        while start.elapsed() < quorum_timeout {
            let mut acked = 1usize; // Count self (leader) as 1
            for peer in &self.config.peer_addrs {
                if let Some(w) = self
                    .replica_watermarks
                    .get(&(topic.to_string(), partition, peer.clone()))
                {
                    if *w.value() >= target_offset {
                        acked += 1;
                    }
                }
            }

            if acked >= needed_replicas {
                return true;
            }

            sleep(Duration::from_millis(5)).await;
        }

        false
    }

    /// Streams produced record batch to all follower peer nodes over TCP.
    /// Called from a tokio::spawn task in engine.rs — replicates concurrently to all peers.
    /// P3: If a peer returns a stale-epoch ACK (0x01), triggers leader step-down.
    pub async fn replicate_batch(
        &self,
        topic: &str,
        partition: u32,
        frames: &[RecordFrame],
    ) -> IoResult<()> {
        if self.config.peer_addrs.is_empty() || frames.is_empty() {
            return Ok(());
        }

        let last_offset = frames.last().unwrap().offset;
        let epoch = self.epoch.load(std::sync::atomic::Ordering::Acquire);

        // Replicate to each peer concurrently
        let mut handles = Vec::with_capacity(self.config.peer_addrs.len());
        for peer in &self.config.peer_addrs {
            let peer_addr = peer.clone();
            let topic_name = topic.to_string();
            let frames_vec = frames.to_vec();
            handles.push(tokio::spawn(async move {
                let result = send_replication_push(&peer_addr, &topic_name, partition, epoch, &frames_vec).await;
                (peer_addr, result)
            }));
        }

        for handle in handles {
            match handle.await {
                Ok((peer_addr, Ok(()))) => {
                    self.replica_watermarks.insert(
                        (topic.to_string(), partition, peer_addr),
                        last_offset,
                    );
                }
                Ok((peer_addr, Err(e))) => {
                    // Check if the error string indicates a stale-epoch rejection
                    let err_str = e.to_string();
                    if err_str.contains("STALE_EPOCH") {
                        // P3: Peer has higher epoch — step down to follower
                        let peer_epoch = self.consensus.current_term() + 1;
                        self.consensus.step_down_to_follower(peer_epoch);
                        // Clear leader_addr so produce forwarding re-discovers new leader
                        let mut la = self.leader_addr.write().unwrap();
                        *la = None;
                        tracing::warn!(
                            "P3 Fencing: Node stepping down to Follower — peer {} reported stale epoch.",
                            peer_addr
                        );
                    } else {
                        tracing::error!(
                            "HA Replication: Failed to replicate to peer {}: {}",
                            peer_addr,
                            e
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("HA Replication: Spawn join error: {}", e);
                }
            }
        }

        Ok(())
    }
}

/// Sends a leader heartbeat packet (0xAC) including the leader's bind address and current term.
///
/// Wire format: `[0xAC] [cluster_id: pascal] [node_id: 4b] [term: 8b] [bind_addr: pascal]`
pub async fn send_leader_heartbeat(
    peer_addr: &str,
    cluster_id: &str,
    node_id: u32,
    term: u64,
    leader_bind_addr: &str,
) -> IoResult<()> {
    let mut stream = match timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Heartbeat connection to {} timed out", peer_addr),
            ))
        }
    };

    let mut buf = Vec::with_capacity(128);
    buf.put_u8(0xAC); // Heartbeat Magic
    crate::protocol::wire::write_pascal_string(&mut buf, cluster_id);
    buf.put_u32(node_id);
    buf.put_u64(term);  // P4: include current term
    crate::protocol::wire::write_pascal_string(&mut buf, leader_bind_addr);

    stream.write_all(&buf).await?;

    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).await?;
    if ack[0] == 0 {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("Peer {} rejected heartbeat: cluster.id mismatch", peer_addr),
        ))
    }
}

/// Streams replication batch frames to a peer follower node over TCP (0xAA protocol).
///
/// Wire format: `[0xAA] [Topic: pascal] [Partition: 4b] [Epoch: 8b] [Count: 4b] [RecordFrame...]`
pub async fn send_replication_push(
    peer_addr: &str,
    topic: &str,
    partition: u32,
    epoch: u64,
    frames: &[RecordFrame],
) -> IoResult<()> {
    let mut stream = match timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::warn!("HA Replication: Could not connect to peer {}: {}", peer_addr, e);
            return Err(e);
        }
        Err(_) => {
            let err = std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Connection to {} timed out", peer_addr),
            );
            tracing::warn!("HA Replication: Connection to peer {} timed out", peer_addr);
            return Err(err);
        }
    };

    let first_offset = frames.first().map_or(0, |f| f.offset);
    let last_offset = frames.last().map_or(0, |f| f.offset);

    let mut buf = Vec::with_capacity(256);
    buf.put_u8(0xAA);
    crate::protocol::wire::write_pascal_string(&mut buf, topic);
    buf.put_u32(partition);
    buf.put_u64(epoch);
    buf.put_u32(frames.len() as u32);
    for frame in frames {
        frame.encode_into(&mut buf);
    }

    stream.write_all(&buf).await.map_err(|e| {
        tracing::error!("HA Replication: Write to peer {} failed: {}", peer_addr, e);
        e
    })?;

    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).await.map_err(|e| {
        tracing::error!("HA Replication: ACK from peer {} failed: {}", peer_addr, e);
        e
    })?;

    if ack[0] == 0 {
        tracing::info!(
            "HA Replication: Replicated {} record(s) [offsets {}..={}] Topic '{}' Partition {} -> peer {}",
            frames.len(), first_offset, last_offset, topic, partition, peer_addr
        );
        Ok(())
    } else {
        tracing::warn!("HA Replication: Peer {} returned error ACK 0x{:02X}", peer_addr, ack[0]);
        Err(std::io::Error::new(std::io::ErrorKind::Other, "Replication ACK failed"))
    }
}

/// Sends a Raft VoteRequest RPC (0xAE) to a peer and returns whether the vote was granted.
///
/// Wire format (request):  `[0xAE] [cluster_id: pascal] [candidate_id: 4b] [term: 8b]`
/// Wire format (response): `[granted: 1b]`  — 0x01 = granted, 0x00 = denied
pub async fn send_vote_request(
    peer_addr: &str,
    cluster_id: &str,
    candidate_id: u32,
    term: u64,
) -> IoResult<bool> {
    let mut stream = match timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::warn!("Hermes Election: Cannot connect to {} for VoteRequest: {}", peer_addr, e);
            return Err(e);
        }
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("VoteRequest connection to {} timed out", peer_addr),
            ));
        }
    };

    let mut buf = Vec::with_capacity(64);
    buf.put_u8(VOTE_REQUEST_MAGIC);
    crate::protocol::wire::write_pascal_string(&mut buf, cluster_id);
    buf.put_u32(candidate_id);
    buf.put_u64(term);

    stream.write_all(&buf).await?;

    let mut resp = [0u8; 1];
    stream.read_exact(&mut resp).await?;

    Ok(resp[0] == 0x01)
}
