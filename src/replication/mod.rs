pub mod consensus;
pub mod grpc;
pub mod metadata;

pub use consensus::{ConsensusState, HermesConsensus};
pub use grpc::{
    send_grpc_replication_fetch, ReplicationFetchRequest, ReplicationFetchResponse,
    GRPC_REPLICATION_MAGIC,
};
pub use metadata::MetadataRecord;

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
    /// Wall-clock time of the most recent successful replication ACK per
    /// (topic, partition, peer_addr) — the ISR-membership signal: a replica that hasn't
    /// acked recently is lagging and should be dropped from the ISR (Kafka
    /// `replica.lag.time.max.ms`), independent of whether it happens to be caught up on
    /// the specific offset that triggered the check.
    replica_ack_time: Arc<DashMap<(String, u32, String), std::time::Instant>>,
    /// Wall-clock time this node (as cluster leader) last received a heartbeat ACK from
    /// each peer broker — used to detect a dead broker so any partition it leads can be
    /// failed over to a surviving in-sync replica.
    broker_last_seen: Arc<DashMap<u32, std::time::Instant>>,
    consensus: HermesConsensus,
    /// Dynamically tracked leader address (set by followers from heartbeat)
    leader_addr: Arc<RwLock<Option<String>>>,
    /// Last time a heartbeat was received (used for election timeout)
    last_heartbeat: Arc<RwLock<std::time::Instant>>,
    /// This node's last-known `__cluster_metadata` log index (LEO), kept in sync by
    /// `StorageEngine::apply_metadata_record`/`propose_metadata` on every local append
    /// or applied replicated record. Used as the "last log index" this node advertises
    /// in its own VoteRequest when it becomes a candidate (Raft §5.4.1 log-completeness
    /// check) — kept here rather than looked up live because the election-timeout loop
    /// runs inside `ReplicationManager`, which (to avoid a construction cycle with
    /// `StorageEngine`) has no direct handle to the partition log itself.
    local_metadata_log_index: Arc<std::sync::atomic::AtomicU64>,
    /// Active per-partition follower fetcher tasks: (topic, partition) -> JoinHandle
    fetchers: Arc<DashMap<(String, u32), tokio::task::JoinHandle<()>>>,
    /// Shared broker address registry (node_id -> bind_addr), also used by StorageEngine
    /// for partition-leader routing.  Populated locally on startup, from replicated
    /// BrokerRegister metadata records, and — importantly — from the follower identity
    /// each peer returns in its heartbeat ACK, so a Raft Leader learns every follower's
    /// bind address even though only the Leader can write to `__cluster_metadata`.
    broker_addrs: Arc<DashMap<u32, String>>,
    /// Persistent peer TCP connection pool (peer_addr -> Arc<Mutex<Option<TcpStream>>>)
    /// Prevents ephemeral OS port exhaustion under high-throughput replication & heartbeats.
    peer_connections: Arc<DashMap<String, Arc<tokio::sync::Mutex<Option<TcpStream>>>>>,
}

impl ReplicationManager {
    pub fn new(
        config: ClusterConfig,
        bind_addr: String,
        broker_addrs: Arc<DashMap<u32, String>>,
    ) -> Self {
        let cluster_size = config.peer_addrs.len() + 1;
        // Bootstrap consensus state from configured role so config-declared
        // leaders start immediately accepting writes without running an election.
        let initial_consensus_state = if config.role == NodeRole::Leader {
            ConsensusState::Leader
        } else {
            ConsensusState::Follower
        };
        let consensus =
            HermesConsensus::new_with_state(config.node_id, cluster_size, initial_consensus_state);

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
            replica_ack_time: Arc::new(DashMap::new()),
            broker_last_seen: Arc::new(DashMap::new()),
            consensus,
            leader_addr,
            last_heartbeat: Arc::new(RwLock::new(std::time::Instant::now())),
            local_metadata_log_index: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            fetchers: Arc::new(DashMap::new()),
            broker_addrs,
            peer_connections: Arc::new(DashMap::new()),
        };

        // Always run the election-timeout watchdog. `check_election_timeout` is a no-op
        // while this node's consensus state is Leader, so this is safe to start
        // unconditionally — and it is what lets a node that steps down from a
        // statically-configured Leader role (e.g. on a STALE_EPOCH nack) ever become a
        // candidate again and re-acquire leadership. Previously this loop was only
        // started for nodes that booted as Follower, so a stepped-down static Leader
        // could never recontest an election.
        mgr.start_election_timeout_loop();
        // Leader: additionally start the heartbeat broadcaster to all followers.
        if mgr.config.role == NodeRole::Leader && !mgr.config.peer_addrs.is_empty() {
            mgr.start_leader_heartbeat_loop();
        }

        mgr
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn get_or_connect_peer(
        &self,
        peer_addr: &str,
    ) -> Arc<tokio::sync::Mutex<Option<TcpStream>>> {
        self.peer_connections
            .entry(peer_addr.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .value()
            .clone()
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
        self.epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
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

    /// Returns this node's last-known `__cluster_metadata` log index (LEO).
    pub fn local_metadata_log_index(&self) -> u64 {
        self.local_metadata_log_index
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Records this node's `__cluster_metadata` log index. Monotonic — a stale/out-of-order
    /// caller can never regress it.
    pub fn set_local_metadata_log_index(&self, idx: u64) {
        self.local_metadata_log_index
            .fetch_max(idx, std::sync::atomic::Ordering::AcqRel);
    }

    pub fn update_replica_watermark(
        &self,
        topic: &str,
        partition: u32,
        peer_addr: &str,
        offset: u64,
    ) {
        self.replica_watermarks.insert(
            (topic.to_string(), partition, peer_addr.to_string()),
            offset,
        );
        self.replica_ack_time.insert(
            (topic.to_string(), partition, peer_addr.to_string()),
            std::time::Instant::now(),
        );
    }

    /// Returns how long ago `peer_addr` last acknowledged a replicated write for this
    /// partition, if ever. `None` means no ack has ever been observed (e.g. a replica
    /// that just joined, or one this leader has never successfully pushed to).
    pub fn replica_ack_age(
        &self,
        topic: &str,
        partition: u32,
        peer_addr: &str,
    ) -> Option<std::time::Duration> {
        self.replica_ack_time
            .get(&(topic.to_string(), partition, peer_addr.to_string()))
            .map(|t| t.elapsed())
    }

    /// Returns how long ago this node (as cluster leader) last heard from broker `node_id`
    /// via a heartbeat ACK. `None` means it's never been observed alive at all.
    pub fn broker_last_seen_age(&self, node_id: u32) -> Option<std::time::Duration> {
        self.broker_last_seen.get(&node_id).map(|t| t.elapsed())
    }

    /// Records that `node_id` was just observed alive (via heartbeat or replication ACK).
    pub fn note_broker_alive(&self, node_id: u32) {
        self.broker_last_seen
            .insert(node_id, std::time::Instant::now());
    }

    /// Leader heartbeat broadcaster: sends periodic heartbeats to all follower peers,
    /// announcing this node as the cluster leader and providing its bind address.
    fn start_leader_heartbeat_loop(&self) {
        let peer_addrs = self.config.peer_addrs.clone();
        let cluster_id = self.config.cluster_id.clone();
        let node_id = self.config.node_id;
        let bind_addr = self.bind_addr.clone();
        let epoch = self.epoch.clone(); // share epoch for heartbeat term
        let broker_addrs = self.broker_addrs.clone();
        let consensus = self.consensus.clone();
        let manager = self.clone();

        tokio::spawn(async move {
            tracing::info!(
                "HA Cluster [{}]: Leader Node {} starting heartbeat broadcaster to {} peer(s)",
                cluster_id,
                node_id,
                peer_addrs.len()
            );

            loop {
                // Bonus fix: this loop is also started unconditionally at boot for a
                // statically-configured Leader. Without this guard, a node that later
                // steps down (e.g. on a STALE_EPOCH nack) would keep broadcasting leader
                // heartbeats forever, causing two nodes to simultaneously claim
                // leadership — a direct Raft safety violation. The election-winner path
                // below already guards its own spawned loop the same way.
                if consensus.state() != ConsensusState::Leader {
                    tracing::info!(
                        "HA Cluster [{}]: Node {} is no longer Leader — stopping heartbeat broadcaster",
                        cluster_id,
                        node_id
                    );
                    break;
                }
                let current_term = epoch.load(std::sync::atomic::Ordering::Acquire);
                for peer in &peer_addrs {
                    let peer_conn = manager.get_or_connect_peer(peer);
                    match send_leader_heartbeat_pooled(
                        &peer_conn,
                        peer,
                        &cluster_id,
                        node_id,
                        current_term,
                        &bind_addr,
                    )
                    .await
                    {
                        Ok((follower_id, follower_addr)) => {
                            // Learn the follower's own identity from its heartbeat ACK so this
                            // leader's broker_addrs registry stays complete even without any
                            // manual registration step (fixes real-cluster broker discovery).
                            broker_addrs.insert(follower_id, follower_addr);
                            manager.note_broker_alive(follower_id);
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

    /// Starts background per-partition follower fetch loops (Phase 3)
    pub fn start_per_partition_fetcher_manager(&self, engine: crate::server::StorageEngine) {
        let fetchers = self.fetchers.clone();
        let node_id = self.config.node_id;

        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(100)).await;

                let topics = engine.list_topics();
                let mut active_partition_keys = std::collections::HashSet::new();

                for topic in &topics {
                    if let Some(partitions) = engine.describe_topic(topic) {
                        for part in partitions {
                            let p_id = part.partition_id;
                            let leader_id = part.leader_id;

                            if leader_id != node_id && part.replicas.contains(&node_id) {
                                active_partition_keys.insert((topic.clone(), p_id));

                                if !fetchers.contains_key(&(topic.clone(), p_id)) {
                                    let engine_c = engine.clone();
                                    let topic_c = topic.clone();
                                    let handle = tokio::spawn(async move {
                                        loop {
                                            let last_offset = match engine_c
                                                .get_or_create_partition(&topic_c, p_id)
                                            {
                                                Ok(pm) => pm.latest_offset(),
                                                Err(_) => 0,
                                            };

                                            let req = ReplicationFetchRequest {
                                                follower_node_id: node_id,
                                                topic: topic_c.clone(),
                                                partition: p_id,
                                                fetch_offset: last_offset,
                                                max_bytes: 64 * 1024,
                                            };

                                            if let Some(leader_addr) =
                                                engine_c.get_broker_address(leader_id)
                                            {
                                                if let Ok(resp) =
                                                    send_grpc_replication_fetch(&leader_addr, &req)
                                                        .await
                                                {
                                                    if let Ok(pm) = engine_c
                                                        .get_or_create_partition(&topic_c, p_id)
                                                    {
                                                        for frame in resp.frames {
                                                            let _ = pm.produce_frame_eos(
                                                                &frame.payload,
                                                                0,
                                                                0,
                                                                0,
                                                            );
                                                        }
                                                    }
                                                }
                                            }

                                            sleep(Duration::from_millis(50)).await;
                                        }
                                    });

                                    fetchers.insert((topic.clone(), p_id), handle);
                                }
                            }
                        }
                    }
                }

                // Cleanup fetchers for partitions where this node is no longer a follower
                let to_remove: Vec<_> = fetchers
                    .iter()
                    .filter(|entry| !active_partition_keys.contains(entry.key()))
                    .map(|entry| entry.key().clone())
                    .collect();

                for key in to_remove {
                    if let Some((_, handle)) = fetchers.remove(&key) {
                        handle.abort();
                    }
                }
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
        let broker_addrs = self.broker_addrs.clone();
        let local_metadata_log_index = self.local_metadata_log_index.clone();
        let broker_last_seen = self.broker_last_seen.clone();

        tokio::spawn(async move {
            let mut tick: u64 = 0;
            loop {
                sleep(Duration::from_secs(1)).await;
                tick = tick.wrapping_add(1);

                // Derive jitter: mix node_id with tick using a simple multiplicative hash.
                // This ensures different nodes time out at different moments.
                let jitter = (node_id as u64).wrapping_mul(2654435761).wrapping_add(tick)
                    % ELECTION_TIMEOUT_JITTER_SECS;
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

                // Broadcast VoteRequest to each peer and collect granted votes. Includes
                // our own last-applied metadata-log index so voters can enforce Raft's
                // log-completeness rule (§5.4.1): a candidate whose metadata log is behind
                // a voter's must not win the election, or committed metadata could be lost.
                let candidate_last_log_index =
                    local_metadata_log_index.load(std::sync::atomic::Ordering::Acquire);
                let mut votes_granted = 1usize; // vote for self
                for peer in &peer_addrs {
                    match send_vote_request(
                        peer,
                        &cluster_id,
                        node_id,
                        new_term,
                        candidate_last_log_index,
                    )
                    .await
                    {
                        Ok(true) => {
                            votes_granted += 1;
                            tracing::info!(
                                "Hermes Election: Vote GRANTED by {} (term {})",
                                peer,
                                new_term
                            );
                        }
                        Ok(false) => {
                            tracing::info!(
                                "Hermes Election: Vote DENIED by {} (term {})",
                                peer,
                                new_term
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Hermes Election: No response from {} (term {}): {}",
                                peer,
                                new_term,
                                e
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
                        node_id,
                        new_term,
                        votes_granted,
                        peer_addrs.len() + 1
                    );
                    // Reset heartbeat timestamp so we don't re-trigger election.
                    *last_heartbeat.write().unwrap() = std::time::Instant::now();

                    // REP-01 / H10: Newly elected leader starts heartbeat loop to peers.
                    // Each election win previously spawned a permanent loop with no
                    // cancellation token, so repeated Follower→Leader transitions
                    // accumulated unbounded tasks.  Now the loop checks the consensus
                    // state on every iteration and exits as soon as this node is no
                    // longer the Leader (step_down_to_follower sets state=Follower),
                    // so at most one active heartbeat loop exists per node at any time.
                    if !peer_addrs.is_empty() {
                        let peer_addrs_c = peer_addrs.clone();
                        let cluster_id_c = cluster_id.clone();
                        let bind_addr_c = bind_addr.clone();
                        let epoch_c = epoch.clone();
                        let consensus_c = consensus.clone();
                        let broker_addrs_c = broker_addrs.clone();
                        let broker_last_seen_c = broker_last_seen.clone();
                        tokio::spawn(async move {
                            loop {
                                if consensus_c.state() != ConsensusState::Leader {
                                    tracing::info!(
                                        "HA Heartbeat: Node {} is no longer Leader — stopping heartbeat loop",
                                        node_id
                                    );
                                    break;
                                }
                                let current_term =
                                    epoch_c.load(std::sync::atomic::Ordering::Acquire);
                                for peer in &peer_addrs_c {
                                    if let Ok((follower_id, follower_addr)) = send_leader_heartbeat(
                                        peer,
                                        &cluster_id_c,
                                        node_id,
                                        current_term,
                                        &bind_addr_c,
                                    )
                                    .await
                                    {
                                        broker_addrs_c.insert(follower_id, follower_addr);
                                        broker_last_seen_c
                                            .insert(follower_id, std::time::Instant::now());
                                    }
                                }
                                sleep(HEARTBEAT_INTERVAL).await;
                            }
                        });
                    }
                } else {
                    tracing::warn!(
                        "Hermes Election: Node {} failed to reach quorum ({} votes) for term {}.",
                        node_id,
                        votes_granted,
                        new_term
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
                if let Some(w) =
                    self.replica_watermarks
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
    /// CRIT-01: cluster_id is now included in the wire packet so followers can authenticate the sender.
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
        let cluster_id = self.config.cluster_id.clone();

        // Replicate to each peer concurrently over persistent pooled TCP streams
        let mut handles = Vec::with_capacity(self.config.peer_addrs.len());
        for peer in &self.config.peer_addrs {
            let peer_addr = peer.clone();
            let topic_name = topic.to_string();
            let frames_vec = frames.to_vec();
            let cid = cluster_id.clone();
            let peer_conn = self.get_or_connect_peer(&peer_addr);
            handles.push(tokio::spawn(async move {
                // CRIT-01: pass cluster_id so the follower can authenticate this replication push.
                let result = send_replication_push_pooled(
                    &peer_conn,
                    &peer_addr,
                    &cid,
                    &topic_name,
                    partition,
                    epoch,
                    &frames_vec,
                )
                .await;
                (peer_addr, result)
            }));
        }

        for handle in handles {
            match handle.await {
                Ok((peer_addr, Ok(()))) => {
                    self.replica_watermarks.insert(
                        (topic.to_string(), partition, peer_addr.clone()),
                        last_offset,
                    );
                    self.replica_ack_time.insert(
                        (topic.to_string(), partition, peer_addr),
                        std::time::Instant::now(),
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
/// Wire format (request):  `[0xAC] [cluster_id: pascal] [node_id: 4b] [term: 8b] [bind_addr: pascal]`
/// Wire format (response): `[0x00] [follower_node_id: 4b] [follower_bind_addr: pascal]` on success,
///                          `[0x01]` on rejection (cluster mismatch / not whitelisted).
///
/// Returning the follower's own node_id + bind_addr lets the Leader populate its broker
/// address registry purely from the existing heartbeat round-trip, without requiring any
/// out-of-band broker registration step — followers are otherwise unable to publish their
/// own address since only the partition leader for `__cluster_metadata` may write to it.
pub async fn send_leader_heartbeat_pooled(
    peer_conn: &Arc<tokio::sync::Mutex<Option<TcpStream>>>,
    peer_addr: &str,
    cluster_id: &str,
    node_id: u32,
    term: u64,
    leader_bind_addr: &str,
) -> IoResult<(u32, String)> {
    let mut buf = Vec::with_capacity(128);
    buf.put_u8(0xAC);
    crate::protocol::wire::write_pascal_string(&mut buf, cluster_id);
    buf.put_u32(node_id);
    buf.put_u64(term);
    crate::protocol::wire::write_pascal_string(&mut buf, leader_bind_addr);

    let mut lock = peer_conn.lock().await;

    if let Some(ref mut stream) = *lock {
        if stream.write_all(&buf).await.is_ok() {
            let mut status = [0u8; 1];
            if stream.read_exact(&mut status).await.is_ok() && status[0] == 0 {
                let mut id_buf = [0u8; 4];
                if stream.read_exact(&mut id_buf).await.is_ok() {
                    let follower_node_id = u32::from_be_bytes(id_buf);
                    let mut len_buf = [0u8; 2];
                    if stream.read_exact(&mut len_buf).await.is_ok() {
                        let addr_len = u16::from_be_bytes(len_buf) as usize;
                        let mut addr_buf = vec![0u8; addr_len];
                        if stream.read_exact(&mut addr_buf).await.is_ok() {
                            let follower_bind_addr = String::from_utf8_lossy(&addr_buf).to_string();
                            return Ok((follower_node_id, follower_bind_addr));
                        }
                    }
                }
            }
        }
    }

    *lock = None;
    let mut stream = match timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Heartbeat connection to {} timed out", peer_addr),
            ));
        }
    };

    stream.write_all(&buf).await?;
    let mut status = [0u8; 1];
    stream.read_exact(&mut status).await?;
    if status[0] != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("Peer {} rejected heartbeat: cluster.id mismatch", peer_addr),
        ));
    }

    let mut id_buf = [0u8; 4];
    stream.read_exact(&mut id_buf).await?;
    let follower_node_id = u32::from_be_bytes(id_buf);

    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf).await?;
    let addr_len = u16::from_be_bytes(len_buf) as usize;
    let mut addr_buf = vec![0u8; addr_len];
    stream.read_exact(&mut addr_buf).await?;
    let follower_bind_addr = String::from_utf8_lossy(&addr_buf).to_string();

    *lock = Some(stream);
    Ok((follower_node_id, follower_bind_addr))
}

pub async fn send_leader_heartbeat(
    peer_addr: &str,
    cluster_id: &str,
    node_id: u32,
    term: u64,
    leader_bind_addr: &str,
) -> IoResult<(u32, String)> {
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
    buf.put_u64(term); // P4: include current term
    crate::protocol::wire::write_pascal_string(&mut buf, leader_bind_addr);

    stream.write_all(&buf).await?;

    let mut status = [0u8; 1];
    stream.read_exact(&mut status).await?;
    if status[0] != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("Peer {} rejected heartbeat: cluster.id mismatch", peer_addr),
        ));
    }

    let mut id_buf = [0u8; 4];
    stream.read_exact(&mut id_buf).await?;
    let follower_node_id = u32::from_be_bytes(id_buf);

    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf).await?;
    let addr_len = u16::from_be_bytes(len_buf) as usize;
    let mut addr_buf = vec![0u8; addr_len];
    stream.read_exact(&mut addr_buf).await?;
    let follower_bind_addr = String::from_utf8_lossy(&addr_buf).to_string();

    Ok((follower_node_id, follower_bind_addr))
}

/// Streams replication batch frames to a peer follower node over TCP (0xAA protocol).
///
/// Wire format: `[0xAA] [ClusterId: pascal] [Topic: pascal] [Partition: 4b] [Epoch: 8b] [Count: 4b] [RecordFrame...]`
///
/// CRIT-01: cluster_id is prepended immediately after the magic byte so followers can authenticate
/// the sender before touching any partition state.
pub async fn send_replication_push_pooled(
    peer_conn: &Arc<tokio::sync::Mutex<Option<TcpStream>>>,
    peer_addr: &str,
    cluster_id: &str,
    topic: &str,
    partition: u32,
    epoch: u64,
    frames: &[RecordFrame],
) -> IoResult<()> {
    let mut buf = Vec::with_capacity(256 + frames.len() * 64);
    buf.put_u8(0xAA);
    crate::protocol::wire::write_pascal_string(&mut buf, cluster_id);
    crate::protocol::wire::write_pascal_string(&mut buf, topic);
    buf.put_u32(partition);
    buf.put_u64(epoch);
    buf.put_u32(frames.len() as u32);
    for frame in frames {
        frame.encode_into(&mut buf);
    }

    let mut lock = peer_conn.lock().await;

    if let Some(ref mut stream) = *lock {
        if stream.write_all(&buf).await.is_ok() {
            let mut ack = [0u8; 1];
            if stream.read_exact(&mut ack).await.is_ok() {
                if ack[0] == 0 {
                    return Ok(());
                } else if ack[0] == 0x02 {
                    return Err(std::io::Error::other("STALE_EPOCH: peer epoch is higher"));
                }
            }
        }
    }

    *lock = None;
    let mut stream = match timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Replication push connection to {} timed out", peer_addr),
            ));
        }
    };

    stream.write_all(&buf).await?;
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).await?;
    if ack[0] == 0 {
        *lock = Some(stream);
        Ok(())
    } else if ack[0] == 0x02 {
        *lock = Some(stream);
        Err(std::io::Error::other("STALE_EPOCH: peer epoch is higher"))
    } else {
        Err(std::io::Error::other(format!(
            "Peer {} returned error ACK 0x{:02X}",
            peer_addr, ack[0]
        )))
    }
}

pub async fn send_replication_push(
    peer_addr: &str,
    cluster_id: &str,
    topic: &str,
    partition: u32,
    epoch: u64,
    frames: &[RecordFrame],
) -> IoResult<()> {
    let mut stream = match timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::warn!(
                "HA Replication: Could not connect to peer {}: {}",
                peer_addr,
                e
            );
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
    // CRIT-01: cluster_id is the first field so the follower can reject unknown senders immediately.
    crate::protocol::wire::write_pascal_string(&mut buf, cluster_id);
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
    } else if ack[0] == 0x02 {
        // H5: Follower returned STALE_EPOCH sentinel — our epoch is behind the cluster.
        tracing::warn!(
            "HA Replication: Peer {} rejected with STALE_EPOCH (0x02)",
            peer_addr
        );
        Err(std::io::Error::other("STALE_EPOCH: peer epoch is higher"))
    } else {
        tracing::warn!(
            "HA Replication: Peer {} returned error ACK 0x{:02X}",
            peer_addr,
            ack[0]
        );
        Err(std::io::Error::other("Replication ACK failed"))
    }
}

/// Sends a Raft VoteRequest RPC (0xAE) to a peer and returns whether the vote was granted.
///
/// Wire format (request):  `[0xAE] [cluster_id: pascal] [candidate_id: 4b] [term: 8b] [candidate_last_log_index: 8b]`
/// Wire format (response): `[granted: 1b]`  — 0x01 = granted, 0x00 = denied
///
/// `candidate_last_log_index` lets voters enforce Raft §5.4.1: they must not grant a vote
/// to a candidate whose `__cluster_metadata` log is behind their own.
pub async fn send_vote_request(
    peer_addr: &str,
    cluster_id: &str,
    candidate_id: u32,
    term: u64,
    candidate_last_log_index: u64,
) -> IoResult<bool> {
    let mut stream = match timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::warn!(
                "Hermes Election: Cannot connect to {} for VoteRequest: {}",
                peer_addr,
                e
            );
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
    buf.put_u64(candidate_last_log_index);

    stream.write_all(&buf).await?;

    let mut resp = [0u8; 1];
    stream.read_exact(&mut resp).await?;

    Ok(resp[0] == 0x01)
}
