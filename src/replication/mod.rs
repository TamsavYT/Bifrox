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
use parking_lot::RwLock;
use std::io::Result as IoResult;
use std::sync::Arc;
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

/// Draws a fresh election-timeout jitter, in seconds, for one election timer period.
///
/// Deliberately not a pure function of `node_id`: deriving the offset from the node id
/// alone gives every node a fixed, permanently distinct-but-predictable slot, so two nodes
/// whose ids happen to collide modulo the jitter range would tie on *every* election
/// forever rather than just once. Seeding from the clock (mixed with `node_id` and `tick`,
/// so nodes drawing within the same nanosecond still diverge) re-randomizes each period,
/// which is what actually breaks repeated split votes.
///
/// Uses a SplitMix64 finalizer over the seed rather than pulling in the `rand` crate —
/// this needs uniform spread across a ~15-value range, not cryptographic quality.
fn next_election_jitter(node_id: u32, tick: u64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
        .unwrap_or(tick);
    let mut z = nanos
        .wrapping_add((node_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(tick.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    z % ELECTION_TIMEOUT_JITTER_SECS
}

/// Magic byte for vote-request RPC (Candidate -> Peers)
pub const VOTE_REQUEST_MAGIC: u8 = 0xAE;
/// Magic byte for vote-response RPC (Peer -> Candidate)
pub const VOTE_RESPONSE_MAGIC: u8 = 0xAF;

/// This build's inter-node protocol version, advertised to every peer via the
/// heartbeat's compatible trailing section (see `note_peer_version`) the same way
/// process roles already are.
///
/// STATUS: this mechanism (issue #48) is implemented but PARKED — not adopted as part of
/// a shipped upgrade path. Hermes/Kafka functional parity is the current priority;
/// enabling this for real is a separate decision with its own rollout communication. The
/// notes below are for whoever resumes that work.
///
/// IMPORTANT — this is a one-time flag day, not a rolling-upgrade boundary: adopting
/// version `1` (this mechanism's introduction, issue #48) requires every broker in the
/// cluster to be upgraded together. A pre-`1` peer's heartbeat handling has no concept of
/// a trailing version byte at all — appended to the *request*, it desyncs that peer's own
/// framing (see `handler::decode_heartbeat_packet`'s module doc); appended to the *ACK*
/// over a pooled connection, it is read back as the *next* heartbeat's status byte by a
/// pre-`1` leader, which then sees this node as permanently rejecting it. Neither
/// direction degrades gracefully — mixing a pre-`1` and a `>= 1` node is not a supported
/// topology, full stop.
///
/// `0` is still tracked (see `peer_protocol_version`) purely as a defensive value for a
/// cluster caught *mid* flag-day (some brokers already restarted onto this build, some
/// not yet): it lets an incompletely-upgraded cluster keep talking over the original
/// `0xAA` framing rather than corrupting a connection, but it is not itself a supported
/// end state to run in — see `MIN_VERSION_FOR_VERSIONED_PUSH`.
///
/// Once every node is on version `1` or later, THAT boundary is the one issue #48 is
/// actually for: bump this constant when a new inter-node wire capability is added that a
/// receiver must be able to opt into per-peer, and it rolls out one broker at a time from
/// here on — see `REPLICATION_PUSH_V2_MAGIC`, the first such capability.
pub const INTER_NODE_PROTOCOL_VERSION: u8 = 1;

/// Minimum peer-advertised `INTER_NODE_PROTOCOL_VERSION` before the leader will send the
/// versioned replication push (`REPLICATION_PUSH_V2_MAGIC`) instead of the original
/// `0xAA` layout. A peer advertising less than this — including one tracked as `0`
/// because it has never advertised anything (see `INTER_NODE_PROTOCOL_VERSION`'s doc for
/// why that is a mid-upgrade fallback, not a peer this cluster is expected to run
/// alongside indefinitely) — keeps receiving `0xAA` byte-for-byte, unchanged.
const MIN_VERSION_FOR_VERSIONED_PUSH: u8 = 1;

/// Magic byte for the versioned inter-node replication push: the same push `0xAA` carries,
/// framed behind an explicit version byte instead of a layout every peer must simply
/// assume, so a receiver always knows which fields follow rather than having to trust
/// that every broker in the cluster runs the same build. That trust requirement is
/// exactly what issue #48 exists to remove — `0xAA` carries no version field today, so a
/// receiver reading `0xAA` cannot tell an old sender from a new one, and adding a version
/// byte to `0xAA` itself would just be one more lockstep-only change, the precise thing
/// this issue is about ending.
///
/// This mirrors how the client protocol already solved the identical problem:
/// `VERSIONED_ENVELOPE_MAGIC` (`0xF1`, see `protocol::wire`) sits in front of a versioned
/// request while the legacy `[cmd][len][payload]` framing still decodes completely
/// unchanged. Being self-describing at the very first byte is what makes a mixed-version
/// cluster safe — matching a pattern already proven in this codebase is worth more than
/// inventing a second one.
///
/// `0xB0` was chosen deliberately: it collides with no magic byte used anywhere on the
/// wire today. It sits immediately next to the other inter-node magics without matching
/// any of them (`0xAA` push, `0xAC` heartbeat, `0xAE`/`0xAF` Raft vote, `0xBB` gRPC-style
/// pull fetch); it is outside the `RecordFrame` magic bytes used *inside* a push at a
/// nested framing level (`protocol::frame`'s `0xAB`/`0xAC`/`0xAD`/`0xAE`, which a receiver
/// only ever interprets once already inside a decoded `0xAA`/`0xB0` payload, so those
/// bytes are never compared against a fresh connection's first byte); and it is nowhere
/// near the client protocol's command-code range (`0x01..0x0A`) or its own `0xF1`
/// versioned-envelope magic.
pub const REPLICATION_PUSH_V2_MAGIC: u8 = 0xB0;

/// Bound on how long a leader waits for the bytes of a heartbeat ACK's compatible
/// trailing extensions — the role list, then the protocol version — once the mandatory
/// `[status][node_id][addr]` prefix has already been read successfully.
///
/// A fully compliant peer (version `>= 1`, see `INTER_NODE_PROTOCOL_VERSION`) writes its
/// entire ACK — prefix and both extensions — in one `write_all` call, so on a live
/// connection those extension bytes are normally already sitting in the socket's receive
/// buffer by the time this fires; this is not a meaningful latency budget for the
/// ordinary case. It exists as general defensive hardening against any peer that hangs
/// mid-response for whatever reason (a stall, a bug, a partial write never completed) —
/// this read would otherwise have no bound at all and could wedge a pooled connection
/// open forever. It is NOT a mechanism for tolerating a pre-`1` peer: talking to one over
/// a pooled connection has a different, unmitigated failure mode entirely (see
/// `INTER_NODE_PROTOCOL_VERSION`'s doc) that this timeout does not and cannot fix — a
/// timeout here only ever fires against a peer that is already `>= 1` but has stalled.
const HEARTBEAT_EXT_READ_TIMEOUT: Duration = Duration::from_secs(2);

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
    /// This node's process role(s) — see `crate::config::ProcessRole`. Defaults elsewhere
    /// to both (combined mode).
    pub roles: Vec<crate::config::ProcessRole>,
    /// The subset of `peer_addrs` that are controller-eligible. Empty means "assume every
    /// peer is controller-eligible" (combined-mode fallback).
    pub controller_peer_addrs: Vec<String>,
}

impl ClusterConfig {
    pub fn is_controller(&self) -> bool {
        self.roles.contains(&crate::config::ProcessRole::Controller)
    }

    pub fn is_broker_role(&self) -> bool {
        self.roles.contains(&crate::config::ProcessRole::Broker)
    }

    /// See the identical logic (and its rationale) on `crate::config::EngineConfig` —
    /// kept in sync here since `ClusterConfig` is `EngineConfig`'s slimmed-down copy
    /// passed into `ReplicationManager`.
    pub fn effective_controller_peer_addrs(&self) -> Vec<String> {
        let is_default_combined_roles = self.is_controller() && self.is_broker_role();
        if self.controller_peer_addrs.is_empty() && is_default_combined_roles {
            self.peer_addrs.clone()
        } else {
            self.controller_peer_addrs.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationManager {
    config: ClusterConfig,
    /// Current epoch (term) for consensus (RACE-03 atomic)
    epoch: Arc<std::sync::atomic::AtomicU64>,
    /// This node's advertised identity — the address announced in heartbeats, heartbeat
    /// ACKs, and self `BrokerRegister` writes. Wrapped in a shared cell rather than a
    /// plain `String` because it starts life as whatever `bind_addr` the caller passed to
    /// `new` (often a wildcard host or an ephemeral `:0` port — neither dialable by a
    /// peer) and is only known for certain once the TCP listener actually binds, which
    /// happens *after* this manager is constructed (and, for a statically-configured
    /// Leader, after the heartbeat broadcaster would otherwise have already started
    /// using it). `set_advertised_addr` corrects it in place once the real address (or an
    /// operator override) is known — see `StorageEngine::finalize_advertised_addr` and
    /// issue #62. Every heartbeat send re-reads this cell fresh rather than closing over
    /// a snapshot taken at loop-spawn time, so the correction reaches an already-running
    /// broadcaster too.
    advertised_addr: Arc<RwLock<String>>,
    /// Tracking partition high watermarks for replicas: (topic, partition, peer_addr) -> watermark_offset
    replica_watermarks: Arc<DashMap<(String, u32, String), u64>>,
    /// Wall-clock time of the most recent successful replication ACK per
    /// (topic, partition, peer_addr) — the ISR-membership signal: a replica that hasn't
    /// acked recently is lagging and should be dropped from the ISR (Kafka
    /// `replica.lag.time.max.ms`), independent of whether it happens to be caught up on
    /// the specific offset that triggered the check.
    replica_ack_time: Arc<DashMap<(String, u32, String), std::time::Instant>>,
    /// Data partitions whose last replication push was rejected as stale, with when that
    /// happened. Scoped per `(topic, partition)` precisely so a rejection on one partition
    /// cannot affect any other partition or the cluster consensus group — see the
    /// `STALE_EPOCH` handling in `replicate_batch`.
    stale_partitions: Arc<DashMap<(String, u32), std::time::Instant>>,
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
    /// Shared broker process-role registry (node_id -> roles), kept in sync alongside
    /// `broker_addrs` from the same sources (BrokerRegister replication, heartbeat ACKs).
    /// Used to decide which peers are eligible for data-partition assignment (`Broker`
    /// role) versus the metadata Raft quorum (`Controller` role).
    broker_roles: Arc<DashMap<u32, Vec<crate::config::ProcessRole>>>,
    /// Shared broker inter-node protocol-version registry (node_id -> version), kept in
    /// sync from the same heartbeat round trip as `broker_addrs`/`broker_roles` — see
    /// `note_peer_version`. A node id never present here has never advertised a version at
    /// all and is treated as `0` ("pre-versioning") by `peer_protocol_version`/
    /// `peer_protocol_version_by_node_id` — see issue #48.
    broker_versions: Arc<DashMap<u32, u8>>,
    /// Persistent peer TCP connection pool (peer_addr -> Arc<Mutex<Option<TcpStream>>>)
    /// Prevents ephemeral OS port exhaustion under high-throughput replication & heartbeats.
    peer_connections: Arc<DashMap<String, Arc<tokio::sync::Mutex<Option<TcpStream>>>>>,
}

impl ReplicationManager {
    pub fn new(
        config: ClusterConfig,
        bind_addr: String,
        broker_addrs: Arc<DashMap<u32, String>>,
        broker_roles: Arc<DashMap<u32, Vec<crate::config::ProcessRole>>>,
    ) -> Self {
        // Only controller-eligible peers participate in metadata Raft quorum math — a
        // broker-only peer never votes, so counting it here would make the majority
        // threshold wrong (too high) once role separation is in effect. Falls back to
        // "every peer votes" (today's combined-mode assumption) when
        // `controller_peer_addrs` wasn't set.
        let cluster_size = config.effective_controller_peer_addrs().len() + 1;
        // Bootstrap consensus state from configured role so config-declared
        // leaders start immediately accepting writes without running an election. Gated
        // on `is_controller()` too: a broker-only node must never bootstrap (or remain)
        // as the metadata Raft leader even if `role` is misconfigured as `Leader`.
        let initial_consensus_state = if config.role == NodeRole::Leader && config.is_controller() {
            ConsensusState::Leader
        } else {
            ConsensusState::Follower
        };
        let consensus =
            HermesConsensus::new_with_state(config.node_id, cluster_size, initial_consensus_state);

        let leader_addr = if initial_consensus_state == ConsensusState::Leader {
            // Leader knows its own address
            Arc::new(RwLock::new(Some(bind_addr.clone())))
        } else {
            Arc::new(RwLock::new(None))
        };

        if config.peer_addrs.is_empty() {
            // Issue #62: an empty `peer_addrs` now means "no static peer allowlist
            // configured" rather than "reject every peer" — see the heartbeat
            // acceptance check in `handler.rs`. Log this once at startup so the weaker
            // posture (membership gated by cluster_id + authentication, not by a static
            // address list) is visible to an operator rather than silently in effect.
            tracing::warn!(
                "HA Cluster [{}]: Node {} starting with no `peer_addrs` configured — \
                 inter-node heartbeats will be accepted from any sender presenting the \
                 correct cluster_id (subject to the CRIT-03 same-address check), rather \
                 than only from a statically whitelisted address. Configure `peer_addrs` \
                 to restore the static allowlist.",
                config.cluster_id,
                config.node_id
            );
        }

        let mgr = Self {
            config,
            epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            advertised_addr: Arc::new(RwLock::new(bind_addr)),
            replica_watermarks: Arc::new(DashMap::new()),
            replica_ack_time: Arc::new(DashMap::new()),
            stale_partitions: Arc::new(DashMap::new()),
            broker_last_seen: Arc::new(DashMap::new()),
            consensus,
            leader_addr,
            last_heartbeat: Arc::new(RwLock::new(std::time::Instant::now())),
            local_metadata_log_index: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            fetchers: Arc::new(DashMap::new()),
            broker_addrs,
            broker_roles,
            broker_versions: Arc::new(DashMap::new()),
            peer_connections: Arc::new(DashMap::new()),
        };

        // Always run the election-timeout watchdog. `check_election_timeout` is a no-op
        // while this node's consensus state is Leader, so this is safe to start
        // unconditionally — and it is what lets a node that steps down from a
        // statically-configured Leader role (e.g. on a STALE_EPOCH nack) ever become a
        // candidate again and re-acquire leadership. Previously this loop was only
        // started for nodes that booted as Follower, so a stepped-down static Leader
        // could never recontest an election. The loop itself only lets a *controller*
        // node actually contest — see `start_election_timeout_loop`.
        mgr.start_election_timeout_loop();

        // Leader: additionally start the heartbeat broadcaster to all followers.
        //
        // Deliberately NOT started here for a statically-configured Leader (issue #62).
        // At this point in construction the address above is still whatever `bind_addr`
        // the caller passed in — for the real server that's `EngineConfig::bind_addr`
        // read before the TCP listener has bound, which may be a wildcard host or an
        // ephemeral `:0` port. A heartbeat broadcast right now would announce that
        // unusable address to every peer on its very first (and, at a 10s interval,
        // possibly its only-for-a-while) beat. `StorageEngine::finalize_advertised_addr`
        // (called from `Server::bind` once the real address is known) starts this
        // broadcaster instead, via `start_heartbeat_broadcasting_if_leader` below. A node
        // that boots as Follower and is later elected takes the equivalent path in
        // `start_election_timeout_loop`, which by then always runs well after bind.
        mgr
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Returns this node's currently advertised address — the identity announced in
    /// heartbeats, heartbeat ACKs, and self `BrokerRegister` writes. Read fresh (not
    /// cached) since it can be corrected after construction — see `set_advertised_addr`.
    pub fn advertised_addr(&self) -> String {
        self.advertised_addr.read().clone()
    }

    /// Corrects this node's advertised address once the real bound address (or an
    /// operator-configured override) is known. See `advertised_addr`'s docs and issue
    /// #62. If this node currently believes itself the cluster leader, also updates the
    /// locally-known leader address to match — a leader's own address is always itself.
    pub fn set_advertised_addr(&self, addr: String) {
        *self.advertised_addr.write() = addr.clone();
        if self.is_leader() {
            *self.leader_addr.write() = Some(addr);
        }
    }

    /// Starts the leader heartbeat broadcaster if (and only if) this node is currently
    /// the cluster leader with peers configured to hear it. Idempotent to call
    /// speculatively — see the constructor-time comment on why this is deferred rather
    /// than run unconditionally from `new`. Safe to call more than once in principle
    /// (each call spawns its own loop, which self-terminates the moment this node is no
    /// longer Leader), but callers should only need to call it the one time the real
    /// advertised address becomes known.
    pub fn start_heartbeat_broadcasting_if_leader(&self) {
        if self.is_leader() && !self.config.peer_addrs.is_empty() {
            self.start_leader_heartbeat_loop();
        }
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
        self.leader_addr.read().clone()
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
        let mut guard = self.leader_addr.write();
        *guard = Some(addr);
        drop(guard);
        self.record_heartbeat();
    }

    /// Record receipt of a valid heartbeat (BUG-05)
    pub fn record_heartbeat(&self) {
        let mut guard = self.last_heartbeat.write();
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

    /// Highest offset `peer_addr` has confirmed it holds for this partition, if any.
    /// `None` means this leader has never observed an ack from that replica.
    pub fn replica_watermark(&self, topic: &str, partition: u32, peer_addr: &str) -> Option<u64> {
        self.replica_watermarks
            .get(&(topic.to_string(), partition, peer_addr.to_string()))
            .map(|w| *w.value())
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

    /// Records the inter-node protocol version `node_id` most recently advertised via a
    /// heartbeat — called from both directions: the leader learns a follower's version
    /// from its ACK (`send_leader_heartbeat_pooled`/`send_leader_heartbeat`), and a
    /// follower learns the leader's version from the heartbeat request itself
    /// (`handler::decode_heartbeat_packet`). Monotonicity is deliberately NOT enforced
    /// (unlike `set_local_metadata_log_index`): a peer's advertised version is a live fact
    /// about which build it is currently running, not a counter that only ever grows — a
    /// restart onto an older binary is a real (if unusual) event this should reflect, not
    /// resist.
    pub fn note_peer_version(&self, node_id: u32, version: u8) {
        self.broker_versions.insert(node_id, version);
    }

    /// The inter-node protocol version `node_id` last advertised, or `0`
    /// ("pre-versioning") if this node has never heard from it at all.
    pub fn peer_protocol_version_by_node_id(&self, node_id: u32) -> u8 {
        self.broker_versions
            .get(&node_id)
            .map(|v| *v.value())
            .unwrap_or(0)
    }

    /// The inter-node protocol version most recently advertised by the peer reachable at
    /// `peer_addr`, or `0` if unknown. Versions are tracked per `node_id` — the identity a
    /// peer states about itself in its heartbeat (see `note_peer_version`) — while
    /// replication is addressed by `peer_addr` (see `replication_targets`), so this
    /// resolves between the two via `broker_addrs`' reverse mapping, the same way
    /// `replication_targets` itself already cross-references `broker_roles` by `node_id`
    /// while working in terms of addresses.
    pub fn peer_protocol_version(&self, peer_addr: &str) -> u8 {
        self.broker_addrs
            .iter()
            .find(|entry| entry.value() == peer_addr)
            .map(|entry| self.peer_protocol_version_by_node_id(*entry.key()))
            .unwrap_or(0)
    }

    /// Leader heartbeat broadcaster: sends periodic heartbeats to all follower peers,
    /// announcing this node as the cluster leader and providing its bind address.
    fn start_leader_heartbeat_loop(&self) {
        let peer_addrs = self.config.peer_addrs.clone();
        let cluster_id = self.config.cluster_id.clone();
        let node_id = self.config.node_id;
        // Shared cell, not a snapshot — read fresh on every tick (see its docs) so a
        // correction applied after this loop already started (or even mid-loop, on a
        // wildcard->real address fixup) reaches the very next heartbeat rather than
        // waiting a full `HEARTBEAT_INTERVAL`.
        let advertised_addr = self.advertised_addr.clone();
        let epoch = self.epoch.clone(); // share epoch for heartbeat term
        let broker_addrs = self.broker_addrs.clone();
        let broker_roles = self.broker_roles.clone();
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
                let current_addr = advertised_addr.read().clone();
                for peer in &peer_addrs {
                    let peer_conn = manager.get_or_connect_peer(peer);
                    match send_leader_heartbeat_pooled(
                        &peer_conn,
                        peer,
                        &cluster_id,
                        node_id,
                        current_term,
                        &current_addr,
                    )
                    .await
                    {
                        Ok((follower_id, follower_addr, follower_roles, follower_version)) => {
                            // Learn the follower's own identity from its heartbeat ACK so this
                            // leader's broker_addrs registry stays complete even without any
                            // manual registration step (fixes real-cluster broker discovery).
                            broker_addrs.insert(follower_id, follower_addr);
                            broker_roles.insert(follower_id, follower_roles);
                            manager.note_broker_alive(follower_id);
                            manager.note_peer_version(follower_id, follower_version);
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
                let topics = engine.list_topics();
                let mut active_partition_keys = std::collections::HashSet::new();

                for topic in &topics {
                    // `__cluster_metadata` still replicates via leader-push
                    // (`propose_metadata_unchecked`/`replicate_batch`) — running the pull
                    // fetcher for it too would apply the same records through two
                    // independent paths concurrently, racing `append_verbatim`'s
                    // expected-offset checks against `append_replica_frame_verbatim`'s.
                    if topic == "__cluster_metadata" {
                        continue;
                    }
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
                                            // Re-resolve the leader every iteration rather
                                            // than trusting the value captured when this
                                            // fetcher was spawned — otherwise a partition
                                            // failover would leave this loop stuck pulling
                                            // from a stale (possibly dead) former leader
                                            // forever, since the outer management loop only
                                            // tears a fetcher down when this node stops
                                            // being a follower, not when the leader changes.
                                            // Whether this round actually applied records —
                                            // drives whether the loop re-fetches at once or
                                            // backs off (see the end of the loop body).
                                            let mut made_progress = false;
                                            let current_leader_id = engine_c
                                                .partition_leader_id(&topic_c, p_id)
                                                .unwrap_or(leader_id);
                                            if current_leader_id == node_id {
                                                // We became this partition's leader
                                                // ourselves — stop pulling; the outer sweep
                                                // will remove this fetcher next tick.
                                                break;
                                            }

                                            let fetch_offset = match engine_c
                                                .get_or_create_partition(&topic_c, p_id)
                                            {
                                                Ok(pm) => pm.latest_offset(),
                                                Err(_) => 0,
                                            };

                                            let req = ReplicationFetchRequest {
                                                follower_node_id: node_id,
                                                topic: topic_c.clone(),
                                                partition: p_id,
                                                fetch_offset,
                                                max_bytes: 64 * 1024,
                                            };

                                            if let Some(leader_addr) =
                                                engine_c.get_broker_address(current_leader_id)
                                            {
                                                if let Ok(resp) =
                                                    send_grpc_replication_fetch(&leader_addr, &req)
                                                        .await
                                                {
                                                    if let Ok(pm) = engine_c
                                                        .get_or_create_partition(&topic_c, p_id)
                                                    {
                                                        let mut applied_any = false;
                                                        for frame in &resp.frames {
                                                            // Verbatim, not
                                                            // `produce_frame_eos`: preserves
                                                            // the leader's exact
                                                            // offset/timestamp/magic/CRC so
                                                            // this replica's log stays
                                                            // byte-identical to the leader's
                                                            // for the same offset range,
                                                            // matching the push-replication
                                                            // path's guarantee.
                                                            match pm
                                                                .append_replica_frame_verbatim(
                                                                    frame,
                                                                ) {
                                                                Ok(
                                                                    crate::segment::VerbatimAppendResult::Appended,
                                                                ) => applied_any = true,
                                                                Ok(
                                                                    crate::segment::VerbatimAppendResult::AlreadyApplied,
                                                                ) => {}
                                                                Ok(
                                                                    crate::segment::VerbatimAppendResult::Gap {
                                                                        expected,
                                                                    },
                                                                ) => {
                                                                    tracing::warn!(
                                                                        "Pull Replication: Gap on '{}' P{} — got offset {} but expected {}. Will retry from current LEO.",
                                                                        topic_c,
                                                                        p_id,
                                                                        frame.offset,
                                                                        expected
                                                                    );
                                                                    break;
                                                                }
                                                                Err(e) => {
                                                                    tracing::error!(
                                                                        "Pull Replication: Failed to persist frame on '{}' P{}: {}",
                                                                        topic_c,
                                                                        p_id,
                                                                        e
                                                                    );
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        if applied_any {
                                                            let _ = pm.flush_if_sync_policy();
                                                        }

                                                        // Adopt the leader's committed
                                                        // watermark, clamped to what this
                                                        // replica actually holds.
                                                        //
                                                        // The response has always carried
                                                        // `leader_watermark` and this loop
                                                        // used to ignore it entirely,
                                                        // leaving the follower to advance
                                                        // its own HW to its LEO as it
                                                        // appended. That marks records
                                                        // committed on the follower that
                                                        // the leader has NOT yet committed
                                                        // — so a follower-fetch read could
                                                        // return records that are still
                                                        // uncommitted cluster-wide, and a
                                                        // follower promoted to leader
                                                        // would start out claiming a
                                                        // higher committed point than the
                                                        // data warrants.
                                                        //
                                                        // The clamp matters in both
                                                        // directions: `min` with the local
                                                        // LEO so we never claim to have
                                                        // committed data we haven't
                                                        // received, and the leader's value
                                                        // so we never outrun the cluster's
                                                        // committed point.
                                                        let local_leo = pm.latest_offset();
                                                        let follower_hw =
                                                            resp.leader_watermark.min(local_leo);
                                                        pm.advance_committed_hw(follower_hw);
                                                        made_progress = applied_any;
                                                    }
                                                }
                                            }

                                            // Re-fetch immediately after a productive round
                                            // instead of always sleeping.
                                            //
                                            // The leader learns a follower's progress from
                                            // that follower's *next* fetch offset, so an
                                            // unconditional sleep put a fixed delay between
                                            // "follower has the data" and "leader knows it"
                                            // — which is exactly the wait an `acks=all`
                                            // produce blocks on. Polling only when idle
                                            // keeps the catch-up loop tight while leaving
                                            // an idle follower cheap.
                                            if !made_progress {
                                                sleep(Duration::from_millis(50)).await;
                                            }
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

                // Scan immediately on startup (a brand-new follower assignment shouldn't
                // sit idle for a full tick before its fetcher spins up), then pace
                // subsequent scans.
                sleep(Duration::from_millis(50)).await;
            }
        });
    }

    /// Election timeout task: monitors heartbeat arrivals and triggers elections.
    ///
    /// Uses node_id-derived jitter to avoid split-vote storms (no `rand` crate needed).
    fn start_election_timeout_loop(&self) {
        let last_heartbeat = self.last_heartbeat.clone();
        let consensus = self.consensus.clone();
        // Only controller-eligible peers are sent VoteRequests — a broker-only peer never
        // votes, and blasting it with vote RPCs it can't meaningfully answer is just
        // wasted work. Falls back to all peers when `controller_peer_addrs` wasn't set.
        let controller_peer_addrs = self.config.effective_controller_peer_addrs();
        // Once elected, the leader's heartbeat still goes out to *every* peer (not just
        // controllers) — brokers need it to learn the current leader (for forwarding
        // controller-plane mutations) and it's also the address/role discovery channel
        // (see `read_heartbeat_ack_roles`), independent of who gets to vote.
        let all_peer_addrs = self.config.peer_addrs.clone();
        let is_controller = self.config.is_controller();
        let cluster_id = self.config.cluster_id.clone();
        let node_id = self.config.node_id;
        let epoch = self.epoch.clone();
        let leader_addr = self.leader_addr.clone();
        // Shared cell — see `advertised_addr`'s docs. Read fresh at use, not snapshotted
        // here, so a correction applied between construction and a (possibly much later)
        // election win is picked up rather than advertising a stale address forever.
        let advertised_addr = self.advertised_addr.clone();
        let broker_addrs = self.broker_addrs.clone();
        let broker_roles = self.broker_roles.clone();
        let broker_versions = self.broker_versions.clone();
        let local_metadata_log_index = self.local_metadata_log_index.clone();
        let broker_last_seen = self.broker_last_seen.clone();

        tokio::spawn(async move {
            let mut tick: u64 = 0;
            // The heartbeat instant the current jitter draw belongs to, and the draw
            // itself. Held stable until the election timer actually resets — see below.
            let mut jitter_anchor = *last_heartbeat.read();
            let mut current_jitter_secs = next_election_jitter(node_id, tick);
            loop {
                sleep(Duration::from_secs(1)).await;
                tick = tick.wrapping_add(1);

                // A broker-only node never contests leadership of the metadata Raft
                // quorum — it just passively tracks whichever controller is currently
                // sending it heartbeats (handled by `decode_heartbeat_packet`
                // updating `last_heartbeat`/`leader_addr` directly). Skip the entire
                // candidacy path for it.
                if !is_controller {
                    continue;
                }

                // Draw the jitter ONCE per election timer, not once per tick.
                //
                // This used to mix `tick` into the hash, so the deadline moved every
                // second: a node fired as soon as the current value happened to dip below
                // its elapsed time, which collapsed the effective timeout to roughly the
                // same value on every node — the exact opposite of what jitter is for. All
                // controllers then timed out together, split the vote, bumped the term and
                // retried in lockstep, so the cluster could sit for long stretches with no
                // leader (and, before per-partition epochs, every term bump also
                // invalidated in-flight replication).
                //
                // The deadline is now redrawn only when the timer resets — i.e. when a
                // heartbeat advances `last_heartbeat`, or after an election concludes.
                let last = *last_heartbeat.read();
                if last != jitter_anchor {
                    // Timer reset since we last looked: pick a fresh deadline and hold it.
                    jitter_anchor = last;
                    current_jitter_secs = next_election_jitter(node_id, tick);
                }
                let timeout_duration =
                    Duration::from_secs(ELECTION_TIMEOUT_BASE_SECS + current_jitter_secs);

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
                    "Hermes Election: Node {} became Candidate for term {}. Broadcasting VoteRequest to {} controller peer(s).",
                    node_id, new_term, controller_peer_addrs.len()
                );

                // Broadcast VoteRequest to each controller peer and collect granted
                // votes. Includes our own last-applied metadata-log index so voters can
                // enforce Raft's log-completeness rule (§5.4.1): a candidate whose
                // metadata log is behind a voter's must not win the election, or
                // committed metadata could be lost.
                let candidate_last_log_index =
                    local_metadata_log_index.load(std::sync::atomic::Ordering::Acquire);
                let mut votes_granted = 1usize; // vote for self
                for peer in &controller_peer_addrs {
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
                        let mut la = leader_addr.write();
                        *la = Some(advertised_addr.read().clone());
                    }
                    tracing::info!(
                        "Hermes Election: Node {} is now LEADER for term {} with {}/{} votes.",
                        node_id,
                        new_term,
                        votes_granted,
                        controller_peer_addrs.len() + 1
                    );
                    // Reset heartbeat timestamp so we don't re-trigger election.
                    *last_heartbeat.write() = std::time::Instant::now();

                    // REP-01 / H10: Newly elected leader starts heartbeat loop to peers.
                    // Each election win previously spawned a permanent loop with no
                    // cancellation token, so repeated Follower→Leader transitions
                    // accumulated unbounded tasks.  Now the loop checks the consensus
                    // state on every iteration and exits as soon as this node is no
                    // longer the Leader (step_down_to_follower sets state=Follower),
                    // so at most one active heartbeat loop exists per node at any time.
                    // Broadcasts to *every* peer (not just controllers) — see the
                    // `all_peer_addrs` comment above.
                    if !all_peer_addrs.is_empty() {
                        let peer_addrs_c = all_peer_addrs.clone();
                        let cluster_id_c = cluster_id.clone();
                        let advertised_addr_c = advertised_addr.clone();
                        let epoch_c = epoch.clone();
                        let consensus_c = consensus.clone();
                        let broker_addrs_c = broker_addrs.clone();
                        let broker_roles_c = broker_roles.clone();
                        let broker_versions_c = broker_versions.clone();
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
                                let current_addr = advertised_addr_c.read().clone();
                                for peer in &peer_addrs_c {
                                    if let Ok((
                                        follower_id,
                                        follower_addr,
                                        follower_roles,
                                        follower_version,
                                    )) = send_leader_heartbeat(
                                        peer,
                                        &cluster_id_c,
                                        node_id,
                                        current_term,
                                        &current_addr,
                                    )
                                    .await
                                    {
                                        broker_addrs_c.insert(follower_id, follower_addr);
                                        broker_roles_c.insert(follower_id, follower_roles);
                                        broker_versions_c.insert(follower_id, follower_version);
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

    /// Streams produced record batch to all follower peer nodes over TCP.
    /// Called from a tokio::spawn task in engine.rs — replicates concurrently to all peers.
    /// P3: If a peer returns a stale-epoch ACK (0x01), triggers leader step-down.
    /// CRIT-01: cluster_id is now included in the wire packet so followers can authenticate the sender.
    /// Peers that should receive replication traffic for this partition.
    ///
    /// `__cluster_metadata` still replicates to every peer — brokers need to learn
    /// topics/ACLs/broker registrations too, even though they never vote on it. Real
    /// data-topic partitions no longer reach this function at all: as of the fix for
    /// issue #22, `StorageEngine::produce_batch` replicates data topics exclusively via
    /// the pull fetcher and never calls `replicate_batch`/`broadcast_high_watermark` for
    /// them.
    ///
    /// Push and pull used to run unconditionally side by side for data topics, which meant
    /// every record crossed the wire and hit the follower's append path twice, a pushed
    /// and a pulled batch covering the same offsets could race on append, and follower
    /// progress was written by two independent paths so ISR decisions read a value neither
    /// owned. A first fix excluded pull-covered peers from push (`pull_covered` below); once
    /// partition assignment became universal (issue #40, `default_replication_factor` is
    /// now 3) that exclusion emptied the push target list for every data partition on its
    /// own, so push was removed from the produce path outright rather than left calling
    /// this function to compute an always-empty target list.
    ///
    /// The data-topic filtering below (controller-only peers, `pull_covered` exclusion) is
    /// therefore dead in practice — nothing currently calls this with a topic other than
    /// `__cluster_metadata` — but it is left in place rather than deleted, since
    /// `replicate_batch`/`broadcast_high_watermark` remain generic public API taking an
    /// arbitrary `topic`, and stripping it would silently drop the controller-only safety
    /// filter for any future data-topic caller.
    fn replication_targets(
        &self,
        topic: &str,
        _partition: u32,
        pull_covered: &[String],
    ) -> Vec<String> {
        let exclude: std::collections::HashSet<&str> =
            pull_covered.iter().map(|s| s.as_str()).collect();
        if topic == "__cluster_metadata" {
            // The metadata log is deliberately excluded from the pull fetcher (applying its
            // records through two paths at once would race), so it is push-only and never
            // duplicated.
            return self.config.peer_addrs.clone();
        }
        let controller_only_addrs: std::collections::HashSet<String> = self
            .broker_addrs
            .iter()
            .filter(|entry| {
                let node_id = *entry.key();
                self.broker_roles
                    .get(&node_id)
                    .map(|roles| {
                        roles.contains(&crate::config::ProcessRole::Controller)
                            && !roles.contains(&crate::config::ProcessRole::Broker)
                    })
                    .unwrap_or(false)
            })
            .map(|entry| entry.value().clone())
            .collect();
        self.config
            .peer_addrs
            .iter()
            .filter(|addr| !controller_only_addrs.contains(*addr))
            .filter(|addr| !exclude.contains(addr.as_str()))
            .cloned()
            .collect()
    }

    /// Tells every follower the leader's newly-advanced committed high watermark.
    ///
    /// A batch is pushed *before* it is committed (commit requires the ISR to acknowledge
    /// it first), so the `leader_hw` carried by that push is necessarily behind the batch
    /// it delivers. Something has to deliver the updated committed point afterwards, or a
    /// follower would sit indefinitely holding replicated-but-not-readable records — with
    /// no pull fetcher to converge it, which is exactly the case for `__cluster_metadata`
    /// (its only caller as of issue #22 — data topics converge their watermark via the
    /// `leader_watermark` carried on every pull fetch response instead).
    ///
    /// This sends the same 0xAA packet carrying zero frames: the follower's decoder skips
    /// the (empty) record loop and applies the watermark clamp. Failures are logged, not
    /// propagated — the write is already durable and committed on the leader, and the next
    /// push or fetch carries the watermark again.
    /// Pushes frames to a single peer, rather than to every replication target.
    ///
    /// Used by the metadata catch-up sweep, which needs to send one lagging follower the
    /// specific range it is missing — the fan-out path would re-send that range to peers
    /// that are already current.
    ///
    /// On success the peer's watermark is recorded exactly as the fan-out path does, so a
    /// caught-up follower stops being selected for catch-up on the next sweep.
    pub async fn push_frames_to_peer(
        &self,
        peer_addr: &str,
        topic: &str,
        partition: u32,
        fencing_epoch: u64,
        leader_hw: u64,
        frames: &[RecordFrame],
    ) -> IoResult<()> {
        if frames.is_empty() {
            return Ok(());
        }
        let last_offset = frames.last().unwrap().offset;
        let peer_conn = self.get_or_connect_peer(peer_addr);
        // Issue #48: use the versioned frame only once this peer has told us (via
        // heartbeat) that it understands it — everyone else, including a peer we've never
        // heard a version from at all, keeps getting `0xAA` byte-for-byte.
        if self.peer_protocol_version(peer_addr) >= MIN_VERSION_FOR_VERSIONED_PUSH {
            send_replication_push_v2_pooled(
                &peer_conn,
                peer_addr,
                &self.config.cluster_id,
                topic,
                partition,
                fencing_epoch,
                leader_hw,
                frames,
            )
            .await?;
        } else {
            send_replication_push_pooled(
                &peer_conn,
                peer_addr,
                &self.config.cluster_id,
                topic,
                partition,
                fencing_epoch,
                leader_hw,
                frames,
            )
            .await?;
        }
        self.update_replica_watermark(topic, partition, peer_addr, last_offset);
        Ok(())
    }

    pub async fn broadcast_high_watermark(
        &self,
        topic: &str,
        partition: u32,
        fencing_epoch: u64,
        leader_hw: u64,
        pull_covered: &[String],
    ) {
        let target_peers = self.replication_targets(topic, partition, pull_covered);
        if target_peers.is_empty() {
            return;
        }
        let epoch = fencing_epoch;
        let cluster_id = self.config.cluster_id.clone();

        let mut handles = Vec::with_capacity(target_peers.len());
        for peer_addr in target_peers {
            let cid = cluster_id.clone();
            let topic_name = topic.to_string();
            let peer_conn = self.get_or_connect_peer(&peer_addr);
            // Decided synchronously, before the peer's own send task spawns — see the
            // matching comment in `push_frames_to_peer`.
            let use_v2 = self.peer_protocol_version(&peer_addr) >= MIN_VERSION_FOR_VERSIONED_PUSH;
            handles.push(tokio::spawn(async move {
                let result = if use_v2 {
                    send_replication_push_v2_pooled(
                        &peer_conn,
                        &peer_addr,
                        &cid,
                        &topic_name,
                        partition,
                        epoch,
                        leader_hw,
                        &[],
                    )
                    .await
                } else {
                    send_replication_push_pooled(
                        &peer_conn,
                        &peer_addr,
                        &cid,
                        &topic_name,
                        partition,
                        epoch,
                        leader_hw,
                        &[],
                    )
                    .await
                };
                (peer_addr, result)
            }));
        }

        for handle in handles {
            if let Ok((peer_addr, Err(e))) = handle.await {
                tracing::debug!(
                    "HW Propagation: failed to send watermark {} for {}-{} to {}: {}",
                    leader_hw,
                    topic,
                    partition,
                    peer_addr,
                    e
                );
            }
        }
    }

    /// `leader_hw` is this leader's committed high watermark at call time. It is normally
    /// *behind* the batch being pushed — the batch commits only once the ISR acknowledges
    /// it — which is exactly the point: followers must not treat in-flight records as
    /// committed. They pick up the newer committed point on a subsequent push or pull.
    ///
    /// `fencing_epoch` is what the follower validates this push against, and its meaning
    /// is per-topic by design:
    ///
    /// - `__cluster_metadata` — the controller's Raft term. Leadership of the metadata log
    ///   *is* controller leadership, so the term is the right fence.
    /// - any data partition — that partition's own `leader_epoch`.
    ///
    /// Data partitions used to be stamped with the controller term as well, which is wrong
    /// in both directions. Controller elections and partition leadership change for
    /// unrelated reasons, so every controller election invalidated in-flight pushes for
    /// *every* partition in the cluster (followers rejecting them as stale even though no
    /// partition leadership had changed), while a partition leader that had actually been
    /// superseded stayed unfenced as long as the controller term happened to be unchanged
    /// — a split-brain window on that partition.
    pub async fn replicate_batch(
        &self,
        topic: &str,
        partition: u32,
        fencing_epoch: u64,
        leader_hw: u64,
        pull_covered: &[String],
        frames: &[RecordFrame],
    ) -> IoResult<()> {
        if self.config.peer_addrs.is_empty() || frames.is_empty() {
            return Ok(());
        }

        let target_peers = self.replication_targets(topic, partition, pull_covered);
        if target_peers.is_empty() {
            return Ok(());
        }

        let last_offset = frames.last().unwrap().offset;
        let epoch = fencing_epoch;
        let cluster_id = self.config.cluster_id.clone();

        // Replicate to each peer concurrently over persistent pooled TCP streams
        let mut handles = Vec::with_capacity(target_peers.len());
        for peer in &target_peers {
            let peer_addr = peer.clone();
            let topic_name = topic.to_string();
            let frames_vec = frames.to_vec();
            let cid = cluster_id.clone();
            let peer_conn = self.get_or_connect_peer(&peer_addr);
            // Issue #48: pick the versioned frame only for a peer that has advertised
            // support for it — decided synchronously, before this peer's send task
            // spawns, from the version this leader last learned via that peer's
            // heartbeat ACK (see `peer_protocol_version`/`note_peer_version`).
            let use_v2 = self.peer_protocol_version(&peer_addr) >= MIN_VERSION_FOR_VERSIONED_PUSH;
            handles.push(tokio::spawn(async move {
                // CRIT-01: pass cluster_id so the follower can authenticate this replication push.
                let result = if use_v2 {
                    send_replication_push_v2_pooled(
                        &peer_conn,
                        &peer_addr,
                        &cid,
                        &topic_name,
                        partition,
                        epoch,
                        leader_hw,
                        &frames_vec,
                    )
                    .await
                } else {
                    send_replication_push_pooled(
                        &peer_conn,
                        &peer_addr,
                        &cid,
                        &topic_name,
                        partition,
                        epoch,
                        leader_hw,
                        &frames_vec,
                    )
                    .await
                };
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
                        // A rejected push for ONE data partition is handled locally: this
                        // node is no longer the accepted leader for that partition, so it
                        // stops pushing and lets the metadata layer tell it who is.
                        //
                        // It must NOT resign from cluster consensus. That is what this
                        // used to do (`consensus.step_down_to_follower`), which resigned
                        // the broker from the *cluster metadata* Raft group over a single
                        // data-partition rejection — taking down the controller for the
                        // whole cluster and forcing a cluster-wide re-election. Since a
                        // controller term change is itself one of the things that causes
                        // these rejections, the failure was self-reinforcing and could
                        // leave the cluster flapping between controllers under ordinary
                        // produce load. Cluster step-down belongs to the consensus layer's
                        // own messages (`handle_vote_request`/heartbeat), never to the
                        // data plane.
                        if topic != "__cluster_metadata" {
                            self.stale_partitions
                                .insert((topic.to_string(), partition), std::time::Instant::now());
                            tracing::warn!(
                                "Partition Fencing: peer {} rejected a push for {}-{} as stale — \
                                 pausing replication for this partition pending a metadata refresh \
                                 (cluster consensus untouched).",
                                peer_addr,
                                topic,
                                partition
                            );
                        } else {
                            // A stale-epoch rejection on the metadata partition genuinely
                            // is a consensus-level signal: someone else is a newer
                            // controller, so stepping down is the correct response.
                            let peer_epoch = self.consensus.current_term() + 1;
                            self.consensus.step_down_to_follower(peer_epoch);
                            let mut la = self.leader_addr.write();
                            *la = None;
                            tracing::warn!(
                                "Controller Fencing: stepping down — peer {} reported a stale epoch \
                                 on __cluster_metadata.",
                                peer_addr
                            );
                        }
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
/// Wire format (request):  `[0xAC] [cluster_id: pascal] [node_id: 4b] [term: 8b]
/// [bind_addr: pascal] [leader_version: 1b]`
/// Wire format (response): `[0x00] [follower_node_id: 4b] [follower_bind_addr: pascal]
/// [role_count: 1b][role_bytes...] [follower_version: 1b]` on success, `[0x01]` on
/// rejection (cluster mismatch / not whitelisted).
///
/// Returning the follower's own node_id + bind_addr lets the Leader populate its broker
/// address registry purely from the existing heartbeat round-trip, without requiring any
/// out-of-band broker registration step — followers are otherwise unable to publish their
/// own address since only the partition leader for `__cluster_metadata` may write to it.
///
/// `leader_version`/`follower_version` (issue #48) are each appended as a trailing byte,
/// in the same position the ACK's role list occupies. This is NOT the same compatibility
/// guarantee roles have: a pre-`1` peer has no code path that skips an unrecognized
/// trailing byte, so on either end this desyncs that peer's own framing rather than being
/// harmlessly ignored (see `INTER_NODE_PROTOCOL_VERSION`'s doc for exactly how, in each
/// direction). Adopting this field is therefore a one-time flag day, not a rolling
/// upgrade — every node must already be `>= 1` before any of them appends it. What
/// `read_heartbeat_ack_version`/`handler::decode_heartbeat_packet` tolerate is a *missing*
/// byte from a peer that IS `>= 1` (a stalled write, a slow peer, or simply this node's
/// own bookkeeping not having heard from that peer yet) — never a genuinely pre-`1` peer,
/// which this mechanism does not and cannot support.
///
/// Reads the trailing `[role_count: 1b][role_bytes...]` that
/// `handler::decode_heartbeat_packet` now appends to its ACK, after the caller has
/// already consumed the `[status][node_id][addr_len][addr_bytes]` prefix.
async fn read_heartbeat_ack_roles<S>(stream: &mut S) -> IoResult<Vec<crate::config::ProcessRole>>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut count_buf = [0u8; 1];
    stream.read_exact(&mut count_buf).await?;
    let count = count_buf[0] as usize;
    let mut role_buf = vec![0u8; count];
    if count > 0 {
        stream.read_exact(&mut role_buf).await?;
    }
    Ok(crate::config::parse_process_role_bytes(&role_buf))
}

/// Reads the trailing `[protocol_version: 1b]` a version-aware follower appends to its
/// heartbeat ACK immediately after the role list `read_heartbeat_ack_roles` reads (issue
/// #48; see `handler::decode_heartbeat_packet`, which writes both, in that order).
///
/// Absence here is never propagated as an error, unlike `read_heartbeat_ack_roles`:
/// failing the whole heartbeat over one missing byte would throw away the
/// already-successfully-read prefix and role list too. This is deliberately NOT a claim
/// that a genuinely pre-`1` peer is being tolerated — see `INTER_NODE_PROTOCOL_VERSION`'s
/// doc; talking to one over a pooled connection is unsafe regardless of what this
/// function does, because a pre-`1` peer never reaches this point on its own side (it
/// stopped reading before this byte and moved on to reusing the connection). What this
/// *does* correctly handle is this node's own bookkeeping being incomplete or a `>= 1`
/// peer stalling mid-response — bounded by `HEARTBEAT_EXT_READ_TIMEOUT` so such a peer
/// cannot hold this read open forever. A timeout, a short read, or a closed connection all
/// resolve the same way — to version `0` — which also happens to be the value a genuinely
/// pre-`1` peer would be tracked as, if a connection to one somehow reaches this function
/// at all.
async fn read_heartbeat_ack_version<S>(stream: &mut S) -> u8
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut version_buf = [0u8; 1];
    match timeout(
        HEARTBEAT_EXT_READ_TIMEOUT,
        stream.read_exact(&mut version_buf),
    )
    .await
    {
        Ok(Ok(_)) => version_buf[0],
        _ => 0,
    }
}

/// Reads a heartbeat ACK's full body from an already-connected stream, immediately after
/// the request has been written: the mandatory `[status][node_id][addr]` prefix, then both
/// of its compatible trailing extensions — the role list, then the protocol version (see
/// `read_heartbeat_ack_roles`/`read_heartbeat_ack_version`).
///
/// Returns `Ok(None)` for a cluster-mismatch rejection (`status != 0`) and `Ok(Some(..))`
/// on success, complete with whatever extensions the peer actually sent. Only a genuine
/// I/O failure while reading the mandatory prefix or the role list produces `Err` — a
/// missing protocol-version byte never does (see `read_heartbeat_ack_version`).
async fn read_heartbeat_ack_body<S>(
    stream: &mut S,
) -> IoResult<Option<(u32, String, Vec<crate::config::ProcessRole>, u8)>>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut status = [0u8; 1];
    stream.read_exact(&mut status).await?;
    if status[0] != 0 {
        return Ok(None);
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

    let roles = read_heartbeat_ack_roles(stream).await?;
    let version = read_heartbeat_ack_version(stream).await;

    Ok(Some((follower_node_id, follower_bind_addr, roles, version)))
}

/// Bound on the *entire* heartbeat ACK read (from right after the request is written
/// through to the end of both trailing extensions), wrapped around
/// `read_heartbeat_ack_body`. This is broader than `HEARTBEAT_EXT_READ_TIMEOUT`: it also
/// covers the mandatory prefix, protecting this node against ANY peer that stalls before
/// writing a response at all — a hung process, a slow disk, a network stall, or any other
/// reason a live `>= 1` peer can fail to answer promptly. This read had no bound before
/// this constant existed, which is the actual gap it closes; it is general hardening, not
/// a mechanism for surviving a pre-`1` peer.
///
/// It does NOT make talking to a pre-`1` peer safe, and does not attempt to. A pre-`1`
/// leader reading this node's version-tagged ACK over a *pooled* connection stops before
/// the version byte, leaves it unread, and reads it back as the *next* heartbeat's status
/// byte — a nonzero status, i.e. a permanent false rejection — with no timeout able to
/// help, because that leader never even reaches a blocked read: it gets a (wrong) answer
/// immediately. See `INTER_NODE_PROTOCOL_VERSION`'s doc; this is exactly why the version
/// field is a one-time flag day rather than a rolling-upgrade boundary.
const HEARTBEAT_ACK_READ_TIMEOUT: Duration = Duration::from_secs(3);

pub async fn send_leader_heartbeat_pooled(
    peer_conn: &Arc<tokio::sync::Mutex<Option<TcpStream>>>,
    peer_addr: &str,
    cluster_id: &str,
    node_id: u32,
    term: u64,
    leader_bind_addr: &str,
) -> IoResult<(u32, String, Vec<crate::config::ProcessRole>, u8)> {
    let mut buf = Vec::with_capacity(128);
    buf.put_u8(0xAC);
    crate::protocol::wire::write_pascal_string(&mut buf, cluster_id);
    buf.put_u32(node_id);
    buf.put_u64(term);
    crate::protocol::wire::write_pascal_string(&mut buf, leader_bind_addr);
    // Issue #48: advertise this node's inter-node protocol version (see
    // `note_peer_version` on the receiving end). Unconditional: a pre-`1` receiver
    // desyncs its own framing on this byte rather than skipping it (see
    // `INTER_NODE_PROTOCOL_VERSION`'s doc) — writing it is safe ONLY because adopting
    // this field requires every node already be `>= 1` before any of them sends it (a
    // one-time flag day, not a rolling upgrade).
    buf.put_u8(INTER_NODE_PROTOCOL_VERSION);

    let mut lock = peer_conn.lock().await;

    if let Some(ref mut stream) = *lock {
        if stream.write_all(&buf).await.is_ok() {
            if let Ok(Ok(Some((follower_node_id, follower_bind_addr, roles, version)))) =
                timeout(HEARTBEAT_ACK_READ_TIMEOUT, read_heartbeat_ack_body(stream)).await
            {
                return Ok((follower_node_id, follower_bind_addr, roles, version));
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
    match timeout(
        HEARTBEAT_ACK_READ_TIMEOUT,
        read_heartbeat_ack_body(&mut stream),
    )
    .await
    {
        Ok(Ok(Some((follower_node_id, follower_bind_addr, roles, version)))) => {
            *lock = Some(stream);
            Ok((follower_node_id, follower_bind_addr, roles, version))
        }
        Ok(Ok(None)) => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("Peer {} rejected heartbeat: cluster.id mismatch", peer_addr),
        )),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("Heartbeat ACK from {} timed out", peer_addr),
        )),
    }
}

pub async fn send_leader_heartbeat(
    peer_addr: &str,
    cluster_id: &str,
    node_id: u32,
    term: u64,
    leader_bind_addr: &str,
) -> IoResult<(u32, String, Vec<crate::config::ProcessRole>, u8)> {
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
    // Issue #48: see the identical line (and its doc) in `send_leader_heartbeat_pooled`.
    buf.put_u8(INTER_NODE_PROTOCOL_VERSION);

    stream.write_all(&buf).await?;

    match timeout(
        HEARTBEAT_ACK_READ_TIMEOUT,
        read_heartbeat_ack_body(&mut stream),
    )
    .await
    {
        Ok(Ok(Some((follower_node_id, follower_bind_addr, roles, version)))) => {
            Ok((follower_node_id, follower_bind_addr, roles, version))
        }
        Ok(Ok(None)) => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("Peer {} rejected heartbeat: cluster.id mismatch", peer_addr),
        )),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("Heartbeat ACK from {} timed out", peer_addr),
        )),
    }
}

/// Streams replication batch frames to a peer follower node over TCP (0xAA protocol).
///
/// Wire format: `[0xAA] [ClusterId: pascal] [Topic: pascal] [Partition: 4b] [Epoch: 8b] [Count: 4b] [RecordFrame...]`
///
/// CRIT-01: cluster_id is prepended immediately after the magic byte so followers can authenticate
/// the sender before touching any partition state.
// The 0xAA packet genuinely carries this many independent fields; bundling them into a
// struct purely to satisfy the lint would add indirection without making the call sites
// clearer.
#[allow(clippy::too_many_arguments)]
pub async fn send_replication_push_pooled(
    peer_conn: &Arc<tokio::sync::Mutex<Option<TcpStream>>>,
    peer_addr: &str,
    cluster_id: &str,
    topic: &str,
    partition: u32,
    epoch: u64,
    leader_hw: u64,
    frames: &[RecordFrame],
) -> IoResult<()> {
    let mut buf = Vec::with_capacity(256 + frames.len() * 64);
    buf.put_u8(0xAA);
    crate::protocol::wire::write_pascal_string(&mut buf, cluster_id);
    crate::protocol::wire::write_pascal_string(&mut buf, topic);
    buf.put_u32(partition);
    buf.put_u64(epoch);
    // The leader's committed high watermark at push time. Followers clamp their own HW to
    // this (see the 0xAA decoder) instead of assuming everything they were pushed is
    // committed — a pushed record is not committed until the ISR has acknowledged it, so a
    // follower that advanced its HW on append was marking uncommitted data as readable.
    //
    // This layout is intentionally frozen (issue #48): it has no version field, so every
    // peer must already agree on it byte-for-byte, and it stays that way — a future field
    // goes on `REPLICATION_PUSH_V2_MAGIC`'s versioned frame instead
    // (`send_replication_push_v2_pooled`), sent only to peers that have advertised support
    // for it. This function keeps serving every other peer, including a pre-versioning one,
    // exactly as it always has.
    buf.put_u64(leader_hw);
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

/// Streams replication batch frames to a peer follower node over TCP using the versioned
/// push frame (issue #48). Sent in place of `send_replication_push_pooled` only once the
/// peer has advertised (via heartbeat) a protocol version that supports it — see
/// `ReplicationManager::replicate_batch`/`broadcast_high_watermark`/`push_frames_to_peer`
/// and `MIN_VERSION_FOR_VERSIONED_PUSH`. Every other peer, including a pre-versioning one,
/// keeps receiving `0xAA` byte-for-byte from the function above, unchanged.
///
/// Wire format: `[0xB0] [frame_version: 1b] [ClusterId: pascal] [Topic: pascal]
/// [Partition: 4b] [Epoch: 8b] [leader_hw: 8b] [Count: 4b] [RecordFrame...]
/// [ext_count: 1b] ([tag: 1b][len: 2b][bytes: len])*`
///
/// The fixed fields are identical, in the same order, to `0xAA` — this frame's purpose
/// isn't to change what's already there, only to make the *next* change safe. That's what
/// the trailing `[ext_count][(tag, len, bytes)]*` section is for: a future field is added
/// as one more tagged entry here, never by widening the fixed header. A receiver that
/// doesn't recognize a tag skips exactly `len` bytes of it and moves on (see
/// `decode_replication_packet_v2` in `server::handler`) — the same tolerant, additive
/// pattern the client protocol already uses for its own tagged fields
/// (`protocol::wire`'s `RequestTags`). Today that section is always empty: no field has
/// needed it yet.
#[allow(clippy::too_many_arguments)]
pub async fn send_replication_push_v2_pooled(
    peer_conn: &Arc<tokio::sync::Mutex<Option<TcpStream>>>,
    peer_addr: &str,
    cluster_id: &str,
    topic: &str,
    partition: u32,
    epoch: u64,
    leader_hw: u64,
    frames: &[RecordFrame],
) -> IoResult<()> {
    let mut buf = Vec::with_capacity(256 + frames.len() * 64);
    buf.put_u8(REPLICATION_PUSH_V2_MAGIC);
    buf.put_u8(INTER_NODE_PROTOCOL_VERSION);
    crate::protocol::wire::write_pascal_string(&mut buf, cluster_id);
    crate::protocol::wire::write_pascal_string(&mut buf, topic);
    buf.put_u32(partition);
    buf.put_u64(epoch);
    buf.put_u64(leader_hw);
    buf.put_u32(frames.len() as u32);
    for frame in frames {
        frame.encode_into(&mut buf);
    }
    // Extension count: always 0 today — see the wire-format doc above for how a future
    // field is added here without breaking a peer still on this exact build.
    buf.put_u8(0u8);

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

/// Issue #48 — inter-node protocol versioning: version discovery over the heartbeat's
/// compatible trailing section, per-peer version bookkeeping, and the `0xAA`-vs-`0xB0`
/// framing decision it drives. See `server::handler`'s tests for the matching coverage of
/// the `0xB0` decoder's own extension section.
#[cfg(test)]
mod protocol_version_tests {
    use super::*;
    use crate::config::ProcessRole;
    use tokio::io::AsyncWriteExt;

    fn test_config(node_id: u32, peer_addrs: Vec<String>) -> ClusterConfig {
        ClusterConfig {
            cluster_id: "test-cluster".to_string(),
            node_id,
            role: NodeRole::Leader,
            peer_addrs,
            min_insync_replicas: 1,
            roles: vec![ProcessRole::Controller, ProcessRole::Broker],
            controller_peer_addrs: vec![],
        }
    }

    fn test_manager(node_id: u32, peer_addrs: Vec<String>) -> ReplicationManager {
        ReplicationManager::new(
            test_config(node_id, peer_addrs.clone()),
            "127.0.0.1:0".to_string(),
            Arc::new(DashMap::new()),
            Arc::new(DashMap::new()),
        )
    }

    // --- read_heartbeat_ack_version: absence, presence, and never-hanging tolerance ---

    #[tokio::test]
    async fn read_heartbeat_ack_version_reads_a_present_byte() {
        let (mut reader, mut writer) = tokio::io::duplex(64);
        writer.write_all(&[7u8]).await.unwrap();
        assert_eq!(read_heartbeat_ack_version(&mut reader).await, 7);
        drop(writer);
    }

    #[tokio::test]
    async fn read_heartbeat_ack_version_treats_a_closed_connection_as_pre_versioning() {
        // A function-level property only: if nothing more ever arrives on this stream,
        // this read must resolve to 0 rather than error the whole ACK read. This does NOT
        // model a supported pre-`1` peer — see `INTER_NODE_PROTOCOL_VERSION`'s doc for why
        // a genuinely pre-`1` peer never gets a connection into this state in the first
        // place (it's read back as a status byte before this function is ever called).
        let (mut reader, writer) = tokio::io::duplex(64);
        drop(writer);
        assert_eq!(read_heartbeat_ack_version(&mut reader).await, 0);
    }

    #[tokio::test]
    async fn read_heartbeat_ack_version_resolves_within_its_bound_on_an_idle_connection() {
        // The connection stays open but nothing more ever arrives — the harder case than
        // an outright close, and the one `HEARTBEAT_EXT_READ_TIMEOUT` exists for: this
        // must resolve on its own, not hang the caller forever.
        let (mut reader, writer) = tokio::io::duplex(64);
        let started = std::time::Instant::now();
        assert_eq!(read_heartbeat_ack_version(&mut reader).await, 0);
        assert!(
            started.elapsed() < HEARTBEAT_EXT_READ_TIMEOUT + Duration::from_secs(1),
            "must not block past its own bound"
        );
        drop(writer);
    }

    // --- per-peer version bookkeeping ---

    #[tokio::test]
    async fn peer_protocol_version_defaults_to_zero_for_an_unknown_peer() {
        let mgr = test_manager(1, vec![]);
        assert_eq!(mgr.peer_protocol_version("127.0.0.1:1"), 0);
        assert_eq!(mgr.peer_protocol_version_by_node_id(99), 0);
    }

    #[tokio::test]
    async fn note_peer_version_is_retrievable_by_node_id_and_by_address() {
        let mgr = test_manager(1, vec![]);
        mgr.broker_addrs.insert(2, "127.0.0.1:9999".to_string());
        mgr.note_peer_version(2, INTER_NODE_PROTOCOL_VERSION);
        assert_eq!(
            mgr.peer_protocol_version_by_node_id(2),
            INTER_NODE_PROTOCOL_VERSION
        );
        assert_eq!(
            mgr.peer_protocol_version("127.0.0.1:9999"),
            INTER_NODE_PROTOCOL_VERSION
        );
    }

    #[tokio::test]
    async fn note_peer_version_is_not_monotonic() {
        // A peer's advertised version is a live fact about the build it's currently
        // running, not a high-water mark — a downgrade/restart must be reflected.
        let mgr = test_manager(1, vec![]);
        mgr.note_peer_version(5, 9);
        mgr.note_peer_version(5, 1);
        assert_eq!(mgr.peer_protocol_version_by_node_id(5), 1);
    }

    // --- the key interop guarantee: which magic byte a push actually uses ---

    /// Spins up a bare TCP listener that captures the first byte (the magic) of whatever
    /// it's sent, ACKs with a plain success byte so the sender's write path completes
    /// normally, and reports the captured magic back over the returned channel.
    async fn spawn_magic_capturing_peer() -> (String, tokio::sync::oneshot::Receiver<u8>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut magic = [0u8; 1];
                if sock.read_exact(&mut magic).await.is_ok() {
                    let _ = tx.send(magic[0]);
                }
                let _ = sock.write_all(&[0u8]).await;
            }
        });
        (addr, rx)
    }

    #[tokio::test]
    async fn push_uses_legacy_0xaa_for_a_peer_that_has_never_advertised_a_version() {
        // NOTE: this only proves the send-side framing default is safe when this node's
        // own bookkeeping has no confirmed version for a peer — a defensive fallback for
        // an incomplete/in-progress upgrade, not a claim that a genuinely pre-`1` peer is
        // supported (heartbeat exchange with one is unsafe in both directions; see
        // `INTER_NODE_PROTOCOL_VERSION`'s doc).
        let (peer_addr, magic_rx) = spawn_magic_capturing_peer().await;
        let mgr = test_manager(1, vec![peer_addr]);
        let frame = RecordFrame::create(0, 0, b"hello".to_vec());
        let _ = mgr
            .replicate_batch("__cluster_metadata", 0, 0, 0, &[], &[frame])
            .await;

        let magic = tokio::time::timeout(Duration::from_secs(2), magic_rx)
            .await
            .expect("peer never received a byte")
            .unwrap();
        assert_eq!(
            magic, 0xAA,
            "a pre-versioning peer must keep receiving the original frame byte-for-byte"
        );
    }

    #[tokio::test]
    async fn push_uses_the_versioned_frame_for_a_peer_that_advertised_support() {
        let (peer_addr, magic_rx) = spawn_magic_capturing_peer().await;
        let mgr = test_manager(1, vec![peer_addr.clone()]);
        mgr.broker_addrs.insert(2, peer_addr);
        mgr.note_peer_version(2, INTER_NODE_PROTOCOL_VERSION);

        let frame = RecordFrame::create(0, 0, b"hello".to_vec());
        let _ = mgr
            .replicate_batch("__cluster_metadata", 0, 0, 0, &[], &[frame])
            .await;

        let magic = tokio::time::timeout(Duration::from_secs(2), magic_rx)
            .await
            .expect("peer never received a byte")
            .unwrap();
        assert_eq!(
            magic, REPLICATION_PUSH_V2_MAGIC,
            "a peer that advertised support must receive the versioned frame"
        );
    }
}
