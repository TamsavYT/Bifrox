use crate::replication::NodeRole;
use std::fs;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A node's process role(s) — the `process.roles` setting (`controller`,
/// `broker`, or `broker,controller`). A node with only `Controller` participates in the
/// metadata Raft quorum but never hosts data-topic partitions; a node with only `Broker`
/// hosts data partitions and replicates `__cluster_metadata` as a non-voting observer but
/// never contests leadership of it; a node with both (the historical Bifrox default)
/// does everything, same as today's combined-mode behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessRole {
    Controller,
    Broker,
}

impl std::str::FromStr for ProcessRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "controller" => Ok(ProcessRole::Controller),
            "broker" => Ok(ProcessRole::Broker),
            _ => Err(format!("Unknown process role: '{}'", s)),
        }
    }
}

/// Parses a comma-separated `process.roles` value (e.g. `"broker,controller"`,
/// `"controller"`, `"broker"`) as written in the properties file. An empty/unparseable
/// value falls back to combined mode (both roles) — the historical Bifrox default,
/// so existing configs that never set this keep working unchanged.
pub fn parse_process_roles(s: &str) -> Vec<ProcessRole> {
    let roles: Vec<ProcessRole> = s
        .split(',')
        .filter_map(|part| part.parse::<ProcessRole>().ok())
        .collect();
    if roles.is_empty() {
        vec![ProcessRole::Controller, ProcessRole::Broker]
    } else {
        roles
    }
}

impl ProcessRole {
    pub fn to_byte(self) -> u8 {
        match self {
            ProcessRole::Controller => 1,
            ProcessRole::Broker => 2,
        }
    }

    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(ProcessRole::Controller),
            2 => Some(ProcessRole::Broker),
            _ => None,
        }
    }
}

pub fn roles_to_bytes(roles: &[ProcessRole]) -> Vec<u8> {
    roles.iter().map(|r| r.to_byte()).collect()
}

/// Inverse of `roles_to_bytes`. Empty or all-unrecognized input means "unknown" and maps
/// to combined mode (both roles) — matches how `BrokerRegister` records written before
/// this field existed are interpreted on replay.
pub fn parse_process_role_bytes(bytes: &[u8]) -> Vec<ProcessRole> {
    let roles: Vec<ProcessRole> = bytes
        .iter()
        .filter_map(|&b| ProcessRole::from_byte(b))
        .collect();
    if roles.is_empty() {
        vec![ProcessRole::Controller, ProcessRole::Broker]
    } else {
        roles
    }
}

/// Largest segment size the sparse index can address. Index entries hold a 4-byte byte
/// position within their segment, so anything beyond this would wrap (see
/// `IndexSegment::append`).
pub const MAX_ADDRESSABLE_SEGMENT_BYTES: u64 = u32::MAX as u64;

/// WAL Durability Flush Policy
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushPolicy {
    /// Calls `file.sync_data()` after writing an in-memory batch.
    SyncEveryBatch,
    /// Flushes in-memory buffers to disk asynchronously on time interval or byte threshold.
    AsyncPeriodic {
        interval: Duration,
        max_bytes: usize,
    },
    /// Direct OS pass-through writes and syncs per record batch.
    UnbufferedSync,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        FlushPolicy::AsyncPeriodic {
            interval: Duration::from_millis(5),
            max_bytes: 64 * 1024, // 64 KB
        }
    }
}

/// Share-group state durability knob (`share.group.state.sync`) —
/// `ShareGroupManager::persist_partition_state` runs on every acquire, acknowledge, and
/// lock-timeout sweep, so how hard each of those pushes its write toward the platter before
/// returning is a direct trade against share-group throughput, not a free safety upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ShareStateSyncPolicy {
    /// `flush()` only — the write reaches the OS page cache and nothing further. A process
    /// restart sees everything that was flushed; a machine crash (power loss, kernel panic,
    /// the whole host disappearing) can still lose whatever the OS had not written back to
    /// disk yet. **This is not crash-durable.** It remains the default because it is the
    /// behavior this manager already had before this option existed, not because it is safe
    /// against a hard crash — do not read "default" as "durable" here.
    #[default]
    Buffered,
    /// Calls `sync_data()` after every persisted record. True crash durability for each
    /// acquire, acknowledge, and lock-timeout sweep — but this file is written on every one
    /// of those, so an unconditional sync on each is a hard ceiling on share-group
    /// throughput. That cost, not an oversight, is why this is not the default; pick it when
    /// losing any record's tail state is unacceptable and the throughput cost is affordable.
    EveryWrite,
    /// Calls `sync_data()` at most once per `Duration`, no matter how many persists land in
    /// between. Bounds the crash-loss window to roughly one interval without paying a sync
    /// on every write — the same bounded-risk-for-throughput trade `FlushPolicy::
    /// AsyncPeriodic` already makes for the WAL, applied here to share-group state instead.
    Interval(Duration),
}

/// `compression.type`.
///
/// Read this alongside how compression actually works here: **the broker never compresses
/// or decompresses a client's records.** A producer builds and compresses its own
/// [`crate::protocol::RecordBatch`], the broker stores those bytes as they arrive, and the
/// consumer decompresses. Compaction is the sole exception, because it cannot compare keys
/// without reading them.
///
/// So this setting does *not* decide how client data is stored — the producer's own codec
/// does, whatever this says. It applies only to records the broker authors itself: the
/// single frames written to internal system partitions (cluster metadata, DLQ routing,
/// consumer offsets, transaction state, bootstrap) via `PartitionManager::produce_frame`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionCodec {
    /// Keep whatever the producer sent, imposing nothing. The default for
    /// `compression.type`, and the only setting that describes the produce path honestly —
    /// the broker has no codec of its own to apply to a client's batch.
    ///
    /// Broker-authored frames are written uncompressed under this setting: there is no
    /// producer whose choice could be honoured for a record the broker wrote itself.
    #[default]
    Producer = 3,
    /// Broker-authored frames are stored uncompressed. Identical to [`Self::Producer`] for
    /// client data, which is never touched either way.
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl CompressionCodec {
    /// How a broker-authored frame should be compressed under this setting. `Producer` has
    /// no producer to defer to, so it behaves as uncompressed.
    pub fn for_broker_authored_frame(self) -> Self {
        match self {
            CompressionCodec::Producer => CompressionCodec::None,
            other => other,
        }
    }
}

impl std::fmt::Display for CompressionCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionCodec::Producer => write!(f, "producer"),
            CompressionCodec::None => write!(f, "none"),
            CompressionCodec::Lz4 => write!(f, "lz4"),
            CompressionCodec::Zstd => write!(f, "zstd"),
        }
    }
}

impl std::str::FromStr for CompressionCodec {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.trim().to_lowercase();
        match normalized.as_str() {
            // "producer" is its own thing, not an alias for "none": it says the broker
            // imposes no codec, which is the truth for every client-produced batch.
            "producer" => Ok(CompressionCodec::Producer),
            "none" | "" | "uncompressed" => Ok(CompressionCodec::None),
            "lz4" => Ok(CompressionCodec::Lz4),
            "zstd" => Ok(CompressionCodec::Zstd),
            _ => Err(format!("Unknown compression.type: '{}'", s)),
        }
    }
}

/// Topic and Server Log Cleanup Policy (cleanup.policy)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanupPolicy {
    /// Time/size based retention (old log segments exceeding retention limit are unlinked)
    #[default]
    Delete,
    /// Key-based log compaction (retains latest record per key in historical log segments)
    Compact,
    /// Both key-based compaction and time/size retention apply
    CompactAndDelete,
}

impl std::str::FromStr for CleanupPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.trim().to_lowercase();
        match normalized.as_str() {
            "delete" => Ok(CleanupPolicy::Delete),
            "compact" => Ok(CleanupPolicy::Compact),
            "compact,delete" | "delete,compact" => Ok(CleanupPolicy::CompactAndDelete),
            _ => Err(format!("Unknown cleanup.policy: '{}'", s)),
        }
    }
}

impl std::fmt::Display for CleanupPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CleanupPolicy::Delete => write!(f, "delete"),
            CleanupPolicy::Compact => write!(f, "compact"),
            CleanupPolicy::CompactAndDelete => write!(f, "compact,delete"),
        }
    }
}

impl CleanupPolicy {
    pub fn is_compact(&self) -> bool {
        matches!(
            self,
            CleanupPolicy::Compact | CleanupPolicy::CompactAndDelete
        )
    }

    pub fn is_delete(&self) -> bool {
        matches!(
            self,
            CleanupPolicy::Delete | CleanupPolicy::CompactAndDelete
        )
    }
}

/// Security Protocol (security.protocol)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecurityProtocol {
    #[default]
    Plaintext,
    SaslPlaintext,
    Ssl,
    SaslSsl,
}

impl std::str::FromStr for SecurityProtocol {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.trim().to_uppercase();
        match normalized.as_str() {
            "PLAINTEXT" => Ok(SecurityProtocol::Plaintext),
            "SASL_PLAINTEXT" => Ok(SecurityProtocol::SaslPlaintext),
            "SSL" => Ok(SecurityProtocol::Ssl),
            "SASL_SSL" => Ok(SecurityProtocol::SaslSsl),
            _ => Err(format!("Unknown security.protocol: '{}'", s)),
        }
    }
}

impl std::fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityProtocol::Plaintext => write!(f, "PLAINTEXT"),
            SecurityProtocol::SaslPlaintext => write!(f, "SASL_PLAINTEXT"),
            SecurityProtocol::Ssl => write!(f, "SSL"),
            SecurityProtocol::SaslSsl => write!(f, "SASL_SSL"),
        }
    }
}

/// Engine Configuration Parameters
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Cluster ID for cluster membership verification (cluster.id)
    pub cluster_id: String,
    /// Cluster Node ID (broker.id)
    pub node_id: u32,
    /// Node HA Cluster Role (leader or follower)
    pub role: NodeRole,
    /// Base directory for storage data logs and indexes (log.dirs)
    pub data_dir: PathBuf,
    /// Maximum byte size for a single log segment file before rotation (default 10MB)
    pub max_segment_bytes: u64,
    /// Byte interval for inserting sparse binary index entries (e.g. 4KB)
    pub index_interval_bytes: u64,
    /// Durability flush strategy
    pub flush_policy: FlushPolicy,
    /// Whether to pre-allocate segment files using `set_len` to prevent NTFS fragmentation
    pub preallocate_segments: bool,
    /// Directory for server diagnostic log files (log.file.dir)
    pub log_file_dir: PathBuf,
    /// Server bind socket address (listeners)
    pub bind_addr: String,
    /// Explicit override for the address this node advertises to peers and clients as
    /// its own identity (`advertised.listeners`) — in inter-node heartbeats,
    /// heartbeat ACKs, and `BrokerRegister` metadata records. Takes precedence over the
    /// TCP listener's actual bound address when set, which is what makes an address
    /// expressible at all when `bind_addr` is a wildcard host (`0.0.0.0`) or sits behind
    /// NAT/a load balancer, where the bound address itself is not what a peer should
    /// dial. `None` (the default) derives the advertised address from the listener's
    /// real bound address once it's known — see `StorageEngine::finalize_advertised_addr`
    /// and issue #62.
    pub advertised_addr: Option<String>,
    /// Peer node addresses for HA cluster replication
    pub peer_addrs: Vec<String>,
    /// Minimum In-Sync Replicas required for write commits
    pub min_insync_replicas: usize,
    /// Default replication factor for newly created topics (default.replication.factor)
    pub default_replication_factor: u16,
    /// Log Retention threshold in total bytes per partition (optional)
    pub retention_bytes: Option<u64>,
    /// Log Retention threshold in milliseconds (optional, e.g. 24 hours)
    pub retention_millis: Option<u64>,
    /// Interval for running background retention garbage collector
    pub retention_check_interval: Duration,
    /// Server-default Log Cleanup Policy (cleanup.policy / log.cleanup.policy)
    pub cleanup_policy: CleanupPolicy,
    /// Security Protocol (PLAINTEXT, SASL_PLAINTEXT, SSL, SASL_SSL)
    pub security_protocol: SecurityProtocol,
    /// TLS/SSL X.509 Certificate file path (ssl.keystore.location / ssl.cert.path)
    pub ssl_cert_path: Option<PathBuf>,
    /// TLS/SSL Private Key file path (ssl.keystore.password / ssl.key.path)
    pub ssl_key_path: Option<PathBuf>,
    /// TLS/SSL CA Certificate file path (ssl.truststore.location / ssl.ca.path)
    pub ssl_ca_path: Option<PathBuf>,
    /// TLS/SSL Client Authentication mode (ssl.client.auth: none, requested, required)
    pub ssl_client_auth: String,
    /// Enabled SASL mechanisms (e.g. PLAIN, SCRAM-SHA-256)
    pub sasl_mechanisms: Vec<String>,
    /// Legacy bootstrap SASL user accounts (username -> password). At startup Bifrox
    /// imports these into the persistent SCRAM credential store if that user does not
    /// already exist there.
    pub sasl_users: std::collections::HashMap<String, String>,
    /// Whether ACL authorization is enabled
    pub acls_enabled: bool,
    /// Superuser principals exempt from ACL enforcement
    pub super_users: Vec<String>,
    /// Shared-secret token required from client connections (None = no auth check).
    /// Inter-node peers identified by `peer_addrs` are exempt from this check.
    pub auth_token: Option<String>,
    /// Default per-client produce byte-rate quota in bytes/sec
    /// (`quota.producer.default.bytes.per.second`). `None` = unlimited (default).
    /// Clients exceeding this rate have their produce response delayed rather than
    /// rejected.
    pub produce_quota_bytes_per_sec: Option<u64>,
    /// Default per-client fetch byte-rate quota in bytes/sec
    /// (`quota.consumer.default.bytes.per.second`). `None` = unlimited (default).
    pub fetch_quota_bytes_per_sec: Option<u64>,
    /// Compression Codec for Record Batch Storage (`compression.type`)
    pub compression_codec: CompressionCodec,
    /// Configurable bind address for Prometheus metrics endpoint (e.g. "0.0.0.0:9090")
    pub metrics_bind_addr: Option<String>,
    /// Optional Bearer token for Prometheus metrics scrape authentication
    pub metrics_auth_token: Option<String>,
    /// Optional network IP whitelist for Prometheus metrics scrape endpoint
    pub metrics_allowed_ips: Vec<String>,
    /// How long a replica may go without acknowledging a replicated write before the
    /// partition leader drops it from the ISR (`replica.lag.time.max.ms`).
    pub replica_lag_max_ms: u64,
    /// How often the ISR-membership and broker-liveness sweep runs.
    pub isr_check_interval_ms: u64,
    /// How long a broker may go without a heartbeat ACK before the cluster leader
    /// considers it dead and, for any partition it currently leads, elects a new leader
    /// from the remaining ISR.
    pub broker_down_threshold_ms: u64,
    /// Whether a partition may fail over to a replica outside the last-known ISR when no
    /// in-sync replica survives a leader's death (`unclean.leader.election.enable`).
    /// Defaults to false: an unrecoverable partition is left leaderless rather than
    /// silently accepting data loss.
    pub allow_unclean_leader_election: bool,
    /// This node's process role(s) — `process.roles`. Defaults to both
    /// (combined mode, today's historical behavior).
    pub roles: Vec<ProcessRole>,
    /// The subset of `peer_addrs` that are controller-eligible (participate in the
    /// metadata Raft quorum) — `controller.quorum.voters`. Only meaningful when
    /// this node itself has the `Controller` role. Empty means "assume every peer is
    /// controller-eligible", which matches combined-mode clusters where every node votes.
    pub controller_peer_addrs: Vec<String>,
    /// How long a tombstone (a compacted-topic record whose value is *null* — Bifrox's
    /// convention for a "delete marker") is kept as the latest record for its
    /// key before log compaction erases the key entirely (`delete.retention.ms`).
    /// `None` disables tombstone expiry: tombstones are kept forever, same as any other
    /// record, once written (today's behavior).
    pub delete_retention_millis: Option<u64>,
    /// Minimum fraction of a historical segment's bytes that must be "dirty" (superseded
    /// by a newer record for the same key) before log compaction will rewrite that segment
    /// (`min.cleanable.dirty.ratio`). Segments below this ratio are left untouched on
    /// a given compaction pass, avoiding low-value rewrite I/O.
    pub min_cleanable_dirty_ratio: f64,
    /// Maximum number of partitions whose retention/compaction pass may run concurrently
    /// within a single GC tick (`log.cleaner.threads`).
    pub compaction_worker_threads: usize,
    /// Maximum age (ms) of the active segment before it's rolled purely due to time,
    /// independent of `max_segment_bytes` (`segment.ms`). `None` disables time-based
    /// rolling — a low-volume topic's active segment then only rotates once it hits the
    /// byte threshold, which may be never.
    pub segment_ms: Option<u64>,
    /// Maximum accepted size (bytes) of a single record payload
    /// (`message.max.bytes`). `None` means no explicit limit beyond what already applies
    /// (segment size, wire framing limits).
    pub message_max_bytes: Option<u64>,
    /// Worker threads dedicated to the Tokio runtime driving network I/O
    /// (`num.network.threads`, approximately — Bifrox doesn't separate network from
    /// request-handling threads, so this sizes the whole async runtime).
    /// `None` uses Tokio's own default (one per logical CPU).
    pub num_network_threads: Option<usize>,
    /// Whether the controller publishes a replica assignment for partitions that don't
    /// have one (see `StorageEngine::reconcile_unassigned_partitions`).
    ///
    /// A partition created implicitly by a produce gets no `PartitionLeadershipChange`
    /// record, so cluster metadata never learns who should hold it. Such a partition has
    /// no ISR to manage and nothing to fail over to — a follower may physically hold a
    /// complete copy while metadata does not consider it a replica at all. This sweep
    /// retrofits an assignment onto those partitions without moving any data.
    ///
    /// Turning it off leaves implicitly-created partitions unreplicated and unable to fail
    /// over, which is the pre-existing behavior.
    pub auto_assign_partitions_enable: bool,
    /// How long a consumer group's join window stays open for further members to arrive
    /// before the coordinator forms the generation and replies to everyone waiting
    /// (`group.initial.rebalance.delay.ms`).
    ///
    /// Raising it lets a larger group settle into a single generation at the cost of
    /// slower first assignment; 0 disables the barrier entirely and restores the old
    /// join-immediately behavior.
    pub group_initial_rebalance_delay_ms: u64,
    /// How long a consumer group member may go without making fetch progress — a `Fetch`
    /// attributed to it via the request envelope's `GROUP_MEMBER` tag — before the
    /// coordinator evicts it for stalling even though it keeps heartbeating on schedule
    /// (`max.poll.interval.ms`; issue #54: a member deadlocked, stuck on a poisoned
    /// record, or blocked on a downstream call still reports itself healthy through
    /// heartbeats alone).
    ///
    /// Defaults to five minutes.
    pub max_poll_interval_ms: u64,
    /// How long the broker leases an acquired record to a share-group member before it
    /// becomes eligible for redelivery to someone else (`share.group.lock.timeout.ms`).
    ///
    /// A member may override this per request via `ShareFetch`'s own `lock_timeout_ms`;
    /// this value is only what a member gets when it sends `0` there, declining to pick a
    /// duration itself (see `StorageEngine::share_fetch`). Too low and a record gets
    /// redelivered to someone else while the original member is still legitimately working
    /// it — duplicate processing; too high and a crashed or stuck member's records stay
    /// stuck with it for that long before anyone else can pick them up.
    pub share_group_lock_timeout_ms: u64,
    /// How many times a share-group record may have its *lease expire* before it is
    /// archived and routed to `<topic>-dlq` instead of being offered again
    /// (`share.group.max.delivery.attempts`). `delivery_count` starts at 1 on first
    /// delivery, so a value of `1` means no second chance: the first expired lease sends
    /// the record straight to the DLQ.
    ///
    /// This governs the lease-expiry path only — `SharePartition::check_lock_timeouts` is
    /// the sole place the count is compared. An explicit `Reject` bypasses it and archives
    /// on the spot, which is the point of rejecting rather than letting a lease lapse: the
    /// member is asserting the record is bad, not that it failed to finish in time. An
    /// explicit `Release` does not consult it either — a released record simply returns to
    /// `Available`, so a member that keeps releasing the same record keeps getting it back.
    ///
    /// Too low and a member that merely crashed mid-record sidelines it after one lapse;
    /// too high and a record no one can finish is redelivered many times before the DLQ
    /// ever sees it.
    pub share_group_max_delivery_attempts: u16,
    /// How hard `ShareGroupManager` pushes its state file toward disk on every acquire,
    /// acknowledge, and lock-timeout sweep (`share.group.state.sync`). See
    /// [`ShareStateSyncPolicy`] for what each level actually buys — the default,
    /// `Buffered`, survives a process restart but **not** a machine crash.
    pub share_state_sync_policy: ShareStateSyncPolicy,
    /// Whether a topic may be created implicitly by a *produce* to a topic that doesn't
    /// exist yet (`auto.create.topics.enable`). Defaults to **true**, matching Bifrox's
    /// long-standing behavior.
    ///
    /// This is deliberately not the whole answer to unbounded topic creation, and is not
    /// the part doing the security work. The two changes that actually bound it are:
    /// read paths never create anything at all (a `Fetch` for an unknown topic is a
    /// lookup, not a write — see `StorageEngine::partition_for_read`), and
    /// `max_partitions_per_topic`/`max_partitions_per_broker` cap how much can exist no
    /// matter who asks. With those in place, implicit creation is a bounded, operator-
    /// visible resource rather than an unbounded one, so defaulting it off would break
    /// every existing deployment for no remaining security benefit. Operators who want
    /// creation to be strictly explicit can still set this to false.
    pub auto_create_topics_enable: bool,
    /// Maximum partitions accepted for a single topic (`num.partitions` upper
    /// bound). Requests asking for more are rejected rather than creating the directories.
    pub max_partitions_per_topic: u32,
    /// Maximum total partitions this broker will host across all topics. Once reached,
    /// creating a new partition fails instead of consuming more inodes.
    pub max_partitions_per_broker: usize,
    /// How long a transaction restored from `__transaction_state` on startup (still
    /// `Ongoing`/`PrepareCommit`/`PrepareAbort` — i.e. never reached a terminal state
    /// before the last shutdown) is given before it's presumed abandoned and aborted to
    /// release the partitions it was blocking (`transaction.max.timeout.ms`, applied
    /// here at replay time since Bifrox has no live producer session to time out against
    /// across a restart).
    pub transaction_timeout_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            cluster_id: "bifrox-prod-cluster-01".to_string(),
            node_id: 1,
            role: NodeRole::Leader,
            data_dir: PathBuf::from("./data"),
            max_segment_bytes: 10 * 1024 * 1024, // 10 MB per segment
            index_interval_bytes: 4096,          // 4 KB sparse index interval
            flush_policy: FlushPolicy::default(),
            // `log.preallocate` defaults to `false` here: pre-touching the full
            // segment size up front helps HDDs avoid fragmentation, but on SSDs (the
            // common case today) it just means writing (and later trimming) bytes nobody
            // asked for. Still fully configurable via `preallocate.segments` for anyone
            // who wants the old NTFS-fragmentation-avoidance behavior back.
            preallocate_segments: false,
            log_file_dir: PathBuf::from("./logs"),
            bind_addr: "127.0.0.1:9092".to_string(),
            advertised_addr: None,
            peer_addrs: Vec::new(),
            min_insync_replicas: 1,
            // Clamped to the number of available brokers at assignment time, so a
            // single-node deployment still lands on 1.
            default_replication_factor: 3,
            segment_ms: None,
            message_max_bytes: None,
            num_network_threads: None,
            auto_assign_partitions_enable: true,
            group_initial_rebalance_delay_ms: 3_000,
            max_poll_interval_ms: 300_000,        // 5 minutes
            share_group_lock_timeout_ms: 30_000,  // 30 seconds
            share_group_max_delivery_attempts: 5, // attempts
            share_state_sync_policy: ShareStateSyncPolicy::default(),
            auto_create_topics_enable: true,
            max_partitions_per_topic: 10_000,
            max_partitions_per_broker: 200_000,
            transaction_timeout_ms: 60_000,           // 60 seconds
            retention_bytes: Some(100 * 1024 * 1024), // 100 MB retention limit
            retention_millis: Some(86400 * 1000),     // 24 hours retention
            retention_check_interval: Duration::from_secs(10),
            cleanup_policy: CleanupPolicy::Delete,
            security_protocol: SecurityProtocol::Plaintext,
            ssl_cert_path: None,
            ssl_key_path: None,
            ssl_ca_path: None,
            ssl_client_auth: "none".to_string(),
            sasl_mechanisms: vec![
                "PLAIN".to_string(),
                "SCRAM-SHA-256".to_string(),
                "SCRAM-SHA-512".to_string(),
            ],
            sasl_users: std::collections::HashMap::new(),
            acls_enabled: false,
            super_users: Vec::new(),
            auth_token: None,
            produce_quota_bytes_per_sec: None,
            fetch_quota_bytes_per_sec: None,
            compression_codec: CompressionCodec::default(),
            metrics_bind_addr: None,
            metrics_auth_token: None,
            metrics_allowed_ips: Vec::new(),
            replica_lag_max_ms: 10_000,
            isr_check_interval_ms: 2_000,
            broker_down_threshold_ms: 30_000,
            allow_unclean_leader_election: false,
            roles: vec![ProcessRole::Controller, ProcessRole::Broker],
            controller_peer_addrs: Vec::new(),
            delete_retention_millis: Some(24 * 60 * 60 * 1000), // 24 hours
            min_cleanable_dirty_ratio: 0.5,
            compaction_worker_threads: 4,
        }
    }
}

impl EngineConfig {
    pub fn is_controller_role(&self) -> bool {
        self.roles.contains(&ProcessRole::Controller)
    }

    pub fn is_broker_role(&self) -> bool {
        self.roles.contains(&ProcessRole::Broker)
    }

    /// The peers this node should treat as controller-eligible for Raft election/
    /// heartbeat purposes.
    ///
    /// An empty `controller_peer_addrs` is ambiguous on its own — it could mean "not
    /// configured, please infer it" or "there are genuinely zero peer controllers" (a
    /// real, valid topology: one controller node plus N broker-only nodes). To resolve
    /// that: the fallback to `peer_addrs` verbatim (today's combined-mode assumption,
    /// where every peer votes) only applies when this node's own `roles` is still the
    /// untouched default (both Controller and Broker) — i.e. an existing config that
    /// never mentions either new field keeps working exactly as before. The moment an
    /// operator opts into role separation (`roles` is anything other than the default),
    /// `controller_peer_addrs` is taken literally, empty included, so a lone controller
    /// with only broker-only peers correctly computes a quorum of one instead of
    /// mistaking its brokers for fellow voters.
    pub fn effective_controller_peer_addrs(&self) -> Vec<String> {
        let is_default_combined_roles = self.roles.contains(&ProcessRole::Controller)
            && self.roles.contains(&ProcessRole::Broker);
        if self.controller_peer_addrs.is_empty() && is_default_combined_roles {
            self.peer_addrs.clone()
        } else {
            self.controller_peer_addrs.clone()
        }
    }
}

impl EngineConfig {
    /// Loads configuration from a `server.properties` file
    pub fn from_properties_file(path: impl AsRef<Path>) -> IoResult<Self> {
        let content = fs::read_to_string(path)?;
        let mut config = Self::default();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }

            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim();

                if let Some(username) = key.strip_prefix("sasl.user.") {
                    config
                        .sasl_users
                        .insert(username.trim().to_string(), value.to_string());
                    continue;
                }

                match key {
                    "cluster.id" => {
                        config.cluster_id = value.to_string();
                    }
                    "node.id" | "broker.id" => {
                        if let Ok(v) = value.parse() {
                            config.node_id = v;
                        }
                    }
                    "role" => {
                        config.role = match value.to_lowercase().as_str() {
                            "follower" => NodeRole::Follower,
                            _ => NodeRole::Leader,
                        };
                    }
                    "listeners" | "bind.addr" => {
                        let clean_addr = value
                            .strip_prefix("PLAINTEXT://")
                            .or_else(|| value.strip_prefix("SASL_PLAINTEXT://"))
                            .or_else(|| value.strip_prefix("SSL://"))
                            .or_else(|| value.strip_prefix("SASL_SSL://"))
                            .unwrap_or(value);
                        config.bind_addr = clean_addr.to_string();
                    }
                    "advertised.listeners" | "advertised.bind.addr" => {
                        let clean_addr = value
                            .strip_prefix("PLAINTEXT://")
                            .or_else(|| value.strip_prefix("SASL_PLAINTEXT://"))
                            .or_else(|| value.strip_prefix("SSL://"))
                            .or_else(|| value.strip_prefix("SASL_SSL://"))
                            .unwrap_or(value);
                        config.advertised_addr = Some(clean_addr.to_string());
                    }
                    "security.protocol" => {
                        if let Ok(sp) = value.parse() {
                            config.security_protocol = sp;
                        }
                    }
                    "ssl.keystore.location" | "ssl.cert.path" => {
                        config.ssl_cert_path = Some(PathBuf::from(value));
                    }
                    "ssl.keystore.password" | "ssl.key.path" => {
                        config.ssl_key_path = Some(PathBuf::from(value));
                    }
                    "ssl.truststore.location" | "ssl.ca.path" => {
                        config.ssl_ca_path = Some(PathBuf::from(value));
                    }
                    "ssl.client.auth" => {
                        config.ssl_client_auth = value.to_lowercase();
                    }
                    "sasl.enabled.mechanisms" | "sasl.mechanism.inter.broker.protocol" => {
                        config.sasl_mechanisms = value
                            .split(',')
                            .map(|s| s.trim().to_uppercase())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                    "acls.enabled" | "authorizer.class.name" => {
                        if value.eq_ignore_ascii_case("true")
                            || value.contains("AclAuthorizer")
                            || value.contains("StandardAuthorizer")
                        {
                            config.acls_enabled = true;
                        } else if let Ok(v) = value.parse::<bool>() {
                            config.acls_enabled = v;
                        }
                    }
                    "super.users" => {
                        config.super_users = value
                            .split(';')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                    "log.dirs" | "data.dir" => {
                        config.data_dir = PathBuf::from(value);
                    }
                    "log.file.dir" | "logs.dir" | "server.log.dir" => {
                        config.log_file_dir = PathBuf::from(value);
                    }
                    "max.segment.bytes" | "segment.bytes" => {
                        if let Ok(v) = value.parse::<u64>() {
                            // Clamp to what a sparse-index entry can address. The index
                            // stores a 4-byte position within the segment, so a larger
                            // segment would produce positions that wrap and point at the
                            // wrong bytes. Clamping (loudly) is better than accepting a
                            // value that silently corrupts lookups past the 4 GiB mark.
                            if v > MAX_ADDRESSABLE_SEGMENT_BYTES {
                                eprintln!(
                                    "config: segment.bytes {} exceeds the {} byte index limit — clamping",
                                    v, MAX_ADDRESSABLE_SEGMENT_BYTES
                                );
                                config.max_segment_bytes = MAX_ADDRESSABLE_SEGMENT_BYTES;
                            } else {
                                config.max_segment_bytes = v;
                            }
                        }
                    }
                    "segment.ms" => {
                        if let Ok(v) = value.parse() {
                            config.segment_ms = Some(v);
                        }
                    }
                    "message.max.bytes" | "max.message.bytes" => {
                        if let Ok(v) = value.parse() {
                            config.message_max_bytes = Some(v);
                        }
                    }
                    "num.network.threads" => {
                        if let Ok(v) = value.parse::<usize>() {
                            config.num_network_threads = Some(v.max(1));
                        }
                    }
                    "group.initial.rebalance.delay.ms" => {
                        if let Ok(v) = value.parse::<u64>() {
                            config.group_initial_rebalance_delay_ms = v;
                        }
                    }
                    "max.poll.interval.ms" => {
                        if let Ok(v) = value.parse::<u64>() {
                            config.max_poll_interval_ms = v;
                        }
                    }
                    "share.group.lock.timeout.ms" => {
                        if let Ok(v) = value.parse::<u64>() {
                            // A 0ms lease would expire the instant it's granted, so an
                            // operator-supplied 0 is a typo, not an intent — clamp to the
                            // floor rather than reject the file (same convention as
                            // `log.cleaner.threads` below).
                            config.share_group_lock_timeout_ms = v.max(1);
                        }
                    }
                    "share.group.max.delivery.attempts" => {
                        if let Ok(v) = value.parse::<u16>() {
                            // `check_lock_timeouts` compares with `>=`, so 0 and 1 both
                            // still deliver once — 0 would just be a dishonest way to spell
                            // 1. Clamp to the real floor instead of accepting a value that
                            // doesn't mean what it says.
                            config.share_group_max_delivery_attempts = v.max(1);
                        }
                    }
                    // `share.group.state.sync`: "buffered" (the default) leaves the write in
                    // the OS page cache — a process restart survives it, a machine crash can
                    // still lose the tail. "always" forces `sync_data()` after every persist —
                    // real crash durability, paid on every acquire/acknowledge/sweep.
                    // "interval" bounds that cost to at most one sync per window; if
                    // `share.group.state.sync.interval.ms` elsewhere in this same file already
                    // set a window, that value is kept rather than clobbered back to the
                    // 1000ms default — same idiom as `flush.ms` / `flush.messages` below
                    // preserving each other's field of `FlushPolicy::AsyncPeriodic`.
                    "share.group.state.sync" => match value.trim().to_lowercase().as_str() {
                        "buffered" => {
                            config.share_state_sync_policy = ShareStateSyncPolicy::Buffered;
                        }
                        "always" => {
                            config.share_state_sync_policy = ShareStateSyncPolicy::EveryWrite;
                        }
                        "interval" => {
                            let interval = match config.share_state_sync_policy {
                                ShareStateSyncPolicy::Interval(d) => d,
                                _ => Duration::from_millis(1000),
                            };
                            config.share_state_sync_policy =
                                ShareStateSyncPolicy::Interval(interval);
                        }
                        // Unrecognized: leave whatever's already set (the default, unless an
                        // earlier line already changed it) rather than panic — matches every
                        // other arm in this function.
                        _ => {}
                    },
                    // Setting an interval is an unambiguous statement of intent, so — same as
                    // `log.flush.interval.ms` switching `FlushPolicy` to `AsyncPeriodic` —
                    // this switches the policy to `Interval` outright, whether or not
                    // `share.group.state.sync=interval` is also present in the file.
                    //
                    // The one thing it will not do is *downgrade* an explicit
                    // `share.group.state.sync=always`. Both keys writing the policy would
                    // otherwise make the outcome depend on which line came last in the file,
                    // and a stray interval line silently costing an operator the durability
                    // they asked for is the wrong direction to fail on a durability setting.
                    // `EveryWrite` is the strictest level, so it wins regardless of order.
                    "share.group.state.sync.interval.ms" => {
                        if let Ok(ms) = value.parse::<u64>() {
                            if config.share_state_sync_policy != ShareStateSyncPolicy::EveryWrite {
                                // A 0ms window would sync on literally every write, silently
                                // reproducing `EveryWrite` under a different name — clamp to
                                // the floor rather than accept a value that doesn't mean what
                                // it says.
                                config.share_state_sync_policy = ShareStateSyncPolicy::Interval(
                                    Duration::from_millis(ms.max(1)),
                                );
                            }
                        }
                    }
                    "auto.assign.partitions.enable" => {
                        if let Ok(v) = value.parse::<bool>() {
                            config.auto_assign_partitions_enable = v;
                        }
                    }
                    "auto.create.topics.enable" => {
                        if let Ok(v) = value.parse::<bool>() {
                            config.auto_create_topics_enable = v;
                        }
                    }
                    "max.partitions.per.topic" => {
                        if let Ok(v) = value.parse::<u32>() {
                            config.max_partitions_per_topic = v.max(1);
                        }
                    }
                    "max.partitions.per.broker" => {
                        if let Ok(v) = value.parse::<usize>() {
                            config.max_partitions_per_broker = v.max(1);
                        }
                    }
                    "transaction.timeout.ms" | "transaction.max.timeout.ms" => {
                        if let Ok(v) = value.parse() {
                            config.transaction_timeout_ms = v;
                        }
                    }
                    "log.preallocate" | "preallocate.segments" => {
                        if let Ok(v) = value.parse::<bool>() {
                            config.preallocate_segments = v;
                        }
                    }
                    // `log.flush.interval.ms` / `log.flush.interval.messages`:
                    // switches the flush policy to a periodic/byte-threshold flush timed
                    // by `flush.ms` (byte threshold left at its existing default unless
                    // `flush.messages` — interpreted here as an approximate byte budget,
                    // since Bifrox's flush threshold is byte- not message-count-based —
                    // is also given).
                    "flush.ms" | "log.flush.interval.ms" => {
                        if let Ok(ms) = value.parse::<u64>() {
                            let max_bytes = match config.flush_policy {
                                FlushPolicy::AsyncPeriodic { max_bytes, .. } => max_bytes,
                                _ => 64 * 1024,
                            };
                            config.flush_policy = FlushPolicy::AsyncPeriodic {
                                interval: Duration::from_millis(ms.max(1)),
                                max_bytes,
                            };
                        }
                    }
                    "flush.messages" | "log.flush.interval.messages" => {
                        if let Ok(bytes) = value.parse::<usize>() {
                            let interval = match config.flush_policy {
                                FlushPolicy::AsyncPeriodic { interval, .. } => interval,
                                _ => Duration::from_millis(5),
                            };
                            config.flush_policy = FlushPolicy::AsyncPeriodic {
                                interval,
                                max_bytes: bytes,
                            };
                        }
                    }
                    "compression.type" => {
                        if let Ok(codec) = value.parse::<CompressionCodec>() {
                            config.compression_codec = codec;
                        }
                    }
                    "index.interval.bytes" => {
                        if let Ok(v) = value.parse() {
                            config.index_interval_bytes = v;
                        }
                    }
                    "peer.addresses" | "replica.peer.addresses" => {
                        config.peer_addrs = value
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                    // `process.roles`: "controller", "broker", or
                    // "broker,controller" (order doesn't matter). Unset/unrecognized
                    // falls back to combined mode (both roles) — see
                    // `parse_process_roles`.
                    "process.roles" => {
                        config.roles = parse_process_roles(value);
                    }
                    // `controller.quorum.voters`: normally
                    // `id1@host1:port1,id2@host2:port2,...`; Bifrox only needs the
                    // host:port half for peer targeting, so the optional `id@` prefix is
                    // accepted and discarded if present.
                    "controller.quorum.voters" => {
                        config.controller_peer_addrs = value
                            .split(',')
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.split('@').next_back().unwrap_or(s).to_string())
                            .collect();
                    }
                    "min.insync.replicas" => {
                        if let Ok(v) = value.parse() {
                            config.min_insync_replicas = v;
                        }
                    }
                    "default.replication.factor" => {
                        if let Ok(v) = value.parse::<u16>() {
                            config.default_replication_factor = v.max(1);
                        }
                    }
                    "retention.bytes" | "log.retention.bytes" => {
                        if let Ok(v) = value.parse() {
                            config.retention_bytes = Some(v);
                        }
                    }
                    "retention.millis" | "log.retention.ms" => {
                        if let Ok(v) = value.parse() {
                            config.retention_millis = Some(v);
                        }
                    }
                    "delete.retention.ms" => {
                        if let Ok(v) = value.parse() {
                            config.delete_retention_millis = Some(v);
                        }
                    }
                    "min.cleanable.dirty.ratio" => {
                        if let Ok(v) = value.parse() {
                            config.min_cleanable_dirty_ratio = v;
                        }
                    }
                    "log.cleaner.threads" => {
                        if let Ok(v) = value.parse::<usize>() {
                            config.compaction_worker_threads = v.max(1);
                        }
                    }
                    "cleanup.policy" | "log.cleanup.policy" => {
                        if let Ok(v) = value.parse() {
                            config.cleanup_policy = v;
                        }
                    }
                    "auth.token" if !value.is_empty() => {
                        config.auth_token = Some(value.to_string());
                    }
                    "quota.producer.default.bytes.per.second" | "producer.byte.rate" => {
                        if let Ok(v) = value.parse::<u64>() {
                            config.produce_quota_bytes_per_sec = Some(v);
                        }
                    }
                    "quota.consumer.default.bytes.per.second" | "consumer.byte.rate" => {
                        if let Ok(v) = value.parse::<u64>() {
                            config.fetch_quota_bytes_per_sec = Some(v);
                        }
                    }
                    "metrics.bind.address" | "metrics.bind.addr" if !value.is_empty() => {
                        config.metrics_bind_addr = Some(value.to_string());
                    }
                    "metrics.auth.token" | "metrics.token" if !value.is_empty() => {
                        config.metrics_auth_token = Some(value.to_string());
                    }
                    "metrics.allowed.ips" => {
                        config.metrics_allowed_ips = value
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                    _ => {}
                }
            }
        }

        Ok(config)
    }
}

#[cfg(test)]
mod compression_type_tests {
    use super::*;

    /// `producer` is the default and is its own value, not an alias for `none`. It used to
    /// parse to `None`, which quietly conflated "the broker imposes no codec" with "the
    /// broker imposes no compression" — the same outcome on the produce path today, but
    /// different statements, and only one of them describes what actually happens.
    #[test]
    fn producer_is_the_default_and_not_an_alias_for_none() {
        assert_eq!(CompressionCodec::default(), CompressionCodec::Producer);
        assert_eq!(
            "producer".parse::<CompressionCodec>().unwrap(),
            CompressionCodec::Producer
        );
        assert_ne!(
            "producer".parse::<CompressionCodec>().unwrap(),
            "none".parse::<CompressionCodec>().unwrap()
        );
    }

    #[test]
    fn parses_every_accepted_spelling() {
        for (input, expected) in [
            ("producer", CompressionCodec::Producer),
            ("PRODUCER", CompressionCodec::Producer),
            ("none", CompressionCodec::None),
            ("uncompressed", CompressionCodec::None),
            ("", CompressionCodec::None),
            ("lz4", CompressionCodec::Lz4),
            ("  Zstd  ", CompressionCodec::Zstd),
        ] {
            assert_eq!(
                input.parse::<CompressionCodec>().unwrap(),
                expected,
                "input {input:?}"
            );
        }
        assert!("gzip".parse::<CompressionCodec>().is_err());
    }

    /// Round-trips through `Display`, so a config read back out means the same thing it did
    /// going in — `producer` in particular must not come back as `none`.
    #[test]
    fn display_round_trips_through_parse() {
        for codec in [
            CompressionCodec::Producer,
            CompressionCodec::None,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
        ] {
            assert_eq!(
                codec.to_string().parse::<CompressionCodec>().unwrap(),
                codec
            );
        }
        assert_eq!(CompressionCodec::Producer.to_string(), "producer");
    }

    /// A broker-authored frame has no producer whose choice could be honoured, so
    /// `Producer` resolves to uncompressed there. Every explicit codec passes through.
    #[test]
    fn broker_authored_frames_resolve_producer_to_uncompressed() {
        assert_eq!(
            CompressionCodec::Producer.for_broker_authored_frame(),
            CompressionCodec::None
        );
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
        ] {
            assert_eq!(codec.for_broker_authored_frame(), codec);
        }
    }
}

#[cfg(test)]
mod share_group_config_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Writes `contents` to a fresh, uniquely-named temp file and returns its path —
    /// nanosecond timestamp plus a counter (not just the timestamp) so two calls in the
    /// same test process never collide even when the clock's resolution is coarser than a
    /// nanosecond on some platforms.
    fn write_temp_properties(contents: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let count = COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bifrox_config_test_{}_{}_{}.properties",
            std::process::id(),
            nanos,
            count
        ));
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn defaults_are_thirty_seconds_and_five_attempts() {
        let config = EngineConfig::default();
        assert_eq!(config.share_group_lock_timeout_ms, 30_000);
        assert_eq!(config.share_group_max_delivery_attempts, 5);
    }

    #[test]
    fn parses_both_keys_from_properties_file() {
        let path = write_temp_properties(
            "share.group.lock.timeout.ms=45000\n\
             share.group.max.delivery.attempts=3\n",
        );
        let config = EngineConfig::from_properties_file(&path).unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(config.share_group_lock_timeout_ms, 45_000);
        assert_eq!(config.share_group_max_delivery_attempts, 3);
    }

    #[test]
    fn zero_clamps_to_one_for_both_keys() {
        let path = write_temp_properties(
            "share.group.lock.timeout.ms=0\n\
             share.group.max.delivery.attempts=0\n",
        );
        let config = EngineConfig::from_properties_file(&path).unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(
            config.share_group_lock_timeout_ms, 1,
            "a 0ms lease expires the instant it's granted — must clamp to the floor, not \
             accept it literally"
        );
        assert_eq!(
            config.share_group_max_delivery_attempts, 1,
            "check_lock_timeouts compares with >=, so 0 and 1 both still deliver once — \
             must clamp to the honest floor"
        );
    }

    #[test]
    fn unparseable_value_leaves_default_intact() {
        let path = write_temp_properties("share.group.max.delivery.attempts=banana\n");
        let config = EngineConfig::from_properties_file(&path).unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(
            config.share_group_max_delivery_attempts, 5,
            "an unparseable value must be ignored, not panic, leaving the default in place"
        );
    }
}

#[cfg(test)]
mod share_state_sync_policy_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Same unique-temp-file helper as `share_group_config_tests::write_temp_properties`,
    /// duplicated locally rather than shared across `#[cfg(test)]` modules.
    fn write_temp_properties(contents: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let count = COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bifrox_config_sync_policy_test_{}_{}_{}.properties",
            std::process::id(),
            nanos,
            count
        ));
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn default_is_buffered() {
        assert_eq!(
            EngineConfig::default().share_state_sync_policy,
            ShareStateSyncPolicy::Buffered
        );
    }

    #[test]
    fn parses_each_named_level() {
        for (value, expected) in [
            ("buffered", ShareStateSyncPolicy::Buffered),
            ("always", ShareStateSyncPolicy::EveryWrite),
            (
                "interval",
                ShareStateSyncPolicy::Interval(Duration::from_millis(1000)),
            ),
        ] {
            let path = write_temp_properties(&format!("share.group.state.sync={value}\n"));
            let config = EngineConfig::from_properties_file(&path).unwrap();
            let _ = fs::remove_file(&path);
            assert_eq!(
                config.share_state_sync_policy, expected,
                "share.group.state.sync={value}"
            );
        }
    }

    #[test]
    fn interval_ms_key_alone_switches_the_policy() {
        let path = write_temp_properties("share.group.state.sync.interval.ms=250\n");
        let config = EngineConfig::from_properties_file(&path).unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(
            config.share_state_sync_policy,
            ShareStateSyncPolicy::Interval(Duration::from_millis(250)),
            "setting the interval key alone must switch the policy to Interval, with no \
             share.group.state.sync key needed"
        );
    }

    /// An explicit `always` must not be silently downgraded by a stray interval line, in
    /// either file order. Both keys write the same field, so without an explicit precedence
    /// rule the outcome would depend on which line came last — and an operator who asked for
    /// per-write durability would quietly get less of it. Asserted in both orders because
    /// only testing one would pass on a plain last-write-wins implementation.
    #[test]
    fn explicit_always_is_not_downgraded_by_an_interval_line() {
        for body in [
            "share.group.state.sync=always\nshare.group.state.sync.interval.ms=1000\n",
            "share.group.state.sync.interval.ms=1000\nshare.group.state.sync=always\n",
        ] {
            let path = write_temp_properties(body);
            let config = EngineConfig::from_properties_file(&path).unwrap();
            let _ = fs::remove_file(&path);

            assert_eq!(
                config.share_state_sync_policy,
                ShareStateSyncPolicy::EveryWrite,
                "an explicit share.group.state.sync=always must win over an interval line \
                 regardless of order; config body was: {body:?}"
            );
        }
    }

    #[test]
    fn interval_ms_zero_clamps_to_one() {
        let path = write_temp_properties("share.group.state.sync.interval.ms=0\n");
        let config = EngineConfig::from_properties_file(&path).unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(
            config.share_state_sync_policy,
            ShareStateSyncPolicy::Interval(Duration::from_millis(1)),
            "a 0ms window would sync on every write, silently reproducing EveryWrite under a \
             different name — must clamp to the floor instead"
        );
    }

    #[test]
    fn unrecognized_value_leaves_default_intact() {
        let path = write_temp_properties("share.group.state.sync=eventually\n");
        let config = EngineConfig::from_properties_file(&path).unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(
            config.share_state_sync_policy,
            ShareStateSyncPolicy::Buffered,
            "an unrecognized value must be ignored, not panic, leaving the default in place"
        );
    }
}
