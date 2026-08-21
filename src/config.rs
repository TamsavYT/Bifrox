use crate::replication::NodeRole;
use std::fs;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A node's process role(s) — matches Kafka KRaft's `process.roles` (`controller`,
/// `broker`, or `broker,controller`). A node with only `Controller` participates in the
/// metadata Raft quorum but never hosts data-topic partitions; a node with only `Broker`
/// hosts data partitions and replicates `__cluster_metadata` as a non-voting observer but
/// never contests leadership of it; a node with both (the historical Hermes default)
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
/// `"controller"`, `"broker"`) the way Kafka's own config does. An empty/unparseable
/// value falls back to combined mode (both roles) — the historical Hermes default,
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

/// Compression Codec for Record Batch Storage (compression.type)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionCodec {
    #[default]
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl std::str::FromStr for CompressionCodec {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.trim().to_lowercase();
        match normalized.as_str() {
            "none" | "" | "uncompressed" | "producer" => Ok(CompressionCodec::None),
            "lz4" => Ok(CompressionCodec::Lz4),
            "zstd" => Ok(CompressionCodec::Zstd),
            _ => Err(format!("Unknown compression.type: '{}'", s)),
        }
    }
}

/// Topic and Server Log Cleanup Policy (Kafka cleanup.policy)
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

/// Kafka Security Protocol (Kafka security.protocol)
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

/// Engine Configuration Parameters (Kafka-style configuration support)
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Cluster ID for cluster membership verification (Kafka cluster.id)
    pub cluster_id: String,
    /// Cluster Node ID (Kafka broker.id)
    pub node_id: u32,
    /// Node HA Cluster Role (leader or follower)
    pub role: NodeRole,
    /// Base directory for storage data logs and indexes (Kafka log.dirs)
    pub data_dir: PathBuf,
    /// Maximum byte size for a single log segment file before rotation (default 10MB)
    pub max_segment_bytes: u64,
    /// Byte interval for inserting sparse binary index entries (e.g. 4KB)
    pub index_interval_bytes: u64,
    /// Durability flush strategy
    pub flush_policy: FlushPolicy,
    /// Whether to pre-allocate segment files using `set_len` to prevent NTFS fragmentation
    pub preallocate_segments: bool,
    /// Directory for server diagnostic log files (Kafka log.file.dir)
    pub log_file_dir: PathBuf,
    /// Server bind socket address (Kafka listeners)
    pub bind_addr: String,
    /// Explicit override for the address this node advertises to peers and clients as
    /// its own identity (Kafka `advertised.listeners`) — in inter-node heartbeats,
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
    /// Default replication factor for newly created topics (Kafka default.replication.factor)
    pub default_replication_factor: u16,
    /// Log Retention threshold in total bytes per partition (optional)
    pub retention_bytes: Option<u64>,
    /// Log Retention threshold in milliseconds (optional, e.g. 24 hours)
    pub retention_millis: Option<u64>,
    /// Interval for running background retention garbage collector
    pub retention_check_interval: Duration,
    /// Server-default Log Cleanup Policy (Kafka cleanup.policy / log.cleanup.policy)
    pub cleanup_policy: CleanupPolicy,
    /// Kafka Security Protocol (PLAINTEXT, SASL_PLAINTEXT, SSL, SASL_SSL)
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
    /// Legacy bootstrap SASL user accounts (username -> password). At startup Hermes
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
    /// Default per-client produce byte-rate quota in bytes/sec (Kafka
    /// `quota.producer.default.bytes.per.second`). `None` = unlimited (default).
    /// Clients exceeding this rate have their produce response delayed rather than
    /// rejected, matching Kafka's throttling behavior.
    pub produce_quota_bytes_per_sec: Option<u64>,
    /// Default per-client fetch byte-rate quota in bytes/sec (Kafka
    /// `quota.consumer.default.bytes.per.second`). `None` = unlimited (default).
    pub fetch_quota_bytes_per_sec: Option<u64>,
    /// Compression Codec for Record Batch Storage (Kafka `compression.type`)
    pub compression_codec: CompressionCodec,
    /// Configurable bind address for Prometheus metrics endpoint (e.g. "0.0.0.0:9090")
    pub metrics_bind_addr: Option<String>,
    /// Optional Bearer token for Prometheus metrics scrape authentication
    pub metrics_auth_token: Option<String>,
    /// Optional network IP whitelist for Prometheus metrics scrape endpoint
    pub metrics_allowed_ips: Vec<String>,
    /// How long a replica may go without acknowledging a replicated write before the
    /// partition leader drops it from the ISR (Kafka `replica.lag.time.max.ms`).
    pub replica_lag_max_ms: u64,
    /// How often the ISR-membership and broker-liveness sweep runs.
    pub isr_check_interval_ms: u64,
    /// How long a broker may go without a heartbeat ACK before the cluster leader
    /// considers it dead and, for any partition it currently leads, elects a new leader
    /// from the remaining ISR.
    pub broker_down_threshold_ms: u64,
    /// Whether a partition may fail over to a replica outside the last-known ISR when no
    /// in-sync replica survives a leader's death (Kafka `unclean.leader.election.enable`).
    /// Defaults to false: an unrecoverable partition is left leaderless rather than
    /// silently accepting data loss.
    pub allow_unclean_leader_election: bool,
    /// This node's process role(s) — Kafka KRaft's `process.roles`. Defaults to both
    /// (combined mode, today's historical behavior).
    pub roles: Vec<ProcessRole>,
    /// The subset of `peer_addrs` that are controller-eligible (participate in the
    /// metadata Raft quorum) — Kafka's `controller.quorum.voters`. Only meaningful when
    /// this node itself has the `Controller` role. Empty means "assume every peer is
    /// controller-eligible", which matches combined-mode clusters where every node votes.
    pub controller_peer_addrs: Vec<String>,
    /// How long a tombstone (a compacted-topic record whose value is empty — Hermes's
    /// convention for a Kafka-style "delete marker") is kept as the latest record for its
    /// key before log compaction erases the key entirely (Kafka `delete.retention.ms`).
    /// `None` disables tombstone expiry: tombstones are kept forever, same as any other
    /// record, once written (today's behavior).
    pub delete_retention_millis: Option<u64>,
    /// Minimum fraction of a historical segment's bytes that must be "dirty" (superseded
    /// by a newer record for the same key) before log compaction will rewrite that segment
    /// (Kafka `min.cleanable.dirty.ratio`). Segments below this ratio are left untouched on
    /// a given compaction pass, avoiding low-value rewrite I/O.
    pub min_cleanable_dirty_ratio: f64,
    /// Maximum number of partitions whose retention/compaction pass may run concurrently
    /// within a single GC tick (Kafka `log.cleaner.threads`).
    pub compaction_worker_threads: usize,
    /// Maximum age (ms) of the active segment before it's rolled purely due to time,
    /// independent of `max_segment_bytes` (Kafka `segment.ms`). `None` disables time-based
    /// rolling — a low-volume topic's active segment then only rotates once it hits the
    /// byte threshold, which may be never.
    pub segment_ms: Option<u64>,
    /// Maximum accepted size (bytes) of a single record payload (Kafka
    /// `message.max.bytes`). `None` means no explicit limit beyond what already applies
    /// (segment size, wire framing limits).
    pub message_max_bytes: Option<u64>,
    /// Worker threads dedicated to the Tokio runtime driving network I/O (Kafka
    /// `num.network.threads`, approximately — Hermes doesn't separate network/request-
    /// handling threads the way Kafka does, so this sizes the whole async runtime).
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
    /// (Kafka `group.initial.rebalance.delay.ms`).
    ///
    /// Raising it lets a larger group settle into a single generation at the cost of
    /// slower first assignment; 0 disables the barrier entirely and restores the old
    /// join-immediately behavior.
    pub group_initial_rebalance_delay_ms: u64,
    /// How long a consumer group member may go without making fetch progress — a `Fetch`
    /// attributed to it via the request envelope's `GROUP_MEMBER` tag — before the
    /// coordinator evicts it for stalling even though it keeps heartbeating on schedule
    /// (Kafka `max.poll.interval.ms`; issue #54: a member deadlocked, stuck on a poisoned
    /// record, or blocked on a downstream call still reports itself healthy through
    /// heartbeats alone).
    ///
    /// Defaults to five minutes, matching Kafka's own default.
    pub max_poll_interval_ms: u64,
    /// Whether a topic may be created implicitly by a *produce* to a topic that doesn't
    /// exist yet (Kafka `auto.create.topics.enable`). Defaults to **true**, matching both
    /// Kafka's own default and Hermes's long-standing behavior.
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
    /// Maximum partitions accepted for a single topic (Kafka `num.partitions` upper
    /// bound). Requests asking for more are rejected rather than creating the directories.
    pub max_partitions_per_topic: u32,
    /// Maximum total partitions this broker will host across all topics. Once reached,
    /// creating a new partition fails instead of consuming more inodes.
    pub max_partitions_per_broker: usize,
    /// How long a transaction restored from `__transaction_state` on startup (still
    /// `Ongoing`/`PrepareCommit`/`PrepareAbort` — i.e. never reached a terminal state
    /// before the last shutdown) is given before it's presumed abandoned and aborted to
    /// release the partitions it was blocking (Kafka `transaction.max.timeout.ms`, applied
    /// here at replay time since Hermes has no live producer session to time out against
    /// across a restart).
    pub transaction_timeout_ms: u64,
    /// Whether `StorageEngine::produce_batch` writes a produced request's records as a
    /// single [`crate::protocol::RecordBatch`] (issue #18 stage 1b-ii) instead of one
    /// `RecordFrame` per record (today's format, matching Kafka's `log.message.format.version`
    /// in spirit — a log-format gate, not a per-request knob). Defaults to **false**:
    /// batches on disk are so far only *tolerated* by replication's verbatim append paths
    /// and log compaction's `extract_key` (`src/segment/manager.rs`), not understood by
    /// them — enabling this before those land (issue #18 stage 1b-iii) would corrupt
    /// replication and compaction the moment a batch appeared. Flip only once stage
    /// 1b-iii has merged.
    pub produce_record_batches_enable: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            cluster_id: "hermes-prod-cluster-01".to_string(),
            node_id: 1,
            role: NodeRole::Leader,
            data_dir: PathBuf::from("./data"),
            max_segment_bytes: 10 * 1024 * 1024, // 10 MB per segment
            index_interval_bytes: 4096,          // 4 KB sparse index interval
            flush_policy: FlushPolicy::default(),
            // Kafka's own `log.preallocate` defaults to `false`: pre-touching the full
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
            max_poll_interval_ms: 300_000, // 5 minutes, matching Kafka's max.poll.interval.ms
            auto_create_topics_enable: true,
            max_partitions_per_topic: 10_000,
            max_partitions_per_broker: 200_000,
            transaction_timeout_ms: 60_000, // matches Kafka's transaction.timeout.ms default
            produce_record_batches_enable: false,
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
            delete_retention_millis: Some(24 * 60 * 60 * 1000), // 24 hours, matches Kafka's default
            min_cleanable_dirty_ratio: 0.5,                     // matches Kafka's default
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
    /// Loads configuration from a Kafka-style `server.properties` file
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
                    "produce.record.batches.enable" => {
                        if let Ok(v) = value.parse::<bool>() {
                            config.produce_record_batches_enable = v;
                        }
                    }
                    "log.preallocate" | "preallocate.segments" => {
                        if let Ok(v) = value.parse::<bool>() {
                            config.preallocate_segments = v;
                        }
                    }
                    // Kafka's `log.flush.interval.ms` / `log.flush.interval.messages`:
                    // switches the flush policy to a periodic/byte-threshold flush timed
                    // by `flush.ms` (byte threshold left at its existing default unless
                    // `flush.messages` — interpreted here as an approximate byte budget,
                    // since Hermes's flush threshold is byte- not message-count-based —
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
                    // Kafka KRaft's `process.roles`: "controller", "broker", or
                    // "broker,controller" (order doesn't matter). Unset/unrecognized
                    // falls back to combined mode (both roles) — see
                    // `parse_process_roles`.
                    "process.roles" => {
                        config.roles = parse_process_roles(value);
                    }
                    // Kafka KRaft's `controller.quorum.voters`: normally
                    // `id1@host1:port1,id2@host2:port2,...`; Hermes only needs the
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
