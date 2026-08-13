use crate::replication::NodeRole;
use std::fs;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    /// SASL user accounts (username -> password)
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
            preallocate_segments: true,
            log_file_dir: PathBuf::from("./logs"),
            bind_addr: "127.0.0.1:9092".to_string(),
            peer_addrs: Vec::new(),
            min_insync_replicas: 1,
            default_replication_factor: 1,
            retention_bytes: Some(100 * 1024 * 1024), // 100 MB retention limit
            retention_millis: Some(86400 * 1000),     // 24 hours retention
            retention_check_interval: Duration::from_secs(10),
            cleanup_policy: CleanupPolicy::Delete,
            security_protocol: SecurityProtocol::Plaintext,
            ssl_cert_path: None,
            ssl_key_path: None,
            ssl_ca_path: None,
            ssl_client_auth: "none".to_string(),
            sasl_mechanisms: vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()],
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
                        if let Ok(v) = value.parse() {
                            config.max_segment_bytes = v;
                        }
                    }
                    "compression.type" => {
                        if value.eq_ignore_ascii_case("lz4") {
                            config.compression_codec = CompressionCodec::Lz4;
                        } else {
                            config.compression_codec = CompressionCodec::None;
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
