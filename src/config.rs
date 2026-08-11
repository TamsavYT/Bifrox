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
    /// Log Retention threshold in total bytes per partition (optional)
    pub retention_bytes: Option<u64>,
    /// Log Retention threshold in milliseconds (optional, e.g. 24 hours)
    pub retention_millis: Option<u64>,
    /// Interval for running background retention garbage collector
    pub retention_check_interval: Duration,
    /// Shared-secret token required from client connections (None = no auth check).
    /// Inter-node peers identified by `peer_addrs` are exempt from this check.
    pub auth_token: Option<String>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            cluster_id: "hermes-prod-cluster-01".to_string(),
            node_id: 1,
            role: NodeRole::Leader,
            data_dir: PathBuf::from("./data"),
            max_segment_bytes: 10 * 1024 * 1024, // 10 MB per segment
            index_interval_bytes: 4096,           // 4 KB sparse index interval
            flush_policy: FlushPolicy::default(),
            preallocate_segments: true,
            log_file_dir: PathBuf::from("./logs"),
            bind_addr: "127.0.0.1:9092".to_string(),
            peer_addrs: Vec::new(),
            min_insync_replicas: 1,
            retention_bytes: Some(100 * 1024 * 1024), // 100 MB retention limit
            retention_millis: Some(86400 * 1000),      // 24 hours retention
            retention_check_interval: Duration::from_secs(10),
            auth_token: None,
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
                            .unwrap_or(value);
                        config.bind_addr = clean_addr.to_string();
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
                    "auth.token"
                        if !value.is_empty() => {
                            config.auth_token = Some(value.to_string());
                        }
                    _ => {}
                }
            }
        }

        Ok(config)
    }
}
