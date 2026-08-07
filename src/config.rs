use std::path::PathBuf;
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

/// Engine Configuration Parameters for Milestone 2
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Base directory for storage data logs and indexes
    pub data_dir: PathBuf,
    /// Maximum byte size for a single log segment file before rotation (default 10MB)
    pub max_segment_bytes: u64,
    /// Byte interval for inserting sparse binary index entries (e.g. 4KB)
    pub index_interval_bytes: u64,
    /// Durability flush strategy
    pub flush_policy: FlushPolicy,
    /// Whether to pre-allocate segment files using `set_len` to prevent NTFS fragmentation
    pub preallocate_segments: bool,
    /// Server bind socket address
    pub bind_addr: String,
    /// Log Retention threshold in total bytes per partition (optional)
    pub retention_bytes: Option<u64>,
    /// Log Retention threshold in milliseconds (optional, e.g. 24 hours)
    pub retention_millis: Option<u64>,
    /// Interval for running background retention garbage collector
    pub retention_check_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            max_segment_bytes: 10 * 1024 * 1024, // 10 MB per segment
            index_interval_bytes: 4096,           // 4 KB sparse index interval
            flush_policy: FlushPolicy::default(),
            preallocate_segments: true,
            bind_addr: "127.0.0.1:9092".to_string(),
            retention_bytes: Some(100 * 1024 * 1024), // 100 MB retention limit
            retention_millis: Some(86400 * 1000),      // 24 hours retention
            retention_check_interval: Duration::from_secs(10),
        }
    }
}
