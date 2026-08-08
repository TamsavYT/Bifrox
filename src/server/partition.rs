use crate::config::EngineConfig;
use crate::protocol::RecordFrame;
use crate::segment::SegmentManager;
use crate::wal::WalEngine;
use parking_lot::Mutex;
use std::io::Result as IoResult;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Thread-safe PartitionManager managing WAL engine, segment manager, and atomic watermark
#[derive(Debug)]
pub struct PartitionManager {
    topic: String,
    partition: u32,
    segment_manager: Mutex<SegmentManager>,
    wal_engine: Mutex<WalEngine>,
    high_watermark: AtomicU64,
}

impl PartitionManager {
    pub fn open(
        base_data_dir: impl AsRef<Path>,
        topic: impl Into<String>,
        partition: u32,
        config: EngineConfig,
    ) -> IoResult<Self> {
        let topic = topic.into();
        let partition_dir = base_data_dir.as_ref().to_path_buf();

        let segment_manager = SegmentManager::open(&partition_dir, config.clone())?;
        let high_watermark = AtomicU64::new(segment_manager.high_watermark());

        let wal_engine = WalEngine::new(config.flush_policy.clone(), 64 * 1024);

        Ok(Self {
            topic,
            partition,
            segment_manager: Mutex::new(segment_manager),
            wal_engine: Mutex::new(wal_engine),
            high_watermark,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn latest_offset(&self) -> u64 {
        self.high_watermark.load(Ordering::Acquire)
    }

    /// Appends payload to event log stream, updates high watermark atomic, and returns produced RecordFrame
    pub fn produce_frame(&self, payload: &[u8]) -> IoResult<RecordFrame> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let (frame, should_sync) = {
            let mut seg_guard = self.segment_manager.lock();
            let frame = seg_guard.append(payload, timestamp)?;

            let assigned_offset = frame.offset;
            self.high_watermark.store(assigned_offset + 1, Ordering::Release);

            let mut wal_guard = self.wal_engine.lock();
            wal_guard.push(&frame);
            let should_sync = wal_guard.should_flush();
            (frame, should_sync)
        };

        if should_sync {
            self.segment_manager.lock().sync()?;
        }

        Ok(frame)
    }

    /// Appends a control marker to the partition.
    pub fn produce_control_marker(&self, control_type: u8, producer_id: u64, transaction_id: &str) -> IoResult<RecordFrame> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let (frame, should_sync) = {
            let mut seg_guard = self.segment_manager.lock();
            let frame = seg_guard.append_control_marker(control_type, producer_id, transaction_id, timestamp)?;

            let assigned_offset = frame.offset;
            self.high_watermark.store(assigned_offset + 1, Ordering::Release);

            let mut wal_guard = self.wal_engine.lock();
            wal_guard.push(&frame);
            let should_sync = wal_guard.should_flush();
            (frame, should_sync)
        };

        if should_sync {
            self.segment_manager.lock().sync()?;
        }

        Ok(frame)
    }

    /// Appends payload to event log stream, updates high watermark atomic, and triggers flush if policy requires
    pub fn produce(&self, payload: &[u8]) -> IoResult<u64> {
        let frame = self.produce_frame(payload)?;
        Ok(frame.offset)
    }

    /// Reads event records starting from target logical offset
    pub fn fetch(&self, start_offset: u64, max_bytes: u32) -> IoResult<Vec<RecordFrame>> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.fetch(start_offset, max_bytes as usize)
    }

    /// Reads event records starting from target timestamp
    pub fn fetch_by_timestamp(&self, target_timestamp: u64, max_bytes: u32) -> IoResult<Vec<RecordFrame>> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.fetch_by_timestamp(target_timestamp, max_bytes as usize)
    }

    /// Seeks nearest physical position for target logical offset
    pub fn seek(&self, target_offset: u64) -> Option<(u64, u64)> {
        let seg_guard = self.segment_manager.lock();
        seg_guard.seek(target_offset)
    }

    /// Triggers retention garbage collection for partition log segments
    pub fn apply_retention(&self) -> IoResult<usize> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.apply_retention()
    }

    /// Explicitly flushes partition log and index files to physical disk
    pub fn flush(&self) -> IoResult<()> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.sync()
    }
}
