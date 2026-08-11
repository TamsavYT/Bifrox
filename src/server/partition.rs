use crate::config::EngineConfig;
use crate::protocol::RecordFrame;
use crate::segment::SegmentManager;
use parking_lot::{Mutex, RwLock};
use std::io::Result as IoResult;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

#[derive(Debug, Clone, Copy)]
pub struct ProducerStateEntry {
    pub epoch: i16,
    pub last_sequence: i32,
    pub last_offset: u64,
}

#[derive(Debug)]
pub struct ProducerStateManager {
    pub states: HashMap<u64, ProducerStateEntry>,
    snapshot_path: std::path::PathBuf,
}

impl ProducerStateManager {
    pub fn open(path: std::path::PathBuf) -> IoResult<Self> {
        let mut states = HashMap::new();
        if path.exists() {
            let mut file = File::open(&path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let mut cursor = &buf[..];
            while cursor.len() >= 22 {
                let producer_id = u64::from_be_bytes(cursor[0..8].try_into().unwrap());
                let epoch = i16::from_be_bytes(cursor[8..10].try_into().unwrap());
                let last_sequence = i32::from_be_bytes(cursor[10..14].try_into().unwrap());
                let last_offset = u64::from_be_bytes(cursor[14..22].try_into().unwrap());
                cursor = &cursor[22..];
                states.insert(producer_id, ProducerStateEntry { epoch, last_sequence, last_offset });
            }
        }
        Ok(Self { states, snapshot_path: path })
    }

    pub fn validate_sequence(&self, producer_id: u64, epoch: i16, base_sequence: i32) -> Result<(), (bool, u64)> {
        if let Some(entry) = self.states.get(&producer_id) {
            if epoch < entry.epoch {
                return Err((false, 0)); // Fenced
            }
            if epoch == entry.epoch {
                if base_sequence <= entry.last_sequence {
                    return Err((true, entry.last_offset)); // Duplicate
                }
                if base_sequence > entry.last_sequence + 1 {
                    return Err((false, 0)); // Out of order
                }
            }
        }
        Ok(())
    }

    pub fn update(&mut self, producer_id: u64, epoch: i16, last_sequence: i32, last_offset: u64) {
        self.states.insert(producer_id, ProducerStateEntry { epoch, last_sequence, last_offset });
    }

    pub fn take_snapshot(&self) -> IoResult<()> {
        let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(&self.snapshot_path)?;
        for (pid, state) in &self.states {
            file.write_all(&pid.to_be_bytes())?;
            file.write_all(&state.epoch.to_be_bytes())?;
            file.write_all(&state.last_sequence.to_be_bytes())?;
            file.write_all(&state.last_offset.to_be_bytes())?;
        }
        file.sync_all()?;
        Ok(())
    }
}



/// Thread-safe PartitionManager managing segment manager, atomic watermark, and granular leadership state
#[derive(Debug)]
pub struct PartitionManager {
    topic: String,
    partition: u32,
    segment_manager: Mutex<SegmentManager>,
    high_watermark: AtomicU64,
    producer_state_manager: Mutex<ProducerStateManager>,
    leader_id: AtomicU32,
    leader_epoch: AtomicU32,
    replicas: RwLock<Vec<u32>>,
    isr: RwLock<Vec<u32>>,
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
        let snapshot_path = partition_dir.join(format!("{}.snapshot", partition));
        let producer_state_manager = ProducerStateManager::open(snapshot_path)?;

        Ok(Self {
            topic,
            partition,
            segment_manager: Mutex::new(segment_manager),
            high_watermark,
            producer_state_manager: Mutex::new(producer_state_manager),
            leader_id: AtomicU32::new(config.node_id),
            leader_epoch: AtomicU32::new(0),
            replicas: RwLock::new(vec![config.node_id]),
            isr: RwLock::new(vec![config.node_id]),
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn is_leader(&self, self_node_id: u32) -> bool {
        self.leader_id.load(Ordering::Acquire) == self_node_id
    }

    pub fn update_leadership(&self, leader_id: u32, leader_epoch: u32, replicas: Vec<u32>, isr: Vec<u32>) {
        self.leader_id.store(leader_id, Ordering::Release);
        self.leader_epoch.store(leader_epoch, Ordering::Release);
        *self.replicas.write() = replicas;
        *self.isr.write() = isr;
    }

    pub fn leader_id(&self) -> u32 {
        self.leader_id.load(Ordering::Acquire)
    }

    pub fn leader_epoch(&self) -> u32 {
        self.leader_epoch.load(Ordering::Acquire)
    }

    pub fn replicas(&self) -> Vec<u32> {
        self.replicas.read().clone()
    }

    pub fn isr(&self) -> Vec<u32> {
        self.isr.read().clone()
    }

    pub fn latest_offset(&self) -> u64 {
        self.high_watermark.load(Ordering::Acquire)
    }

    /// Appends payload to event log stream, updates high watermark atomic, and returns produced RecordFrame
    pub fn produce_frame_eos(&self, payload: &[u8], producer_id: u64, epoch: i16, sequence: i32) -> IoResult<Result<RecordFrame, u64>> {
        let mut psm = self.producer_state_manager.lock();
        if producer_id != 0 {
            if let Err((is_duplicate, last_offset)) = psm.validate_sequence(producer_id, epoch, sequence) {
                if is_duplicate {
                    return Ok(Err(last_offset));
                } else {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Out of order sequence"));
                }
            }
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let (frame, rolled) = {
            let mut seg_guard = self.segment_manager.lock();
            let base_before = seg_guard.active_base_offset();
            let frame = seg_guard.append(payload, timestamp)?;
            let base_after = seg_guard.active_base_offset();

            let assigned_offset = frame.offset;
            self.high_watermark.store(assigned_offset + 1, Ordering::Release);

            (frame, base_before != base_after)
        };

        if producer_id != 0 {
            psm.update(producer_id, epoch, sequence, frame.offset);
        }

        if rolled {
            let _ = psm.take_snapshot();
        }

        Ok(Ok(frame))
    }

    pub fn produce_frame(&self, payload: &[u8]) -> IoResult<RecordFrame> {
        let f = self.produce_frame_eos(payload, 0, 0, 0)?;
        Ok(f.unwrap())
    }

    /// Appends a control marker to the partition.
    pub fn produce_control_marker(&self, control_type: u8, producer_id: u64, transaction_id: &str) -> IoResult<RecordFrame> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let frame = {
            let mut seg_guard = self.segment_manager.lock();
            let frame = seg_guard.append_control_marker(control_type, producer_id, transaction_id, timestamp)?;

            let assigned_offset = frame.offset;
            self.high_watermark.store(assigned_offset + 1, Ordering::Release);

            frame
        };

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
        seg_guard.sync()?;
        let psm = self.producer_state_manager.lock();
        let _ = psm.take_snapshot();
        Ok(())
    }

    /// Appends an aborted transaction range to the partition's transaction index
    pub fn append_aborted_txn(&self, producer_id: u64, first_offset: u64, last_offset: u64) -> IoResult<()> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.append_aborted_txn(producer_id, first_offset, last_offset)
    }

    /// Checks if a given offset belongs to an aborted transaction in the partition's transaction index
    pub fn is_offset_aborted(&self, offset: u64) -> bool {
        let seg_guard = self.segment_manager.lock();
        seg_guard.is_offset_aborted(offset)
    }
}
