use crate::protocol::wire::{AckBatch, AcquiredRecordBatch};
use crate::server::partition::PartitionManager;
use crate::server::share::partition::SharePartition;
use bytes::BufMut;
use crc32fast::Hasher;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

pub const SHARE_GROUP_STATE_MAGIC: u8 = 0xCE;

#[derive(Debug, Clone)]
pub struct ShareGroupManager {
    /// In-memory state: (group_id, topic, partition) -> SharePartition
    partitions: Arc<DashMap<(String, String, u32), Arc<SharePartition>>>,
    /// Heartbeat tracker: (group_id, member_id) -> last_heartbeat
    heartbeats: Arc<DashMap<(String, String), Instant>>,
    /// Tracks last persisted watermark to prevent redundant disk I/O
    persisted_watermarks: Arc<DashMap<(String, String, u32), u64>>,
    /// Disk persistence for share group watermarks
    state_file: Arc<Mutex<File>>,
    #[allow(dead_code)]
    state_path: Arc<std::path::PathBuf>,
    default_lock_timeout: Duration,
    max_delivery_attempts: u16,
}

impl ShareGroupManager {
    /// Opens or creates the `__share_group_state.log` persistent file and recovers watermarks
    pub fn open(data_dir: impl AsRef<Path>) -> IoResult<Self> {
        let dir = data_dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let state_path = dir.join("__share_group_state.log");

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(windows)]
        options.share_mode(7); // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE

        let mut file = options.open(&state_path)?;
        let recovered_offsets = DashMap::new();

        // Recover state from existing system log
        file.seek(SeekFrom::Start(0))?;
        let raw_len = file.metadata()?.len();
        if raw_len > 0 {
            let mut buf = Vec::new();
            let mut chunk = vec![0u8; 64 * 1024];
            let mut is_corrupt = false;

            loop {
                let n = file.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);

                let mut cursor = 0usize;
                while cursor < buf.len() {
                    let mut temp = cursor;
                    if buf[temp] != SHARE_GROUP_STATE_MAGIC {
                        is_corrupt = true;
                        break;
                    }
                    temp += 1;

                    if temp + 2 > buf.len() {
                        break;
                    }
                    let group_len =
                        u16::from_be_bytes(buf[temp..temp + 2].try_into().unwrap()) as usize;
                    temp += 2;

                    if temp + group_len > buf.len() {
                        break;
                    }
                    let group_id = match String::from_utf8(buf[temp..temp + group_len].to_vec()) {
                        Ok(s) => s,
                        Err(_) => {
                            is_corrupt = true;
                            break;
                        }
                    };
                    temp += group_len;

                    if temp + 2 > buf.len() {
                        break;
                    }
                    let topic_len =
                        u16::from_be_bytes(buf[temp..temp + 2].try_into().unwrap()) as usize;
                    temp += 2;

                    if temp + topic_len > buf.len() {
                        break;
                    }
                    let topic = match String::from_utf8(buf[temp..temp + topic_len].to_vec()) {
                        Ok(s) => s,
                        Err(_) => {
                            is_corrupt = true;
                            break;
                        }
                    };
                    temp += topic_len;

                    if temp + 4 + 8 + 4 > buf.len() {
                        break;
                    }
                    let partition = u32::from_be_bytes(buf[temp..temp + 4].try_into().unwrap());
                    temp += 4;
                    let start_offset = u64::from_be_bytes(buf[temp..temp + 8].try_into().unwrap());
                    temp += 8;
                    let expected_crc = u32::from_be_bytes(buf[temp..temp + 4].try_into().unwrap());
                    temp += 4;

                    // Verify CRC
                    let mut hasher = Hasher::new();
                    hasher.update(&buf[cursor..temp - 4]);
                    if hasher.finalize() != expected_crc {
                        is_corrupt = true;
                        break;
                    }

                    recovered_offsets.insert((group_id, topic, partition), start_offset);
                    cursor = temp;
                }

                if is_corrupt {
                    break;
                }
            }
        }

        // Seek to end for future append writes
        file.seek(SeekFrom::End(0))?;

        let partitions = DashMap::new();
        let persisted_watermarks = DashMap::new();
        let default_lock_timeout = Duration::from_secs(30);
        let max_delivery_attempts = 5;

        for entry in recovered_offsets.into_iter() {
            let ((group_id, topic, partition), start_offset) = entry;
            persisted_watermarks.insert((group_id.clone(), topic.clone(), partition), start_offset);
            partitions.insert(
                (group_id.clone(), topic.clone(), partition),
                Arc::new(SharePartition::new(
                    topic,
                    partition,
                    group_id,
                    default_lock_timeout,
                    max_delivery_attempts,
                    start_offset,
                )),
            );
        }

        Ok(Self {
            partitions: Arc::new(partitions),
            heartbeats: Arc::new(DashMap::new()),
            persisted_watermarks: Arc::new(persisted_watermarks),
            state_file: Arc::new(Mutex::new(file)),
            state_path: Arc::new(state_path),
            default_lock_timeout,
            max_delivery_attempts,
        })
    }

    /// Gets or creates a SharePartition instance for (group_id, topic, partition)
    pub fn get_or_create_partition(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Arc<SharePartition> {
        let key = (group_id.to_string(), topic.to_string(), partition);
        if let Some(sp) = self.partitions.get(&key) {
            return sp.clone();
        }

        let sp = Arc::new(SharePartition::new(
            topic.to_string(),
            partition,
            group_id.to_string(),
            self.default_lock_timeout,
            self.max_delivery_attempts,
            0,
        ));
        self.partitions.insert(key, sp.clone());
        sp
    }

    /// Records member heartbeat
    pub fn record_heartbeat(&self, group_id: &str, member_id: &str) {
        self.heartbeats.insert(
            (group_id.to_string(), member_id.to_string()),
            Instant::now(),
        );
    }

    /// Fetches acquired records for ShareFetch
    #[allow(clippy::too_many_arguments)]
    pub fn share_fetch(
        &self,
        group_id: &str,
        member_id: &str,
        topic: &str,
        partition: u32,
        max_records: usize,
        lock_timeout: Option<Duration>,
        partition_manager: &PartitionManager,
    ) -> Result<Vec<AcquiredRecordBatch>, String> {
        self.record_heartbeat(group_id, member_id);
        let sp = self.get_or_create_partition(group_id, topic, partition);
        let acquired =
            sp.acquire_records(member_id, max_records, lock_timeout, partition_manager)?;

        if acquired.is_empty() {
            return Ok(Vec::new());
        }

        // Group continuous offsets into AcquiredRecordBatch
        let mut batches: Vec<AcquiredRecordBatch> = Vec::new();
        for info in acquired {
            let offset = info.offset;
            let delivery_count = info.delivery_count;
            let frame = info.frame;

            if let Some(last_batch) = batches.last_mut() {
                if last_batch.last_offset + 1 == offset
                    && last_batch.delivery_count == delivery_count
                {
                    last_batch.last_offset = offset;
                    last_batch.records.push(frame);
                    continue;
                }
            }

            batches.push(AcquiredRecordBatch {
                first_offset: offset,
                last_offset: offset,
                delivery_count,
                records: vec![frame],
            });
        }

        Ok(batches)
    }

    /// Acknowledges records for ShareAcknowledge or piggybacked on ShareFetch
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fn share_acknowledge(
        &self,
        group_id: &str,
        member_id: &str,
        topic: &str,
        partition: u32,
        batches: &[AckBatch],
        dlq_writer: Option<&dyn Fn(&[u64])>,
    ) -> Result<(), String> {
        self.record_heartbeat(group_id, member_id);
        let sp = self.get_or_create_partition(group_id, topic, partition);
        let dlq_offsets = sp.acknowledge(member_id, batches)?;

        if !dlq_offsets.is_empty() {
            if let Some(writer) = dlq_writer {
                writer(&dlq_offsets);
            }
        }

        // Check if watermark advanced beyond last persisted value
        let current_start = sp.start_offset.load(Ordering::SeqCst);
        let key = (group_id.to_string(), topic.to_string(), partition);
        let last_persisted = self.persisted_watermarks.get(&key).map(|v| *v).unwrap_or(0);

        if current_start > last_persisted {
            self.persisted_watermarks.insert(key, current_start);
            let _ = self.persist_offset(group_id, topic, partition, current_start);
        }

        Ok(())
    }

    /// Appends state watermark snapshot to disk log with CRC32
    fn persist_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> IoResult<()> {
        let mut file = self.state_file.lock();
        let mut buf = Vec::with_capacity(32 + group_id.len() + topic.len());
        buf.put_u8(SHARE_GROUP_STATE_MAGIC);
        crate::protocol::wire::write_pascal_string(&mut buf, group_id);
        crate::protocol::wire::write_pascal_string(&mut buf, topic);
        buf.put_u32(partition);
        buf.put_u64(start_offset);

        let mut hasher = Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        file.write_all(&buf)?;
        file.flush()?;
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    /// Sweeps all share partitions for expired locks and routes DLQ messages
    pub fn sweep_lock_timeouts(&self, dlq_writer: Option<&dyn Fn(&str, u32, &[u64])>) {
        for entry in self.partitions.iter() {
            let sp = entry.value();
            let dlq_offsets = sp.check_lock_timeouts();
            if !dlq_offsets.is_empty() {
                if let Some(writer) = dlq_writer {
                    writer(&sp.topic, sp.partition, &dlq_offsets);
                }
            }

            // Flush dirty watermark to disk if advanced
            let current_start = sp.start_offset.load(Ordering::SeqCst);
            let key = (sp.group_id.clone(), sp.topic.clone(), sp.partition);
            let last_persisted = self.persisted_watermarks.get(&key).map(|v| *v).unwrap_or(0);
            if current_start > last_persisted {
                self.persisted_watermarks.insert(key, current_start);
                let _ = self.persist_offset(&sp.group_id, &sp.topic, sp.partition, current_start);
            }
        }
    }

    /// Returns list of active members in a group (heartbeated within 60s)
    pub fn list_active_members(&self, group_id: &str) -> Vec<String> {
        let now = Instant::now();
        let timeout = Duration::from_secs(60);
        self.heartbeats
            .iter()
            .filter(|entry| {
                entry.key().0 == group_id && now.duration_since(*entry.value()) <= timeout
            })
            .map(|entry| entry.key().1.clone())
            .collect()
    }
}
