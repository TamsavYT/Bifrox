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

        // Recover state from existing system log.
        //
        // Genuinely streaming: each parsed prefix is drained out of `buf` at the end of
        // every chunk (see the `buf.drain(..cursor)` below), so peak memory stays bounded
        // by the chunk size plus one partially-parsed record — not by the file size. It
        // also keeps parsing linear: without the drain, `cursor` restarting at 0 on each
        // chunk meant every already-parsed record was re-parsed once per subsequent
        // chunk, making recovery O(file_size²).
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

                if cursor > 0 {
                    buf.drain(..cursor);
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

    /// Appends state watermark snapshot to disk log with CRC32. Every acknowledgement that
    /// advances a partition's watermark appends a brand new record rather than updating one
    /// in place — the log is otherwise strictly append-only, so its size is unbounded by
    /// the number of *events*, not the number of (group, topic, partition) keys it actually
    /// needs to remember. `maybe_compact_log` below bounds that growth the same way
    /// `ConsumerGroupManager::compact_log` already does for `__consumer_offsets.log`.
    fn persist_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> IoResult<()> {
        let entry = Self::encode_entry(group_id, topic, partition, start_offset);
        {
            let mut file = self.state_file.lock();
            file.write_all(&entry)?;
            file.flush()?;

            if file.metadata()?.len() <= Self::COMPACT_THRESHOLD_BYTES {
                return Ok(());
            }
        }
        self.compact_log()
    }

    const COMPACT_THRESHOLD_BYTES: u64 = 1024 * 1024; // matches __consumer_offsets.log's threshold

    /// Encodes one `[magic][group][topic][partition][offset][crc32]` record — the shared
    /// wire format used both for a single incremental append (`persist_offset`) and for
    /// every retained key when rewriting the whole log (`compact_log`).
    fn encode_entry(group_id: &str, topic: &str, partition: u32, start_offset: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32 + group_id.len() + topic.len());
        buf.put_u8(SHARE_GROUP_STATE_MAGIC);
        crate::protocol::wire::write_pascal_string(&mut buf, group_id);
        crate::protocol::wire::write_pascal_string(&mut buf, topic);
        buf.put_u32(partition);
        buf.put_u64(start_offset);

        let mut hasher = Hasher::new();
        hasher.update(&buf);
        buf.put_u32(hasher.finalize());
        buf
    }

    /// Rewrites `__share_group_state.log` keeping only the latest persisted watermark per
    /// (group, topic, partition) — same strategy and same Windows-safe
    /// remove-then-rename swap as `ConsumerGroupManager::compact_log`.
    fn compact_log(&self) -> IoResult<()> {
        let mut entry_bytes = Vec::new();
        for item in self.persisted_watermarks.iter() {
            let (group_id, topic, partition) = item.key();
            let start_offset = *item.value();
            entry_bytes.extend_from_slice(&Self::encode_entry(
                group_id,
                topic,
                *partition,
                start_offset,
            ));
        }

        let tmp_path = self.state_path.with_extension("log.tmp");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(windows)]
        options.share_mode(7);

        let mut tmp_file = options.open(&tmp_path)?;
        tmp_file.write_all(&entry_bytes)?;
        tmp_file.sync_data()?;

        let mut file = self.state_file.lock();
        // Windows cannot atomically rename over an existing destination path — remove it
        // first while still holding the lock, matching `ConsumerGroupManager::compact_log`.
        match std::fs::remove_file(&*self.state_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        std::fs::rename(&tmp_path, &*self.state_path)?;

        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create(true);
        #[cfg(windows)]
        open_opts.share_mode(7);

        let mut new_file = open_opts.open(&*self.state_path)?;
        new_file.seek(SeekFrom::End(0))?;
        *file = new_file;

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
