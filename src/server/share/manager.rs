use crate::config::ShareStateSyncPolicy;
use crate::protocol::wire::{AckBatch, AcquiredRecordBatch};
use crate::server::partition::PartitionManager;
use crate::server::share::partition::{
    state_from_byte, state_to_byte, PersistedBatch, SharePartition,
};
use bytes::BufMut;
use crc32fast::Hasher;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

/// Version 2: adds the per-batch in-flight array (`delivery_count` and `ShareRecordState`)
/// after `start_offset`. Bumping the magic byte means an old `0xCE` file is rejected
/// outright on the very first byte instead of being half-parsed into garbage by luck of the
/// CRC — the intended behavior, since nobody is running Bifrox yet and there is no
/// version-1 deployment to migrate.
pub const SHARE_GROUP_STATE_MAGIC: u8 = 0xCF;

/// Fixed on-disk size of one `PersistedBatch` entry: `first_offset:8 + last_offset:8 +
/// state:1 + delivery_count:2`.
const PERSISTED_BATCH_LEN: usize = 19;

#[derive(Debug, Clone)]
pub struct ShareGroupManager {
    /// In-memory state: (group_id, topic, partition) -> SharePartition
    partitions: Arc<DashMap<(String, String, u32), Arc<SharePartition>>>,
    /// Heartbeat tracker: (group_id, member_id) -> last_heartbeat
    heartbeats: Arc<DashMap<(String, String), Instant>>,
    /// Tracks each partition's last-persisted `SharePartition::state_version` to prevent
    /// redundant disk I/O. Version comparison replaces the old watermark comparison because
    /// `delivery_count` (and `ShareRecordState`) can change with `start_offset` sitting
    /// still — an acquisition, a release, a re-delivery — so "did the watermark move" is no
    /// longer a sufficient dirty check.
    persisted_versions: Arc<DashMap<(String, String, u32), u64>>,
    /// Disk persistence for share group state
    state_file: Arc<Mutex<File>>,
    state_path: Arc<std::path::PathBuf>,
    default_lock_timeout: Duration,
    max_delivery_attempts: u16,
    /// How hard `persist_partition_state` pushes each write toward disk before returning —
    /// see [`ShareStateSyncPolicy`] for what each level costs and guarantees.
    sync_policy: ShareStateSyncPolicy,
    /// Wall-clock time of the last `sync_data()` call made under `Interval`, checked on
    /// every persist to decide whether the configured window has elapsed yet. Untouched by
    /// `Buffered` and `EveryWrite`, which don't need it.
    last_sync: Arc<Mutex<Instant>>,
    /// Counts every `sync_data()` call this manager has actually issued against the state
    /// file — incremented at each of the (at most) two call sites that can make one:
    /// `persist_partition_state`'s policy check and the forced call in `sync()`. This exists
    /// so tests can prove the configured policy actually *drives* behavior rather than being
    /// parsed and then quietly ignored — a stat that looks wired in but isn't is exactly the
    /// kind of bug that hides until someone asks a test to depend on it.
    sync_count: Arc<AtomicU64>,
}

impl ShareGroupManager {
    /// Opens or creates the `__share_group_state.log` persistent file and recovers
    /// watermarks. `default_lock_timeout` and `max_delivery_attempts` are supplied by the
    /// caller from `EngineConfig` (`share_group_lock_timeout_ms` /
    /// `share_group_max_delivery_attempts`) — every recovered and newly created
    /// `SharePartition` is seeded with these two values. `sync_policy` comes from the same
    /// config (`share_state_sync_policy`) and governs how hard every later persist pushes
    /// toward disk — see [`ShareStateSyncPolicy`].
    pub fn open(
        data_dir: impl AsRef<Path>,
        default_lock_timeout: Duration,
        max_delivery_attempts: u16,
        sync_policy: ShareStateSyncPolicy,
    ) -> IoResult<Self> {
        let dir = data_dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let state_path = dir.join("__share_group_state.log");

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(windows)]
        options.share_mode(7); // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE

        let mut file = options.open(&state_path)?;

        // Recover state from the existing log.
        file.seek(SeekFrom::Start(0))?;
        let raw_len = file.metadata()?.len();
        let (recovered, is_corrupt) = if raw_len > 0 {
            Self::decode_stream(&mut file)?
        } else {
            (DashMap::new(), false)
        };
        if is_corrupt {
            tracing::warn!(
                "share group state file {} is corrupt: recovery stopped early and any \
                 later share-group state (delivery counts, archived records) in this file \
                 was dropped",
                state_path.display()
            );
        }

        // Seek to end for future append writes
        file.seek(SeekFrom::End(0))?;

        let partitions = DashMap::new();
        let persisted_versions = DashMap::new();

        for entry in recovered.into_iter() {
            let ((group_id, topic, partition), (start_offset, batches)) = entry;
            // The freshly reconstructed `SharePartition` starts at `state_version() == 0`
            // (see `SharePartition::restore`), so recording that same value here means "no
            // change since what's on disk" — matching what was actually just read back.
            persisted_versions.insert((group_id.clone(), topic.clone(), partition), 0u64);
            partitions.insert(
                (group_id.clone(), topic.clone(), partition),
                Arc::new(SharePartition::restore(
                    topic,
                    partition,
                    group_id,
                    default_lock_timeout,
                    max_delivery_attempts,
                    start_offset,
                    batches,
                )),
            );
        }

        Ok(Self {
            partitions: Arc::new(partitions),
            heartbeats: Arc::new(DashMap::new()),
            persisted_versions: Arc::new(persisted_versions),
            state_file: Arc::new(Mutex::new(file)),
            state_path: Arc::new(state_path),
            default_lock_timeout,
            max_delivery_attempts,
            sync_policy,
            last_sync: Arc::new(Mutex::new(Instant::now())),
            sync_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Total number of `sync_data()` calls this manager has actually issued against the
    /// state file so far. Exists to let tests prove the configured `sync_policy` drives real
    /// syscalls rather than sitting parsed-but-unused — see the `sync_count` field doc.
    pub fn sync_count(&self) -> u64 {
        self.sync_count.load(Ordering::Relaxed)
    }

    /// Forces a `sync_data()` on the state file regardless of `sync_policy`, and resets the
    /// `Interval` clock so its next window starts counting from here.
    ///
    /// Under `Buffered` and `Interval`, the most recent persist(s) can still be sitting in
    /// the OS page cache, unsynced, at any given moment — that is the whole point of those
    /// policies. A graceful shutdown is the one place that gap must be closed regardless of
    /// policy: without forcing a sync here, a clean restart of a broker configured for
    /// anything but `EveryWrite` could still lose the tail of share-group state, which would
    /// defeat the purpose of shutting down cleanly in the first place. Called from
    /// `StorageEngine::flush_all`, the existing graceful-shutdown hook.
    pub fn sync(&self) -> IoResult<()> {
        let file = self.state_file.lock();
        file.sync_data()?;
        self.sync_count.fetch_add(1, Ordering::Relaxed);
        *self.last_sync.lock() = Instant::now();
        Ok(())
    }

    /// Streams and decodes every well-formed record from `reader`, returning the recovered
    /// `(group_id, topic, partition) -> (start_offset, batches)` map and whether corruption
    /// (bad magic, bad CRC, bad UTF-8, or an unrecognized state byte) was hit before the
    /// input ran out. Factored out of `open` so the parser used to recover the real state
    /// file is the exact same code exercised by the round-trip and corruption unit tests
    /// below — the two can never quietly drift apart.
    ///
    /// Genuinely streaming: each parsed prefix is drained out of `buf` at the end of every
    /// chunk (see the `buf.drain(..cursor)` below), so peak memory stays bounded by the
    /// chunk size plus one partially-parsed record — not by the input size. It also keeps
    /// parsing linear: without the drain, `cursor` restarting at 0 on each chunk meant every
    /// already-parsed record was re-parsed once per subsequent chunk, making recovery
    /// O(input_size²). A record is variable-length in a second dimension (its batch array),
    /// so every "not enough bytes yet" check below has to account for that array's length
    /// too, not just the fixed-size header.
    #[allow(clippy::type_complexity)]
    fn decode_stream<R: Read>(
        mut reader: R,
    ) -> IoResult<(
        DashMap<(String, String, u32), (u64, Vec<PersistedBatch>)>,
        bool,
    )> {
        let recovered: DashMap<(String, String, u32), (u64, Vec<PersistedBatch>)> = DashMap::new();
        let mut buf = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        let mut is_corrupt = false;

        'outer: loop {
            let n = reader.read(&mut chunk)?;
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
                let batch_count = u32::from_be_bytes(buf[temp..temp + 4].try_into().unwrap());
                temp += 4;

                // Computed as u64 so a corrupt (huge) `batch_count` can never overflow
                // `usize` arithmetic while we're only checking "do we have enough bytes yet"
                // — it just keeps waiting for more input, same as any other truncated-length
                // case above, and the CRC still catches it once the whole record is in hand.
                let batch_array_len = batch_count as u64 * PERSISTED_BATCH_LEN as u64;
                let needed = temp as u64 + batch_array_len + 4;
                if needed > buf.len() as u64 {
                    break;
                }

                let mut batches = Vec::with_capacity(batch_count as usize);
                for _ in 0..batch_count {
                    let first_offset = u64::from_be_bytes(buf[temp..temp + 8].try_into().unwrap());
                    temp += 8;
                    let last_offset = u64::from_be_bytes(buf[temp..temp + 8].try_into().unwrap());
                    temp += 8;
                    let state_byte = buf[temp];
                    temp += 1;
                    let delivery_count =
                        u16::from_be_bytes(buf[temp..temp + 2].try_into().unwrap());
                    temp += 2;

                    let state = match state_from_byte(state_byte) {
                        Some(s) => s,
                        None => {
                            is_corrupt = true;
                            break;
                        }
                    };
                    batches.push(PersistedBatch {
                        first_offset,
                        last_offset,
                        state,
                        delivery_count,
                    });
                }
                if is_corrupt {
                    break;
                }

                let expected_crc = u32::from_be_bytes(buf[temp..temp + 4].try_into().unwrap());
                temp += 4;

                // Verify CRC
                let mut hasher = Hasher::new();
                hasher.update(&buf[cursor..temp - 4]);
                if hasher.finalize() != expected_crc {
                    is_corrupt = true;
                    break;
                }

                recovered.insert((group_id, topic, partition), (start_offset, batches));
                cursor = temp;
            }

            if cursor > 0 {
                buf.drain(..cursor);
            }

            if is_corrupt {
                break 'outer;
            }
        }

        Ok((recovered, is_corrupt))
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

        // Acquisition bumps `delivery_count` on every batch it touches, and unlike
        // acknowledgement this path never moves `start_offset` — so without this call
        // nothing here was ever persisted at all, and a restart mid-delivery would forget
        // every attempt this fetch just counted.
        self.maybe_persist(group_id, topic, partition, &sp);

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

        self.maybe_persist(group_id, topic, partition, &sp);

        Ok(())
    }

    /// Persists `sp` if (and only if) its `state_version` has moved since the last time
    /// this manager wrote it to disk — the dirty check that replaces the old
    /// watermark-comparison (see the `persisted_versions` field doc for why).
    ///
    /// Reads `state_version()` *before* snapshotting rather than after: a concurrent
    /// mutation landing between the two calls means the snapshot can reflect a newer
    /// version than the one recorded here, so at worst a later call redundantly re-persists
    /// data that was already durable — never the reverse, where a real change gets recorded
    /// as already-persisted and silently skipped.
    fn maybe_persist(&self, group_id: &str, topic: &str, partition: u32, sp: &SharePartition) {
        let key = (group_id.to_string(), topic.to_string(), partition);
        let version = sp.state_version();
        let last_persisted = self.persisted_versions.get(&key).map(|v| *v).unwrap_or(0);
        if version == last_persisted {
            return;
        }

        let (start_offset, batches) = sp.snapshot();
        if self
            .persist_partition_state(group_id, topic, partition, start_offset, &batches)
            .is_ok()
        {
            self.persisted_versions.insert(key, version);
        }
    }

    /// Appends one partition's full state snapshot to disk log with CRC32. Every persisted
    /// change appends a brand new record rather than updating one in place — the log is
    /// otherwise strictly append-only, so its size is unbounded by the number of *events*,
    /// not the number of (group, topic, partition) keys it actually needs to remember.
    /// `compact_log` below bounds that growth the same way
    /// `ConsumerGroupManager::compact_log` already does for `__consumer_offsets.log`.
    ///
    /// Durability note: every persist calls `file.flush()` unconditionally, then applies
    /// `sync_policy` on top (see [`ShareStateSyncPolicy`]):
    ///   - `Buffered` (the default): nothing further. The write reaches the OS page cache
    ///     and survives a process restart, but a hard machine crash can still lose it —
    ///     bytes handed to the OS but not yet on platter can be lost. This is the same
    ///     best-effort behavior this file had before the policy existed.
    ///   - `EveryWrite`: `sync_data()` after this record, unconditionally. True crash
    ///     durability, paid on every acquire/acknowledge/lock-timeout sweep.
    ///   - `Interval(d)`: `sync_data()` only if at least `d` has elapsed since the last one,
    ///     bounding the crash-loss window without a sync on every write.
    ///
    /// Whichever policy is in force, an unsynced loss resets the most recent delivery counts
    /// rather than corrupting anything — the same failure mode this file already tolerated
    /// for every record before any of this existed.
    fn persist_partition_state(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        start_offset: u64,
        batches: &[PersistedBatch],
    ) -> IoResult<()> {
        let entry = Self::encode_entry(group_id, topic, partition, start_offset, batches);
        {
            let mut file = self.state_file.lock();
            file.write_all(&entry)?;
            file.flush()?;

            match self.sync_policy {
                ShareStateSyncPolicy::Buffered => {}
                ShareStateSyncPolicy::EveryWrite => {
                    file.sync_data()?;
                    self.sync_count.fetch_add(1, Ordering::Relaxed);
                }
                ShareStateSyncPolicy::Interval(interval) => {
                    let mut last_sync = self.last_sync.lock();
                    if last_sync.elapsed() >= interval {
                        file.sync_data()?;
                        self.sync_count.fetch_add(1, Ordering::Relaxed);
                        *last_sync = Instant::now();
                    }
                }
            }

            if file.metadata()?.len() <= Self::COMPACT_THRESHOLD_BYTES {
                return Ok(());
            }
        }
        self.compact_log()
    }

    const COMPACT_THRESHOLD_BYTES: u64 = 1024 * 1024; // matches __consumer_offsets.log's threshold

    /// Encodes one `[magic][group][topic][partition][start_offset][batch_count][batches...]
    /// [crc32]` record — the shared wire format used both for a single incremental append
    /// (`persist_partition_state`) and for every partition snapshotted when rewriting the
    /// whole log (`compact_log`). The CRC covers everything from the magic byte up to (not
    /// including) itself.
    ///
    /// `acquired_by`/`acquired_at`/`lock_timeout` are deliberately not part of
    /// `PersistedBatch` and so never appear here — a lease belongs to a member whose
    /// connection and process are gone once the broker restarts, so there is nothing about
    /// it worth making durable; the record must become available to whoever is running now
    /// (see `SharePartition::restore`).
    fn encode_entry(
        group_id: &str,
        topic: &str,
        partition: u32,
        start_offset: u64,
        batches: &[PersistedBatch],
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            32 + group_id.len() + topic.len() + batches.len() * PERSISTED_BATCH_LEN,
        );
        buf.put_u8(SHARE_GROUP_STATE_MAGIC);
        crate::protocol::wire::write_pascal_string(&mut buf, group_id);
        crate::protocol::wire::write_pascal_string(&mut buf, topic);
        buf.put_u32(partition);
        buf.put_u64(start_offset);
        buf.put_u32(batches.len() as u32);
        for b in batches {
            buf.put_u64(b.first_offset);
            buf.put_u64(b.last_offset);
            buf.put_u8(state_to_byte(b.state));
            buf.put_u16(b.delivery_count);
        }

        let mut hasher = Hasher::new();
        hasher.update(&buf);
        buf.put_u32(hasher.finalize());
        buf
    }

    /// Rewrites `__share_group_state.log` keeping only the latest state per (group, topic,
    /// partition) — same strategy and same Windows-safe remove-then-rename swap as
    /// `ConsumerGroupManager::compact_log`.
    ///
    /// Rewrites from the live `self.partitions` map — the authoritative current state —
    /// rather than from `persisted_versions`: the latter only tracks *versions*, not the
    /// data itself, so it cannot be the source rewritten from. Each partition is
    /// snapshotted the same way an incremental persist would (read lock taken once, no
    /// lock held across the I/O below), and `persisted_versions` is refreshed to the
    /// versions actually written afterward, or the very next persist would see a version
    /// mismatch and pointlessly rewrite everything all over again.
    fn compact_log(&self) -> IoResult<()> {
        let mut entry_bytes = Vec::new();
        let mut written_versions = Vec::new();
        for item in self.partitions.iter() {
            let (group_id, topic, partition) = item.key();
            let sp = item.value();
            let version = sp.state_version();
            let (start_offset, batches) = sp.snapshot();
            entry_bytes.extend_from_slice(&Self::encode_entry(
                group_id,
                topic,
                *partition,
                start_offset,
                &batches,
            ));
            written_versions.push(((group_id.clone(), topic.clone(), *partition), version));
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

        // The rewrite above is now durable, so every version snapshotted into it is the
        // last-persisted version for its key — refresh `persisted_versions` to match, or
        // the very next `maybe_persist` call would see a version mismatch (this rewrite
        // happened without going through `maybe_persist`'s own bookkeeping) and immediately
        // trigger another full rewrite for no reason.
        for (key, version) in written_versions {
            self.persisted_versions.insert(key, version);
        }

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

            self.maybe_persist(&sp.group_id, &sp.topic, sp.partition, sp);
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

    /// Aggregates in-flight record count and the low-water start offset across every
    /// (topic, partition) this group has ever fetched from — used by `ShareGroupDescribe`
    /// to report the group's real state. Each `SharePartition` already tracks both
    /// correctly (`inflight_count` and the `start_offset` watermark); this just sums the
    /// former and takes the minimum of the latter across every partition keyed under
    /// `group_id`, so a caller filtering to `group_id` alone (a group can span more than
    /// one topic) sees one aggregate answer instead of having to know every partition by
    /// name up front.
    pub fn group_stats(&self, group_id: &str) -> (usize, u64) {
        let mut inflight = 0usize;
        let mut start_offset: Option<u64> = None;
        for entry in self.partitions.iter() {
            if entry.key().0 != group_id {
                continue;
            }
            let sp = entry.value();
            inflight += sp.inflight_count();
            let sp_start = sp.start_offset.load(Ordering::SeqCst);
            start_offset = Some(start_offset.map_or(sp_start, |current| current.min(sp_start)));
        }
        (inflight, start_offset.unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::share::partition::ShareRecordState;

    /// Opens a fresh `ShareGroupManager` under a uniquely-named temp directory. Returns the
    /// manager and the directory so the caller can remove it afterward — every test below
    /// does, via `let _ = std::fs::remove_dir_all(&dir);`.
    fn open_manager(policy: ShareStateSyncPolicy) -> (ShareGroupManager, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let count = COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "bifrox_share_sync_policy_test_{}_{}_{}",
            std::process::id(),
            nanos,
            count
        ));
        let manager = ShareGroupManager::open(&dir, Duration::from_secs(30), 5, policy).unwrap();
        (manager, dir)
    }

    /// Every batch, encoded then decoded through `decode_stream` (the exact parser `open`
    /// runs against the real state file), must come back byte-for-byte identical — this is
    /// the wire-level round trip, independent of the `Acquired` → `Available` transform
    /// `SharePartition::restore` applies afterward.
    #[test]
    fn round_trip_preserves_mixed_batch_states() {
        let batches = vec![
            PersistedBatch {
                first_offset: 0,
                last_offset: 2,
                state: ShareRecordState::Available,
                delivery_count: 0,
            },
            PersistedBatch {
                first_offset: 3,
                last_offset: 3,
                state: ShareRecordState::Acquired,
                delivery_count: 4,
            },
            PersistedBatch {
                first_offset: 4,
                last_offset: 9,
                state: ShareRecordState::Acknowledged,
                delivery_count: 1,
            },
            PersistedBatch {
                first_offset: 10,
                last_offset: 10,
                state: ShareRecordState::Archived,
                delivery_count: 5,
            },
        ];

        let entry =
            ShareGroupManager::encode_entry("round-trip-group", "round-trip-topic", 7, 4, &batches);

        let (recovered, is_corrupt) = ShareGroupManager::decode_stream(&entry[..]).unwrap();
        assert!(
            !is_corrupt,
            "a well-formed record must not be flagged corrupt"
        );
        let (start_offset, decoded) = recovered
            .get(&(
                "round-trip-group".to_string(),
                "round-trip-topic".to_string(),
                7u32,
            ))
            .map(|v| v.clone())
            .expect("record must be recovered");
        assert_eq!(start_offset, 4);
        assert_eq!(
            decoded, batches,
            "decoded batches must exactly match what was encoded"
        );
    }

    /// A partition with an empty in-flight window (`batch_count == 0`, the common case
    /// right after every batch has been acknowledged) must still round-trip — the array
    /// being empty is not itself a form of corruption.
    #[test]
    fn round_trip_empty_batch_array() {
        let entry = ShareGroupManager::encode_entry("g", "t", 0, 42, &[]);
        let (recovered, is_corrupt) = ShareGroupManager::decode_stream(&entry[..]).unwrap();
        assert!(!is_corrupt);
        let (start_offset, decoded) = recovered
            .get(&("g".to_string(), "t".to_string(), 0u32))
            .map(|v| v.clone())
            .expect("record must be recovered");
        assert_eq!(start_offset, 42);
        assert!(decoded.is_empty());
    }

    /// A state byte outside `state_from_byte`'s known range (0-3) must be treated as
    /// corruption and reject the whole record — never silently defaulted to some state,
    /// which would quietly resurrect a record with the wrong lifecycle state.
    #[test]
    fn unknown_state_byte_is_corruption_not_a_default() {
        let batches = vec![PersistedBatch {
            first_offset: 0,
            last_offset: 0,
            state: ShareRecordState::Available,
            delivery_count: 1,
        }];
        let mut entry = ShareGroupManager::encode_entry("g", "t", 0, 0, &batches);

        // Layout: magic(1) group_len(2) "g"(1) topic_len(2) "t"(1) partition(4)
        // start_offset(8) batch_count(4) [first_offset(8) last_offset(8) state(1) ...].
        // Locate the state byte by walking the same fixed-size fields decode_stream does.
        let state_byte_index = 1 + 2 + 1 + 2 + 1 + 4 + 8 + 4 + 8 + 8;
        assert_eq!(
            entry[state_byte_index], 0,
            "sanity check: this must be the Available (0) state byte before corrupting it"
        );
        entry[state_byte_index] = 0xFF; // not a valid ShareRecordState encoding

        // Recompute the CRC over the corrupted bytes so the failure is specifically the
        // unknown-state-byte check, not an incidental CRC mismatch masking it.
        let crc_start = entry.len() - 4;
        let mut hasher = Hasher::new();
        hasher.update(&entry[..crc_start]);
        let recomputed = hasher.finalize().to_be_bytes();
        entry[crc_start..].copy_from_slice(&recomputed);

        let (recovered, is_corrupt) = ShareGroupManager::decode_stream(&entry[..]).unwrap();
        assert!(
            is_corrupt,
            "an unrecognized state byte must be reported as corruption"
        );
        assert!(
            recovered.is_empty(),
            "the record carrying the bad state byte must not be recovered at all"
        );
    }

    // The tests below prove the configured `sync_policy` actually drives the `sync_data()`
    // syscall — not that the resulting bytes survive a real machine crash, which nothing in
    // a test process can simulate. What they establish: `sync_count()` moves (or doesn't)
    // exactly the way each policy documents it should.

    /// Under `Buffered`, no number of persists ever calls `sync_data()` — the write reaches
    /// only the OS page cache, by design.
    #[test]
    fn buffered_never_syncs_on_persist() {
        let (manager, dir) = open_manager(ShareStateSyncPolicy::Buffered);
        for i in 0..5u64 {
            manager
                .persist_partition_state("g", "t", 0, i, &[])
                .unwrap();
        }
        assert_eq!(
            manager.sync_count(),
            0,
            "Buffered must never call sync_data() from a persist"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Under `EveryWrite`, `sync_count()` tracks the number of persists one-for-one — the
    /// policy that pays for true crash durability on every write.
    #[test]
    fn every_write_syncs_once_per_persist() {
        let (manager, dir) = open_manager(ShareStateSyncPolicy::EveryWrite);
        const PERSISTS: u64 = 5;
        for i in 0..PERSISTS {
            manager
                .persist_partition_state("g", "t", 0, i, &[])
                .unwrap();
        }
        assert_eq!(
            manager.sync_count(),
            PERSISTS,
            "EveryWrite must call sync_data() exactly once per persist"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Under `Interval`, a burst of persists inside one window must sync at most once, and a
    /// persist after the window has genuinely elapsed must add exactly one more sync. An
    /// exact count for the burst isn't asserted — only the bound `Interval` actually
    /// promises — since real scheduling could legitimately let it land at 0 or 1.
    #[test]
    fn interval_syncs_at_most_once_per_window() {
        let interval = Duration::from_millis(20);
        let (manager, dir) = open_manager(ShareStateSyncPolicy::Interval(interval));

        for i in 0..5u64 {
            manager
                .persist_partition_state("g", "t", 0, i, &[])
                .unwrap();
        }
        let after_burst = manager.sync_count();
        assert!(
            after_burst <= 1,
            "a burst of persists within one interval must sync at most once, got {after_burst}"
        );

        std::thread::sleep(interval + Duration::from_millis(50));
        manager
            .persist_partition_state("g", "t", 0, 99, &[])
            .unwrap();
        assert_eq!(
            manager.sync_count(),
            after_burst + 1,
            "once the interval has genuinely elapsed, the next persist must sync exactly once \
             more"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `sync()` forces a sync regardless of policy — proven here specifically under
    /// `Buffered`, the one policy that otherwise never calls `sync_data()` on its own. This
    /// is the call `StorageEngine::flush_all` relies on so a graceful shutdown doesn't leave
    /// the most recent writes stranded in the page cache.
    #[test]
    fn sync_forces_a_sync_even_under_buffered() {
        let (manager, dir) = open_manager(ShareStateSyncPolicy::Buffered);
        manager
            .persist_partition_state("g", "t", 0, 1, &[])
            .unwrap();
        assert_eq!(manager.sync_count(), 0);

        manager.sync().unwrap();
        assert_eq!(
            manager.sync_count(),
            1,
            "sync() must call sync_data() even though Buffered never would on its own"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
