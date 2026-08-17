use bytes::Bytes;
use crate::config::EngineConfig;
use crate::protocol::RecordFrame;
use crate::segment::SegmentManager;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Result as IoResult;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
                states.insert(
                    producer_id,
                    ProducerStateEntry {
                        epoch,
                        last_sequence,
                        last_offset,
                    },
                );
            }
        }
        Ok(Self {
            states,
            snapshot_path: path,
        })
    }

    pub fn validate_sequence(
        &self,
        producer_id: u64,
        epoch: i16,
        base_sequence: i32,
    ) -> Result<(), (bool, u64)> {
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
        self.states.insert(
            producer_id,
            ProducerStateEntry {
                epoch,
                last_sequence,
                last_offset,
            },
        );
    }

    /// Writes the snapshot atomically: a crash or a fresh `.truncate(true)` write directly
    /// over `self.snapshot_path` risks leaving a half-written (truncated-but-not-yet-
    /// rewritten) file behind that the next startup would then load as valid producer
    /// state, silently losing idempotence tracking for every producer whose entry landed
    /// after the crash point. Writing to a sibling `.tmp` file, fsyncing it, and only then
    /// renaming it into place means the file at `self.snapshot_path` is always either the
    /// complete previous snapshot or the complete new one — never a partial write.
    pub fn take_snapshot(&self) -> IoResult<()> {
        let tmp_path = self.snapshot_path.with_extension("snapshot.tmp");
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            for (pid, state) in &self.states {
                file.write_all(&pid.to_be_bytes())?;
                file.write_all(&state.epoch.to_be_bytes())?;
                file.write_all(&state.last_sequence.to_be_bytes())?;
                file.write_all(&state.last_offset.to_be_bytes())?;
            }
            file.sync_all()?;
        }
        // Windows cannot atomically rename over an existing destination path (unlike
        // POSIX `rename(2)`) — remove it first, matching the same pattern already used by
        // `ConsumerGroupManager::compact_log`.
        match std::fs::remove_file(&self.snapshot_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        std::fs::rename(&tmp_path, &self.snapshot_path)?;
        if let Some(parent) = self.snapshot_path.parent() {
            crate::segment::log::fsync_dir(parent);
        }
        Ok(())
    }
}

/// Thread-safe PartitionManager managing segment manager, atomic watermark, and granular leadership state
#[derive(Debug)]
pub struct PartitionManager {
    topic: String,
    partition: u32,
    segment_manager: Mutex<SegmentManager>,
    /// Log-end-offset: the next offset to be assigned locally. Bumped synchronously on
    /// every local append (leader produce, replica verbatim-append, control marker).
    log_end_offset: AtomicU64,
    /// The real Kafka-style high watermark: the highest offset guaranteed replicated to
    /// a full ISR quorum. Only advanced explicitly (see `advance_committed_hw`) — never
    /// implicitly by an append — so fetch/describe can expose only what's actually
    /// durable across the ISR instead of whatever the leader happens to have on disk.
    committed_hw: AtomicU64,
    /// Bytes appended since the last successful `sync()` — the group-commit signal for
    /// `FlushPolicy::AsyncPeriodic`'s `max_bytes` threshold (previously declared but never
    /// read anywhere, so periodic flush only ever fired on the timer, never eagerly under
    /// write pressure). Reset to 0 by every path that actually calls `sync()`.
    unsynced_bytes: AtomicU64,
    producer_state_manager: Mutex<ProducerStateManager>,
    leader_id: AtomicU32,
    leader_epoch: AtomicU32,
    replicas: RwLock<Vec<u32>>,
    isr: RwLock<Vec<u32>>,
    compression_codec: RwLock<crate::config::CompressionCodec>,
    flush_policy: crate::config::FlushPolicy,
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
        // Everything already on disk at startup is treated as committed — consistent
        // with how recovery elsewhere in this codebase already trusts on-disk state.
        let recovered_offset = segment_manager.high_watermark();
        let log_end_offset = AtomicU64::new(recovered_offset);
        let committed_hw = AtomicU64::new(recovered_offset);
        let snapshot_path = partition_dir.join(format!("{}.snapshot", partition));
        let producer_state_manager = ProducerStateManager::open(snapshot_path)?;

        Ok(Self {
            topic,
            partition,
            segment_manager: Mutex::new(segment_manager),
            log_end_offset,
            committed_hw,
            unsynced_bytes: AtomicU64::new(0),
            producer_state_manager: Mutex::new(producer_state_manager),
            leader_id: AtomicU32::new(config.node_id),
            leader_epoch: AtomicU32::new(0),
            replicas: RwLock::new(vec![config.node_id]),
            isr: RwLock::new(vec![config.node_id]),
            compression_codec: RwLock::new(config.compression_codec),
            flush_policy: config.flush_policy,
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

    pub fn update_leadership(
        &self,
        leader_id: u32,
        leader_epoch: u32,
        replicas: Vec<u32>,
        isr: Vec<u32>,
    ) {
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

    /// Returns the log-end-offset: the next offset that will be assigned to a new local
    /// append. This includes data that may not yet be replicated to the ISR — use
    /// `high_watermark()` for the committed-only view.
    pub fn latest_offset(&self) -> u64 {
        self.log_end_offset.load(Ordering::Acquire)
    }

    /// Returns the committed high watermark: the highest offset guaranteed replicated to
    /// a full ISR quorum. Consumers should never be shown data beyond this.
    pub fn high_watermark(&self) -> u64 {
        self.committed_hw.load(Ordering::Acquire)
    }

    /// Monotonically advances the committed high watermark to `candidate` (a no-op if
    /// `candidate` isn't past the current value).
    pub fn advance_committed_hw(&self, candidate: u64) {
        self.committed_hw.fetch_max(candidate, Ordering::AcqRel);
    }

    /// Appends payload to event log stream, updates high watermark atomic, and returns produced RecordFrame.
    /// Takes `payload` by value as `Bytes` (a cheap refcounted clone at the caller, not a
    /// copy) so it can be threaded straight through to `SegmentManager::append_with_codec`
    /// without a redundant `Vec<u8>` allocation on this hot path.
    pub fn produce_frame_eos(
        &self,
        payload: Bytes,
        producer_id: u64,
        epoch: i16,
        sequence: i32,
    ) -> IoResult<Result<RecordFrame, u64>> {
        let mut psm = self.producer_state_manager.lock();
        if producer_id != 0 {
            if let Err((is_duplicate, last_offset)) =
                psm.validate_sequence(producer_id, epoch, sequence)
            {
                if is_duplicate {
                    return Ok(Err(last_offset));
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Out of order sequence",
                    ));
                }
            }
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let codec = *self.compression_codec.read();

        let (frame, rolled) = {
            let mut seg_guard = self.segment_manager.lock();
            let base_before = seg_guard.active_base_offset();
            let frame = seg_guard.append_with_codec(payload, timestamp, codec)?;
            let base_after = seg_guard.active_base_offset();

            let assigned_offset = frame.offset;
            // Only LEO advances here. `committed_hw` is deliberately NOT bumped on local
            // leader append — it only advances once `produce_batch` confirms ISR quorum
            // (see engine.rs), so a crashed/partitioned leader can never have exposed data
            // to consumers that no other replica ever received.
            self.log_end_offset
                .store(assigned_offset + 1, Ordering::Release);

            (frame, base_before != base_after)
        };

        if producer_id != 0 {
            psm.update(producer_id, epoch, sequence, frame.offset);
        }

        if rolled {
            let _ = psm.take_snapshot();
        }

        // Note: this function deliberately does NOT sync for `SyncEveryBatch`/
        // `UnbufferedSync` — `produce_batch` (engine.rs) calls `produce_frame_eos` once
        // per record in a batch, and syncing here would mean an N-record batch fsyncs N
        // times instead of once. Callers that write a single record per call (the
        // `produce_frame()` wrapper — DLQ, offset commits, metadata proposals, etc.) sync
        // immediately there instead; batched callers sync once via
        // `flush_if_sync_policy()` after their whole batch completes.
        let frame_size = frame.encoded_size() as u64;
        let prev_unsynced = self.unsynced_bytes.fetch_add(frame_size, Ordering::AcqRel);
        if let crate::config::FlushPolicy::AsyncPeriodic { max_bytes, .. } = &self.flush_policy {
            let max_bytes = *max_bytes as u64;
            if max_bytes > 0 && prev_unsynced + frame_size >= max_bytes {
                let mut seg_guard = self.segment_manager.lock();
                seg_guard.sync()?;
                self.unsynced_bytes.store(0, Ordering::Release);
            }
        }

        Ok(Ok(frame))
    }

    /// Appends a record and immediately marks it committed. Used by every internal
    /// system-partition writer (cluster metadata proposals, DLQ routing, consumer-offset
    /// commits, transaction-state records, bootstrap) that doesn't itself integrate with
    /// ISR-quorum gating — unlike `produce_batch`'s per-record use of `produce_frame_eos`
    /// directly, which leaves `committed_hw` ungated so it can advance only after quorum.
    pub fn produce_frame(&self, payload: &[u8]) -> IoResult<RecordFrame> {
        let f = self.produce_frame_eos(Bytes::copy_from_slice(payload), 0, 0, 0)?;
        let frame = f.unwrap();
        self.advance_committed_hw(frame.offset + 1);
        // Single-record callers (this wrapper) sync immediately under a sync-every-write
        // policy, same as before this file split fsync out of `produce_frame_eos`.
        self.flush_if_sync_policy()?;
        Ok(frame)
    }

    /// Syncs the segment to disk if the partition's flush policy requires syncing on every
    /// write. Batched callers (`produce_batch` in engine.rs, which calls
    /// `produce_frame_eos` directly once per record) should call this ONCE after their
    /// whole batch, achieving real group-commit instead of one fsync per record.
    pub fn flush_if_sync_policy(&self) -> IoResult<()> {
        if matches!(
            self.flush_policy,
            crate::config::FlushPolicy::SyncEveryBatch | crate::config::FlushPolicy::UnbufferedSync
        ) {
            let mut seg_guard = self.segment_manager.lock();
            seg_guard.sync()?;
            self.unsynced_bytes.store(0, Ordering::Release);
        }
        Ok(())
    }

    /// Appends a frame received from another node (leader push or catch-up fetch) verbatim,
    /// preserving its original offset/timestamp/magic/CRC exactly rather than reassigning
    /// them locally. See `SegmentManager::append_verbatim`.
    pub fn append_replica_frame_verbatim(
        &self,
        frame: &RecordFrame,
    ) -> IoResult<crate::segment::VerbatimAppendResult> {
        let result = {
            let mut seg_guard = self.segment_manager.lock();
            seg_guard.append_verbatim(frame)?
        };

        if result == crate::segment::VerbatimAppendResult::Appended {
            self.log_end_offset
                .store(frame.offset + 1, Ordering::Release);
            // A follower's own durably-written data is immediately safe for it to serve
            // to directly-connected consumers (KIP-392 follower fetch) — unlike the
            // leader, a follower never has to guess whether anyone else has the data,
            // since committing was the leader's job before this push was ever sent.
            self.advance_committed_hw(frame.offset + 1);

            // Same group-commit reasoning as `produce_frame_eos`: this is called once per
            // frame from a replication-push batch that may contain many frames (see
            // `decode_replication_packet` in handler.rs), so it must not sync here — the
            // caller syncs once via `flush_if_sync_policy()` after the whole batch.
            // AsyncPeriodic's byte threshold still applies eagerly per-frame, same as the
            // local-produce path.
            let frame_size = frame.encoded_size() as u64;
            let prev_unsynced = self.unsynced_bytes.fetch_add(frame_size, Ordering::AcqRel);
            if let crate::config::FlushPolicy::AsyncPeriodic { max_bytes, .. } = &self.flush_policy
            {
                let max_bytes = *max_bytes as u64;
                if max_bytes > 0 && prev_unsynced + frame_size >= max_bytes {
                    let mut seg_guard = self.segment_manager.lock();
                    seg_guard.sync()?;
                    self.unsynced_bytes.store(0, Ordering::Release);
                }
            }
        }

        Ok(result)
    }

    /// Discards any locally-stored entries at or beyond `offset` (Raft-style conflicting-
    /// suffix truncation). Also rewinds LEO, and clamps the committed HW down if it had
    /// advanced past the truncation point (it can never legitimately exceed LEO).
    pub fn truncate_after(&self, offset: u64) -> IoResult<()> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.truncate_after(offset)?;
        self.log_end_offset.store(offset, Ordering::Release);
        let mut hw = self.committed_hw.load(Ordering::Acquire);
        while hw > offset {
            match self.committed_hw.compare_exchange_weak(
                hw,
                offset,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => hw = actual,
            }
        }
        Ok(())
    }

    /// Deletes historical segments fully covered by a snapshot at `offset` — see
    /// `SegmentManager::trim_before`.
    pub fn trim_before(&self, offset: u64) -> IoResult<usize> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.trim_before(offset)
    }

    /// Appends a control marker to the partition.
    pub fn produce_control_marker(
        &self,
        control_type: u8,
        producer_id: u64,
        transaction_id: &str,
    ) -> IoResult<RecordFrame> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let frame = {
            let mut seg_guard = self.segment_manager.lock();
            let frame = seg_guard.append_control_marker(
                control_type,
                producer_id,
                transaction_id,
                timestamp,
            )?;

            let assigned_offset = frame.offset;
            // Control markers (transaction commit/abort boundaries) are leader-internal
            // bookkeeping, not user-visible records — keep them immediately visible like
            // before rather than gating them behind ISR quorum, which the transaction
            // manager's own commit/abort flow doesn't currently integrate with.
            self.log_end_offset
                .store(assigned_offset + 1, Ordering::Release);
            self.advance_committed_hw(assigned_offset + 1);

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

    /// Plans a zero-copy fetch (frame-aligned physical byte range + a cloned file handle)
    /// for `start_offset`, clamped to the committed high watermark exactly like `fetch`.
    /// See `SegmentManager::plan_zero_copy_fetch`. The segment lock is held only for the
    /// duration of this planning call, not for the caller's subsequent network transmit.
    pub fn plan_zero_copy_fetch(
        &self,
        start_offset: u64,
        max_bytes: u32,
    ) -> IoResult<Option<crate::segment::ZeroCopyFetchPlan>> {
        let hw = self.high_watermark();
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.plan_zero_copy_fetch(start_offset, max_bytes as usize, hw)
    }

    /// Reads event records starting from target timestamp
    pub fn fetch_by_timestamp(
        &self,
        target_timestamp: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
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

    pub fn set_cleanup_policy(&self, policy: crate::config::CleanupPolicy) {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.set_cleanup_policy(policy);
    }

    /// Dynamic per-topic config override (Kafka `compression.type` `AlterConfigs`).
    pub fn set_compression_codec(&self, codec: crate::config::CompressionCodec) {
        *self.compression_codec.write() = codec;
    }

    /// Dynamic per-topic config override (Kafka `retention.ms` `AlterConfigs`).
    /// `None` reverts to no time-based retention for this partition.
    pub fn set_retention_millis(&self, millis: Option<u64>) {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.set_retention_millis(millis);
    }

    /// Dynamic per-topic config override (Kafka `retention.bytes` `AlterConfigs`).
    /// `None` reverts to no size-based retention for this partition.
    pub fn set_retention_bytes(&self, bytes: Option<u64>) {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.set_retention_bytes(bytes);
    }

    /// Dynamic per-topic config override (Kafka `delete.retention.ms` `AlterConfigs`).
    /// `None` disables tombstone expiry — tombstones are kept forever, like any other
    /// record, once written.
    pub fn set_delete_retention_millis(&self, millis: Option<u64>) {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.set_delete_retention_millis(millis);
    }

    /// Dynamic per-topic config override (Kafka `min.cleanable.dirty.ratio` `AlterConfigs`).
    pub fn set_min_cleanable_dirty_ratio(&self, ratio: f64) {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.set_min_cleanable_dirty_ratio(ratio);
    }

    pub fn cleanup_policy(&self) -> crate::config::CleanupPolicy {
        let seg_guard = self.segment_manager.lock();
        seg_guard.cleanup_policy()
    }

    /// Explicitly flushes partition log and index files to physical disk
    pub fn flush(&self) -> IoResult<()> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.sync()?;
        drop(seg_guard);
        self.unsynced_bytes.store(0, Ordering::Release);
        let psm = self.producer_state_manager.lock();
        let _ = psm.take_snapshot();
        Ok(())
    }

    /// Appends an aborted transaction range to the partition's transaction index
    pub fn append_aborted_txn(
        &self,
        producer_id: u64,
        first_offset: u64,
        last_offset: u64,
    ) -> IoResult<()> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.append_aborted_txn(producer_id, first_offset, last_offset)
    }

    /// Checks if a given offset belongs to an aborted transaction in the partition's transaction index
    pub fn is_offset_aborted(&self, offset: u64) -> bool {
        let seg_guard = self.segment_manager.lock();
        seg_guard.is_offset_aborted(offset)
    }
}
