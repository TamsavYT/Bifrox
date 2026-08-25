use crate::config::EngineConfig;
use crate::protocol::RecordBatch;
use crate::segment::SegmentManager;
use bytes::Bytes;
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
    /// Woken every time the committed high watermark advances, i.e. whenever new data
    /// becomes visible to consumers.
    ///
    /// This is what lets a fetch wait for data instead of returning empty and being polled
    /// again. A consumer can only read up to the committed watermark, so that is the only
    /// event worth waking a waiting fetch for.
    hw_notify: tokio::sync::Notify,
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
            hw_notify: tokio::sync::Notify::new(),
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
        let previous = self.committed_hw.fetch_max(candidate, Ordering::AcqRel);
        // Only wake waiting fetches when the watermark actually moved. `fetch_max` returns
        // the previous value, so a no-op advance (a candidate at or below where we already
        // are) costs nothing and wakes nobody.
        if candidate > previous {
            self.hw_notify.notify_waiters();
        }
    }

    /// Waits for the committed high watermark to exceed `current`, or for `timeout` to
    /// elapse, whichever comes first. Returns `true` if the watermark moved.
    ///
    /// This is the broker half of a long-poll fetch: rather than answering an idle
    /// consumer with an empty response and being asked again milliseconds later, the fetch
    /// parks here until there is something to send. It cuts request volume on an idle
    /// partition *and* lowers delivery latency, since the consumer is woken the instant
    /// data commits rather than on its next poll tick.
    pub async fn await_hw_beyond(&self, current: u64, timeout: std::time::Duration) -> bool {
        // Registered before the watermark is re-checked, so a commit landing between the
        // check and the wait cannot be missed. `Notified::enable()` is what arms the
        // registration without awaiting it.
        let notified = self.hw_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if self.high_watermark() > current {
            return true;
        }
        tokio::time::timeout(timeout, notified).await.is_ok()
    }

    /// Appends payload to the log as a single-record batch and returns it.
    /// Takes `payload` by value as `Bytes` (a cheap refcounted clone at the caller, not a
    /// copy) so it can be threaded straight through to `SegmentManager::append_with_codec`
    /// without a redundant `Vec<u8>` allocation on this hot path.
    pub fn produce_frame_eos(
        &self,
        payload: Bytes,
        producer_id: u64,
        epoch: i16,
        sequence: i32,
    ) -> IoResult<Result<RecordBatch, u64>> {
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
            let frame = seg_guard.append_record(payload, timestamp, codec)?;
            let base_after = seg_guard.active_base_offset();

            let assigned_offset = frame.base_offset;
            // Only LEO advances here. `committed_hw` is deliberately NOT bumped on local
            // leader append — it only advances once `produce_batch` confirms ISR quorum
            // (see engine.rs), so a crashed/partitioned leader can never have exposed data
            // to consumers that no other replica ever received.
            self.log_end_offset
                .store(assigned_offset + 1, Ordering::Release);

            (frame, base_before != base_after)
        };

        if producer_id != 0 {
            psm.update(producer_id, epoch, sequence, frame.base_offset);
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

    /// Appends a whole produce request's records as one atomic [`RecordBatch`]. This is the
    /// only way client-produced records reach the log — `StorageEngine::produce_batch`
    /// calls it once per request.
    ///
    /// Offset assignment matches `produce_frame_eos`'s "assign fresh offsets" case: the
    /// batch's base offset is the current log-end-offset, same as the first frame a
    /// per-record loop would get, so a client sees the same
    /// `(first_offset, last_offset)` for the same produce request whichever path wrote
    /// it (see `StorageEngine::produce_batch`'s round-trip tests, which assert this).
    ///
    /// Idempotent-producer dedup here is necessarily batch-level rather than per-record:
    /// a batch is atomic on disk (`SegmentManager::append_batch`), so a retried produce
    /// can only be recognized as "the whole batch was already durably written" (checked
    /// once, via `base_sequence`) or "the whole batch is new" — there's no such thing as
    /// writing half of one. This is actually closer to real Kafka's own idempotent-
    /// producer semantics, which dedups by `(baseSequence, lastSequence)` per batch, than
    /// the frame path's per-record check is.
    #[allow(clippy::too_many_arguments)]
    pub fn produce_batch_eos(&self, batch: RecordBatch) -> IoResult<Result<RecordBatch, u64>> {
        let producer_id = batch.producer_id;
        let mut psm = self.producer_state_manager.lock();
        if producer_id != 0 {
            if let Err((is_duplicate, last_offset)) =
                psm.validate_sequence(producer_id, batch.producer_epoch, batch.base_sequence)
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

        let (batch, rolled) = {
            let mut seg_guard = self.segment_manager.lock();
            let base_before = seg_guard.active_base_offset();
            let batch = seg_guard.append_batch(batch, self.leader_epoch())?;
            let base_after = seg_guard.active_base_offset();

            let last_offset = batch.base_offset + batch.last_offset_delta as u64;
            // Same LEO-only-advances contract as `produce_frame_eos` — see its comment.
            self.log_end_offset
                .store(last_offset + 1, Ordering::Release);

            (batch, base_before != base_after)
        };

        if producer_id != 0 {
            let last_sequence = batch
                .base_sequence
                .wrapping_add(batch.record_count as i32 - 1);
            let last_offset = batch.base_offset + batch.last_offset_delta as u64;
            psm.update(
                producer_id,
                batch.producer_epoch,
                last_sequence,
                last_offset,
            );
        }

        if rolled {
            let _ = psm.take_snapshot();
        }

        // Same group-commit contract as `produce_frame_eos`: this never syncs itself for
        // `SyncEveryBatch`/`UnbufferedSync` — the caller (`StorageEngine::produce_batch`)
        // syncs once via `flush_if_sync_policy()` after this call returns.
        let batch_size = batch.encoded_size() as u64;
        let prev_unsynced = self.unsynced_bytes.fetch_add(batch_size, Ordering::AcqRel);
        if let crate::config::FlushPolicy::AsyncPeriodic { max_bytes, .. } = &self.flush_policy {
            let max_bytes = *max_bytes as u64;
            if max_bytes > 0 && prev_unsynced + batch_size >= max_bytes {
                let mut seg_guard = self.segment_manager.lock();
                seg_guard.sync()?;
                self.unsynced_bytes.store(0, Ordering::Release);
            }
        }

        Ok(Ok(batch))
    }

    /// Appends a record and immediately marks it committed. Used by every internal
    /// system-partition writer (cluster metadata proposals, DLQ routing, consumer-offset
    /// commits, transaction-state records, bootstrap) that doesn't itself integrate with
    /// ISR-quorum gating — unlike `produce_batch`'s use of `produce_batch_eos`, which
    /// leaves `committed_hw` ungated so it can advance only after quorum.
    pub fn produce_frame(&self, payload: &[u8]) -> IoResult<RecordBatch> {
        let f = self.produce_frame_eos(Bytes::copy_from_slice(payload), 0, 0, 0)?;
        let frame = f.unwrap();
        self.advance_committed_hw(frame.base_offset + 1);
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

    /// Syncs the segment to disk unconditionally, ignoring the configured flush policy.
    ///
    /// `flush_if_sync_policy` is a no-op under `FlushPolicy::AsyncPeriodic` (the default)
    /// until its time interval or byte threshold is reached, which is the right trade for
    /// data topics — Kafka's model is that durability comes from replication, not fsync,
    /// and paying an fsync per produce there would be a large, unrequested performance
    /// regression. `__cluster_metadata` is different: it is low-volume control-plane
    /// traffic (topic/ACL/partition-assignment changes), so an unconditional fsync costs
    /// almost nothing, and a majority acknowledgement of a metadata record is supposed to
    /// *be* a durability guarantee — the record drives authorization and partition
    /// placement, so applying it (locally or on a follower, per issue #24) must be safe
    /// even across a correlated crash. Callers use this instead of `flush_if_sync_policy`
    /// specifically for the `__cluster_metadata` replication and self-append paths.
    pub fn flush_durable(&self) -> IoResult<()> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.sync()?;
        self.unsynced_bytes.store(0, Ordering::Release);
        Ok(())
    }

    /// Test-only observation hook: `unsynced_bytes` resets to 0 only along a path that
    /// actually called `sync()` successfully, so reading it back after an operation is a
    /// way to tell, from outside this module, whether a real sync happened — without
    /// exposing any way for production code to read it.
    #[cfg(test)]
    pub(crate) fn unsynced_bytes_for_test(&self) -> u64 {
        self.unsynced_bytes.load(Ordering::Acquire)
    }

    /// Appends a frame received from another node (leader push or catch-up fetch) verbatim,
    /// preserving its original offset/timestamp/magic/CRC exactly rather than reassigning
    /// them locally. See `SegmentManager::append_verbatim`.
    /// Applies a leader's stored bytes to this replica's log, entry by entry, byte-for-byte.
    ///
    /// Each entry — frame or batch — is appended exactly as the leader stored it, so the
    /// replica's log is a byte-identical copy rather than a re-encoded one. A batch stays
    /// batched and stays compressed in the producer's codec; nothing here decodes records
    /// or decompresses anything.
    ///
    /// Returns the outcome of the last entry attempted, so a caller can react to a gap.
    /// Stops at the first entry that is not `Appended`.
    pub fn append_replica_entries_verbatim(
        &self,
        entries: &[u8],
    ) -> IoResult<(usize, crate::segment::VerbatimAppendResult)> {
        let mut cursor = 0usize;
        let mut appended = 0usize;
        let mut last = crate::segment::VerbatimAppendResult::AlreadyApplied;

        while cursor < entries.len() {
            let Ok((entry, consumed)) = crate::segment::decode_entry(&entries[cursor..]) else {
                break;
            };
            let (result, next_offset) = {
                let mut seg_guard = self.segment_manager.lock();
                let crate::segment::LogEntry::Batch(batch) = &entry;
                (
                    seg_guard.append_batch_verbatim(batch)?,
                    batch.base_offset + batch.last_offset_delta as u64 + 1,
                )
            };
            last = result;
            match result {
                crate::segment::VerbatimAppendResult::Appended => {
                    // Same contract as `append_replica_frame_verbatim`: advance the log end
                    // but deliberately not the committed high watermark, which only moves
                    // once the leader tells this follower the record is ISR-committed.
                    self.log_end_offset.store(next_offset, Ordering::Release);
                    // And the same AsyncPeriodic byte-threshold accounting as every other
                    // append path, so a follower's unsynced bytes stay bounded.
                    self.unsynced_bytes
                        .fetch_add(consumed as u64, Ordering::AcqRel);
                    appended += 1;
                }
                crate::segment::VerbatimAppendResult::AlreadyApplied => {}
                crate::segment::VerbatimAppendResult::Gap { .. } => break,
            }
            cursor += consumed;
        }

        Ok((appended, last))
    }

    /// Applies one leader entry to this replica's log byte-for-byte, whichever kind it is.
    pub fn append_replica_entry_verbatim(
        &self,
        entry: &crate::segment::LogEntry,
    ) -> IoResult<crate::segment::VerbatimAppendResult> {
        let (result, next_offset) = {
            let mut seg_guard = self.segment_manager.lock();
            let crate::segment::LogEntry::Batch(batch) = entry;
            (
                seg_guard.append_batch_verbatim(batch)?,
                batch.base_offset + batch.last_offset_delta as u64 + 1,
            )
        };
        if result == crate::segment::VerbatimAppendResult::Appended {
            // Same contract as `append_replica_frame_verbatim`: the log end advances but
            // the committed watermark does not — that only moves when the leader says the
            // record is ISR-committed.
            self.log_end_offset.store(next_offset, Ordering::Release);

            // AsyncPeriodic's byte threshold applies here exactly as it does on the local
            // produce path. Without this the threshold never fires for replicated data, so
            // a follower accumulates unsynced bytes without bound and flushes only on the
            // timer — the caller's `flush_if_sync_policy()` after the whole push does not
            // cover the eager, mid-push case the threshold exists for.
            let crate::segment::LogEntry::Batch(b) = entry;
            let entry_size = b.encoded_size() as u64;
            let prev_unsynced = self.unsynced_bytes.fetch_add(entry_size, Ordering::AcqRel);
            if let crate::config::FlushPolicy::AsyncPeriodic { max_bytes, .. } = &self.flush_policy
            {
                let max_bytes = *max_bytes as u64;
                if max_bytes > 0 && prev_unsynced + entry_size >= max_bytes {
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
    ///
    /// Rewinds to `SegmentManager::truncate_after`'s own resulting high watermark, not
    /// blindly to the requested `offset` — the same reason that function no longer
    /// trusts `offset` unconditionally: when `offset` lands inside a batch, the whole
    /// batch is physically removed (a batch is atomic on disk), so the log's real end
    /// drops to that batch's base offset instead. Storing the literal `offset` here would
    /// leave LEO claiming data that no longer exists, right after truncation was supposed
    /// to resolve exactly that kind of divergence.
    pub fn truncate_after(&self, offset: u64) -> IoResult<()> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.truncate_after(offset)?;
        let effective_offset = seg_guard.high_watermark();
        drop(seg_guard);
        self.log_end_offset
            .store(effective_offset, Ordering::Release);
        let mut hw = self.committed_hw.load(Ordering::Acquire);
        while hw > effective_offset {
            match self.committed_hw.compare_exchange_weak(
                hw,
                effective_offset,
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
    ) -> IoResult<RecordBatch> {
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

            let assigned_offset = frame.base_offset;
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
        let batch = self.produce_frame(payload)?;
        Ok(batch.base_offset)
    }

    /// Reads event records starting from target logical offset
    pub fn fetch(
        &self,
        start_offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<crate::segment::Record>> {
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.fetch(start_offset, max_bytes as usize)
    }

    /// Reads the stored bytes for `start_offset` onward without decoding or decompressing
    /// anything, stopping at the committed high watermark. This is the consumer-facing
    /// bound: a consumer must never be shown data that is not yet ISR-committed.
    /// See [`SegmentManager::fetch_entries`].
    pub fn fetch_entries(&self, start_offset: u64, max_bytes: u32) -> IoResult<Bytes> {
        let hw = self.high_watermark();
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.fetch_entries(start_offset, max_bytes as usize, hw)
    }

    /// The zero-copy form of [`Self::fetch_entries`]: the same byte range, expressed as a
    /// physical range plus its own file handle so the kernel can stream it straight to a
    /// socket. Bounded by the committed high watermark, exactly as `fetch_entries` is.
    /// See [`SegmentManager::plan_entries_fetch`].
    pub fn plan_entries_fetch(
        &self,
        start_offset: u64,
        max_bytes: u32,
    ) -> IoResult<Option<crate::segment::EntriesFetchPlan>> {
        let hw = self.high_watermark();
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.plan_entries_fetch(start_offset, max_bytes as usize, hw)
    }

    /// Same as [`Self::fetch_entries`], but bounded by the log end rather than the
    /// committed high watermark.
    ///
    /// A follower *must* be able to read past the committed point: the high watermark only
    /// advances once the ISR has acknowledged, and it can only acknowledge what it has
    /// fetched. Bounding replication at the high watermark would make the watermark unable
    /// to advance at all.
    pub fn fetch_entries_for_replication(
        &self,
        start_offset: u64,
        max_bytes: u32,
    ) -> IoResult<Bytes> {
        let leo = self.latest_offset();
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.fetch_entries(start_offset, max_bytes as usize, leo)
    }

    /// Reads event records starting from target timestamp
    /// Stored bytes from the first offset reaching `target_timestamp`, clamped to the
    /// committed high watermark. See [`SegmentManager::fetch_entries_by_timestamp`] — the
    /// reader applies the timestamp filter.
    pub fn fetch_entries_by_timestamp(
        &self,
        target_timestamp: u64,
        max_bytes: u32,
    ) -> IoResult<Bytes> {
        let hw = self.high_watermark();
        let mut seg_guard = self.segment_manager.lock();
        seg_guard.fetch_entries_by_timestamp(target_timestamp, max_bytes as usize, hw)
    }

    pub fn fetch_by_timestamp(
        &self,
        target_timestamp: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<crate::segment::Record>> {
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
    /// Aborted transaction ranges recorded for this partition — see
    /// [`SegmentManager::aborted_ranges`].
    pub fn aborted_ranges(&self) -> Vec<(u64, u64)> {
        let seg_guard = self.segment_manager.lock();
        seg_guard.aborted_ranges()
    }

    pub fn is_offset_aborted(&self, offset: u64) -> bool {
        let seg_guard = self.segment_manager.lock();
        seg_guard.is_offset_aborted(offset)
    }
}
