use crate::config::EngineConfig;
use crate::protocol::{BatchCompression, RecordBatch, RecordFrame, BATCH_MAGIC_BYTE, HEADER_SIZE};
use crate::segment::entry::{decode_entry, LogEntry};
use crate::segment::index::IndexSegment;
use crate::segment::log::{format_segment_filename, LogSegment};
use crate::segment::timeindex::TimeIndexSegment;
use bytes::Bytes;
use std::fs;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

use crate::segment::txnindex::TxnIndexSegment;

/// Outcome of `SegmentManager::append_verbatim`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerbatimAppendResult {
    /// The frame was appended at its own offset, byte-for-byte.
    Appended,
    /// `frame.offset` is already present locally (idempotent replay/double-delivery) — no-op.
    AlreadyApplied,
    /// `frame.offset` is beyond the local log end; the caller must resync from `expected`
    /// before this frame (or a replacement for it) can be appended.
    Gap { expected: u64 },
}

/// A frame-aligned physical byte range within one segment's log file, ready for a
/// zero-copy transmit (`TransmitFile` on Windows, `sendfile(2)` on Linux) straight from
/// `file` to a socket — the caller never needs to copy payload bytes into a Rust buffer.
/// See `SegmentManager::plan_zero_copy_fetch`.
#[derive(Debug)]
pub struct ZeroCopyFetchPlan {
    pub file: std::fs::File,
    pub physical_start: u64,
    pub physical_len: u64,
    pub frame_count: u32,
}

impl ZeroCopyFetchPlan {
    /// Streams this plan's byte range straight from disk (well, the OS page cache) to
    /// `socket` via the platform's kernel copy primitive. `self.file` is an independently
    /// owned handle, so no `SegmentManager` lock is held for the duration of this call.
    #[cfg(any(windows, target_os = "linux"))]
    pub async fn transmit(&self, socket: &tokio::net::TcpStream) -> IoResult<()> {
        crate::segment::log::transmit_zero_copy(
            &self.file,
            socket,
            self.physical_start,
            self.physical_len,
        )
        .await
    }
}

/// Segment pair holding associated log segment and index segment
#[derive(Debug)]
pub struct SegmentPair {
    pub base_offset: u64,
    pub log: LogSegment,
    pub index: IndexSegment,
    pub time_index: TimeIndexSegment,
    pub txn_index: TxnIndexSegment,
    pub mmap: Option<crate::segment::mmap::MmapLogSegment>,
}

impl SegmentPair {
    /// Reads all valid record frames sequentially from this log segment. A `RecordBatch`
    /// found along the way is skipped by its own reported length rather than decoded into
    /// the result — its presence must never truncate the scan and lose the frames after it.
    /// For a scan that decodes batches too instead of skipping them, see `read_all_entries`.
    pub fn read_all_frames(&mut self) -> IoResult<Vec<RecordFrame>> {
        Ok(self
            .read_all_entries()?
            .into_iter()
            .filter_map(|entry| match entry {
                LogEntry::Frame(frame) => Some(frame),
                LogEntry::Batch(_) => None,
            })
            .collect())
    }

    /// Reads every entry in this log segment in order, fully decoded — both `RecordFrame`s
    /// and `RecordBatch`es, unlike `read_all_frames` which silently skips batches. Used by
    /// `SegmentManager::compact_segments` (via `expand_entries_for_compaction`): compaction
    /// rewrites the segment in place, so skipping a batch there would permanently destroy
    /// every record inside it rather than merely omitting it from one read.
    fn read_all_entries(&mut self) -> IoResult<Vec<LogEntry>> {
        let size = self.log.physical_size as usize;
        if size == 0 {
            return Ok(Vec::new());
        }
        let raw_bytes = self.log.read_at(0, size)?;
        let mut entries = Vec::new();
        let mut cursor = 0usize;

        while cursor < raw_bytes.len() {
            if cursor + HEADER_SIZE > raw_bytes.len() {
                break;
            }
            let slice = &raw_bytes[cursor..];
            match decode_entry(slice) {
                Ok((entry, consumed)) => {
                    cursor += consumed;
                    entries.push(entry);
                }
                Err(_) => break,
            }
        }

        Ok(entries)
    }
}

/// Flattens a segment's raw entries into individual `RecordFrame`s for log compaction: a
/// `RecordFrame` entry passes through unchanged, and a `RecordBatch` entry's records are
/// decoded — already decompressed by `RecordBatch::records` (batch compression applies once
/// to the whole batch, never per record, so there is nothing left to decompress here, unlike
/// a compressed `RecordFrame`'s payload) — and each becomes its own uncompressed
/// `RecordFrame` via `RecordFrame::create`, exactly the flattening `SegmentManager::fetch`
/// already does when serving a batch to a client.
///
/// Design choice (see `SegmentManager::compact_segments`'s doc comment for the fuller
/// reasoning): compaction always rewrites surviving records as plain frames, never as
/// reconstituted batches. A batch can be partially compacted away — some of its records
/// superseded or tombstoned, others kept — at which point it is no longer the batch that was
/// produced, so the original batch's `base_sequence`/`producer_id`/`producer_epoch` no
/// longer describe its contents and cannot be preserved meaningfully. Emitting plain frames
/// sidesteps that: each surviving record stands on its own, with no batch-level metadata
/// left half-true. The cost is that a compacted segment loses batching entirely — no more
/// batch-level compression or producer/base-sequence metadata for what remains — but nothing
/// in Hermes reconstructs idempotent-producer dedup state by reading `base_sequence`/
/// `producer_id` back off disk (that state is tracked in memory, at produce time —
/// `PartitionManager::validate_sequence`), so no correctness behavior depends on retaining
/// it.
///
/// A batch whose record data fails to decode (corrupt, but structurally valid enough to
/// pass its own header CRC in `decode_entry`) has its records skipped rather than aborting
/// the whole flatten. This differs from `fetch`'s "decode failure stops the scan" behavior,
/// but `fetch` decodes as a single pass over an uncertain byte stream where a failure means
/// the rest of the read is unusable; here `entries` was already fully parsed into discrete
/// entries by `read_all_entries` before this function ever runs, so a bad batch's neighbors
/// are known-good and there's no reason they should be lost too.
/// Encoded size of a log entry on disk, whichever kind it is.
fn entry_encoded_size(entry: &LogEntry) -> usize {
    match entry {
        LogEntry::Frame(frame) => frame.encoded_size(),
        LogEntry::Batch(batch) => batch.encoded_size(),
    }
}

/// Encodes a log entry exactly as it is stored.
fn encode_entry_into(entry: &LogEntry, buf: &mut Vec<u8>) {
    match entry {
        LogEntry::Frame(frame) => frame.encode_into(buf),
        LogEntry::Batch(batch) => batch.encode_into(buf),
    }
}

/// The offset an entry is indexed by: a frame's own offset, a batch's base offset.
fn entry_index_offset(entry: &LogEntry) -> u64 {
    match entry {
        LogEntry::Frame(frame) => frame.offset,
        LogEntry::Batch(batch) => batch.base_offset,
    }
}

/// The timestamp an entry is time-indexed by.
fn entry_index_timestamp(entry: &LogEntry) -> u64 {
    match entry {
        LogEntry::Frame(frame) => frame.timestamp,
        LogEntry::Batch(batch) => batch.base_timestamp,
    }
}

/// One record as compaction sees it, whichever entry kind it came from.
///
/// Compaction is the one place the broker legitimately decodes records — it cannot dedupe
/// by key without reading keys — so this is where a batch is decompressed and a frame is
/// decompressed, and nowhere else on any serving path.
#[derive(Debug, Clone)]
struct CompactionRecord {
    offset: u64,
    timestamp: u64,
    /// The key the producer wrote. `None` means the record carries no key at all — it
    /// cannot participate in dedup and is always kept. Produce rejects keyless records on
    /// a compacted topic, so this only arises for records written before the topic became
    /// compacted, or for a plain frame, which has no key field.
    key: Option<Bytes>,
    /// `None` is a tombstone: it marks the key deleted. Distinct from `Some(empty)`, an
    /// ordinary record whose value happens to be empty.
    value: Option<Bytes>,
    is_control: bool,
}

/// Decodes every record out of a segment's entries for compaction's key scan.
///
/// A batch whose record data fails to decode has its records skipped rather than aborting
/// the whole scan: `entries` was already fully parsed into discrete entries by
/// `read_all_entries`, so a bad batch's neighbours are known-good and there is no reason
/// to lose them too.
fn records_for_compaction(entries: &[LogEntry]) -> Vec<CompactionRecord> {
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry {
            LogEntry::Frame(frame) => {
                // A frame's payload is compressed per-record, unlike a batch's, so it must
                // be decompressed before its bytes mean anything. A frame has no key field
                // at all, so it can never be deduped by key.
                let payload = frame
                    .decompress_payload()
                    .unwrap_or_else(|_| frame.payload.clone());
                out.push(CompactionRecord {
                    offset: frame.offset,
                    timestamp: frame.timestamp,
                    key: None,
                    value: Some(payload),
                    is_control: frame.is_control_marker(),
                });
            }
            LogEntry::Batch(batch) => {
                let Ok(records) = batch.records() else {
                    continue;
                };
                for r in records {
                    out.push(CompactionRecord {
                        offset: r.offset,
                        timestamp: r.timestamp,
                        key: r.key,
                        value: r.value,
                        is_control: false,
                    });
                }
            }
        }
    }
    out
}

/// Rotates log and index segments, manages historical segments, and performs index-accelerated seeks
#[derive(Debug)]
pub struct SegmentManager {
    dir: PathBuf,
    config: EngineConfig,
    active: SegmentPair,
    historical: Vec<SegmentPair>,
    bytes_since_last_index: u64,
    high_watermark: u64,
    /// Reused scratch buffer for single-frame header+payload encoding before writing to
    /// the active segment (see `append_frame_to_active`). Appends are always serialized
    /// behind this `SegmentManager`'s owning `Mutex` (one partition, one writer at a time),
    /// so a single reused buffer is safe and avoids a fresh `Vec` allocation on every
    /// produced/replicated record.
    frame_encode_scratch: bytes::BytesMut,
    /// Wall-clock time (ms since epoch) the current active segment became active — either
    /// this `SegmentManager` opening it for the first time or the last `rotate_segment`
    /// call. Used for `config.segment_ms`-based time rolling (Kafka `segment.ms`):
    /// low-volume topics that never hit `max_segment_bytes` still get rotated eventually,
    /// so their data becomes eligible for retention/compaction instead of sitting in a
    /// perpetually-active segment forever.
    active_created_at_ms: u64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Sibling `<name>.deleted` path used to hold a segment file aside during compaction's
/// crash-safe swap (see `compact_segments`).
fn deleted_backup_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".deleted");
    PathBuf::from(s)
}

/// Resolves any half-finished compaction swap left behind by a crash, before the segments
/// in `dir` are opened.
///
/// Compaction moves each live file to `<name>.deleted`, renames the compacted `<name>
/// .compact` into place, fsyncs, then unlinks the backups. A crash can therefore leave:
///
/// - a `.deleted` backup with **no** live file — the crash landed between the two renames,
///   so the compacted copy was never installed. Restore the backup; the segment reverts to
///   its pre-compaction (still fully valid) contents.
/// - a `.deleted` backup **and** a live file — the swap completed and only the cleanup
///   unlink was lost. Drop the backup.
///
/// Leftover `.compact` temporaries are always discarded: they're only meaningful within
/// the single `compact_segments` call that created them, and that call did not complete.
fn recover_interrupted_compaction(dir: &Path) -> IoResult<()> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(());
    };

    let mut backups = Vec::new();
    let mut temporaries = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        match path.extension().and_then(|e| e.to_str()) {
            Some("deleted") => backups.push(path),
            Some("compact") => temporaries.push(path),
            _ => {}
        }
    }

    for tmp in temporaries {
        tracing::warn!(
            "Compaction recovery: discarding leftover temporary {}",
            tmp.display()
        );
        let _ = fs::remove_file(&tmp);
    }

    let mut restored = 0usize;
    let mut dropped = 0usize;
    for backup in backups {
        // Strip the trailing `.deleted` to get the live path it was taken from.
        let live = PathBuf::from(
            backup
                .as_os_str()
                .to_string_lossy()
                .trim_end_matches(".deleted")
                .to_string(),
        );
        if live.exists() {
            let _ = fs::remove_file(&backup);
            dropped += 1;
        } else {
            tracing::warn!(
                "Compaction recovery: restoring {} from interrupted compaction",
                live.display()
            );
            fs::rename(&backup, &live)?;
            // The restored file is the pre-compaction original, whose recorded clean-size
            // marker no longer necessarily matches — force a full verifying scan.
            crate::segment::log::remove_clean_marker(&live);
            restored += 1;
        }
    }

    if restored > 0 || dropped > 0 {
        tracing::info!(
            "Compaction recovery in {}: restored {} file(s), dropped {} stale backup(s)",
            dir.display(),
            restored,
            dropped
        );
        crate::segment::log::fsync_dir(dir);
    }
    Ok(())
}

impl SegmentManager {
    pub fn open(dir: impl AsRef<Path>, config: EngineConfig) -> IoResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Finish or roll back any compaction swap a crash interrupted, before segment
        // discovery below — otherwise a segment whose live file is momentarily absent
        // (backup taken, replacement not yet renamed in) would simply be missed and its
        // data silently lost from the log.
        recover_interrupted_compaction(&dir)?;

        // Discover existing log segments
        let mut base_offsets = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "log") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(base_offset) = stem.parse::<u64>() {
                        base_offsets.push(base_offset);
                    }
                }
            }
        }

        base_offsets.sort_unstable();

        if base_offsets.is_empty() {
            base_offsets.push(0);
        }

        let active_base = *base_offsets.last().unwrap();
        let mut historical = Vec::new();

        for &base in &base_offsets[..base_offsets.len() - 1] {
            let index_path = dir.join(format!("{}.index", format_segment_filename(base)));
            let mut index = IndexSegment::open(index_path, base)?;
            let time_index_path = dir.join(format!("{}.timeindex", format_segment_filename(base)));
            let time_index = TimeIndexSegment::open(time_index_path, base)?;
            let txn_index_path = dir.join(format!("{}.txnindex", format_segment_filename(base)));
            let txn_index = TxnIndexSegment::open(txn_index_path)?;
            // Historical segments are immutable once rotated — trust a valid `.clean`
            // marker (written by `LogSegment::finalize` at rotation time) to skip the
            // O(N) full-segment CRC recovery scan on every startup. Only ever safe for
            // non-active segments; see `LogSegment::open_with_trust`.
            let log = LogSegment::open_with_trust(
                &dir,
                base,
                config.max_segment_bytes,
                config.index_interval_bytes,
                config.preallocate_segments,
                &mut index,
                true,
            )?;
            let mmap = crate::segment::mmap::MmapLogSegment::open(&log.path, base).ok();
            historical.push(SegmentPair {
                base_offset: base,
                log,
                index,
                time_index,
                txn_index,
                mmap,
            });
        }

        let active_index_path = dir.join(format!("{}.index", format_segment_filename(active_base)));
        let mut active_index = IndexSegment::open(active_index_path, active_base)?;
        let active_time_index_path = dir.join(format!(
            "{}.timeindex",
            format_segment_filename(active_base)
        ));
        let active_time_index = TimeIndexSegment::open(active_time_index_path, active_base)?;
        let active_txn_index_path =
            dir.join(format!("{}.txnindex", format_segment_filename(active_base)));
        let active_txn_index = TxnIndexSegment::open(active_txn_index_path)?;
        let active_log = LogSegment::open(
            &dir,
            active_base,
            config.max_segment_bytes,
            config.index_interval_bytes,
            config.preallocate_segments,
            &mut active_index,
        )?;

        let high_watermark = active_log.next_offset;

        Ok(Self {
            dir,
            config,
            active: SegmentPair {
                base_offset: active_base,
                log: active_log,
                index: active_index,
                time_index: active_time_index,
                txn_index: active_txn_index,
                mmap: None,
            },
            historical,
            bytes_since_last_index: 0,
            high_watermark,
            frame_encode_scratch: bytes::BytesMut::new(),
            active_created_at_ms: now_ms(),
        })
    }

    /// Rotates the active segment before an about-to-be-appended frame if either the
    /// size threshold (`max_segment_bytes`) or the age threshold (`segment_ms`, see
    /// `should_roll_by_time`) calls for it. Shared by every append path so a low-volume
    /// topic that never hits the byte threshold still rotates on time, same as one that
    /// does.
    fn maybe_rotate_before_append(&mut self, frame_size: u64) -> IoResult<()> {
        if self.active.log.physical_size + frame_size > self.config.max_segment_bytes
            || self.should_roll_by_time()
        {
            self.rotate_segment()?;
        }
        Ok(())
    }

    /// Whether the active segment should be rolled purely due to age (Kafka
    /// `segment.ms`), independent of `max_segment_bytes`. Never rolls an already-empty
    /// active segment — an idle topic with nothing written yet has nothing to gain from
    /// rotating, and doing so would just churn empty segment files forever.
    fn should_roll_by_time(&self) -> bool {
        match self.config.segment_ms {
            Some(segment_ms) if segment_ms > 0 && self.active.log.physical_size > 0 => {
                now_ms().saturating_sub(self.active_created_at_ms) >= segment_ms
            }
            _ => false,
        }
    }

    pub fn high_watermark(&self) -> u64 {
        self.high_watermark
    }

    pub fn active_base_offset(&self) -> u64 {
        self.active.base_offset
    }

    /// Append record frames into active segment. Performs segment rotation if size limit reached.
    pub fn append(&mut self, payload: &[u8], timestamp: u64) -> IoResult<RecordFrame> {
        self.append_with_codec(
            Bytes::copy_from_slice(payload),
            timestamp,
            crate::config::CompressionCodec::None,
        )
    }

    /// Encodes `frame`'s header+payload into the reused `frame_encode_scratch` buffer and
    /// writes it to the active segment's log file, returning the physical byte position it
    /// was written at. Shared by every single-frame append path (`append_with_codec`,
    /// `append_verbatim`, `append_control_marker`) so none of them need a fresh `Vec`
    /// allocation per call.
    fn append_frame_to_active(&mut self, frame: &RecordFrame) -> IoResult<u64> {
        self.frame_encode_scratch.clear();
        frame.encode_into(&mut self.frame_encode_scratch);
        self.active.log.append_bytes(&self.frame_encode_scratch)
    }

    /// Same as `append_frame_to_active`, for a `RecordBatch` instead of a single frame —
    /// shares the same reused scratch buffer (appends of every kind are already serialized
    /// behind this `SegmentManager`'s owning `Mutex`, so there is never more than one
    /// encode in flight to reuse it for).
    fn append_batch_to_active(&mut self, batch: &RecordBatch) -> IoResult<u64> {
        self.frame_encode_scratch.clear();
        batch.encode_into(&mut self.frame_encode_scratch);
        self.active.log.append_bytes(&self.frame_encode_scratch)
    }

    /// Append record frames with optional payload compression. `payload` is taken by
    /// value as `Bytes` rather than `&[u8]` so the hot produce path (see
    /// `PartitionManager::produce_frame_eos`) can thread the already-decoded, already-owned
    /// `Bytes` from the wire straight through to `RecordFrame::create` — a `Bytes` clone is
    /// a refcount bump, not a copy, whereas the old `&[u8]` signature forced a fresh
    /// `Vec<u8>` allocation + memcpy here on every single produced record.
    pub fn append_with_codec(
        &mut self,
        payload: Bytes,
        timestamp: u64,
        codec: crate::config::CompressionCodec,
    ) -> IoResult<RecordFrame> {
        let assigned_offset = self.high_watermark;
        // This path only ever writes records the broker authored itself, so there is no
        // producer codec to honour — `Producer` resolves to uncompressed here.
        let frame = match codec.for_broker_authored_frame() {
            crate::config::CompressionCodec::Lz4 => {
                RecordFrame::create_compressed_lz4(assigned_offset, timestamp, &payload)
            }
            crate::config::CompressionCodec::Zstd => {
                RecordFrame::create_compressed_zstd(assigned_offset, timestamp, &payload)
            }
            // `for_broker_authored_frame` has already mapped `Producer` to `None`.
            crate::config::CompressionCodec::None | crate::config::CompressionCodec::Producer => {
                RecordFrame::create(assigned_offset, timestamp, payload)
            }
        };
        let frame_size = frame.encoded_size() as u64;

        self.maybe_rotate_before_append(frame_size)?;

        let physical_pos = self.append_frame_to_active(&frame)?;

        // Sparse index entry placement
        if self.bytes_since_last_index >= self.config.index_interval_bytes
            || self.active.index.entries_count() == 0
        {
            self.active.index.append(assigned_offset, physical_pos)?;
            let _ = self.active.time_index.append(timestamp, assigned_offset);
            self.bytes_since_last_index = 0;
        }

        self.bytes_since_last_index += frame_size;
        self.high_watermark += 1;
        self.active.log.next_offset = self.high_watermark;

        Ok(frame)
    }

    /// Appends a batch of records into the active segment as one `RecordBatch`, assigning
    /// its base offset from the current high watermark exactly as `append_with_codec`
    /// assigns a single frame's offset. Performs segment rotation if the size limit is
    /// reached.
    ///
    /// Unlike a frame, a batch spans a *range* of offsets — `base_offset` through
    /// `base_offset + last_offset_delta` — so it is only indexed (sparse offset index and
    /// time index) by that base offset, the same way `open_at_path_with_trust`'s recovery
    /// scan indexes a batch it finds on disk: the rest of the batch's offsets are reached
    /// by decoding forward from there, never by their own index entry.
    ///
    /// Takes the batch as given and stamps only what the broker owns: the base offset
    /// (this log's current end) and the leader epoch. `record_data` is stored exactly as
    /// handed over — never decompressed, never rebuilt — so a batch a producer compressed
    /// stays compressed on disk in the producer's own codec.
    pub fn append_batch(
        &mut self,
        mut batch: RecordBatch,
        leader_epoch: u32,
    ) -> IoResult<RecordBatch> {
        let assigned_base_offset = self.high_watermark;
        let base_timestamp = batch.base_timestamp;
        batch.assign_base_offset_and_leader_epoch(assigned_base_offset, leader_epoch);
        let batch_size = batch.encoded_size() as u64;

        self.maybe_rotate_before_append(batch_size)?;

        let physical_pos = self.append_batch_to_active(&batch)?;

        // Sparse index entry placement, keyed by the batch's base offset (see doc comment).
        if self.bytes_since_last_index >= self.config.index_interval_bytes
            || self.active.index.entries_count() == 0
        {
            self.active
                .index
                .append(assigned_base_offset, physical_pos)?;
            let _ = self
                .active
                .time_index
                .append(base_timestamp, assigned_base_offset);
            self.bytes_since_last_index = 0;
        }

        self.bytes_since_last_index += batch_size;
        // The batch occupies `last_offset_delta + 1` offsets starting at
        // `assigned_base_offset` (this also correctly reserves exactly one offset for a
        // zero-record batch, matching `RecordBatch::create`'s own `last_offset_delta`
        // convention, rather than duplicating its `record_count.saturating_sub(1)` logic).
        self.high_watermark += batch.last_offset_delta as u64 + 1;
        self.active.log.next_offset = self.high_watermark;

        Ok(batch)
    }

    /// Appends a frame produced elsewhere (i.e. a leader's `RecordFrame`) byte-for-byte:
    /// the offset, timestamp, magic byte, and CRC are all taken from `frame` as given,
    /// never reassigned locally. This is what makes a replica's on-disk log byte-identical
    /// to the leader's for the same offset range, instead of diverging (the old replication
    /// path re-derived offset/timestamp/codec through `append_with_codec` on every
    /// replicated record). `frame` has already been CRC-validated by `RecordFrame::decode`,
    /// so no additional integrity check is needed here.
    /// Appends a batch produced elsewhere (a leader's `RecordBatch`) byte-for-byte: base
    /// offset, leader epoch, producer metadata, compression and CRC are all taken exactly
    /// as given, never restamped and never re-compressed. This is what makes a replica's
    /// log byte-identical to its leader's — including staying compressed in the codec the
    /// original producer chose, rather than being decompressed into loose records on the
    /// way across.
    pub fn append_batch_verbatim(&mut self, batch: &RecordBatch) -> IoResult<VerbatimAppendResult> {
        // A batch is atomic: it is either wholly present or wholly absent. Its last offset
        // being below the log end means the whole thing is already here.
        let last_offset = batch.base_offset + batch.last_offset_delta as u64;
        if last_offset < self.high_watermark {
            return Ok(VerbatimAppendResult::AlreadyApplied);
        }
        if batch.base_offset > self.high_watermark {
            return Ok(VerbatimAppendResult::Gap {
                expected: self.high_watermark,
            });
        }
        if batch.base_offset < self.high_watermark {
            // Straddles the log end — the local log holds part of this batch already, which
            // cannot happen for atomic entries and means the follower is out of step.
            return Ok(VerbatimAppendResult::Gap {
                expected: self.high_watermark,
            });
        }

        let batch_size = batch.encoded_size() as u64;
        self.maybe_rotate_before_append(batch_size)?;

        let physical_pos = self.append_batch_to_active(batch)?;

        if self.bytes_since_last_index >= self.config.index_interval_bytes
            || self.active.index.entries_count() == 0
        {
            self.active.index.append(batch.base_offset, physical_pos)?;
            let _ = self
                .active
                .time_index
                .append(batch.base_timestamp, batch.base_offset);
            self.bytes_since_last_index = 0;
        }

        self.bytes_since_last_index += batch_size;
        self.high_watermark = last_offset + 1;
        self.active.log.next_offset = self.high_watermark;

        Ok(VerbatimAppendResult::Appended)
    }

    pub fn append_verbatim(&mut self, frame: &RecordFrame) -> IoResult<VerbatimAppendResult> {
        if frame.offset < self.high_watermark {
            return Ok(VerbatimAppendResult::AlreadyApplied);
        }
        if frame.offset > self.high_watermark {
            return Ok(VerbatimAppendResult::Gap {
                expected: self.high_watermark,
            });
        }

        let frame_size = frame.encoded_size() as u64;
        self.maybe_rotate_before_append(frame_size)?;

        let physical_pos = self.append_frame_to_active(frame)?;

        if self.bytes_since_last_index >= self.config.index_interval_bytes
            || self.active.index.entries_count() == 0
        {
            self.active.index.append(frame.offset, physical_pos)?;
            let _ = self.active.time_index.append(frame.timestamp, frame.offset);
            self.bytes_since_last_index = 0;
        }

        self.bytes_since_last_index += frame_size;
        self.high_watermark = frame.offset + 1;
        self.active.log.next_offset = self.high_watermark;

        Ok(VerbatimAppendResult::Appended)
    }

    /// Discards any locally-stored entries at or beyond `offset`, across the active
    /// segment and (if necessary) historical segments — Raft-style conflicting-suffix
    /// truncation, used when a follower's log diverges from its leader's and must be
    /// brought back in line before appending the leader's authoritative entries.
    pub fn truncate_after(&mut self, offset: u64) -> IoResult<()> {
        if offset >= self.high_watermark {
            return Ok(());
        }

        // Drop whole historical segments that start at/after the truncation point, and
        // delete their files.
        //
        // Removing them from `self.historical` alone is not enough: segment discovery on
        // open scans the directory, so any file left behind is re-adopted on the next
        // restart and the log silently returns to its pre-truncation length. Since
        // truncation is precisely how a follower discards a diverged suffix before
        // rejoining, leaving the files would reintroduce the very records that had to go —
        // at the moment the replica rejoins the cluster.
        let (kept, discarded): (Vec<_>, Vec<_>) = std::mem::take(&mut self.historical)
            .into_iter()
            .partition(|seg| seg.base_offset < offset);
        self.historical = kept;
        for seg in &discarded {
            crate::segment::log::remove_clean_marker(&seg.log.path);
            let _ = fs::remove_file(&seg.log.path);
            let _ = fs::remove_file(seg.index.path());
            let _ = fs::remove_file(seg.time_index.path());
            let _ = fs::remove_file(seg.txn_index.path());
        }
        if !discarded.is_empty() {
            // Make the unlinks durable, so a crash right after truncation can't leave the
            // directory listing still referencing the discarded segments.
            crate::segment::log::fsync_dir(&self.dir);
        }

        // The high watermark after truncation: normally `offset`, but see
        // `truncate_segment_pair`'s doc comment — if `offset` lands inside a batch, the
        // whole batch is physically removed (a batch is atomic on disk) and the
        // watermark must drop to that batch's base offset instead, or it would claim an
        // offset for data that no longer exists.
        let effective_offset;

        if offset < self.active.base_offset {
            // The truncation point falls inside a historical segment (rare: it means the
            // divergence predates the most recent rotation). Promote the highest-based
            // remaining historical segment to active, truncated at `offset`, and discard
            // the old active segment's on-disk files entirely.
            if let Some(mut promoted) = self.historical.pop() {
                effective_offset = Self::truncate_segment_pair(&mut promoted, offset)?;
                let old_active = std::mem::replace(&mut self.active, promoted);
                let _ = fs::remove_file(&old_active.log.path);
                let _ = fs::remove_file(old_active.index.path());
                let _ = fs::remove_file(old_active.time_index.path());
                let _ = fs::remove_file(old_active.txn_index.path());
            } else {
                // No historical segment covers `offset` (shouldn't happen if `offset`
                // is a valid previously-seen index) — fall back to truncating active.
                effective_offset = Self::truncate_segment_pair(&mut self.active, offset)?;
            }
        } else {
            effective_offset = Self::truncate_segment_pair(&mut self.active, offset)?;
        }

        self.high_watermark = effective_offset;
        self.active.log.next_offset = effective_offset;
        self.bytes_since_last_index = 0;
        crate::segment::log::fsync_dir(&self.dir);
        Ok(())
    }

    /// Truncates a single segment pair so no frame with offset >= `offset` remains.
    /// Returns the *effective* truncation offset: normally just `offset`, but when
    /// `offset` falls inside a batch (batches are atomic on disk — see
    /// `physical_pos_for_offset`), the whole batch is removed and the effective offset
    /// drops to that batch's own base offset. Callers must use the returned value (not
    /// the requested `offset`) for anything claiming what the log now ends at, e.g. the
    /// high watermark — see `truncate_after`.
    fn truncate_segment_pair(pair: &mut SegmentPair, offset: u64) -> IoResult<u64> {
        let (phys, effective_offset) = Self::physical_pos_for_offset(pair, offset)?;
        pair.log.truncate_to(phys)?;
        pair.index.truncate_after(effective_offset)?;
        pair.time_index.truncate_after(effective_offset)?;
        pair.txn_index.truncate_after(effective_offset)?;
        pair.mmap = None;
        Ok(effective_offset)
    }

    /// Scans forward from the nearest sparse-index entry to find the physical byte
    /// position at which `offset` begins within this segment pair. Also returns the
    /// *effective* offset that position corresponds to: `offset` itself, unless `offset`
    /// falls inside a batch's range, in which case it is that batch's own base offset —
    /// a batch is atomic, so the only physical position reachable for any offset within
    /// it is the batch's start, and a caller truncating there is removing the whole
    /// batch, not just the tail of it from `offset` onward.
    fn physical_pos_for_offset(pair: &mut SegmentPair, offset: u64) -> IoResult<(u64, u64)> {
        let seek_entry = pair.index.find_nearest_physical_pos(offset);
        let start_pos = seek_entry.map_or(0, |e| e.physical_position as u64);
        let raw = pair
            .log
            .read_at(start_pos, pair.log.physical_size as usize)?;

        let mut cursor = 0usize;
        let mut phys = start_pos;
        while cursor < raw.len() {
            if cursor + HEADER_SIZE > raw.len() {
                break;
            }
            match decode_entry(&raw[cursor..]) {
                Ok((LogEntry::Frame(frame), consumed)) => {
                    if frame.offset == offset {
                        return Ok((phys, offset));
                    }
                    cursor += consumed;
                    phys += consumed as u64;
                }
                Ok((LogEntry::Batch(batch), consumed)) => {
                    // A batch is atomic — there is no physical position for an offset
                    // partway through it, only for the batch as a whole. Any target
                    // offset within its range (including its base) can only be reached
                    // by truncating at the batch's own start, since discarding one of a
                    // batch's records means discarding the whole batch. The effective
                    // offset drops to the batch's base offset accordingly.
                    if offset <= batch.base_offset + batch.last_offset_delta as u64 {
                        return Ok((phys, batch.base_offset));
                    }
                    cursor += consumed;
                    phys += consumed as u64;
                }
                Err(_) => break,
            }
        }
        // `offset` not found in this segment (e.g. it's exactly the segment's end) —
        // truncating at the current physical size is a safe no-op.
        Ok((pair.log.physical_size, offset))
    }

    /// Append a control marker frame into active segment. Performs segment rotation if size limit reached.
    pub fn append_control_marker(
        &mut self,
        control_type: u8,
        producer_id: u64,
        transaction_id: &str,
        timestamp: u64,
    ) -> IoResult<RecordFrame> {
        let assigned_offset = self.high_watermark;
        let frame = RecordFrame::create_control_marker(
            assigned_offset,
            timestamp,
            control_type,
            producer_id,
            transaction_id,
        );
        let frame_size = frame.encoded_size() as u64;
        self.maybe_rotate_before_append(frame_size)?;

        let physical_pos = self.append_frame_to_active(&frame)?;

        if self.bytes_since_last_index >= self.config.index_interval_bytes
            || self.active.index.entries_count() == 0
        {
            self.active.index.append(assigned_offset, physical_pos)?;
            let _ = self.active.time_index.append(timestamp, assigned_offset);
            self.bytes_since_last_index = 0;
        }

        self.bytes_since_last_index += frame_size;
        self.high_watermark += 1;
        self.active.log.next_offset = self.high_watermark;

        Ok(frame)
    }

    /// Rotate active segment to new segment file
    pub fn rotate_segment(&mut self) -> IoResult<()> {
        let new_base_offset = self.high_watermark;
        tracing::info!(
            "Rotating segment at offset {}. Active segment size was {} bytes.",
            new_base_offset,
            self.active.log.physical_size
        );

        self.active.log.finalize()?;
        self.active.index.sync()?;
        let _ = self.active.time_index.sync();

        let new_index_path = self.dir.join(format!(
            "{}.index",
            format_segment_filename(new_base_offset)
        ));
        let mut new_index = IndexSegment::open(new_index_path, new_base_offset)?;
        let new_time_index_path = self.dir.join(format!(
            "{}.timeindex",
            format_segment_filename(new_base_offset)
        ));
        let new_time_index = TimeIndexSegment::open(new_time_index_path, new_base_offset)?;
        let new_txn_index_path = self.dir.join(format!(
            "{}.txnindex",
            format_segment_filename(new_base_offset)
        ));
        let new_txn_index = TxnIndexSegment::open(new_txn_index_path)?;
        let new_log = LogSegment::open(
            &self.dir,
            new_base_offset,
            self.config.max_segment_bytes,
            self.config.index_interval_bytes,
            self.config.preallocate_segments,
            &mut new_index,
        )?;

        let new_active = SegmentPair {
            base_offset: new_base_offset,
            log: new_log,
            index: new_index,
            time_index: new_time_index,
            txn_index: new_txn_index,
            mmap: None,
        };

        let mut old_active = std::mem::replace(&mut self.active, new_active);
        old_active.mmap = crate::segment::mmap::MmapLogSegment::open(
            &old_active.log.path,
            old_active.base_offset,
        )
        .ok();
        self.historical.push(old_active);
        self.bytes_since_last_index = 0;
        self.active_created_at_ms = now_ms();

        // Make the new segment's file creations and the old segment's finalized size
        // durable against a crash, not just their own contents (fsync'd individually
        // above/in `finalize`) — a rename or a new file's directory entry can still be
        // lost on some filesystems without an explicit parent-directory fsync.
        crate::segment::log::fsync_dir(&self.dir);

        Ok(())
    }

    /// Appends an aborted transaction range to the segment manager's transaction index
    pub fn append_aborted_txn(
        &mut self,
        producer_id: u64,
        first_offset: u64,
        last_offset: u64,
    ) -> IoResult<()> {
        let pair = self.find_segment_pair_mut(first_offset);
        pair.txn_index
            .append(producer_id, first_offset, last_offset)
    }

    /// Checks if a given offset belongs to an aborted transaction
    /// The aborted transaction ranges this partition has recorded, as
    /// `(first_offset, last_offset)` inclusive pairs.
    ///
    /// Sent to read-committed consumers so they can drop aborted records themselves: the
    /// broker cannot filter them out of a compressed batch without decoding it, and it does
    /// not decode. This is what Kafka's `aborted_transactions` fetch-response field is for.
    pub fn aborted_ranges(&self) -> Vec<(u64, u64)> {
        // Active *and* historical, matching `is_offset_aborted`'s coverage — a consumer
        // reading from an old offset must be told about aborts recorded in rolled segments
        // too, not just the active one.
        self.historical
            .iter()
            .flat_map(|pair| pair.txn_index.entries())
            .chain(self.active.txn_index.entries())
            .map(|e| (e.first_offset, e.last_offset))
            .collect()
    }

    pub fn is_offset_aborted(&self, offset: u64) -> bool {
        if self.active.txn_index.is_aborted(offset) {
            return true;
        }
        for pair in &self.historical {
            if pair.txn_index.is_aborted(offset) {
                return true;
            }
        }
        false
    }

    /// Whether any segment of this partition records an aborted transaction range.
    ///
    /// Cheap enough to check per fetch (it inspects index sizes, not records), and lets
    /// the zero-copy path decline partitions whose correct answer depends on filtering.
    pub fn has_aborted_transactions(&self) -> bool {
        if !self.active.txn_index.is_empty() {
            return true;
        }
        self.historical.iter().any(|p| !p.txn_index.is_empty())
    }

    /// Read records starting at logical offset using binary search across segments and sparse index ($O(\log N)$)
    ///
    /// A `RecordBatch` encountered along the way is decoded and its records surfaced as
    /// synthetic uncompressed `RecordFrame`s (`RecordFrame::create`, magic `0xAB`) — one
    /// per decoded, already-decompressed `BatchRecord`, filtered to `offset >=
    /// start_offset` exactly like a real frame is. This gives callers the same records,
    /// offsets, and payload bytes a frame-per-record produce would have returned; a
    /// caller that only wants `start_offset` from the middle of a batch still gets it,
    /// because a batch is atomic on disk and must be decoded whole (see
    /// `physical_pos_for_offset`) before its later records can be filtered out.
    pub fn fetch(&mut self, start_offset: u64, max_bytes: usize) -> IoResult<Vec<RecordFrame>> {
        let segment_pair = self.find_segment_pair_mut(start_offset);
        let seek_entry = segment_pair.index.find_nearest_physical_pos(start_offset);
        let start_pos = seek_entry.map_or(0, |e| e.physical_position as u64);

        if let Some(ref mmap) = segment_pair.mmap {
            return Ok(mmap.fetch_zero_copy(start_pos, start_offset, max_bytes));
        }

        let raw_bytes = segment_pair.log.read_at(start_pos, max_bytes)?;
        let raw_bytes = Self::ensure_first_batch_fits(segment_pair, start_pos, raw_bytes)?;

        let mut frames = Vec::new();
        let mut cursor = 0usize;

        while cursor < raw_bytes.len() {
            if cursor + HEADER_SIZE > raw_bytes.len() {
                break;
            }
            match decode_entry(&raw_bytes[cursor..]) {
                Ok((LogEntry::Frame(frame), consumed)) => {
                    cursor += consumed;
                    if frame.offset >= start_offset {
                        frames.push(frame);
                    }
                }
                Ok((LogEntry::Batch(batch), consumed)) => {
                    let Ok(records) = batch.records() else {
                        // Corrupt batch payload — treat exactly like a corrupt frame
                        // would be treated below: stop the scan rather than propagate a
                        // partial/garbage record into the response.
                        break;
                    };
                    for record in records {
                        if record.offset >= start_offset {
                            // See `expand_entries_for_compaction`: batch records now carry
                            // an explicit, nullable value; append_batch never writes a null
                            // one, so this unwrap preserves existing behavior.
                            frames.push(RecordFrame::create(
                                record.offset,
                                record.timestamp,
                                record.value.unwrap_or_default(),
                            ));
                        }
                    }
                    cursor += consumed;
                }
                Err(_) => break,
            }
        }

        Ok(frames)
    }

    /// Reads the stored bytes covering `start_offset` onward, **exactly as they sit on
    /// disk** — entries are never decoded into records, and a compressed batch is never
    /// decompressed.
    ///
    /// This is what a fetch and a replication read are served from. The broker's job on
    /// this path is to find the right byte range and hand it over; deciding what the
    /// records inside mean is the consumer's job. It is also what lets a follower's log
    /// end up byte-identical to its leader's rather than a re-encoded copy.
    ///
    /// Only whole entries are returned. An entry whose offset range *contains*
    /// `start_offset` is included in full — a batch is atomic on disk and cannot be cut in
    /// half — so a caller asking from the middle of a batch receives the whole batch and
    /// filters it itself, exactly as a Kafka consumer does. Entries that end strictly
    /// before `start_offset` are skipped.
    ///
    /// Offset ranges are read from each entry's plaintext header (`base_offset` plus
    /// `last_offset_delta` for a batch), so deciding what to include costs no
    /// decompression either.
    ///
    /// `max_offset_exclusive` is the committed high watermark. An entry is returned only
    /// if it lies entirely below it.
    pub fn fetch_entries(
        &mut self,
        start_offset: u64,
        max_bytes: usize,
        max_offset_exclusive: u64,
    ) -> IoResult<Bytes> {
        let segment_pair = self.find_segment_pair_mut(start_offset);
        let seek_entry = segment_pair.index.find_nearest_physical_pos(start_offset);
        let start_pos = seek_entry.map_or(0, |e| e.physical_position as u64);

        let raw_bytes = segment_pair.log.read_at(start_pos, max_bytes)?;
        let raw_bytes = Self::ensure_first_batch_fits(segment_pair, start_pos, raw_bytes)?;

        let mut cursor = 0usize;
        let mut first_included: Option<usize> = None;
        let mut end = 0usize;

        while cursor < raw_bytes.len() {
            if cursor + HEADER_SIZE > raw_bytes.len() {
                break;
            }
            let Ok((entry, consumed)) = decode_entry(&raw_bytes[cursor..]) else {
                // Same contract as `fetch`: stop the scan rather than hand back bytes we
                // could not account for.
                break;
            };
            let last_offset = match &entry {
                LogEntry::Frame(frame) => frame.offset,
                LogEntry::Batch(batch) => batch.base_offset + batch.last_offset_delta as u64,
            };
            // An entry is included only if it is *entirely* below the committed high
            // watermark. Records inside a batch cannot be filtered individually without
            // decoding it, so a batch straddling the watermark is withheld whole rather
            // than exposing uncommitted records — it becomes visible once the watermark
            // moves past its last offset.
            if last_offset >= max_offset_exclusive {
                break;
            }
            if last_offset >= start_offset {
                if first_included.is_none() {
                    first_included = Some(cursor);
                }
                end = cursor + consumed;
            }
            cursor += consumed;
        }

        Ok(match first_included {
            Some(begin) => Bytes::copy_from_slice(&raw_bytes[begin..end]),
            None => Bytes::new(),
        })
    }

    /// If `raw` begins with a [`RecordBatch`] (magic [`BATCH_MAGIC_BYTE`]) whose
    /// self-declared length is longer than what `raw` already holds, re-reads with a
    /// budget sized to cover the whole batch and returns that instead. A batch is atomic
    /// — there is no such thing as decoding "the first part" of one — so if the entry a
    /// caller's `start_offset` needs is a batch cut short purely because `max_bytes`
    /// happened to be smaller than it, the fetch must still serve it (mirroring Kafka's
    /// own "always return at least one message even if it exceeds the requested budget"
    /// fetch behavior) rather than silently returning nothing for that offset range.
    ///
    /// A no-op (returns `raw` unchanged, no extra read) whenever `raw` is empty, doesn't
    /// start with a batch, or already covers the whole batch.
    fn ensure_first_batch_fits(
        pair: &mut SegmentPair,
        start_pos: u64,
        raw: Vec<u8>,
    ) -> IoResult<Vec<u8>> {
        const PREFIX: usize = 5; // magic + batch_length, see RecordBatch::decode
        if raw.first() != Some(&BATCH_MAGIC_BYTE) {
            return Ok(raw);
        }
        let prefix: Vec<u8> = if raw.len() >= PREFIX {
            raw[..PREFIX].to_vec()
        } else {
            // The initial read didn't even cover the 5-byte prefix — go get it directly
            // rather than guessing.
            pair.log.read_at(start_pos, PREFIX)?
        };
        if prefix.len() < PREFIX {
            // Truncated even for the fixed prefix (e.g. right at the physical end of the
            // segment) — leave it for `decode_entry` to report cleanly.
            return Ok(raw);
        }
        let batch_length = u32::from_be_bytes(prefix[1..5].try_into().unwrap()) as usize;
        let total_needed = PREFIX + batch_length;
        if raw.len() >= total_needed {
            Ok(raw)
        } else {
            pair.log.read_at(start_pos, total_needed)
        }
    }

    /// Plans a zero-copy fetch: locates the exact, frame-aligned physical byte range
    /// covering as much of `[start_offset, high_watermark)` as fits in `max_bytes`
    /// (within a single segment — same single-segment limitation `fetch` already has),
    /// and returns a cloned file handle for it.
    ///
    /// The scan reads only frame *headers* (`HEADER_SIZE` bytes at a time, via seek+read)
    /// to find where each frame starts and how long it is — it deliberately never touches
    /// payload bytes, so this is cheap even for large records. The actual bulk transfer
    /// (the whole point of "zero-copy") happens later, directly from the returned file
    /// handle to the socket via `TransmitFile`/`sendfile`, without this function or its
    /// caller ever copying record payloads into a Rust-owned buffer.
    ///
    /// The returned `std::fs::File` is an independently `try_clone`d handle — safe to use
    /// after this call returns and the caller has released whatever lock guarded this
    /// `SegmentManager` (compaction/retention can freely rename or delete the segment's
    /// path afterwards; the OS keeps the underlying file alive as long as this handle is
    /// open, on both Windows — segments are opened with `FILE_SHARE_DELETE` — and Unix).
    pub fn plan_zero_copy_fetch(
        &mut self,
        start_offset: u64,
        max_bytes: usize,
        high_watermark: u64,
    ) -> IoResult<Option<ZeroCopyFetchPlan>> {
        const MAX_FRAMES_PER_PLAN: u32 = 20_000; // sanity cap against pathological tiny-record scans

        if start_offset >= high_watermark || max_bytes < HEADER_SIZE {
            return Ok(None);
        }

        let segment_pair = self.find_segment_pair_mut(start_offset);
        let seek_entry = segment_pair.index.find_nearest_physical_pos(start_offset);
        let mut phys = seek_entry.map_or(0, |e| e.physical_position as u64);

        let mut plan_start: Option<u64> = None;
        let mut frame_count: u32 = 0;
        let mut bytes_used: usize = 0;

        loop {
            let header = segment_pair.log.read_at(phys, HEADER_SIZE)?;
            if header.len() < HEADER_SIZE {
                break; // reached the end of this segment's valid data
            }
            if header[0] == BATCH_MAGIC_BYTE {
                // This path ships raw bytes straight to the socket (`ZeroCopyFetchPlan::
                // transmit`) with no per-entry reinterpretation at send time — the client
                // parses exactly `frame_count` `RecordFrame`s out of what it receives.
                // A `RecordBatch` is a different wire format entirely, and its header
                // isn't even frame-shaped (the bytes this loop otherwise reads as
                // `frame_offset`/`payload_len` would land on the batch's CRC and base
                // offset fields instead). Sending it through this path would corrupt the
                // client's parse — decided deliberately, not left to chance, precisely
                // because this path already caused one silent bug before (#54, bypassing
                // `record_progress`) by assuming every on-disk entry needed no special
                // handling.
                //
                // `base_offset` (bytes 9..17) and `last_offset_delta` (bytes 17..21) both
                // land within the 25-byte header already read above, so a batch entirely
                // before `start_offset` can still be skipped by its own declared length
                // (`batch_length`, bytes 1..5) exactly like an ordinary too-early frame —
                // no need to decline just because a batch happens to sit earlier in the
                // scan than anything relevant.
                let batch_length = u32::from_be_bytes(header[1..5].try_into().unwrap()) as u64;
                let batch_base_offset = u64::from_be_bytes(header[9..17].try_into().unwrap());
                let batch_last_offset_delta =
                    u32::from_be_bytes(header[17..21].try_into().unwrap());
                let batch_end_offset = batch_base_offset + batch_last_offset_delta as u64;
                const BATCH_PREFIX: u64 = 5; // magic + batch_length, see RecordBatch::decode
                let batch_total_len = BATCH_PREFIX + batch_length;

                if batch_base_offset >= high_watermark {
                    break; // never expose data beyond the committed high watermark
                }
                if plan_start.is_none() && batch_end_offset < start_offset {
                    // Nothing in this batch is relevant — skip clean over it and keep
                    // scanning for start_offset, same as skipping an earlier frame.
                    phys += batch_total_len;
                    continue;
                }
                // Either `start_offset` falls inside this batch (or it's entirely past
                // it, since HW is never checked on this path before the fact), or whole
                // frames were already planned before reaching it. Either way, this batch
                // itself can never be part of a zero-copy plan:
                //   - nothing planned yet: this batch is (or covers) the very entry
                //     `start_offset` needs — decline entirely so the buffered `fetch`
                //     path, which does understand batches, serves this request instead.
                //   - something already planned: stop here and ship just the whole
                //     frames collected so far; the batch itself will be picked up whole
                //     by the buffered path on the caller's next fetch, which naturally
                //     starts at this batch's base offset.
                if plan_start.is_none() {
                    return Ok(None);
                }
                break;
            }
            let frame_offset = u64::from_be_bytes(header[5..13].try_into().unwrap());
            let payload_len = u32::from_be_bytes(header[21..25].try_into().unwrap()) as usize;
            let frame_total_len = HEADER_SIZE + payload_len;

            if frame_offset >= high_watermark {
                break; // never expose data beyond the committed high watermark
            }
            if plan_start.is_none() {
                if frame_offset < start_offset {
                    // The sparse index only guarantees "at or before" start_offset —
                    // skip forward past any earlier frame(s) it landed us on.
                    phys += frame_total_len as u64;
                    continue;
                }
                plan_start = Some(phys);
            }
            if frame_count > 0 && bytes_used + frame_total_len > max_bytes {
                break; // this next frame would exceed the byte budget
            }

            bytes_used += frame_total_len;
            frame_count += 1;
            phys += frame_total_len as u64;

            if bytes_used >= max_bytes || frame_count >= MAX_FRAMES_PER_PLAN {
                break;
            }
        }

        let (Some(plan_start), true) = (plan_start, frame_count > 0) else {
            return Ok(None);
        };

        let file = segment_pair.log.file.try_clone()?;
        Ok(Some(ZeroCopyFetchPlan {
            file,
            physical_start: plan_start,
            physical_len: bytes_used as u64,
            frame_count,
        }))
    }

    /// Fast binary search seek ($O(\log N)$) for nearest physical byte position of logical offset
    pub fn seek(&self, target_offset: u64) -> Option<(u64, u64)> {
        let pair = self.find_segment_pair(target_offset);
        pair.index
            .find_nearest_physical_pos(target_offset)
            .map(|e| {
                (
                    pair.base_offset + e.relative_offset as u64,
                    e.physical_position as u64,
                )
            })
    }

    pub fn set_cleanup_policy(&mut self, policy: crate::config::CleanupPolicy) {
        self.config.cleanup_policy = policy;
    }

    /// Dynamic per-topic config override (Kafka `retention.ms`).
    pub fn set_retention_millis(&mut self, millis: Option<u64>) {
        self.config.retention_millis = millis;
    }

    /// Dynamic per-topic config override (Kafka `retention.bytes`).
    pub fn set_retention_bytes(&mut self, bytes: Option<u64>) {
        self.config.retention_bytes = bytes;
    }

    /// Dynamic per-topic config override (Kafka `delete.retention.ms`).
    pub fn set_delete_retention_millis(&mut self, millis: Option<u64>) {
        self.config.delete_retention_millis = millis;
    }

    /// Dynamic per-topic config override (Kafka `min.cleanable.dirty.ratio`).
    pub fn set_min_cleanable_dirty_ratio(&mut self, ratio: f64) {
        self.config.min_cleanable_dirty_ratio = ratio;
    }

    pub fn cleanup_policy(&self) -> crate::config::CleanupPolicy {
        self.config.cleanup_policy
    }

    /// Garbage collector: applies log compaction and/or time/size retention based on configured cleanup.policy
    pub fn apply_retention(&mut self) -> IoResult<usize> {
        let mut total_affected = 0;

        if self.config.cleanup_policy.is_compact() {
            total_affected += self.compact_segments()?;
        }

        if self.config.cleanup_policy.is_delete() {
            total_affected += self.apply_delete_retention()?;
        }

        Ok(total_affected)
    }

    /// Log compaction garbage collector: deduplicates historical segment entries keeping only the latest offset per key.
    ///
    /// Rewrites at most `MAX_SEGMENTS_COMPACTED_PER_CALL` segments per call rather than
    /// every segment that needs it in one pass. The whole partition's segment-manager
    /// lock is held for the duration of a call (see `PartitionManager::apply_retention`),
    /// so a partition with many historical segments needing compaction previously could
    /// block that partition's produce/fetch traffic for as long as the entire sweep took.
    /// Bounding it per call means the retention GC's periodic tick (see
    /// `StorageEngine::apply_retention_all`) naturally spreads a large backlog across
    /// multiple ticks instead of one long stop-the-world pass.
    ///
    /// Tombstones (a record whose value is empty — see `extract_key_value`) are kept as
    /// the latest record for their key like any other record, until they've been the
    /// latest record for at least `config.delete_retention_millis` (Kafka
    /// `delete.retention.ms`) — at that point the key is purged entirely, including the
    /// tombstone itself, actually finishing the delete. A historical segment is only
    /// rewritten once the fraction of its bytes that are superseded ("dirty") reaches
    /// `config.min_cleanable_dirty_ratio` (Kafka `min.cleanable.dirty.ratio`), so segments
    /// with only a handful of stale keys aren't rewritten on every GC tick.
    ///
    /// Every scan below reads entries via `SegmentPair::read_all_entries` and
    /// `expand_entries_for_compaction`, not `read_all_frames`: a `RecordBatch` is decoded
    /// and its records unpacked into plain frames rather than skipped, because compaction
    /// rewrites the segment in place — skipping a batch here would silently destroy every
    /// record inside it, not just omit it from one read. See `expand_entries_for_compaction`
    /// for why surviving records come back out as plain frames rather than reconstituted
    /// batches, and for the producer-metadata tradeoff that follows from that choice.
    pub fn compact_segments(&mut self) -> IoResult<usize> {
        const MAX_SEGMENTS_COMPACTED_PER_CALL: usize = 4;

        if self.historical.is_empty() {
            return Ok(0);
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Phase 1: build a map of key -> the key's latest (highest-offset) record across
        // all historical and active segments, tracking whether that latest record is a
        // tombstone and how old it is.
        struct LatestRecord {
            offset: u64,
            timestamp: u64,
            is_tombstone: bool,
        }
        let mut latest: std::collections::HashMap<Vec<u8>, LatestRecord> =
            std::collections::HashMap::new();

        let mut observe = |key: &[u8], offset: u64, timestamp: u64, is_tombstone: bool| {
            latest
                .entry(key.to_vec())
                .and_modify(|rec| {
                    if offset > rec.offset {
                        *rec = LatestRecord {
                            offset,
                            timestamp,
                            is_tombstone,
                        };
                    }
                })
                .or_insert(LatestRecord {
                    offset,
                    timestamp,
                    is_tombstone,
                });
        };

        // Dedup is by the key the producer wrote, compared byte-for-byte — the broker never
        // parses a payload looking for one. A record with no key cannot be deduped and is
        // skipped here entirely, so it survives Phase 2 untouched.
        //
        // A tombstone is a record with a *null* value, matching Kafka. An empty-but-present
        // value is an ordinary record, not a delete.
        for pair in &mut self.historical {
            let entries = pair.read_all_entries()?;
            for record in records_for_compaction(&entries) {
                if record.is_control {
                    continue;
                }
                let Some(key) = record.key.as_deref() else {
                    continue;
                };
                observe(key, record.offset, record.timestamp, record.value.is_none());
            }
        }

        let active_entries = self.active.read_all_entries()?;
        for record in records_for_compaction(&active_entries) {
            if record.is_control {
                continue;
            }
            let Some(key) = record.key.as_deref() else {
                continue;
            };
            observe(key, record.offset, record.timestamp, record.value.is_none());
        }
        // Load-bearing despite the closure having no `Drop` impl: `observe` holds a mutable
        // borrow of `latest`, and moving it here is what releases that borrow so `latest`
        // can be read below. Clippy's `drop_non_drop` only considers `Drop` semantics, not
        // the borrow ending, so it flags this as pointless when removing it would not
        // compile.
        #[allow(clippy::drop_non_drop)]
        drop(observe);

        if latest.is_empty() {
            return Ok(0);
        }

        // Phase 1b: keys whose latest record is a tombstone older than
        // `delete_retention_millis` are fully purged — the key disappears from `latest`
        // entirely, which Phase 2 below reads as "discard every frame for this key,
        // including what would otherwise be the kept tombstone."
        let purged_keys: std::collections::HashSet<Vec<u8>> = if let Some(delete_retention_ms) =
            self.config.delete_retention_millis
        {
            let purged: std::collections::HashSet<Vec<u8>> = latest
                .iter()
                .filter(|(_, rec)| {
                    rec.is_tombstone && now_ms.saturating_sub(rec.timestamp) > delete_retention_ms
                })
                .map(|(k, _)| k.clone())
                .collect();
            for k in &purged {
                latest.remove(k);
            }
            purged
        } else {
            std::collections::HashSet::new()
        };
        let latest_offsets: std::collections::HashMap<Vec<u8>, u64> =
            latest.into_iter().map(|(k, rec)| (k, rec.offset)).collect();

        let mut total_compacted_frames = 0;
        let mut segments_compacted = 0usize;
        let mut i = 0;

        while i < self.historical.len() && segments_compacted < MAX_SEGMENTS_COMPACTED_PER_CALL {
            let all_entries = self.historical[i].read_all_entries()?;
            if all_entries.is_empty() {
                i += 1;
                continue;
            }

            let segment_total_bytes: u64 = all_entries
                .iter()
                .map(|e| entry_encoded_size(e) as u64)
                .sum();

            // Whether a single record survives. A keyless record is always kept: with no
            // key it cannot be deduped, and it is not a candidate for tombstone purging
            // either.
            let survives = |record: &CompactionRecord| -> bool {
                if record.is_control {
                    return true;
                }
                let Some(key) = record.key.as_deref() else {
                    return true;
                };
                match latest_offsets.get(key) {
                    Some(&latest_off) => record.offset == latest_off,
                    // The key was purged as an expired tombstone, so every record for it
                    // goes — including this one.
                    None => !purged_keys.contains(key),
                }
            };

            let mut kept_entries: Vec<LogEntry> = Vec::with_capacity(all_entries.len());
            let mut discarded_count = 0usize;
            let mut discarded_bytes = 0u64;

            for entry in all_entries {
                let entry_size = entry_encoded_size(&entry) as u64;
                match entry {
                    LogEntry::Frame(frame) => {
                        // A frame carries no key, so the only thing that can drop it is
                        // being a non-control record whose payload-derived key was purged —
                        // which cannot happen now that keys come from the record itself.
                        // Kept as-is.
                        let record = CompactionRecord {
                            offset: frame.offset,
                            timestamp: frame.timestamp,
                            key: None,
                            value: Some(frame.payload.clone()),
                            is_control: frame.is_control_marker(),
                        };
                        if survives(&record) {
                            kept_entries.push(LogEntry::Frame(frame));
                        } else {
                            discarded_bytes += entry_size;
                            discarded_count += 1;
                        }
                    }
                    LogEntry::Batch(batch) => {
                        let Ok(records) = batch.records() else {
                            // Undecodable payload — keep the batch rather than silently
                            // dropping records we cannot inspect.
                            kept_entries.push(LogEntry::Batch(batch));
                            continue;
                        };
                        let total = records.len();
                        let survivors: Vec<(u64, u64, Option<Bytes>, Option<Bytes>)> = records
                            .into_iter()
                            .filter(|r| {
                                survives(&CompactionRecord {
                                    offset: r.offset,
                                    timestamp: r.timestamp,
                                    key: r.key.clone(),
                                    value: r.value.clone(),
                                    is_control: false,
                                })
                            })
                            .map(|r| (r.offset, r.timestamp, r.key, r.value))
                            .collect();

                        discarded_count += total - survivors.len();
                        if survivors.is_empty() {
                            discarded_bytes += entry_size;
                            continue;
                        }
                        if survivors.len() == total {
                            kept_entries.push(LogEntry::Batch(batch));
                            continue;
                        }

                        // Rebuilt as a batch, not flattened into frames: a compacted
                        // segment stays batched and stays compressed in the producer's
                        // codec, which is what Kafka's cleaner does too. Survivors keep
                        // their original offsets, so the rebuilt batch has gaps — the
                        // format expresses that, `create_with_offsets` builds it.
                        //
                        // `base_sequence`/`producer_id` are carried over even though they
                        // no longer describe the batch's exact contents. Nothing in Hermes
                        // reconstructs idempotent-producer dedup state by reading them back
                        // off disk — that state lives in memory, written at produce time by
                        // `ProducerStateManager` — so preserving them costs nothing and
                        // keeps the batch self-describing.
                        let rebuilt = RecordBatch::create_with_offsets(
                            batch.base_offset,
                            batch.base_timestamp,
                            batch.leader_epoch,
                            batch.producer_id,
                            batch.producer_epoch,
                            batch.base_sequence,
                            batch.is_transactional(),
                            batch.compression().unwrap_or(BatchCompression::None),
                            &survivors,
                        );
                        discarded_bytes += entry_size.saturating_sub(rebuilt.encoded_size() as u64);
                        kept_entries.push(LogEntry::Batch(rebuilt));
                    }
                }
            }

            if discarded_count == 0 {
                i += 1;
                continue;
            }

            let dirty_ratio = discarded_bytes as f64 / segment_total_bytes.max(1) as f64;
            if dirty_ratio < self.config.min_cleanable_dirty_ratio {
                i += 1;
                continue;
            }

            total_compacted_frames += discarded_count;
            segments_compacted += 1;
            let base_offset = self.historical[i].base_offset;

            if kept_entries.is_empty() {
                let pair_to_remove = self.historical.remove(i);
                let log_path = pair_to_remove.log.path.clone();
                let index_path = pair_to_remove.index.path().to_path_buf();
                let time_index_path = pair_to_remove.time_index.path().to_path_buf();
                let txn_index_path = pair_to_remove.txn_index.path().to_path_buf();

                drop(pair_to_remove);

                let _ = fs::remove_file(&log_path);
                let _ = fs::remove_file(&index_path);
                let _ = fs::remove_file(&time_index_path);
                let _ = fs::remove_file(&txn_index_path);
                crate::segment::log::remove_clean_marker(&log_path);
                crate::segment::log::fsync_dir(&self.dir);
                continue;
            }

            let filename = format_segment_filename(base_offset);
            let tmp_log_path = self.dir.join(format!("{}.log.compact", filename));
            let tmp_index_path = self.dir.join(format!("{}.index.compact", filename));
            let tmp_timeindex_path = self.dir.join(format!("{}.timeindex.compact", filename));
            let tmp_txnindex_path = self.dir.join(format!("{}.txnindex.compact", filename));

            let _ = fs::remove_file(&tmp_log_path);
            let _ = fs::remove_file(&tmp_index_path);
            let _ = fs::remove_file(&tmp_timeindex_path);
            let _ = fs::remove_file(&tmp_txnindex_path);

            {
                let mut tmp_index = IndexSegment::open(&tmp_index_path, base_offset)?;
                let mut tmp_timeindex = TimeIndexSegment::open(&tmp_timeindex_path, base_offset)?;
                let mut tmp_txnindex = TxnIndexSegment::open(&tmp_txnindex_path)?;

                for txn_e in self.historical[i].txn_index.entries() {
                    tmp_txnindex.append(
                        txn_e.producer_id,
                        txn_e.first_offset,
                        txn_e.last_offset,
                    )?;
                }

                let mut tmp_log = LogSegment::open_at_path(
                    tmp_log_path.clone(),
                    base_offset,
                    self.config.max_segment_bytes,
                    self.config.index_interval_bytes,
                    false,
                    &mut tmp_index,
                )?;

                let mut bytes_since_last_index = 0u64;

                for (idx, entry) in kept_entries.iter().enumerate() {
                    let mut encoded = Vec::with_capacity(entry_encoded_size(entry));
                    encode_entry_into(entry, &mut encoded);
                    let phys_pos = tmp_log.append_bytes(&encoded)?;

                    if idx == 0 || bytes_since_last_index >= self.config.index_interval_bytes {
                        let index_offset = entry_index_offset(entry);
                        tmp_index.append(index_offset, phys_pos)?;
                        tmp_timeindex.append(entry_index_timestamp(entry), index_offset)?;
                        bytes_since_last_index = 0;
                    }
                    bytes_since_last_index += encoded.len() as u64;
                }

                tmp_log.sync()?;
                tmp_index.sync()?;
                tmp_timeindex.sync()?;
                tmp_txnindex.sync()?;
            }

            let pair_to_replace = self.historical.remove(i);
            let log_path = pair_to_replace.log.path.clone();
            let index_path = pair_to_replace.index.path().to_path_buf();
            let time_index_path = pair_to_replace.time_index.path().to_path_buf();
            let txn_index_path = pair_to_replace.txn_index.path().to_path_buf();

            drop(pair_to_replace);

            // Crash-safe swap. The original files must never be unlinked before their
            // replacements are in place: doing so leaves a window in which the segment
            // does not exist on disk under its real name at all, so a crash (or a failing
            // rename) in that window destroys the data permanently — the original is gone
            // and the compacted copy was never installed, and startup recovery has
            // nothing left to recover from.
            //
            // Instead, move the originals aside to `.deleted`, move the compacted files
            // into place, fsync the directory so both renames are durable, and only then
            // unlink the `.deleted` files. At every instant a complete copy of the
            // segment exists under one name or the other, and `recover_interrupted_
            // compaction` (called on open) can finish or roll back whatever a crash
            // interrupted.
            let backup_paths: Vec<(std::path::PathBuf, std::path::PathBuf)> =
                [&log_path, &index_path, &time_index_path, &txn_index_path]
                    .iter()
                    .map(|p| ((*p).clone(), deleted_backup_path(p)))
                    .collect();

            for (live, backup) in &backup_paths {
                let _ = fs::remove_file(backup);
                match fs::rename(live, backup) {
                    Ok(()) => {}
                    // A missing original is fine (e.g. an index file that was never
                    // created); there's simply nothing to move aside.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }

            // The old segment's `.clean` marker describes its pre-compaction size — stale
            // the instant the file it describes is replaced.
            crate::segment::log::remove_clean_marker(&log_path);

            fs::rename(&tmp_log_path, &log_path)?;
            fs::rename(&tmp_index_path, &index_path)?;
            fs::rename(&tmp_timeindex_path, &time_index_path)?;
            fs::rename(&tmp_txnindex_path, &txn_index_path)?;
            // Make both halves of the swap durable before dropping the backups, so a
            // crash can never leave the directory with neither the old nor the new file.
            crate::segment::log::fsync_dir(&self.dir);

            for (_, backup) in &backup_paths {
                let _ = fs::remove_file(backup);
            }
            crate::segment::log::fsync_dir(&self.dir);

            let mut new_index = IndexSegment::open(&index_path, base_offset)?;
            let new_timeindex = TimeIndexSegment::open(&time_index_path, base_offset)?;
            let new_txnindex = TxnIndexSegment::open(&txn_index_path)?;
            let mut new_log = LogSegment::open(
                &self.dir,
                base_offset,
                self.config.max_segment_bytes,
                self.config.index_interval_bytes,
                false,
                &mut new_index,
            )?;
            // A just-compacted historical segment is immediately immutable again, exactly
            // like one that just finished a normal rotation — `finalize()` records that
            // (and writes a fresh `.clean` marker) so the next startup can trust it too.
            new_log.finalize()?;

            let new_pair = SegmentPair {
                base_offset,
                log: new_log,
                index: new_index,
                time_index: new_timeindex,
                txn_index: new_txnindex,
                mmap: None,
            };

            self.historical.insert(i, new_pair);
            i += 1;
        }

        Ok(total_compacted_frames)
    }

    /// Garbage collector: unlinks closed segments exceeding configured size or time retention limits
    pub fn apply_delete_retention(&mut self) -> IoResult<usize> {
        let mut removed_count = 0;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let retention_bytes = self.config.retention_bytes;
        let retention_millis = self.config.retention_millis;

        let mut i = 0;
        while i < self.historical.len() {
            let pair = &self.historical[i];
            // Prefer the segment's own newest record timestamp (from its time index) over
            // filesystem mtime: mtime tracks when the file was last *written*, which a
            // backup tool, a replica resync, or a plain `touch` can change without
            // touching a single record — silently resetting the segment's retention
            // clock. Falls back to mtime only for the (empty-segment) case where the time
            // index has no entries at all.
            let record_age_ms = pair
                .time_index
                .max_timestamp()
                .unwrap_or_else(|| pair.log.modified_time_ms().unwrap_or(0));

            let mut remove = false;

            if let Some(max_age_ms) = retention_millis {
                if now_ms > record_age_ms && (now_ms - record_age_ms) > max_age_ms {
                    remove = true;
                }
            }

            if let Some(max_bytes) = retention_bytes {
                let total_bytes: u64 = self
                    .historical
                    .iter()
                    .map(|p| p.log.physical_size)
                    .sum::<u64>()
                    + self.active.log.physical_size;
                if total_bytes > max_bytes {
                    remove = true;
                }
            }

            if remove {
                let pair_to_remove = self.historical.remove(i);
                let log_path = pair_to_remove.log.path.clone();
                let index_path = pair_to_remove.index.path().to_path_buf();
                let time_index_path = pair_to_remove.time_index.path().to_path_buf();
                let txn_index_path = pair_to_remove.txn_index.path().to_path_buf();

                tracing::info!(
                    "Garbage Collector: Unlinking expired log segment {} and index {}",
                    log_path.display(),
                    index_path.display()
                );

                // Explicitly drop handles before removing files on Windows
                drop(pair_to_remove);

                let _ = fs::remove_file(&log_path);
                let _ = fs::remove_file(&index_path);
                let _ = fs::remove_file(&time_index_path);
                let _ = fs::remove_file(&txn_index_path);
                crate::segment::log::remove_clean_marker(&log_path);
                removed_count += 1;
            } else {
                i += 1;
            }
        }

        if removed_count > 0 {
            crate::segment::log::fsync_dir(&self.dir);
        }

        Ok(removed_count)
    }

    /// Deletes any historical segment whose entire offset range lies strictly below
    /// `offset` — used after a snapshot durably captures everything up to `offset`, so
    /// the pre-snapshot log data is no longer needed to reconstruct current state (KRaft-
    /// style log trimming). A segment is only removed if the NEXT segment's base_offset
    /// (or the active segment's base_offset, for the last historical segment) is `<=
    /// offset`, i.e. this segment contributes nothing at or after the snapshot point.
    pub fn trim_before(&mut self, offset: u64) -> IoResult<usize> {
        let mut removed_count = 0;
        let i = 0;
        while i < self.historical.len() {
            let next_base_offset = self
                .historical
                .get(i + 1)
                .map(|p| p.base_offset)
                .unwrap_or(self.active.base_offset);

            if next_base_offset > offset {
                // This segment (and everything after it) still contains data at or past
                // the snapshot point — nothing more to trim.
                break;
            }

            let pair_to_remove = self.historical.remove(i);
            let log_path = pair_to_remove.log.path.clone();
            let index_path = pair_to_remove.index.path().to_path_buf();
            let time_index_path = pair_to_remove.time_index.path().to_path_buf();
            let txn_index_path = pair_to_remove.txn_index.path().to_path_buf();

            tracing::info!(
                "Metadata Snapshot: Trimming log segment {} (fully covered by snapshot at offset {})",
                log_path.display(),
                offset
            );

            drop(pair_to_remove);

            let _ = fs::remove_file(&log_path);
            let _ = fs::remove_file(&index_path);
            let _ = fs::remove_file(&time_index_path);
            let _ = fs::remove_file(&txn_index_path);
            crate::segment::log::remove_clean_marker(&log_path);
            removed_count += 1;
            // Don't advance `i` — the next segment shifted down to this index.
        }
        if removed_count > 0 {
            crate::segment::log::fsync_dir(&self.dir);
        }
        Ok(removed_count)
    }

    /// Read records starting at target timestamp (BUG-02)
    /// Stored bytes from the first offset whose timestamp reaches `target_timestamp`,
    /// resolved through the time index — no entry is decoded to find it.
    ///
    /// Unlike [`Self::fetch_by_timestamp`], records are **not** filtered to
    /// `timestamp >= target_timestamp` here: doing that would mean decoding, and
    /// decompressing, every batch. The reader filters instead, exactly as a Kafka consumer
    /// does after resolving an offset through `ListOffsets`. The time index is sparse, so
    /// the returned range can begin slightly before the target.
    pub fn fetch_entries_by_timestamp(
        &mut self,
        target_timestamp: u64,
        max_bytes: usize,
        max_offset_exclusive: u64,
    ) -> IoResult<Bytes> {
        let start_offset = self.find_offset_for_timestamp(target_timestamp);
        self.fetch_entries(start_offset, max_bytes, max_offset_exclusive)
    }

    pub fn fetch_by_timestamp(
        &mut self,
        target_timestamp: u64,
        max_bytes: usize,
    ) -> IoResult<Vec<RecordFrame>> {
        let start_offset = self.find_offset_for_timestamp(target_timestamp);
        let frames = self.fetch(start_offset, max_bytes)?;
        Ok(frames
            .into_iter()
            .filter(|f| f.timestamp >= target_timestamp)
            .collect())
    }

    /// Finds nearest base_offset for target_timestamp (PARTIAL-02 & NEW-02)
    /// Lowest offset whose timestamp is >= `target_timestamp`.
    ///
    /// Two things here used to be wrong, and the second was the damaging one:
    ///
    /// 1. **Search order.** Historical segments were searched newest-to-oldest *before*
    ///    the active segment, so a timestamp living in the active segment could match an
    ///    older segment first and return an offset earlier than the true answer.
    /// 2. **Fallback.** When the target preceded everything indexed, it returned
    ///    `self.active.base_offset` — the *newest* offset in the log rather than the
    ///    oldest. So "give me everything since a timestamp older than my retention
    ///    window", which is the normal way to start a full replay, returned the head of
    ///    the log: the caller silently skipped the entire history it asked for, with no
    ///    error indicating anything had been missed.
    ///
    /// Now: oldest-to-newest across historical (the first match is by definition the
    /// lowest qualifying offset), then the active segment, and a fallback to the oldest
    /// retained offset.
    pub fn find_offset_for_timestamp(&mut self, target_timestamp: u64) -> u64 {
        for pair in self.historical.iter() {
            if let Some(offset) = pair.time_index.find_offset_for_timestamp(target_timestamp) {
                return offset;
            }
        }

        if let Some(offset) = self
            .active
            .time_index
            .find_offset_for_timestamp(target_timestamp)
        {
            return offset;
        }

        // Nothing at or after the target: the caller is asking for a point past the end of
        // the log, so the next record to arrive is the answer.
        if let Some(newest) = self
            .historical
            .last()
            .map(|s| s.time_index.max_timestamp())
            .unwrap_or_else(|| self.active.time_index.max_timestamp())
        {
            if target_timestamp > newest {
                return self.active.log.next_offset;
            }
        }

        // Target precedes all indexed data — start from the oldest offset still retained.
        self.historical
            .first()
            .map(|s| s.base_offset)
            .unwrap_or(self.active.base_offset)
    }

    /// Flushes log and index files to physical disk
    pub fn sync(&mut self) -> IoResult<()> {
        self.active.log.sync()?;
        self.active.index.sync()?;
        Ok(())
    }

    fn find_segment_pair(&self, offset: u64) -> &SegmentPair {
        if offset >= self.active.base_offset || self.historical.is_empty() {
            return &self.active;
        }

        for i in (0..self.historical.len()).rev() {
            if offset >= self.historical[i].base_offset {
                return &self.historical[i];
            }
        }
        &self.historical[0]
    }

    fn find_segment_pair_mut(&mut self, offset: u64) -> &mut SegmentPair {
        if offset >= self.active.base_offset || self.historical.is_empty() {
            return &mut self.active;
        }

        for i in (0..self.historical.len()).rev() {
            if offset >= self.historical[i].base_offset {
                return &mut self.historical[i];
            }
        }
        &mut self.historical[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::MAGIC_BYTE;
    use crate::protocol::BatchCompression;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "hermes_segment_manager_test_{}_{}_{}",
                label,
                std::process::id(),
                unique
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn open_manager(dir: &TempDir) -> SegmentManager {
        SegmentManager::open(&dir.0, EngineConfig::default()).unwrap()
    }

    #[test]
    fn append_verbatim_preserves_offset_timestamp_and_payload() {
        let dir = TempDir::new("verbatim_basic");
        let mut mgr = open_manager(&dir);

        let leader_frame = RecordFrame::create(0, 123_456_789, b"hello".to_vec());
        let result = mgr.append_verbatim(&leader_frame).unwrap();
        assert_eq!(result, VerbatimAppendResult::Appended);

        let fetched = mgr.fetch(0, 4096).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].offset, 0);
        assert_eq!(fetched[0].timestamp, 123_456_789);
        assert_eq!(fetched[0].magic, MAGIC_BYTE);
        assert_eq!(fetched[0].payload.as_ref(), b"hello");
        assert_eq!(mgr.high_watermark(), 1);
    }

    #[test]
    fn append_verbatim_is_idempotent_on_duplicate_offset() {
        let dir = TempDir::new("verbatim_dup");
        let mut mgr = open_manager(&dir);

        let frame = RecordFrame::create(0, 1, b"a".to_vec());
        assert_eq!(
            mgr.append_verbatim(&frame).unwrap(),
            VerbatimAppendResult::Appended
        );
        // Re-delivering the same offset (e.g. a retried/duplicated replication push)
        // must not double-append or error.
        assert_eq!(
            mgr.append_verbatim(&frame).unwrap(),
            VerbatimAppendResult::AlreadyApplied
        );
        assert_eq!(mgr.high_watermark(), 1);
        assert_eq!(mgr.fetch(0, 4096).unwrap().len(), 1);
    }

    #[test]
    fn append_verbatim_reports_gap_instead_of_skipping() {
        let dir = TempDir::new("verbatim_gap");
        let mut mgr = open_manager(&dir);

        let frame = RecordFrame::create(5, 1, b"a".to_vec());
        match mgr.append_verbatim(&frame).unwrap() {
            VerbatimAppendResult::Gap { expected } => assert_eq!(expected, 0),
            other => panic!("expected Gap, got {:?}", other),
        }
        // Nothing should have been written.
        assert_eq!(mgr.high_watermark(), 0);
    }

    #[test]
    fn truncate_after_removes_conflicting_tail_and_allows_reappend() {
        let dir = TempDir::new("truncate_basic");
        let mut mgr = open_manager(&dir);

        for i in 0..5u64 {
            let frame = RecordFrame::create(i, i, format!("v{}", i).into_bytes());
            mgr.append_verbatim(&frame).unwrap();
        }
        assert_eq!(mgr.high_watermark(), 5);

        // Simulate a conflicting suffix: truncate back to offset 3, then append a
        // different value at offset 3 (as a new leader would after a term change).
        mgr.truncate_after(3).unwrap();
        assert_eq!(mgr.high_watermark(), 3);
        assert_eq!(mgr.fetch(0, 4096).unwrap().len(), 3);

        let replacement = RecordFrame::create(3, 999, b"replacement".to_vec());
        assert_eq!(
            mgr.append_verbatim(&replacement).unwrap(),
            VerbatimAppendResult::Appended
        );

        let all = mgr.fetch(0, 4096).unwrap();
        assert_eq!(all.len(), 4);
        assert_eq!(all[3].payload.as_ref(), b"replacement");
        assert_eq!(all[3].timestamp, 999);
    }

    #[test]
    fn truncate_after_offset_beyond_hw_is_a_no_op() {
        let dir = TempDir::new("truncate_noop");
        let mut mgr = open_manager(&dir);
        let frame = RecordFrame::create(0, 1, b"a".to_vec());
        mgr.append_verbatim(&frame).unwrap();

        mgr.truncate_after(50).unwrap();
        assert_eq!(mgr.high_watermark(), 1);
        assert_eq!(mgr.fetch(0, 4096).unwrap().len(), 1);
    }

    #[test]
    fn trim_before_deletes_fully_covered_historical_segments_only() {
        let dir = TempDir::new("trim_before");
        let config = EngineConfig {
            max_segment_bytes: 200,
            ..EngineConfig::default()
        };
        let mut mgr = SegmentManager::open(&dir.0, config).unwrap();

        // One record per historical segment (rotate after every append) so trimming
        // granularity is easy to reason about.
        for i in 0..10u64 {
            let frame = RecordFrame::create(i, i, format!("payload-{}", i).into_bytes());
            mgr.append_verbatim(&frame).unwrap();
            mgr.rotate_segment().unwrap();
        }
        let last = RecordFrame::create(10, 10, b"final".to_vec());
        mgr.append_verbatim(&last).unwrap();

        let historical_before = mgr.historical.len();
        assert_eq!(historical_before, 10);

        // Everything with offset < 7 (i.e. segments 0..7) should be removable; segments
        // 7, 8, 9 and the active segment (offset 10) must survive since they contain
        // data at or after the trim point.
        let removed = mgr.trim_before(7).unwrap();
        assert_eq!(removed, 7);
        assert_eq!(mgr.historical.len(), historical_before - 7);

        // Data at/after the trim point must still be intact — `fetch` only reads within
        // the single segment containing `start_offset` (it doesn't span segments), so
        // check each surviving offset via its own segment's base_offset instead of one
        // multi-segment fetch.
        let surviving_base_offsets: Vec<u64> =
            mgr.historical.iter().map(|p| p.base_offset).collect();
        assert_eq!(surviving_base_offsets, vec![7, 8, 9]);
        for offset in [7u64, 8, 9, 10] {
            let frames = mgr.fetch(offset, 4096).unwrap();
            assert_eq!(frames.first().map(|f| f.offset), Some(offset));
        }

        // Trimming again at the same (or lower) point is a safe no-op.
        let removed_again = mgr.trim_before(7).unwrap();
        assert_eq!(removed_again, 0);
    }

    /// Reads exactly `plan.physical_len` raw bytes from the plan's own cloned file handle
    /// — this is what a real `TransmitFile`/`sendfile` call would stream to the socket.
    fn read_plan_bytes(plan: &ZeroCopyFetchPlan) -> Vec<u8> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = plan.file.try_clone().unwrap();
        file.seek(SeekFrom::Start(plan.physical_start)).unwrap();
        let mut buf = vec![0u8; plan.physical_len as usize];
        file.read_exact(&mut buf).unwrap();
        buf
    }

    #[test]
    fn plan_zero_copy_fetch_matches_buffered_fetch_bytes() {
        let dir = TempDir::new("zero_copy_matches_buffered");
        let mut mgr = open_manager(&dir);

        for i in 0..5u64 {
            let frame = RecordFrame::create(i, 1000 + i, format!("payload-{}", i).into_bytes());
            mgr.append_verbatim(&frame).unwrap();
        }
        let hw = mgr.high_watermark();
        assert_eq!(hw, 5);

        let buffered = mgr.fetch(1, 4096).unwrap();
        let mut expected_bytes = Vec::new();
        for frame in &buffered {
            frame.encode_into(&mut expected_bytes);
        }

        let plan = mgr.plan_zero_copy_fetch(1, 4096, hw).unwrap().unwrap();
        assert_eq!(plan.frame_count, buffered.len() as u32);
        assert_eq!(plan.physical_len as usize, expected_bytes.len());
        assert_eq!(read_plan_bytes(&plan), expected_bytes);
    }

    #[test]
    fn plan_zero_copy_fetch_clamps_to_high_watermark() {
        let dir = TempDir::new("zero_copy_hw_clamp");
        let mut mgr = open_manager(&dir);

        for i in 0..5u64 {
            let frame = RecordFrame::create(i, i, format!("v{}", i).into_bytes());
            mgr.append_verbatim(&frame).unwrap();
        }

        // Only offsets [0, 3) are "committed" as far as this plan is concerned, even
        // though 5 frames physically exist on disk.
        let plan = mgr.plan_zero_copy_fetch(0, 4096, 3).unwrap().unwrap();
        assert_eq!(plan.frame_count, 3);

        let bytes = read_plan_bytes(&plan);
        let mut cursor = 0usize;
        let mut seen_offsets = Vec::new();
        while cursor < bytes.len() {
            let (frame, consumed) = RecordFrame::decode(&bytes[cursor..]).unwrap();
            seen_offsets.push(frame.offset);
            cursor += consumed;
        }
        assert_eq!(seen_offsets, vec![0, 1, 2]);

        // Requesting at/after the watermark must never return a plan.
        assert!(mgr.plan_zero_copy_fetch(3, 4096, 3).unwrap().is_none());
        assert!(mgr.plan_zero_copy_fetch(5, 4096, 3).unwrap().is_none());
    }

    #[test]
    fn plan_zero_copy_fetch_respects_max_bytes_budget() {
        let dir = TempDir::new("zero_copy_budget");
        let mut mgr = open_manager(&dir);

        let mut frame_size = 0usize;
        for i in 0..5u64 {
            let frame = RecordFrame::create(i, i, b"fixed-size-payload".to_vec());
            frame_size = frame.encoded_size();
            mgr.append_verbatim(&frame).unwrap();
        }
        let hw = mgr.high_watermark();

        // Budget for exactly 2 frames plus a few spare bytes (not enough for a 3rd).
        let budget = frame_size * 2 + 4;
        let plan = mgr.plan_zero_copy_fetch(0, budget, hw).unwrap().unwrap();
        assert_eq!(plan.frame_count, 2);
        assert_eq!(plan.physical_len as usize, frame_size * 2);

        // A single frame larger than the budget is still returned whole — the first
        // matching frame is never dropped just because it alone exceeds max_bytes,
        // matching the buffered `fetch` path's "always make progress" behavior.
        // (max_bytes must be at least HEADER_SIZE for planning to proceed at all.)
        assert!(frame_size > HEADER_SIZE);
        let tiny_budget_plan = mgr
            .plan_zero_copy_fetch(0, HEADER_SIZE, hw)
            .unwrap()
            .unwrap();
        assert_eq!(tiny_budget_plan.frame_count, 1);
        assert_eq!(tiny_budget_plan.physical_len as usize, frame_size);
    }

    #[test]
    fn plan_zero_copy_fetch_returns_none_on_empty_log() {
        let dir = TempDir::new("zero_copy_empty");
        let mut mgr = open_manager(&dir);
        assert!(mgr.plan_zero_copy_fetch(0, 4096, 0).unwrap().is_none());
    }

    /// `fetch` must decode a batch's records and hand back the same offsets/timestamps/
    /// payloads a per-record frame produce would have — and, critically, a fetch whose
    /// `start_offset` lands in the *middle* of a batch's range must still return exactly
    /// the records from there onward, never the whole batch and never nothing. This is
    /// the buffered-path half of stage 1b-ii's "fetch must serve batches" requirement.
    #[test]
    fn fetch_starting_mid_batch_returns_only_records_from_that_offset_onward() {
        let dir = TempDir::new("fetch_mid_batch");
        let mut mgr = open_manager(&dir);

        mgr.append(b"frame0", 0).unwrap(); // offset 0
        let records = sample_batch_records(5);
        append_built_batch(
            &mut mgr,
            1_700_000_000_000,
            3,
            42,
            7,
            11,
            false,
            BatchCompression::Zstd,
            &records,
        )
        .unwrap(); // offsets 1..=5
        mgr.append(b"frame6", 6).unwrap(); // offset 6

        // Starting exactly at the batch's base offset returns every record in the batch,
        // plus the frame after it.
        let from_base = mgr.fetch(1, 65536).unwrap();
        assert_eq!(
            from_base.iter().map(|f| f.offset).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );

        // Starting in the middle of the batch must skip the earlier records in that same
        // batch, not return them and not return nothing.
        let mid = mgr.fetch(3, 65536).unwrap();
        assert_eq!(
            mid.iter().map(|f| f.offset).collect::<Vec<_>>(),
            vec![3, 4, 5, 6]
        );
        assert_eq!(mid[0].payload.as_ref(), records[2].1.as_ref());
        assert_eq!(mid[0].timestamp, records[2].0);

        // Starting at the batch's last offset returns just that one record plus what
        // follows.
        let last = mgr.fetch(5, 65536).unwrap();
        assert_eq!(
            last.iter().map(|f| f.offset).collect::<Vec<_>>(),
            vec![5, 6]
        );
    }

    /// A batch decoded by `fetch` is handed back as an uncompressed synthetic
    /// `RecordFrame` per record — the payload must already be decompressed, exactly what
    /// a client calling `decompress_payload()` on a per-record-compressed frame would
    /// have gotten, so a batch-compression-vs-per-record-compression choice on the
    /// produce side is invisible on the fetch side.
    #[test]
    fn fetch_decodes_compressed_batch_records_to_plain_payloads() {
        let dir = TempDir::new("fetch_batch_compressed");
        let mut mgr = open_manager(&dir);

        let records = sample_batch_records(4);
        append_built_batch(
            &mut mgr,
            1_700_000_000_000,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::Lz4,
            &records,
        )
        .unwrap();

        let fetched = mgr.fetch(0, 65536).unwrap();
        assert_eq!(fetched.len(), 4);
        for (i, frame) in fetched.iter().enumerate() {
            assert_eq!(frame.offset, i as u64);
            assert_eq!(frame.payload.as_ref(), records[i].1.as_ref());
            // Synthesized frames are always plain/uncompressed magic — decompressing
            // them is a safe no-op, exactly matching a client's existing
            // `decompress_payload()` call on any already-uncompressed frame.
            assert_eq!(
                frame.decompress_payload().unwrap().as_ref(),
                records[i].1.as_ref()
            );
        }
    }

    /// `fetch` must still make progress when `max_bytes` is smaller than a single batch:
    /// a batch is atomic, so a byte budget that only covers part of it must still return
    /// every record from `start_offset` onward in that batch, mirroring how the buffered
    /// frame path already guarantees at least one record even under a tiny budget.
    #[test]
    fn fetch_expands_read_when_max_bytes_is_smaller_than_the_batch() {
        let dir = TempDir::new("fetch_batch_tiny_budget");
        let mut mgr = open_manager(&dir);

        let records = sample_batch_records(20);
        append_built_batch(
            &mut mgr,
            1_700_000_000_000,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &records,
        )
        .unwrap();

        // A budget far smaller than the whole batch's encoded size.
        let fetched = mgr.fetch(0, 32).unwrap();
        assert_eq!(
            fetched.len(),
            20,
            "a batch cut short only by max_bytes must still be served whole"
        );
    }

    /// `plan_zero_copy_fetch` must decline (return `None`) rather than stream a batch's
    /// raw bytes to the socket, when `start_offset` needs data from within that batch —
    /// the zero-copy wire format is exactly `frame_count` `RecordFrame`s, which a batch's
    /// bytes are not. Declining here is what makes `try_zero_copy_fetch` fall back to the
    /// buffered `fetch` path, which does understand batches.
    #[test]
    fn plan_zero_copy_fetch_declines_when_start_offset_needs_a_batch() {
        let dir = TempDir::new("zero_copy_declines_on_batch");
        let mut mgr = open_manager(&dir);

        mgr.append(b"frame0", 0).unwrap(); // offset 0
        append_built_batch(
            &mut mgr,
            10,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &sample_batch_records(3),
        )
        .unwrap(); // offsets 1..=3
        mgr.append(b"frame4", 40).unwrap(); // offset 4
        let hw = mgr.high_watermark();
        assert_eq!(hw, 5);

        // Starting at the batch's base offset, or anywhere inside it: decline.
        for start in [1u64, 2, 3] {
            assert!(
                mgr.plan_zero_copy_fetch(start, 65536, hw)
                    .unwrap()
                    .is_none(),
                "start_offset {start} lands in the batch; zero-copy must decline"
            );
        }

        // Starting before the batch: zero-copy still serves what it can (just frame0)
        // whole frames before stopping at the batch, rather than declining outright.
        let plan = mgr.plan_zero_copy_fetch(0, 65536, hw).unwrap().unwrap();
        assert_eq!(plan.frame_count, 1);
        assert_eq!(read_plan_bytes(&plan), {
            let mut expected = Vec::new();
            mgr.fetch(0, 65536).unwrap()[0].encode_into(&mut expected);
            expected
        });

        // Starting at the frame right after the batch: normal zero-copy, unaffected.
        let plan = mgr.plan_zero_copy_fetch(4, 65536, hw).unwrap().unwrap();
        assert_eq!(plan.frame_count, 1);
    }

    fn compact_config(
        delete_retention_millis: Option<u64>,
        min_cleanable_dirty_ratio: f64,
    ) -> EngineConfig {
        EngineConfig {
            cleanup_policy: crate::config::CleanupPolicy::Compact,
            delete_retention_millis,
            min_cleanable_dirty_ratio,
            ..EngineConfig::default()
        }
    }

    /// Appends one batch of explicitly keyed records — how a producer writes to a
    /// compacted topic now that keys are on the record rather than guessed from a payload.
    /// A `None` value is a tombstone.
    fn append_keyed(
        mgr: &mut SegmentManager,
        records: &[(&[u8], Option<&[u8]>)],
        timestamp: u64,
        codec: BatchCompression,
    ) -> RecordBatch {
        let triples: Vec<(u64, Option<Bytes>, Option<Bytes>)> = records
            .iter()
            .map(|(k, v)| {
                (
                    timestamp,
                    Some(Bytes::copy_from_slice(k)),
                    v.map(Bytes::copy_from_slice),
                )
            })
            .collect();
        let batch = RecordBatch::create(0, timestamp, 0, 0, 0, 0, false, codec, &triples);
        mgr.append_batch(batch, 0).unwrap()
    }

    /// Every record still in the log, as `(offset, key, value)`, across historical and
    /// active segments. Unlike `fetch`, this preserves keys and distinguishes a null value
    /// (tombstone) from an empty one.
    #[allow(clippy::type_complexity)]
    fn all_keyed_records(mgr: &mut SegmentManager) -> Vec<(u64, Option<Vec<u8>>, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        for pair in &mut mgr.historical {
            for r in records_for_compaction(&pair.read_all_entries().unwrap()) {
                out.push((
                    r.offset,
                    r.key.map(|k| k.to_vec()),
                    r.value.map(|v| v.to_vec()),
                ));
            }
        }
        for r in records_for_compaction(&mgr.active.read_all_entries().unwrap()) {
            out.push((
                r.offset,
                r.key.map(|k| k.to_vec()),
                r.value.map(|v| v.to_vec()),
            ));
        }
        out
    }

    #[test]
    fn compact_segments_keeps_unexpired_tombstone_as_latest_record() {
        let dir = TempDir::new("tombstone_kept");
        // min_cleanable_dirty_ratio: 0.0 so this test only exercises tombstone semantics,
        // not the separate dirty-ratio gate.
        let mut mgr =
            SegmentManager::open(&dir.0, compact_config(Some(24 * 60 * 60 * 1000), 0.0)).unwrap();

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        append_keyed(
            &mut mgr,
            &[(b"user1", Some(b"val_1"))],
            1000,
            BatchCompression::None,
        );
        // Null value = tombstone, written "now" so it is well inside the 24h grace period.
        append_keyed(
            &mut mgr,
            &[(b"user1", None)],
            now_ms,
            BatchCompression::None,
        );
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 1, "the stale user1 record should be dropped");

        let remaining = all_keyed_records(&mut mgr);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 1);
        assert_eq!(remaining[0].1.as_deref(), Some(b"user1".as_ref()));
        assert_eq!(
            remaining[0].2, None,
            "the survivor is the tombstone itself, still null-valued"
        );
    }

    /// An empty-but-present value is an ordinary record, not a delete. Under the old
    /// payload-sniffing convention these were the same thing; the null sentinel is what
    /// makes them distinguishable, and compaction must honour the distinction.
    #[test]
    fn compact_segments_treats_empty_value_as_a_record_not_a_tombstone() {
        let dir = TempDir::new("empty_value_not_tombstone");
        // A 1ms grace period: were this treated as a tombstone it would be expired
        // immediately and the key purged outright.
        let mut mgr = SegmentManager::open(&dir.0, compact_config(Some(1), 0.0)).unwrap();

        append_keyed(
            &mut mgr,
            &[(b"keyA", Some(b"first"))],
            1,
            BatchCompression::None,
        );
        append_keyed(&mut mgr, &[(b"keyA", Some(b""))], 1, BatchCompression::None);
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 1, "only the superseded first value should be dropped");

        let remaining = all_keyed_records(&mut mgr);
        assert_eq!(
            remaining.len(),
            1,
            "an empty value is a normal record — the key must not be purged"
        );
        assert_eq!(remaining[0].0, 1);
        assert_eq!(remaining[0].2.as_deref(), Some(b"".as_ref()));
    }

    /// A record with no key at all cannot be deduped by key, so compaction must leave it
    /// alone rather than guessing one out of its payload.
    #[test]
    fn compact_segments_keeps_keyless_records_untouched() {
        let dir = TempDir::new("keyless_kept");
        let mut mgr = SegmentManager::open(&dir.0, compact_config(None, 0.0)).unwrap();

        // Two keyless records whose payloads are identical — under the old sniffing these
        // hashed to the same "key" and one would have been discarded as stale.
        append_built_batch(
            &mut mgr,
            1,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &[
                (1, Bytes::from_static(b"same:payload")),
                (1, Bytes::from_static(b"same:payload")),
            ],
        )
        .unwrap();
        // Plus a keyed pair where the first really is superseded, so the segment is dirty
        // enough to be rewritten and the keyless records genuinely pass through a rewrite.
        append_keyed(&mut mgr, &[(b"k", Some(b"v1"))], 1, BatchCompression::None);
        append_keyed(&mut mgr, &[(b"k", Some(b"v2"))], 1, BatchCompression::None);
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 1, "only the superseded keyed record should be dropped");

        let remaining = all_keyed_records(&mut mgr);
        let keyless: Vec<_> = remaining.iter().filter(|(_, k, _)| k.is_none()).collect();
        assert_eq!(
            keyless.len(),
            2,
            "both keyless records must survive — identical payloads are not a shared key"
        );
    }

    #[test]
    fn compact_segments_purges_key_once_tombstone_exceeds_delete_retention() {
        let dir = TempDir::new("tombstone_purged");
        // 1-second grace period; both records are written with a far-in-the-past
        // timestamp so the tombstone is unambiguously expired regardless of how fast the
        // test itself runs.
        let mut mgr = SegmentManager::open(&dir.0, compact_config(Some(1000), 0.0)).unwrap();

        append_keyed(
            &mut mgr,
            &[(b"keyA", Some(b"val1"))],
            1,
            BatchCompression::None,
        );
        append_keyed(
            &mut mgr,
            &[(b"keyA", Some(b"val2"))],
            1,
            BatchCompression::None,
        );
        append_keyed(&mut mgr, &[(b"keyB", None)], 1, BatchCompression::None);
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(
            n, 2,
            "offset 0 (stale) and offset 2 (expired tombstone) should be dropped"
        );

        let remaining = all_keyed_records(&mut mgr);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 1);
        assert_eq!(remaining[0].2.as_deref(), Some(b"val2".as_ref()));
    }

    #[test]
    fn compact_segments_skips_segment_below_min_cleanable_dirty_ratio() {
        let dir = TempDir::new("dirty_ratio_gate");
        let mut mgr = SegmentManager::open(&dir.0, compact_config(None, 0.5)).unwrap();

        // 10 same-size records (offsets 0..9) in one historical segment.
        for i in 0..10u64 {
            append_keyed(
                &mut mgr,
                &[(format!("k{i}").as_bytes(), Some(b"v0"))],
                i,
                BatchCompression::None,
            );
        }
        mgr.rotate_segment().unwrap();
        // Supersede k0 from the active segment — exactly 1 of the 10 historical entries
        // (10%) becomes stale, well below the 50% gate.
        append_keyed(
            &mut mgr,
            &[(b"k0", Some(b"v1"))],
            100,
            BatchCompression::None,
        );

        let n = mgr.apply_retention().unwrap();
        assert_eq!(
            n, 0,
            "10% dirty is below the 50% min_cleanable_dirty_ratio gate"
        );
        assert_eq!(
            records_for_compaction(&mgr.historical[0].read_all_entries().unwrap()).len(),
            10
        );

        // Lowering the gate below the segment's actual dirty ratio lets it compact.
        mgr.set_min_cleanable_dirty_ratio(0.05);
        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 1, "10% dirty now clears the lowered 5% gate");
        assert_eq!(
            records_for_compaction(&mgr.historical[0].read_all_entries().unwrap()).len(),
            9
        );
    }

    /// Keys live inside a batch's compressed record data, so compaction must decompress to
    /// read them — this is the one place the broker legitimately does that.
    #[test]
    fn compact_segments_dedups_records_inside_compressed_batches() {
        let dir = TempDir::new("compact_compressed");
        let mut mgr = SegmentManager::open(&dir.0, compact_config(None, 0.0)).unwrap();

        append_keyed(
            &mut mgr,
            &[(b"user1", Some(b"val_1"))],
            1,
            BatchCompression::Lz4,
        );
        append_keyed(
            &mut mgr,
            &[(b"user1", Some(b"val_2"))],
            2,
            BatchCompression::Zstd,
        );
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 1, "the stale compressed user1 record should be dropped");

        let remaining = all_keyed_records(&mut mgr);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 1);
        assert_eq!(remaining[0].2.as_deref(), Some(b"val_2".as_ref()));
    }

    /// A batch that is only *partially* compacted must come back out as a batch, still
    /// compressed in the producer's codec, with its survivors at their original offsets.
    ///
    /// Flattening survivors into plain uncompressed frames — what compaction used to do —
    /// would keep a second record format alive forever and strip compression from exactly
    /// the topics that retain data longest.
    #[test]
    fn compact_segments_rewrites_a_partially_compacted_batch_as_a_compressed_batch() {
        let dir = TempDir::new("compact_rewrites_as_batch");
        let mut mgr = SegmentManager::open(&dir.0, compact_config(None, 0.0)).unwrap();

        append_keyed(
            &mut mgr,
            &[
                (b"k1", Some(b"v1")),
                (b"k2", Some(b"v2")),
                (b"k3", Some(b"v3")),
            ],
            1,
            BatchCompression::Zstd,
        );
        mgr.rotate_segment().unwrap();
        // Supersedes only k2, so the historical batch keeps k1 and k3 and must be rebuilt
        // with a gap where k2 was.
        append_keyed(
            &mut mgr,
            &[(b"k2", Some(b"v2b"))],
            2,
            BatchCompression::None,
        );

        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 1, "only k2's stale record should be dropped");

        let entries = mgr.historical[0].read_all_entries().unwrap();
        assert_eq!(entries.len(), 1, "the segment must still hold one batch");
        let LogEntry::Batch(rebuilt) = &entries[0] else {
            panic!("compaction must rewrite a batch as a batch, not flatten it into frames");
        };
        assert_eq!(
            rebuilt.compression().unwrap(),
            BatchCompression::Zstd,
            "the rebuilt batch must keep the producer's codec"
        );
        assert_eq!(rebuilt.record_count, 2);
        let records = rebuilt.records().unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 2],
            "survivors keep their original offsets, leaving a gap where k2 was"
        );
        assert_eq!(records[0].key.as_deref(), Some(b"k1".as_ref()));
        assert_eq!(records[1].key.as_deref(), Some(b"k3".as_ref()));
    }

    /// The headline regression an earlier stage fixed: compaction once read entries via
    /// `read_all_frames`, which silently skips `RecordBatch` entries, so a rewrite carried
    /// forward only the frames it saw and the entire batch vanished. This keeps that
    /// covered now that records carry explicit keys.
    #[test]
    fn compact_segments_keeps_surviving_batch_records() {
        let dir = TempDir::new("compact_keeps_batch_records");
        let mut mgr = SegmentManager::open(&dir.0, compact_config(None, 0.0)).unwrap();

        append_keyed(
            &mut mgr,
            &[(b"zzz", Some(b"keep"))],
            1,
            BatchCompression::None,
        );
        append_keyed(
            &mut mgr,
            &[
                (b"k1", Some(b"v1")),
                (b"k2", Some(b"v2")),
                (b"k3", Some(b"v3")),
            ],
            2,
            BatchCompression::None,
        );
        append_keyed(
            &mut mgr,
            &[(b"www", Some(b"stale"))],
            3,
            BatchCompression::None,
        );
        mgr.rotate_segment().unwrap();

        append_keyed(
            &mut mgr,
            &[(b"www", Some(b"fresh"))],
            4,
            BatchCompression::None,
        );
        append_keyed(&mut mgr, &[(b"k1", Some(b"v2"))], 5, BatchCompression::None);

        let n = mgr.apply_retention().unwrap();
        assert_eq!(
            n, 2,
            "offset 1 (stale batch record k1) and offset 4 (stale www) should be dropped"
        );

        let remaining = all_keyed_records(&mut mgr);
        let keys: Vec<&[u8]> = remaining
            .iter()
            .map(|(_, k, _)| k.as_deref().unwrap())
            .collect();
        assert_eq!(
            keys,
            vec![
                b"zzz".as_ref(),
                b"k2".as_ref(),
                b"k3".as_ref(),
                b"www".as_ref(),
                b"k1".as_ref(),
            ],
            "k2/k3 came from inside the batch and were never superseded — compaction must \
             not drop them just because they arrived batched"
        );
    }

    /// Dedup-by-key must not care how records were grouped into batches: a key superseded
    /// from a later batch is dropped regardless of which batch it originally arrived in.
    #[test]
    fn compact_segments_dedups_across_batch_boundaries() {
        let dir = TempDir::new("compact_dedup_across_batches");
        let mut mgr = SegmentManager::open(&dir.0, compact_config(None, 0.0)).unwrap();

        append_keyed(
            &mut mgr,
            &[(b"keyA", Some(b"stale"))],
            1,
            BatchCompression::None,
        );
        append_keyed(
            &mut mgr,
            &[(b"keyA", Some(b"fresh")), (b"keyB", Some(b"stale"))],
            2,
            BatchCompression::None,
        );
        append_keyed(
            &mut mgr,
            &[(b"keyB", Some(b"fresh"))],
            3,
            BatchCompression::None,
        );
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(
            n, 2,
            "offset 0 (keyA stale) and offset 2 (keyB stale) should be dropped"
        );

        let remaining = all_keyed_records(&mut mgr);
        let values: Vec<&[u8]> = remaining
            .iter()
            .map(|(_, _, v)| v.as_deref().unwrap())
            .collect();
        assert_eq!(values, vec![b"fresh".as_ref(), b"fresh".as_ref()]);
    }

    /// An expired tombstone purges its key entirely, batch record and all.
    #[test]
    fn compact_segments_purges_expired_tombstone_carried_in_a_batch() {
        let dir = TempDir::new("compact_batch_tombstone_expired");
        let mut mgr = SegmentManager::open(&dir.0, compact_config(Some(1000), 0.0)).unwrap();

        append_keyed(
            &mut mgr,
            &[(b"keyA", Some(b"val1")), (b"keyA", Some(b"val2"))],
            1,
            BatchCompression::None,
        );
        append_keyed(&mut mgr, &[(b"keyB", None)], 1, BatchCompression::None);
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(
            n, 2,
            "offset 0 (stale) and offset 2 (expired tombstone) should be dropped"
        );

        let remaining = all_keyed_records(&mut mgr);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 1);
        assert_eq!(remaining[0].2.as_deref(), Some(b"val2".as_ref()));
    }

    /// `records_for_compaction` must see exactly the records `read_all_frames` sees when a
    /// segment holds only frames — a direct equivalence check between the record scan
    /// compaction uses and the plain frame reader, over the same on-disk data.
    #[test]
    fn records_for_compaction_matches_read_all_frames_when_no_batches_present() {
        let dir = TempDir::new("compact_frame_only_equivalence");
        let mut mgr = open_manager(&dir);

        for i in 0..6u64 {
            mgr.append(format!("k{i}:v{i}").as_bytes(), i).unwrap();
        }
        mgr.rotate_segment().unwrap();
        for i in 6..9u64 {
            mgr.append(format!("k{i}:v{i}").as_bytes(), i).unwrap();
        }

        for pair_frames_and_entries in [
            (
                mgr.historical[0].read_all_frames().unwrap(),
                mgr.historical[0].read_all_entries().unwrap(),
            ),
            (
                mgr.active.read_all_frames().unwrap(),
                mgr.active.read_all_entries().unwrap(),
            ),
        ] {
            let (frames, entries) = pair_frames_and_entries;
            let records = records_for_compaction(&entries);
            assert_eq!(records.len(), frames.len());
            for (record, frame) in records.iter().zip(frames.iter()) {
                assert_eq!(record.offset, frame.offset);
                assert_eq!(record.timestamp, frame.timestamp);
                assert_eq!(record.value.as_deref(), Some(frame.payload.as_ref()));
                // A frame has no key field, so compaction can never dedupe one by key.
                assert_eq!(record.key, None);
            }
        }
    }

    #[test]
    fn restart_after_clean_rotation_trusts_marker_and_skips_full_scan() {
        let dir = TempDir::new("restart_clean_trust");

        // First "process": write across a few rotated (historical) segments plus one
        // active segment, then drop it — simulating a clean shutdown (rotation always
        // finalizes+marks the segment it rotates out, regardless of how the process
        // holding it later exits).
        {
            let config = EngineConfig {
                max_segment_bytes: 200,
                ..EngineConfig::default()
            };
            let mut mgr = SegmentManager::open(&dir.0, config).unwrap();
            for i in 0..8u64 {
                let frame = RecordFrame::create(i, i, format!("payload-{}", i).into_bytes());
                mgr.append_verbatim(&frame).unwrap();
                mgr.rotate_segment().unwrap();
            }
            let last = RecordFrame::create(8, 8, b"active-tail".to_vec());
            mgr.append_verbatim(&last).unwrap();
            assert_eq!(mgr.historical.len(), 8);
        }

        // Every historical segment should now carry a `.clean` marker.
        for base in 0..8u64 {
            let log_path = dir.0.join(format!("{}.log", format_segment_filename(base)));
            let marker_path = log_path.with_extension("clean");
            assert!(
                marker_path.exists(),
                "expected clean marker for segment {}",
                base
            );
        }

        // "Restart": reopen the same directory and verify all data and offsets survived
        // identically, whether or not the trusted fast path was taken for each segment.
        let config = EngineConfig {
            max_segment_bytes: 200,
            ..EngineConfig::default()
        };
        let mut mgr2 = SegmentManager::open(&dir.0, config).unwrap();
        assert_eq!(mgr2.high_watermark(), 9);
        assert_eq!(mgr2.historical.len(), 8);
        for i in 0..9u64 {
            let frames = mgr2.fetch(i, 4096).unwrap();
            assert_eq!(frames.first().map(|f| f.offset), Some(i));
        }

        // The restarted manager must still be fully writable/rotatable afterwards.
        let extra = RecordFrame::create(9, 9, b"post-restart".to_vec());
        assert_eq!(
            mgr2.append_verbatim(&extra).unwrap(),
            VerbatimAppendResult::Appended
        );
        assert_eq!(mgr2.high_watermark(), 10);
    }

    #[test]
    fn corrupted_trusted_segment_falls_back_to_full_scan() {
        let dir = TempDir::new("restart_trust_fallback");
        {
            let config = EngineConfig {
                max_segment_bytes: 200,
                ..EngineConfig::default()
            };
            let mut mgr = SegmentManager::open(&dir.0, config).unwrap();
            let frame = RecordFrame::create(0, 0, b"first".to_vec());
            mgr.append_verbatim(&frame).unwrap();
            mgr.rotate_segment().unwrap();
            let tail = RecordFrame::create(1, 1, b"second".to_vec());
            mgr.append_verbatim(&tail).unwrap();
        }

        // Corrupt the now-historical (rotated, marker-carrying) segment 0's log file by
        // appending a stray byte after it was already marked clean — the size no longer
        // matches the marker, so the trusted fast path must refuse it and fall back to a
        // full scan rather than silently reporting wrong/missing data.
        let log_path = dir.0.join(format!("{}.log", format_segment_filename(0)));
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&log_path)
                .unwrap();
            f.write_all(&[0xFFu8]).unwrap();
        }

        let config = EngineConfig {
            max_segment_bytes: 200,
            ..EngineConfig::default()
        };
        let mgr2 = SegmentManager::open(&dir.0, config).unwrap();
        // The full-scan recovery path truncates the trailing garbage byte and recovers
        // exactly the one valid frame that was there.
        assert_eq!(mgr2.historical.len(), 1);
        assert_eq!(mgr2.historical[0].log.next_offset, 1);
    }

    /// A timestamp older than everything retained must return the OLDEST offset, not the
    /// head of the log. Returning `active.base_offset` meant "replay everything since a
    /// time before my retention window" — the normal way to start a full replay — silently
    /// skipped the entire history, with no error to indicate anything was missed.
    #[test]
    fn find_offset_for_timestamp_falls_back_to_oldest_not_newest() {
        let dir = TempDir::new("ts_lookup_fallback");
        let config = EngineConfig {
            max_segment_bytes: 200,
            index_interval_bytes: 1,
            ..EngineConfig::default()
        };
        let mut mgr = SegmentManager::open(&dir.0, config).unwrap();

        // Several segments' worth of records, all timestamped well after the epoch.
        for i in 0..12u64 {
            mgr.append(format!("k{}:v{}", i, i).as_bytes(), 10_000 + i * 100)
                .unwrap();
        }
        assert!(
            !mgr.historical.is_empty(),
            "precondition: the log must have rotated"
        );
        let oldest_offset = mgr.historical[0].base_offset;

        // A target older than every record.
        let found = mgr.find_offset_for_timestamp(1);
        assert_eq!(
            found, oldest_offset,
            "a pre-history timestamp must resolve to the oldest retained offset, not the log head"
        );
        assert_ne!(
            found, mgr.active.base_offset,
            "must not return the head of the log"
        );
    }

    /// Truncation must delete the discarded segments' files, not merely forget them.
    /// Otherwise startup rediscovers them and the log silently returns to its
    /// pre-truncation length — resurrecting exactly the diverged records a follower
    /// truncated in order to rejoin.
    #[test]
    fn truncate_after_deletes_files_so_data_stays_gone_across_restart() {
        let dir = TempDir::new("truncate_file_deletion");
        let truncate_at;
        {
            let config = EngineConfig {
                max_segment_bytes: 200,
                index_interval_bytes: 1,
                ..EngineConfig::default()
            };
            let mut mgr = SegmentManager::open(&dir.0, config).unwrap();
            for i in 0..40u64 {
                mgr.append(format!("key{:03}:value{:03}", i, i).as_bytes(), i)
                    .unwrap();
            }
            mgr.high_watermark = mgr.active.log.next_offset;
            assert!(
                mgr.historical.len() >= 2,
                "precondition: multiple historical segments, got {}",
                mgr.historical.len()
            );

            // Truncate away everything from the second historical segment onward.
            truncate_at = mgr.historical[1].base_offset;
            mgr.truncate_after(truncate_at).unwrap();
            assert_eq!(mgr.high_watermark, truncate_at);
        }

        // Reopen: the truncated records must NOT come back.
        let mgr = SegmentManager::open(&dir.0, EngineConfig::default()).unwrap();
        let restored_end = mgr.active.log.next_offset;
        assert_eq!(
            restored_end, truncate_at,
            "truncated records must stay gone across a restart (log end {} vs truncation point {})",
            restored_end, truncate_at
        );
    }

    /// Simulates a crash between compaction's two renames: the original was moved aside
    /// to `.deleted` and the compacted replacement was never installed. Startup must
    /// restore the original rather than silently losing the whole segment.
    #[test]
    fn interrupted_compaction_restores_backup_when_replacement_missing() {
        let dir = TempDir::new("compaction_crash_restore");
        {
            let config = EngineConfig {
                max_segment_bytes: 4096,
                ..EngineConfig::default()
            };
            let mut mgr = SegmentManager::open(&dir.0, config).unwrap();
            for i in 0..4u64 {
                mgr.append(format!("k{}:v{}", i, i).as_bytes(), i).unwrap();
            }
            mgr.rotate_segment().unwrap();
        }

        // Move segment 0's files aside exactly as the swap's first phase does, then stop
        // — leaving no live `.log` at all.
        let log_path = dir.0.join(format!("{}.log", format_segment_filename(0)));
        let backup_path = deleted_backup_path(&log_path);
        fs::rename(&log_path, &backup_path).unwrap();
        assert!(!log_path.exists(), "precondition: live log is absent");

        let mut mgr = SegmentManager::open(&dir.0, EngineConfig::default()).unwrap();
        assert!(
            log_path.exists(),
            "startup must restore the backup, not leave the segment missing"
        );
        assert!(
            !backup_path.exists(),
            "backup should be consumed by the restore"
        );
        assert_eq!(mgr.historical.len(), 1);
        assert_eq!(mgr.historical[0].read_all_frames().unwrap().len(), 4);
    }

    /// Simulates a crash after both renames but before the backup cleanup: the live file
    /// is the new compacted one and the `.deleted` backup is just garbage to drop.
    #[test]
    fn interrupted_compaction_drops_backup_when_replacement_present() {
        let dir = TempDir::new("compaction_crash_drop");
        {
            let config = EngineConfig {
                max_segment_bytes: 4096,
                ..EngineConfig::default()
            };
            let mut mgr = SegmentManager::open(&dir.0, config).unwrap();
            for i in 0..3u64 {
                mgr.append(format!("k{}:v{}", i, i).as_bytes(), i).unwrap();
            }
            mgr.rotate_segment().unwrap();
        }

        let log_path = dir.0.join(format!("{}.log", format_segment_filename(0)));
        let backup_path = deleted_backup_path(&log_path);
        // Both present: stale backup alongside the real, current file.
        fs::copy(&log_path, &backup_path).unwrap();
        // A leftover `.compact` temporary from the same interrupted run must also go.
        let tmp_path = dir
            .0
            .join(format!("{}.log.compact", format_segment_filename(0)));
        fs::write(&tmp_path, b"partial garbage").unwrap();

        let mut mgr = SegmentManager::open(&dir.0, EngineConfig::default()).unwrap();
        assert!(log_path.exists(), "the live file must be left untouched");
        assert!(!backup_path.exists(), "stale backup should be dropped");
        assert!(
            !tmp_path.exists(),
            "leftover .compact temporary should be discarded"
        );
        assert_eq!(mgr.historical[0].read_all_frames().unwrap().len(), 3);
    }

    #[test]
    fn segment_ms_rolls_active_segment_purely_on_age() {
        let dir = TempDir::new("segment_ms_roll");
        let config = EngineConfig {
            max_segment_bytes: u64::MAX, // never roll on size alone
            segment_ms: Some(1),         // roll almost immediately once non-empty
            ..EngineConfig::default()
        };
        let mut mgr = SegmentManager::open(&dir.0, config).unwrap();

        mgr.append(b"first", 1).unwrap();
        assert_eq!(
            mgr.historical.len(),
            0,
            "fresh segment shouldn't roll instantly"
        );

        std::thread::sleep(std::time::Duration::from_millis(20));
        mgr.append(b"second", 2).unwrap();
        assert_eq!(
            mgr.historical.len(),
            1,
            "active segment should have rolled purely due to age"
        );
    }

    /// Builds a batch the way a producer does — placeholder base offset, its own codec —
    /// and hands it to `append_batch`, which stamps the real offset and leader epoch.
    #[allow(clippy::too_many_arguments)]
    fn append_built_batch(
        mgr: &mut SegmentManager,
        base_timestamp: u64,
        leader_epoch: u32,
        producer_id: u64,
        producer_epoch: i16,
        base_sequence: i32,
        transactional: bool,
        codec: BatchCompression,
        records: &[(u64, Bytes)],
    ) -> IoResult<RecordBatch> {
        let keyed: Vec<(u64, Option<Bytes>, Option<Bytes>)> = records
            .iter()
            .map(|(ts, payload)| (*ts, None, Some(payload.clone())))
            .collect();
        let prebuilt = RecordBatch::create(
            0,
            base_timestamp,
            0,
            producer_id,
            producer_epoch,
            base_sequence,
            transactional,
            codec,
            &keyed,
        );
        mgr.append_batch(prebuilt, leader_epoch)
    }

    fn sample_batch_records(n: usize) -> Vec<(u64, Bytes)> {
        (0..n)
            .map(|i| {
                (
                    1_700_000_000_000 + i as u64,
                    Bytes::from(format!("rec-{i}")),
                )
            })
            .collect()
    }

    /// A batch written via `append_batch` must read back byte-identical to what was
    /// written, and be locatable through the sparse index by its base offset — the same
    /// contract a frame append already provides.
    #[test]
    fn append_batch_round_trips_and_is_indexed_by_base_offset() {
        let dir = TempDir::new("append_batch_round_trip");
        let mut mgr = open_manager(&dir);

        let records = sample_batch_records(4);
        let written = append_built_batch(
            &mut mgr,
            1_700_000_000_000,
            7,
            42,
            3,
            9,
            false,
            BatchCompression::None,
            &records,
        )
        .unwrap();
        assert_eq!(written.base_offset, 0);
        assert_eq!(written.record_count, 4);

        let size = mgr.active.log.physical_size as usize;
        let raw = mgr.active.log.read_at(0, size).unwrap();
        let (entry, consumed) = crate::segment::entry::decode_entry(&raw).unwrap();
        assert_eq!(consumed, raw.len());
        match entry {
            LogEntry::Batch(decoded) => assert_eq!(decoded, written),
            LogEntry::Frame(_) => panic!("expected a batch, decoded a frame"),
        }

        // Indexed by base offset: looking up any offset in [0, 3] must resolve to the
        // batch's own physical start (0), the same as a fresh index with one entry would
        // for a single frame at that position.
        let seek = mgr.active.index.find_nearest_physical_pos(0).unwrap();
        assert_eq!(seek.physical_position, 0);
    }

    /// A batch occupies as many offsets as it has records (`last_offset_delta + 1`) — the
    /// next append must land right after the batch's whole range, not just after its base.
    #[test]
    fn append_batch_advances_high_watermark_by_record_count() {
        let dir = TempDir::new("append_batch_hw");
        let mut mgr = open_manager(&dir);

        let records = sample_batch_records(5);
        append_built_batch(
            &mut mgr,
            0,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &records,
        )
        .unwrap();
        assert_eq!(mgr.high_watermark(), 5);

        let frame = mgr.append(b"after-the-batch", 123).unwrap();
        assert_eq!(
            frame.offset, 5,
            "the frame after a 5-record batch must be assigned offset 5"
        );
    }

    /// `SegmentPair::read_all_frames` is one of the four scan sites — its callers (log
    /// compaction) only understand individual frames, so a batch in the middle must be
    /// skipped by its own length, not decoded and not mistaken for corruption that would
    /// truncate the scan and lose the frame written after it.
    #[test]
    fn read_all_frames_skips_a_batch_without_losing_frames_after_it() {
        let dir = TempDir::new("read_all_frames_skip_batch");
        let mut mgr = open_manager(&dir);

        let f0 = mgr.append(b"frame0", 0).unwrap();
        append_built_batch(
            &mut mgr,
            10,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &sample_batch_records(3),
        )
        .unwrap();
        let f4 = mgr.append(b"frame4", 40).unwrap();

        let frames = mgr.active.read_all_frames().unwrap();
        assert_eq!(
            frames,
            vec![f0, f4],
            "the batch must be skipped, and the frame after it must still be recovered"
        );
    }

    /// `read_all_frames` on a segment with no batches at all must behave exactly as before
    /// this stage — the regression check for the frame-only path.
    #[test]
    fn read_all_frames_frame_only_segment_is_unaffected() {
        let dir = TempDir::new("read_all_frames_regression");
        let mut mgr = open_manager(&dir);

        let mut expected = Vec::new();
        for i in 0..4u64 {
            expected.push(mgr.append(format!("payload-{i}").as_bytes(), i).unwrap());
        }

        let frames = mgr.active.read_all_frames().unwrap();
        assert_eq!(frames, expected);
    }

    /// `physical_pos_for_offset` is the other manager.rs scan site. A batch is atomic —
    /// there is no on-disk position for an offset partway through one — so any offset
    /// inside a batch's range (its base offset or a record after it) must resolve to the
    /// batch's own start, the same physical point.
    #[test]
    fn physical_pos_for_offset_treats_any_offset_inside_a_batch_as_its_start() {
        let dir = TempDir::new("physical_pos_batch");
        let mut mgr = open_manager(&dir);

        let f0 = mgr.append(b"frame0", 0).unwrap();
        let f0_end = mgr.active.log.physical_size;
        // base_offset=1, 3 records -> offsets 1,2,3
        append_built_batch(
            &mut mgr,
            10,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &sample_batch_records(3),
        )
        .unwrap();
        let batch_end = mgr.active.log.physical_size;
        let f4 = mgr.append(b"frame4", 40).unwrap();

        assert_eq!(f0.offset, 0);
        assert_eq!(f4.offset, 4);

        for target in [1u64, 2, 3] {
            let (phys, effective) =
                SegmentManager::physical_pos_for_offset(&mut mgr.active, target).unwrap();
            assert_eq!(
                phys, f0_end,
                "offset {target} is inside the batch [1,3]; must resolve to the batch's start"
            );
            assert_eq!(
                effective, 1,
                "offset {target} is inside the batch [1,3]; effective offset must be the batch's base"
            );
        }

        let (phys, effective) =
            SegmentManager::physical_pos_for_offset(&mut mgr.active, 4).unwrap();
        assert_eq!(
            phys, batch_end,
            "offset 4 is the frame right after the batch"
        );
        assert_eq!(effective, 4);
    }

    /// End-to-end: truncating exactly at a batch's base offset (the well-defined case,
    /// analogous to truncating at a frame's own offset) removes the whole batch and
    /// everything after it, keeping everything before intact.
    #[test]
    fn truncate_after_batch_base_offset_removes_whole_batch_and_later_frames() {
        let dir = TempDir::new("truncate_after_batch_base");
        let mut mgr = open_manager(&dir);

        mgr.append(b"frame0", 0).unwrap();
        append_built_batch(
            &mut mgr,
            10,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &sample_batch_records(3),
        )
        .unwrap(); // offsets 1-3
        mgr.append(b"frame4", 40).unwrap();
        assert_eq!(mgr.high_watermark(), 5);

        mgr.truncate_after(1).unwrap();
        assert_eq!(mgr.high_watermark(), 1);

        let frames = mgr.active.read_all_frames().unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].offset, 0);

        // The freed offset range is reusable, exactly as it is after truncating a frame.
        let refilled = mgr.append(b"replacement", 99).unwrap();
        assert_eq!(refilled.offset, 1);
    }

    /// Regression for the divergence bug this stage fixes: a batch is atomic on disk, so
    /// `physical_pos_for_offset` resolves any offset *inside* a batch's range to that
    /// batch's own start — meaning `truncate_after` physically removes the whole batch
    /// even when the requested truncation point was one of its later records. The high
    /// watermark must reflect that: it has to drop all the way to the batch's base
    /// offset, not stop at the offset that was actually requested, or it would claim an
    /// offset for a record that no longer exists on disk.
    #[test]
    fn truncate_after_mid_batch_drops_high_watermark_to_batch_base_offset() {
        let dir = TempDir::new("truncate_after_mid_batch");
        let mut mgr = open_manager(&dir);

        mgr.append(b"frame0", 0).unwrap(); // offset 0
        append_built_batch(
            &mut mgr,
            10,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &sample_batch_records(3),
        )
        .unwrap(); // batch occupies offsets 1,2,3
        mgr.append(b"frame4", 40).unwrap(); // offset 4
        assert_eq!(mgr.high_watermark(), 5);

        // Ask to truncate after offset 2 — the middle of the batch. Since the whole
        // batch [1,3] must be removed, the watermark cannot land at 2: nothing on disk
        // claims that offset any more. It must drop to the batch's base offset, 1.
        mgr.truncate_after(2).unwrap();
        assert_eq!(
            mgr.high_watermark(),
            1,
            "truncating mid-batch must drop the high watermark to the batch's base offset, \
             not the requested offset, since the whole batch was physically removed"
        );

        // Only frame0 (offset 0) remains on disk — the whole batch and frame4 are gone.
        let frames = mgr.active.read_all_frames().unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].offset, 0);

        // The freed range starting at the batch's base offset must be reusable, and the
        // watermark and the physical log must agree: the next append lands at 1, exactly
        // where the batch used to start.
        let refilled = mgr.append(b"replacement", 99).unwrap();
        assert_eq!(refilled.offset, 1);

        // Truncating at the batch's last offset (3, still "mid-batch" in the sense that
        // it doesn't fall on a frame boundary of its own — the whole batch is one atomic
        // entry) must land at the same base offset too.
        let dir2 = TempDir::new("truncate_after_mid_batch_last_offset");
        let mut mgr2 = open_manager(&dir2);
        mgr2.append(b"frame0", 0).unwrap();
        append_built_batch(
            &mut mgr2,
            10,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &sample_batch_records(3),
        )
        .unwrap();
        mgr2.append(b"frame4", 40).unwrap();
        mgr2.truncate_after(3).unwrap();
        assert_eq!(mgr2.high_watermark(), 1);
    }
}
