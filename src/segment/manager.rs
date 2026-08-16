use bytes::Bytes;
use crate::config::EngineConfig;
use crate::protocol::{RecordFrame, HEADER_SIZE};
use crate::segment::index::IndexSegment;
use crate::segment::log::{format_segment_filename, LogSegment};
use crate::segment::timeindex::TimeIndexSegment;
use std::fs;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

use crate::segment::txnindex::TxnIndexSegment;

/// Helper to extract record key from payload for log compaction deduplication
pub fn extract_key(payload: &[u8]) -> &[u8] {
    extract_key_value(payload).0
}

/// Extracts a record's key and, when unambiguous, its value — used by log compaction both
/// for dedup-by-key (`extract_key`) and for detecting tombstones (a compacted-topic record
/// whose value is empty, Hermes's convention for a Kafka-style delete marker: see
/// `SegmentManager::compact_segments`).
///
/// The value half is `None` — never `Some(&[])` — whenever the payload doesn't match one of
/// the two unambiguous key/value encodings below and falls back to "treat the whole payload
/// as its own key". That fallback has no reliable value boundary at all, so it must never be
/// mistaken for an empty (tombstone) value: doing so would misclassify every plain,
/// non-keyed record in a compacted topic as a delete marker and eventually erase it.
pub fn extract_key_value(payload: &[u8]) -> (&[u8], Option<&[u8]>) {
    if payload.len() >= 2 {
        let key_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
        if key_len > 0 && 2 + key_len <= payload.len() {
            return (&payload[2..2 + key_len], Some(&payload[2 + key_len..]));
        }
    }
    if let Ok(s) = std::str::from_utf8(payload) {
        if let Some((k, v)) = s.split_once(':') {
            if !k.is_empty() {
                return (k.as_bytes(), Some(v.as_bytes()));
            }
        }
        if let Some((k, v)) = s.split_once('=') {
            if !k.is_empty() {
                return (k.as_bytes(), Some(v.as_bytes()));
            }
        }
    }
    (payload, None)
}

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
    /// Reads all valid record frames sequentially from this log segment
    pub fn read_all_frames(&mut self) -> IoResult<Vec<RecordFrame>> {
        let size = self.log.physical_size as usize;
        if size == 0 {
            return Ok(Vec::new());
        }
        let raw_bytes = self.log.read_at(0, size)?;
        let mut frames = Vec::new();
        let mut cursor = 0usize;

        while cursor < raw_bytes.len() {
            if cursor + HEADER_SIZE > raw_bytes.len() {
                break;
            }
            let slice = &raw_bytes[cursor..];
            match RecordFrame::decode(slice) {
                Ok((frame, consumed)) => {
                    cursor += consumed;
                    frames.push(frame);
                }
                Err(_) => break,
            }
        }

        Ok(frames)
    }
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
}

impl SegmentManager {
    pub fn open(dir: impl AsRef<Path>, config: EngineConfig) -> IoResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

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
            let log = LogSegment::open(
                &dir,
                base,
                config.max_segment_bytes,
                config.index_interval_bytes,
                config.preallocate_segments,
                &mut index,
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
        })
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
        let frame = match codec {
            crate::config::CompressionCodec::Lz4 => {
                RecordFrame::create_compressed_lz4(assigned_offset, timestamp, &payload)
            }
            crate::config::CompressionCodec::Zstd => {
                RecordFrame::create_compressed_zstd(assigned_offset, timestamp, &payload)
            }
            crate::config::CompressionCodec::None => {
                RecordFrame::create(assigned_offset, timestamp, payload)
            }
        };
        let frame_size = frame.encoded_size() as u64;

        // Rotate segment if active log size exceeds configured threshold
        if self.active.log.physical_size + frame_size > self.config.max_segment_bytes {
            self.rotate_segment()?;
        }

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

    /// Appends a frame produced elsewhere (i.e. a leader's `RecordFrame`) byte-for-byte:
    /// the offset, timestamp, magic byte, and CRC are all taken from `frame` as given,
    /// never reassigned locally. This is what makes a replica's on-disk log byte-identical
    /// to the leader's for the same offset range, instead of diverging (the old replication
    /// path re-derived offset/timestamp/codec through `append_with_codec` on every
    /// replicated record). `frame` has already been CRC-validated by `RecordFrame::decode`,
    /// so no additional integrity check is needed here.
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
        if self.active.log.physical_size + frame_size > self.config.max_segment_bytes {
            self.rotate_segment()?;
        }

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

        // Drop whole historical segments that start at/after the truncation point.
        self.historical.retain(|seg| seg.base_offset < offset);

        if offset < self.active.base_offset {
            // The truncation point falls inside a historical segment (rare: it means the
            // divergence predates the most recent rotation). Promote the highest-based
            // remaining historical segment to active, truncated at `offset`, and discard
            // the old active segment's on-disk files entirely.
            if let Some(mut promoted) = self.historical.pop() {
                Self::truncate_segment_pair(&mut promoted, offset)?;
                let old_active = std::mem::replace(&mut self.active, promoted);
                let _ = fs::remove_file(&old_active.log.path);
                let _ = fs::remove_file(old_active.index.path());
                let _ = fs::remove_file(old_active.time_index.path());
                let _ = fs::remove_file(old_active.txn_index.path());
            } else {
                // No historical segment covers `offset` (shouldn't happen if `offset`
                // is a valid previously-seen index) — fall back to truncating active.
                Self::truncate_segment_pair(&mut self.active, offset)?;
            }
        } else {
            Self::truncate_segment_pair(&mut self.active, offset)?;
        }

        self.high_watermark = offset;
        self.active.log.next_offset = offset;
        self.bytes_since_last_index = 0;
        Ok(())
    }

    /// Truncates a single segment pair so no frame with offset >= `offset` remains.
    fn truncate_segment_pair(pair: &mut SegmentPair, offset: u64) -> IoResult<()> {
        let phys = Self::physical_pos_for_offset(pair, offset)?;
        pair.log.truncate_to(phys)?;
        pair.index.truncate_after(offset)?;
        pair.time_index.truncate_after(offset)?;
        pair.txn_index.truncate_after(offset)?;
        pair.mmap = None;
        Ok(())
    }

    /// Scans forward from the nearest sparse-index entry to find the physical byte
    /// position at which `offset` begins within this segment pair.
    fn physical_pos_for_offset(pair: &mut SegmentPair, offset: u64) -> IoResult<u64> {
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
            match RecordFrame::decode(&raw[cursor..]) {
                Ok((frame, consumed)) => {
                    if frame.offset == offset {
                        return Ok(phys);
                    }
                    cursor += consumed;
                    phys += consumed as u64;
                }
                Err(_) => break,
            }
        }
        // `offset` not found in this segment (e.g. it's exactly the segment's end) —
        // truncating at the current physical size is a safe no-op.
        Ok(pair.log.physical_size)
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

        if self.active.log.physical_size + frame_size > self.config.max_segment_bytes {
            self.rotate_segment()?;
        }

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

    /// Read records starting at logical offset using binary search across segments and sparse index ($O(\log N)$)
    pub fn fetch(&mut self, start_offset: u64, max_bytes: usize) -> IoResult<Vec<RecordFrame>> {
        let segment_pair = self.find_segment_pair_mut(start_offset);
        let seek_entry = segment_pair.index.find_nearest_physical_pos(start_offset);
        let start_pos = seek_entry.map_or(0, |e| e.physical_position as u64);

        if let Some(ref mmap) = segment_pair.mmap {
            return Ok(mmap.fetch_zero_copy(start_pos, start_offset, max_bytes));
        }

        let raw_bytes = segment_pair.log.read_at(start_pos, max_bytes)?;

        let mut frames = Vec::new();
        let mut cursor = 0usize;

        while cursor < raw_bytes.len() {
            if cursor + HEADER_SIZE > raw_bytes.len() {
                break;
            }
            let slice = &raw_bytes[cursor..];
            match RecordFrame::decode(slice) {
                Ok((frame, consumed)) => {
                    cursor += consumed;
                    if frame.offset >= start_offset {
                        frames.push(frame);
                    }
                }
                Err(_) => break,
            }
        }

        Ok(frames)
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
                        *rec = LatestRecord { offset, timestamp, is_tombstone };
                    }
                })
                .or_insert(LatestRecord { offset, timestamp, is_tombstone });
        };

        for pair in &mut self.historical {
            let frames = pair.read_all_frames()?;
            for frame in frames {
                if frame.is_control_marker() {
                    continue;
                }
                let (key, value) = extract_key_value(&frame.payload);
                let is_tombstone = value.is_some_and(|v| v.is_empty());
                observe(key, frame.offset, frame.timestamp, is_tombstone);
            }
        }

        let active_frames = self.active.read_all_frames()?;
        for frame in active_frames {
            if frame.is_control_marker() {
                continue;
            }
            let (key, value) = extract_key_value(&frame.payload);
            let is_tombstone = value.is_some_and(|v| v.is_empty());
            observe(key, frame.offset, frame.timestamp, is_tombstone);
        }
        drop(observe);

        if latest.is_empty() {
            return Ok(0);
        }

        // Phase 1b: keys whose latest record is a tombstone older than
        // `delete_retention_millis` are fully purged — the key disappears from `latest`
        // entirely, which Phase 2 below reads as "discard every frame for this key,
        // including what would otherwise be the kept tombstone."
        let purged_keys: std::collections::HashSet<Vec<u8>> =
            if let Some(delete_retention_ms) = self.config.delete_retention_millis {
                let purged: std::collections::HashSet<Vec<u8>> = latest
                    .iter()
                    .filter(|(_, rec)| {
                        rec.is_tombstone
                            && now_ms.saturating_sub(rec.timestamp) > delete_retention_ms
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
        let latest_offsets: std::collections::HashMap<Vec<u8>, u64> = latest
            .into_iter()
            .map(|(k, rec)| (k, rec.offset))
            .collect();

        let mut total_compacted_frames = 0;
        let mut segments_compacted = 0usize;
        let mut i = 0;

        while i < self.historical.len() && segments_compacted < MAX_SEGMENTS_COMPACTED_PER_CALL {
            let all_frames = self.historical[i].read_all_frames()?;
            if all_frames.is_empty() {
                i += 1;
                continue;
            }

            let segment_total_bytes: u64 = all_frames.iter().map(|f| f.encoded_size() as u64).sum();
            let mut kept_frames = Vec::with_capacity(all_frames.len());
            let mut discarded_count = 0;
            let mut discarded_bytes = 0u64;

            for frame in all_frames {
                if frame.is_control_marker() {
                    kept_frames.push(frame);
                    continue;
                }
                let key = extract_key(&frame.payload);
                let keep = match latest_offsets.get(key) {
                    Some(&latest_off) => frame.offset == latest_off,
                    // Either the key was never ambiguous-fallback-eligible for dedup (kept,
                    // matching pre-existing behavior), or it was purged as an expired
                    // tombstone (discarded, including this frame).
                    None => !purged_keys.contains(key),
                };
                if keep {
                    kept_frames.push(frame);
                } else {
                    discarded_bytes += frame.encoded_size() as u64;
                    discarded_count += 1;
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

            if kept_frames.is_empty() {
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

                for (idx, frame) in kept_frames.iter().enumerate() {
                    let mut encoded = Vec::with_capacity(frame.encoded_size());
                    frame.encode_into(&mut encoded);
                    let phys_pos = tmp_log.append_bytes(&encoded)?;

                    if idx == 0 || bytes_since_last_index >= self.config.index_interval_bytes {
                        tmp_index.append(frame.offset, phys_pos)?;
                        tmp_timeindex.append(frame.timestamp, frame.offset)?;
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

            let _ = fs::remove_file(&log_path);
            let _ = fs::remove_file(&index_path);
            let _ = fs::remove_file(&time_index_path);
            let _ = fs::remove_file(&txn_index_path);

            fs::rename(&tmp_log_path, &log_path)?;
            fs::rename(&tmp_index_path, &index_path)?;
            fs::rename(&tmp_timeindex_path, &time_index_path)?;
            fs::rename(&tmp_txnindex_path, &txn_index_path)?;

            let mut new_index = IndexSegment::open(&index_path, base_offset)?;
            let new_timeindex = TimeIndexSegment::open(&time_index_path, base_offset)?;
            let new_txnindex = TxnIndexSegment::open(&txn_index_path)?;
            let new_log = LogSegment::open(
                &self.dir,
                base_offset,
                self.config.max_segment_bytes,
                self.config.index_interval_bytes,
                false,
                &mut new_index,
            )?;

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
            let file_age_ms = pair.log.modified_time_ms().unwrap_or(0);

            let mut remove = false;

            if let Some(max_age_ms) = retention_millis {
                if now_ms > file_age_ms && (now_ms - file_age_ms) > max_age_ms {
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
                removed_count += 1;
            } else {
                i += 1;
            }
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
            removed_count += 1;
            // Don't advance `i` — the next segment shifted down to this index.
        }
        Ok(removed_count)
    }

    /// Read records starting at target timestamp (BUG-02)
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
    pub fn find_offset_for_timestamp(&mut self, target_timestamp: u64) -> u64 {
        for pair in self.historical.iter().rev() {
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

        self.active.base_offset
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

    fn compact_config(delete_retention_millis: Option<u64>, min_cleanable_dirty_ratio: f64) -> EngineConfig {
        EngineConfig {
            cleanup_policy: crate::config::CleanupPolicy::Compact,
            delete_retention_millis,
            min_cleanable_dirty_ratio,
            ..EngineConfig::default()
        }
    }

    #[test]
    fn compact_segments_keeps_unexpired_tombstone_as_latest_record() {
        let dir = TempDir::new("tombstone_kept");
        // min_cleanable_dirty_ratio: 0.0 so this test only exercises tombstone semantics,
        // not the separate dirty-ratio gate.
        let mut mgr = SegmentManager::open(&dir.0, compact_config(Some(24 * 60 * 60 * 1000), 0.0)).unwrap();

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        mgr.append(b"user1:val_1", 1000).unwrap(); // offset 0, superseded below (timestamp irrelevant — not the latest)
        mgr.append(b"user1:", now_ms).unwrap(); // offset 1 — empty value = tombstone, written "now" so it's well within the 24h grace period
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 1, "the stale user1:val_1 record should be dropped");

        let remaining = mgr.fetch(0, 4096).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].offset, 1);
        assert_eq!(remaining[0].payload.as_ref(), b"user1:");
    }

    #[test]
    fn compact_segments_purges_key_once_tombstone_exceeds_delete_retention() {
        let dir = TempDir::new("tombstone_purged");
        // 1-second grace period; both records are written with a far-in-the-past
        // timestamp so the tombstone is unambiguously expired regardless of how fast the
        // test itself runs.
        let mut mgr = SegmentManager::open(&dir.0, compact_config(Some(1000), 0.0)).unwrap();

        mgr.append(b"keyA:val1", 1).unwrap(); // offset 0 — stale, superseded by offset 1
        mgr.append(b"keyA:val2", 1).unwrap(); // offset 1 — current value for keyA, kept
        mgr.append(b"keyB:", 1).unwrap(); // offset 2 — expired tombstone, keyB fully purged
        mgr.rotate_segment().unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 2, "offset 0 (stale) and offset 2 (expired tombstone) should be dropped");

        let remaining = mgr.fetch(0, 4096).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].offset, 1);
        assert_eq!(remaining[0].payload.as_ref(), b"keyA:val2");
    }

    #[test]
    fn compact_segments_skips_segment_below_min_cleanable_dirty_ratio() {
        let dir = TempDir::new("dirty_ratio_gate");
        let mut mgr = SegmentManager::open(&dir.0, compact_config(None, 0.5)).unwrap();

        // 10 same-size records (offsets 0..9) in one historical segment.
        for i in 0..10u64 {
            mgr.append(format!("k{}:v0", i).as_bytes(), i).unwrap();
        }
        mgr.rotate_segment().unwrap();
        // Supersede k0 from the active segment — exactly 1 of the 10 historical frames
        // (10%) becomes stale, well below the 50% gate.
        mgr.append(b"k0:v1", 100).unwrap();

        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 0, "10% dirty is below the 50% min_cleanable_dirty_ratio gate");
        assert_eq!(mgr.historical[0].read_all_frames().unwrap().len(), 10);

        // Lowering the gate below the segment's actual dirty ratio lets it compact.
        mgr.set_min_cleanable_dirty_ratio(0.05);
        let n = mgr.apply_retention().unwrap();
        assert_eq!(n, 1, "10% dirty now clears the lowered 5% gate");
        assert_eq!(mgr.historical[0].read_all_frames().unwrap().len(), 9);
    }
}
