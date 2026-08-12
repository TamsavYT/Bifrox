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
    if payload.len() >= 2 {
        let key_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
        if key_len > 0 && 2 + key_len <= payload.len() {
            return &payload[2..2 + key_len];
        }
    }
    if let Ok(s) = std::str::from_utf8(payload) {
        if let Some((k, _)) = s.split_once(':') {
            if !k.is_empty() {
                return k.as_bytes();
            }
        }
        if let Some((k, _)) = s.split_once('=') {
            if !k.is_empty() {
                return k.as_bytes();
            }
        }
    }
    payload
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
        self.append_with_codec(payload, timestamp, false)
    }

    /// Append record frames with optional LZ4 payload compression
    pub fn append_with_codec(
        &mut self,
        payload: &[u8],
        timestamp: u64,
        use_lz4: bool,
    ) -> IoResult<RecordFrame> {
        let assigned_offset = self.high_watermark;
        let frame = if use_lz4 {
            RecordFrame::create_compressed_lz4(assigned_offset, timestamp, payload)
        } else {
            RecordFrame::create(assigned_offset, timestamp, payload.to_vec())
        };
        let frame_size = frame.encoded_size() as u64;

        // Rotate segment if active log size exceeds configured threshold
        if self.active.log.physical_size + frame_size > self.config.max_segment_bytes {
            self.rotate_segment()?;
        }

        let mut encoded = Vec::with_capacity(frame.encoded_size());
        frame.encode_into(&mut encoded);

        let physical_pos = self.active.log.append_bytes(&encoded)?;

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

        let mut encoded = Vec::with_capacity(frame.encoded_size());
        frame.encode_into(&mut encoded);

        let physical_pos = self.active.log.append_bytes(&encoded)?;

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

    /// Log compaction garbage collector: deduplicates historical segment entries keeping only the latest offset per key
    pub fn compact_segments(&mut self) -> IoResult<usize> {
        if self.historical.is_empty() {
            return Ok(0);
        }

        // Phase 1: Build map of key -> highest (latest) offset across all historical and active segments
        let mut latest_offsets: std::collections::HashMap<Vec<u8>, u64> =
            std::collections::HashMap::new();

        for pair in &mut self.historical {
            let frames = pair.read_all_frames()?;
            for frame in frames {
                if frame.is_control_marker() {
                    continue;
                }
                let key = extract_key(&frame.payload).to_vec();
                let entry = latest_offsets.entry(key).or_insert(frame.offset);
                if frame.offset > *entry {
                    *entry = frame.offset;
                }
            }
        }

        let active_frames = self.active.read_all_frames()?;
        for frame in active_frames {
            if frame.is_control_marker() {
                continue;
            }
            let key = extract_key(&frame.payload).to_vec();
            let entry = latest_offsets.entry(key).or_insert(frame.offset);
            if frame.offset > *entry {
                *entry = frame.offset;
            }
        }

        if latest_offsets.is_empty() {
            return Ok(0);
        }

        let mut total_compacted_frames = 0;
        let mut i = 0;

        while i < self.historical.len() {
            let all_frames = self.historical[i].read_all_frames()?;
            if all_frames.is_empty() {
                i += 1;
                continue;
            }

            let mut kept_frames = Vec::with_capacity(all_frames.len());
            let mut discarded_count = 0;

            for frame in all_frames {
                if frame.is_control_marker() {
                    kept_frames.push(frame);
                } else {
                    let key = extract_key(&frame.payload);
                    if let Some(&latest_off) = latest_offsets.get(key) {
                        if frame.offset == latest_off {
                            kept_frames.push(frame);
                        } else {
                            discarded_count += 1;
                        }
                    } else {
                        kept_frames.push(frame);
                    }
                }
            }

            if discarded_count == 0 {
                i += 1;
                continue;
            }

            total_compacted_frames += discarded_count;
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

                let mut tmp_log = LogSegment::open(
                    &self.dir,
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
