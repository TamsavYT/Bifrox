use crate::config::EngineConfig;
use crate::protocol::{RecordFrame, HEADER_SIZE};
use crate::segment::index::IndexSegment;
use crate::segment::log::{format_segment_filename, LogSegment};
use std::fs;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

/// Segment pair holding associated log segment and index segment
#[derive(Debug)]
pub struct SegmentPair {
    pub base_offset: u64,
    pub log: LogSegment,
    pub index: IndexSegment,
    pub mmap: Option<crate::segment::mmap::MmapLogSegment>,
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
            if path.is_file() && path.extension().map_or(false, |ext| ext == "log") {
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
                mmap,
            });
        }

        let active_index_path = dir.join(format!("{}.index", format_segment_filename(active_base)));
        let mut active_index = IndexSegment::open(active_index_path, active_base)?;
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

    /// Append record frames into active segment. Performs segment rotation if size limit reached.
    pub fn append(&mut self, payload: &[u8], timestamp: u64) -> IoResult<RecordFrame> {
        let assigned_offset = self.high_watermark;
        let frame = RecordFrame::create(assigned_offset, timestamp, payload.to_vec());
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
            self.active
                .index
                .append(assigned_offset, physical_pos)?;
            self.bytes_since_last_index = 0;
        }

        self.bytes_since_last_index += frame_size;
        self.high_watermark += 1;
        self.active.log.next_offset = self.high_watermark;

        Ok(frame)
    }

    /// Append a control marker frame into active segment. Performs segment rotation if size limit reached.
    pub fn append_control_marker(&mut self, control_type: u8, producer_id: u64, transaction_id: &str, timestamp: u64) -> IoResult<RecordFrame> {
        let assigned_offset = self.high_watermark;
        let frame = RecordFrame::create_control_marker(assigned_offset, timestamp, control_type, producer_id, transaction_id);
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
            self.active
                .index
                .append(assigned_offset, physical_pos)?;
            self.bytes_since_last_index = 0;
        }

        self.bytes_since_last_index += frame_size;
        self.high_watermark += 1;
        self.active.log.next_offset = self.high_watermark;

        Ok(frame)
    }

    /// Rotate active segment to new segment file
    fn rotate_segment(&mut self) -> IoResult<()> {
        let new_base_offset = self.high_watermark;
        tracing::info!(
            "Rotating segment at offset {}. Active segment size was {} bytes.",
            new_base_offset,
            self.active.log.physical_size
        );

        self.active.log.finalize()?;
        self.active.index.sync()?;

        let new_index_path = self
            .dir
            .join(format!("{}.index", format_segment_filename(new_base_offset)));
        let mut new_index = IndexSegment::open(new_index_path, new_base_offset)?;
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
            mmap: None,
        };

        let mut old_active = std::mem::replace(&mut self.active, new_active);
        old_active.mmap = crate::segment::mmap::MmapLogSegment::open(&old_active.log.path, old_active.base_offset).ok();
        self.historical.push(old_active);
        self.bytes_since_last_index = 0;

        Ok(())
    }

    /// Read records starting at logical offset using binary search across segments and sparse index ($O(\log N)$)
    pub fn fetch(&mut self, start_offset: u64, max_bytes: usize) -> IoResult<Vec<RecordFrame>> {
        let segment_pair = self.find_segment_pair_mut(start_offset);
        let seek_entry = segment_pair.index.find_nearest_physical_pos(start_offset);
        let start_pos = seek_entry.map_or(0, |e| e.physical_position);

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
            .map(|e| (pair.base_offset, e.physical_position))
    }

    /// Garbage collector: unlinks closed segments exceeding configured size or time retention limits
    pub fn apply_retention(&mut self) -> IoResult<usize> {
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
                let total_bytes: u64 = self.historical.iter().map(|p| p.log.physical_size).sum::<u64>()
                    + self.active.log.physical_size;
                if total_bytes > max_bytes {
                    remove = true;
                }
            }

            if remove {
                let pair_to_remove = self.historical.remove(i);
                let log_path = pair_to_remove.log.path.clone();
                let index_path = pair_to_remove.index.path().to_path_buf();

                tracing::info!(
                    "Garbage Collector: Unlinking expired log segment {} and index {}",
                    log_path.display(),
                    index_path.display()
                );

                // Explicitly drop handles before removing files on Windows
                drop(pair_to_remove);

                let _ = fs::remove_file(&log_path);
                let _ = fs::remove_file(&index_path);
                removed_count += 1;
            } else {
                i += 1;
            }
        }

        Ok(removed_count)
    }

    /// Read records starting at target timestamp (BUG-02)
    pub fn fetch_by_timestamp(&mut self, target_timestamp: u64, max_bytes: usize) -> IoResult<Vec<RecordFrame>> {
        let start_offset = self.find_offset_for_timestamp(target_timestamp);
        let frames = self.fetch(start_offset, max_bytes)?;
        Ok(frames.into_iter().filter(|f| f.timestamp >= target_timestamp).collect())
    }

    /// Finds nearest base_offset for target_timestamp (PARTIAL-02 & NEW-02)
    pub fn find_offset_for_timestamp(&mut self, target_timestamp: u64) -> u64 {
        for i in 0..self.historical.len() {
            let pair = &mut self.historical[i];
            if let Ok(raw) = pair.log.read_at(0, HEADER_SIZE) {
                if let Ok((frame, _)) = RecordFrame::decode(&raw) {
                    if frame.timestamp >= target_timestamp {
                        return if i > 0 { self.historical[i - 1].base_offset } else { pair.base_offset };
                    }
                }
            }
        }
        if !self.historical.is_empty() {
            let last_idx = self.historical.len() - 1;
            let last_hist_base = self.historical[last_idx].base_offset;
            if let Ok(raw) = self.active.log.read_at(0, HEADER_SIZE) {
                if let Ok((active_first_frame, _)) = RecordFrame::decode(&raw) {
                    if active_first_frame.timestamp > target_timestamp {
                        return last_hist_base;
                    }
                }
            }
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
