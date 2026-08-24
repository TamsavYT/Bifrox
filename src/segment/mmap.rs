use crate::protocol::{RecordFrame, BATCH_MAGIC_BYTE, HEADER_SIZE};
use crate::segment::entry::{decode_entry, LogEntry};
use memmap2::Mmap;
use std::fs::OpenOptions;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

/// Memory-Mapped Log Segment providing zero-copy slice access over OS page cache
#[derive(Debug)]
pub struct MmapLogSegment {
    pub base_offset: u64,
    pub path: PathBuf,
    mmap: Mmap,
    len: usize,
}

impl MmapLogSegment {
    /// Memory-maps a log segment file for zero-copy read operations. Opened with
    /// `FILE_SHARE_DELETE` on Windows — without it, a plain `File::open` handle (Rust's
    /// default share mode there is read+write only) blocks any concurrent rename/delete of
    /// this same path, which is exactly what log compaction and segment truncation do to a
    /// historical segment's `.log` file while a live mmap might still be open over it,
    /// surfacing as a spurious `AccessDenied`/`ERROR_SHARING_VIOLATION` on Windows.
    pub fn open(path: impl AsRef<Path>, base_offset: u64) -> IoResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        options.share_mode(7); // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
        let file = options.open(&path)?;
        let metadata = file.metadata()?;
        let len = metadata.len() as usize;

        let mmap = unsafe { Mmap::map(&file)? };

        Ok(Self {
            base_offset,
            path,
            mmap,
            len,
        })
    }

    /// Returns zero-copy byte slice reference directly from memory-mapped OS page cache
    pub fn get_slice(&self, physical_pos: u64, max_bytes: usize) -> Option<&[u8]> {
        let pos = physical_pos as usize;
        if pos >= self.len {
            return None;
        }
        let available = self.len - pos;
        let read_len = std::cmp::min(available, max_bytes);
        Some(&self.mmap[pos..pos + read_len])
    }

    /// Fast zero-copy decode of record frames directly from mmap memory slice.
    ///
    /// A `RecordBatch` (magic [`BATCH_MAGIC_BYTE`]) found along the way is decoded and
    /// its records surfaced as synthetic uncompressed `RecordFrame`s, filtered to
    /// `offset >= start_offset` — same contract as `SegmentManager::fetch`, which this
    /// mirrors. Unlike the file-backed path, no extra read is ever needed to cover a
    /// batch cut short by `max_bytes`: the whole segment is already resident in `mmap`,
    /// so when the very first entry is a batch this simply widens the slice to the rest
    /// of the mapped region (free — it's already in memory) instead of re-reading.
    pub fn fetch_zero_copy(
        &self,
        start_pos: u64,
        start_offset: u64,
        max_bytes: usize,
    ) -> Vec<RecordFrame> {
        let mut frames = Vec::new();
        let Some(mut slice) = self.get_slice(start_pos, max_bytes) else {
            return frames;
        };
        if slice.first() == Some(&BATCH_MAGIC_BYTE) {
            if let Some(whole) = self.get_slice(start_pos, self.len) {
                slice = whole;
            }
        }

        let mut cursor = 0usize;
        while cursor < slice.len() {
            if cursor + HEADER_SIZE > slice.len() {
                break;
            }
            match decode_entry(&slice[cursor..]) {
                Ok((LogEntry::Frame(frame), consumed)) => {
                    cursor += consumed;
                    if frame.offset >= start_offset {
                        frames.push(frame);
                    }
                }
                Ok((LogEntry::Batch(batch), consumed)) => {
                    let Ok(records) = batch.records() else {
                        break;
                    };
                    for record in records {
                        if record.offset >= start_offset {
                            // Batch records now carry an explicit, nullable value;
                            // append_batch never writes a null one, so this unwrap
                            // preserves existing behavior (see `SegmentManager::fetch`).
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
        frames
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
