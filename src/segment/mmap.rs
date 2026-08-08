use crate::protocol::{RecordFrame, HEADER_SIZE};
use memmap2::Mmap;
use std::fs::File;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

/// Memory-Mapped Log Segment providing zero-copy slice access over OS page cache
#[derive(Debug)]
pub struct MmapLogSegment {
    pub base_offset: u64,
    pub path: PathBuf,
    mmap: Mmap,
    len: usize,
}

impl MmapLogSegment {
    /// Memory-maps a log segment file for zero-copy read operations
    pub fn open(path: impl AsRef<Path>, base_offset: u64) -> IoResult<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
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

    /// Fast zero-copy decode of record frames directly from mmap memory slice
    pub fn fetch_zero_copy(&self, start_pos: u64, start_offset: u64, max_bytes: usize) -> Vec<RecordFrame> {
        let mut frames = Vec::new();
        if let Some(slice) = self.get_slice(start_pos, max_bytes) {
            let mut cursor = 0usize;
            while cursor < slice.len() {
                if cursor + HEADER_SIZE > slice.len() {
                    break;
                }
                match RecordFrame::decode(&slice[cursor..]) {
                    Ok((frame, consumed)) => {
                        cursor += consumed;
                        if frame.offset >= start_offset {
                            frames.push(frame);
                        }
                    }
                    Err(_) => break,
                }
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
