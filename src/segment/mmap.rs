use crate::protocol::BATCH_MAGIC_BYTE;
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

    /// Returns the mapped bytes for `max_bytes` starting at `start_pos`, widening to the
    /// rest of the mapping when the first entry is a batch that would otherwise be cut
    /// short.
    ///
    /// Handing back raw bytes rather than decoded records is what keeps this path
    /// zero-copy: the whole segment is already resident, so widening costs nothing, and
    /// the caller decodes once instead of this doing it a second time.
    pub fn raw_from(&self, start_pos: u64, max_bytes: usize) -> &[u8] {
        let Some(mut slice) = self.get_slice(start_pos, max_bytes) else {
            return &[];
        };
        if slice.first() == Some(&BATCH_MAGIC_BYTE) {
            if let Some(whole) = self.get_slice(start_pos, self.len) {
                slice = whole;
            }
        }
        slice
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
