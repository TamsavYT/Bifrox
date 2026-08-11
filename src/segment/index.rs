use std::fs::OpenOptions;
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

pub const INDEX_ENTRY_SIZE: usize = 8; // 4b relative offset + 4b physical pos

/// Sparse Index Entry mapping Relative Offset -> Physical Byte Offset in log file
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub relative_offset: u32,
    pub physical_position: u32,
}

impl IndexEntry {
    pub fn encode(&self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        buf[0..4].copy_from_slice(&self.relative_offset.to_be_bytes());
        buf[4..8].copy_from_slice(&self.physical_position.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8; INDEX_ENTRY_SIZE]) -> Self {
        let relative_offset = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let physical_position = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        Self {
            relative_offset,
            physical_position,
        }
    }
}

/// Sparse Index file manager supporting $O(\log N)$ binary search seek operations
#[derive(Debug)]
pub struct IndexSegment {
    path: PathBuf,
    file: std::fs::File,
    entries: Vec<IndexEntry>,
    base_offset: u64,
}

impl IndexSegment {
    pub fn open(path: impl AsRef<Path>, base_offset: u64) -> IoResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(windows)]
        options.share_mode(7); // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE

        let mut file = options.open(&path)?;

        // Load existing index entries into memory for blazing fast O(log N) binary search
        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len() as usize;
        let count = file_len / INDEX_ENTRY_SIZE;

        let mut entries = Vec::with_capacity(count);
        let mut raw_buf = vec![0u8; file_len];
        if file_len > 0 {
            file.read_exact(&mut raw_buf)?;
            for chunk in raw_buf.chunks_exact(INDEX_ENTRY_SIZE) {
                let chunk_arr: &[u8; INDEX_ENTRY_SIZE] = chunk.try_into().unwrap();
                entries.push(IndexEntry::decode(chunk_arr));
            }
        }

        Ok(Self {
            path,
            file,
            entries,
            base_offset,
        })
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn entries_count(&self) -> usize {
        self.entries.len()
    }

    /// Append a new index entry to both RAM vector and physical disk
    pub fn append(&mut self, logical_offset: u64, physical_position: u64) -> IoResult<()> {
        let relative_offset = logical_offset.saturating_sub(self.base_offset) as u32;
        let entry = IndexEntry {
            relative_offset,
            physical_position: physical_position as u32,
        };
        let encoded = entry.encode();
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&encoded)?;
        self.entries.push(entry);
        Ok(())
    }

    /// Flushes physical index file to disk
    pub fn sync(&mut self) -> IoResult<()> {
        self.file.sync_data()
    }

    /// Binary search seek ($O(\log N)$) for target logical offset.
    /// Returns the nearest IndexEntry where entry.logical_offset <= target_offset.
    pub fn find_nearest_physical_pos(&self, target_offset: u64) -> Option<IndexEntry> {
        if self.entries.is_empty() {
            return None;
        }

        let rel_target = target_offset.saturating_sub(self.base_offset) as u32;

        match self
            .entries
            .binary_search_by_key(&rel_target, |e| e.relative_offset)
        {
            Ok(idx) => Some(self.entries[idx]),
            Err(idx) => {
                if idx == 0 {
                    // Target is before first index entry
                    Some(self.entries[0])
                } else {
                    // Nearest preceding index entry
                    Some(self.entries[idx - 1])
                }
            }
        }
    }

    /// Rebuilds index file from scratch (used during crash recovery)
    pub fn truncate_and_rebuild(&mut self, new_entries: Vec<IndexEntry>) -> IoResult<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.entries.clear();

        for entry in new_entries {
            let encoded = entry.encode();
            self.file.write_all(&encoded)?;
            self.entries.push(entry);
        }
        self.file.sync_data()?;
        Ok(())
    }
}
