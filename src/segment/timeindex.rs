use std::fs::OpenOptions;
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

pub const TIME_INDEX_ENTRY_SIZE: usize = 16; // 8b timestamp + 8b logical offset

/// Time Index Entry mapping Timestamp -> Logical Offset in log stream
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeIndexEntry {
    pub timestamp: u64,
    pub logical_offset: u64,
}

impl TimeIndexEntry {
    pub fn encode(&self) -> [u8; TIME_INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; TIME_INDEX_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.timestamp.to_be_bytes());
        buf[8..16].copy_from_slice(&self.logical_offset.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8; TIME_INDEX_ENTRY_SIZE]) -> Self {
        let timestamp = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let logical_offset = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        Self {
            timestamp,
            logical_offset,
        }
    }
}

/// Sparse Time Index manager mapping Timestamp -> Logical Offset for point-in-time time-travel seeking
#[derive(Debug)]
pub struct TimeIndexSegment {
    path: PathBuf,
    file: std::fs::File,
    entries: Vec<TimeIndexEntry>,
    base_offset: u64,
}

impl TimeIndexSegment {
    pub fn open(path: impl AsRef<Path>, base_offset: u64) -> IoResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(windows)]
        options.share_mode(7); // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE

        let mut file = options.open(&path)?;

        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len() as usize;
        let count = file_len / TIME_INDEX_ENTRY_SIZE;

        let mut entries = Vec::with_capacity(count);
        let mut raw_buf = vec![0u8; file_len];
        if file_len > 0 {
            file.read_exact(&mut raw_buf)?;
            for chunk in raw_buf.as_chunks::<TIME_INDEX_ENTRY_SIZE>().0 {
                entries.push(TimeIndexEntry::decode(chunk));
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

    /// Appends an entry, forcing the timestamp column to be non-decreasing.
    ///
    /// The stored timestamp is `max(previous_timestamp, timestamp)` rather than the
    /// record's own value. Lookups binary-search this file, and a binary search over an
    /// unsorted sequence returns an arbitrary position — so a single out-of-order record
    /// made `find_offset_for_timestamp` return an offset unrelated to the requested time,
    /// sometimes *past* it, silently skipping records the caller asked for.
    ///
    /// Out-of-order timestamps are ordinary, not exceptional: producer clocks drift,
    /// batches arrive late, and retries re-send older records. Clamping keeps the index
    /// searchable while still bounding the answer correctly — the entry for a clamped
    /// record points at an offset no later than the record itself, so a search lands at or
    /// before the true position and the caller scans forward from there.
    pub fn append(&mut self, timestamp: u64, logical_offset: u64) -> IoResult<()> {
        let monotonic_timestamp = match self.entries.last() {
            Some(last) => timestamp.max(last.timestamp),
            None => timestamp,
        };
        let entry = TimeIndexEntry {
            timestamp: monotonic_timestamp,
            logical_offset,
        };
        let encoded = entry.encode();
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&encoded)?;
        self.entries.push(entry);
        Ok(())
    }

    pub fn sync(&mut self) -> IoResult<()> {
        self.file.sync_data()
    }

    /// Drops all entries pointing at or beyond `offset` (Raft-style conflict truncation).
    pub fn truncate_after(&mut self, offset: u64) -> IoResult<()> {
        let kept: Vec<TimeIndexEntry> = self
            .entries
            .iter()
            .copied()
            .filter(|e| e.logical_offset < offset)
            .collect();
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.entries.clear();
        for entry in kept {
            let encoded = entry.encode();
            self.file.write_all(&encoded)?;
            self.entries.push(entry);
        }
        self.file.sync_data()
    }

    /// Largest timestamp recorded in this segment's sparse time index — a cheap O(index
    /// size), not O(segment size), stand-in for "the newest record in this segment", used
    /// by time-based retention instead of the filesystem's mtime (which backup tools,
    /// replica resync, or a plain `touch` can change without touching any record).
    pub fn max_timestamp(&self) -> Option<u64> {
        self.entries.iter().map(|e| e.timestamp).max()
    }

    /// Binary search ($O(\log N)$) for nearest logical offset corresponding to target timestamp
    pub fn find_offset_for_timestamp(&self, target_time_ms: u64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        match self
            .entries
            .binary_search_by_key(&target_time_ms, |e| e.timestamp)
        {
            Ok(idx) => Some(self.entries[idx].logical_offset),
            Err(idx) => {
                if idx == 0 {
                    None // target is before this segment's range
                } else {
                    Some(self.entries[idx - 1].logical_offset)
                }
            }
        }
    }
}
