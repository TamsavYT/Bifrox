use std::fs::OpenOptions;
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

pub const TXN_INDEX_ENTRY_SIZE: usize = 24; // 8b producer_id + 8b first_offset + 8b last_offset

/// Aborted Transaction Index entry recording an aborted transaction range for a producer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnIndexEntry {
    pub producer_id: u64,
    pub first_offset: u64,
    pub last_offset: u64,
}

impl TxnIndexEntry {
    pub fn encode(&self) -> [u8; TXN_INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; TXN_INDEX_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.producer_id.to_be_bytes());
        buf[8..16].copy_from_slice(&self.first_offset.to_be_bytes());
        buf[16..24].copy_from_slice(&self.last_offset.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8; TXN_INDEX_ENTRY_SIZE]) -> Self {
        let producer_id = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let first_offset = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        let last_offset = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        Self {
            producer_id,
            first_offset,
            last_offset,
        }
    }
}

/// Persistent Aborted Transaction Index file manager (.txnindex)
#[derive(Debug)]
pub struct TxnIndexSegment {
    path: PathBuf,
    file: std::fs::File,
    entries: Vec<TxnIndexEntry>,
}

impl TxnIndexSegment {
    pub fn open(path: impl AsRef<Path>) -> IoResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(windows)]
        options.share_mode(7); // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE

        let mut file = options.open(&path)?;

        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len() as usize;
        let count = file_len / TXN_INDEX_ENTRY_SIZE;

        let mut entries = Vec::with_capacity(count);
        let mut raw_buf = vec![0u8; file_len];
        if file_len > 0 {
            file.read_exact(&mut raw_buf)?;
            for chunk in raw_buf.as_chunks::<TXN_INDEX_ENTRY_SIZE>().0 {
                entries.push(TxnIndexEntry::decode(chunk));
            }
        }

        Ok(Self {
            path,
            file,
            entries,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn entries(&self) -> &[TxnIndexEntry] {
        &self.entries
    }

    pub fn append(
        &mut self,
        producer_id: u64,
        first_offset: u64,
        last_offset: u64,
    ) -> IoResult<()> {
        let entry = TxnIndexEntry {
            producer_id,
            first_offset,
            last_offset,
        };
        let encoded = entry.encode();
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&encoded)?;
        self.file.sync_data()?;
        self.entries.push(entry);
        Ok(())
    }

    /// True when this segment records no aborted transaction ranges at all. Lets callers
    /// skip transaction-aware filtering entirely for partitions that never had one.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn is_aborted(&self, offset: u64) -> bool {
        self.entries
            .iter()
            .any(|e| offset >= e.first_offset && offset <= e.last_offset)
    }

    pub fn sync(&mut self) -> IoResult<()> {
        self.file.sync_data()
    }

    /// Drops aborted-txn ranges starting at or beyond `offset` (Raft-style conflict truncation).
    pub fn truncate_after(&mut self, offset: u64) -> IoResult<()> {
        let kept: Vec<TxnIndexEntry> = self
            .entries
            .iter()
            .copied()
            .filter(|e| e.first_offset < offset)
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
}
