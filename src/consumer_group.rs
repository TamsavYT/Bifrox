use bytes::BufMut;
use crc32fast::Hasher;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

pub const CONSUMER_OFFSETS_MAGIC: u8 = 0xCF;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetEntry {
    pub offset: u64,
    pub metadata: String,
}

/// Server-side Consumer Group Offset Manager with disk persistence
#[derive(Debug, Clone)]
pub struct ConsumerGroupManager {
    /// In-memory state: (group_id, topic, partition) -> committed OffsetEntry
    offsets: Arc<DashMap<(String, String, u32), OffsetEntry>>,
    log_file: Arc<Mutex<File>>,
    log_path: Arc<std::path::PathBuf>,
}

impl ConsumerGroupManager {
    /// Opens or creates the `__consumer_offsets.log` persistent system log and recovers committed state
    pub fn open(data_dir: impl AsRef<Path>) -> IoResult<Self> {
        let dir = data_dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let log_path = dir.join("__consumer_offsets.log");

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(windows)]
        options.share_mode(7); // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE

        let mut file = options.open(&log_path)?;
        let offsets = Arc::new(DashMap::new());
        let log_path_arc = Arc::new(log_path);

        // Recover state from existing system log
        file.seek(SeekFrom::Start(0))?;
        let raw_len = file.metadata()?.len();
        if raw_len > 0 {
            let mut buf = Vec::new();
            let mut chunk = vec![0u8; 64 * 1024];
            let mut last_good_pos = 0u64;
            let mut is_corrupt = false;

            loop {
                let n = file.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);

                let mut cursor = 0usize;
                while cursor < buf.len() {
                    let mut temp_cursor = cursor;
                    
                    if buf[temp_cursor] != CONSUMER_OFFSETS_MAGIC {
                        is_corrupt = true;
                        break;
                    }
                    temp_cursor += 1;

                    if temp_cursor + 2 > buf.len() { break; }
                    let group_len = u16::from_be_bytes(buf[temp_cursor..temp_cursor + 2].try_into().unwrap()) as usize;
                    temp_cursor += 2;

                    if temp_cursor + group_len > buf.len() { break; }
                    let group_id = match String::from_utf8(buf[temp_cursor..temp_cursor + group_len].to_vec()) {
                        Ok(s) => s,
                        Err(_) => { is_corrupt = true; break; }
                    };
                    temp_cursor += group_len;

                    if temp_cursor + 2 > buf.len() { break; }
                    let topic_len = u16::from_be_bytes(buf[temp_cursor..temp_cursor + 2].try_into().unwrap()) as usize;
                    temp_cursor += 2;

                    if temp_cursor + topic_len > buf.len() { break; }
                    let topic = match String::from_utf8(buf[temp_cursor..temp_cursor + topic_len].to_vec()) {
                        Ok(s) => s,
                        Err(_) => { is_corrupt = true; break; }
                    };
                    temp_cursor += topic_len;

                    if temp_cursor + 12 > buf.len() { break; }
                    let partition = u32::from_be_bytes(buf[temp_cursor..temp_cursor + 4].try_into().unwrap());
                    temp_cursor += 4;
                    let offset = u64::from_be_bytes(buf[temp_cursor..temp_cursor + 8].try_into().unwrap());
                    temp_cursor += 8;

                    if temp_cursor + 2 > buf.len() { break; }
                    let meta_len = u16::from_be_bytes(buf[temp_cursor..temp_cursor + 2].try_into().unwrap()) as usize;
                    temp_cursor += 2;

                    if temp_cursor + meta_len > buf.len() { break; }
                    let metadata = match String::from_utf8(buf[temp_cursor..temp_cursor + meta_len].to_vec()) {
                        Ok(s) => s,
                        Err(_) => { is_corrupt = true; break; }
                    };
                    temp_cursor += meta_len;

                    if temp_cursor + 4 > buf.len() { break; }
                    let crc = u32::from_be_bytes(buf[temp_cursor..temp_cursor + 4].try_into().unwrap());
                    temp_cursor += 4;

                    let computed_crc = Self::compute_crc(&group_id, &topic, partition, offset, &metadata);
                    if crc == computed_crc {
                        offsets.insert((group_id, topic, partition), OffsetEntry { offset, metadata });
                        cursor = temp_cursor;
                    } else {
                        tracing::warn!("Corrupt consumer offset entry encountered during recovery.");
                        is_corrupt = true;
                        break;
                    }
                }

                if cursor > 0 {
                    buf.drain(..cursor);
                    last_good_pos += cursor as u64;
                }

                if is_corrupt {
                    break;
                }
            }

            if last_good_pos < raw_len {
                tracing::warn!("Truncating __consumer_offsets.log from {} to last valid position {}", raw_len, last_good_pos);
                file.set_len(last_good_pos)?;
            }
            file.seek(SeekFrom::Start(last_good_pos))?;
        } else {
            file.seek(SeekFrom::End(0))?;
        }

        Ok(Self {
            offsets,
            log_file: Arc::new(Mutex::new(file)),
            log_path: log_path_arc,
        })
    }

    /// Commits a consumer group offset to both RAM state and physical system log
    pub fn commit_offset(&self, group_id: &str, topic: &str, partition: u32, offset: u64) -> IoResult<()> {
        self.commit_offset_with_metadata(group_id, topic, partition, offset, "")
    }

    /// Commits a consumer group offset with metadata to both RAM state and physical system log
    pub fn commit_offset_with_metadata(&self, group_id: &str, topic: &str, partition: u32, offset: u64, metadata: &str) -> IoResult<()> {
        let key = (group_id.to_string(), topic.to_string(), partition);
        let entry_obj = OffsetEntry {
            offset,
            metadata: metadata.to_string(),
        };
        self.offsets.insert(key, entry_obj);

        // Serialize offset frame to disk
        let mut entry = Vec::new();
        entry.put_u8(CONSUMER_OFFSETS_MAGIC);

        let group_bytes = group_id.as_bytes();
        entry.put_u16(group_bytes.len() as u16);
        entry.put_slice(group_bytes);

        let topic_bytes = topic.as_bytes();
        entry.put_u16(topic_bytes.len() as u16);
        entry.put_slice(topic_bytes);

        entry.put_u32(partition);
        entry.put_u64(offset);

        let meta_bytes = metadata.as_bytes();
        entry.put_u16(meta_bytes.len() as u16);
        entry.put_slice(meta_bytes);

        let crc = Self::compute_crc(group_id, topic, partition, offset, metadata);
        entry.put_u32(crc);

        let mut lock = self.log_file.lock();
        lock.write_all(&entry)?;
        lock.sync_data()?;

        // Trigger log compaction if log file size exceeds 1MB (BUG-09)
        if lock.metadata()?.len() > 1024 * 1024 {
            drop(lock);
            let _ = self.compact_log();
        }

        Ok(())
    }

    /// Compacts __consumer_offsets.log by rewriting file with only latest offset per key (BUG-09)
    pub fn compact_log(&self) -> IoResult<()> {
        let mut entry_bytes = Vec::new();
        for item in self.offsets.iter() {
            let (group_id, topic, partition) = item.key();
            let entry_obj = item.value();

            entry_bytes.put_u8(CONSUMER_OFFSETS_MAGIC);
            let g_bytes = group_id.as_bytes();
            entry_bytes.put_u16(g_bytes.len() as u16);
            entry_bytes.put_slice(g_bytes);

            let t_bytes = topic.as_bytes();
            entry_bytes.put_u16(t_bytes.len() as u16);
            entry_bytes.put_slice(t_bytes);

            entry_bytes.put_u32(*partition);
            entry_bytes.put_u64(entry_obj.offset);

            let m_bytes = entry_obj.metadata.as_bytes();
            entry_bytes.put_u16(m_bytes.len() as u16);
            entry_bytes.put_slice(m_bytes);

            let crc = Self::compute_crc(group_id, topic, *partition, entry_obj.offset, &entry_obj.metadata);
            entry_bytes.put_u32(crc);
        }

        let tmp_path = self.log_path.with_extension("log.tmp");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(windows)]
        options.share_mode(7);
        
        let mut tmp_file = options.open(&tmp_path)?;
        tmp_file.write_all(&entry_bytes)?;
        tmp_file.sync_data()?;

        let mut lock = self.log_file.lock();
        
        std::fs::rename(&tmp_path, &*self.log_path)?;
        
        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create(true);
        #[cfg(windows)]
        open_opts.share_mode(7);
        
        let mut new_file = open_opts.open(&*self.log_path)?;
        new_file.seek(SeekFrom::End(0))?;
        *lock = new_file;
        
        Ok(())
    }

    /// Fetches the last committed offset for a consumer group
    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: u32) -> Option<u64> {
        let key = (group_id.to_string(), topic.to_string(), partition);
        self.offsets.get(&key).map(|v| v.value().offset)
    }

    /// Fetches the last committed offset with metadata for a consumer group
    pub fn fetch_offset_with_metadata(&self, group_id: &str, topic: &str, partition: u32) -> Option<OffsetEntry> {
        let key = (group_id.to_string(), topic.to_string(), partition);
        self.offsets.get(&key).map(|v| v.value().clone())
    }

    fn compute_crc(group_id: &str, topic: &str, partition: u32, offset: u64, metadata: &str) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(group_id.as_bytes());
        hasher.update(topic.as_bytes());
        hasher.update(&partition.to_be_bytes());
        hasher.update(&offset.to_be_bytes());
        hasher.update(metadata.as_bytes());
        hasher.finalize()
    }
}
