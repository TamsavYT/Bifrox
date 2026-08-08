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

/// Server-side Consumer Group Offset Manager with disk persistence
#[derive(Debug, Clone)]
pub struct ConsumerGroupManager {
    /// In-memory state: (group_id, topic, partition) -> committed_offset
    offsets: Arc<DashMap<(String, String, u32), u64>>,
    log_file: Arc<Mutex<File>>,
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

        // Recover state from existing system log
        file.seek(SeekFrom::Start(0))?;
        let raw_len = file.metadata()?.len() as usize;
        if raw_len > 0 {
            let mut buf = vec![0u8; raw_len];
            file.read_exact(&mut buf)?;

            let mut cursor = 0usize;
            while cursor < raw_len {
                if buf[cursor] != CONSUMER_OFFSETS_MAGIC {
                    break;
                }
                cursor += 1; // Magic 1b

                if cursor + 2 > raw_len {
                    break;
                }
                let group_len = u16::from_be_bytes(buf[cursor..cursor + 2].try_into().unwrap()) as usize;
                cursor += 2;

                if cursor + group_len > raw_len {
                    break;
                }
                let group_id = match String::from_utf8(buf[cursor..cursor + group_len].to_vec()) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                cursor += group_len;

                if cursor + 2 > raw_len {
                    break;
                }
                let topic_len = u16::from_be_bytes(buf[cursor..cursor + 2].try_into().unwrap()) as usize;
                cursor += 2;

                if cursor + topic_len > raw_len {
                    break;
                }
                let topic = match String::from_utf8(buf[cursor..cursor + topic_len].to_vec()) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                cursor += topic_len;

                if cursor + 12 > raw_len { // 4b partition + 8b offset
                    break;
                }
                let partition = u32::from_be_bytes(buf[cursor..cursor + 4].try_into().unwrap());
                cursor += 4;
                let offset = u64::from_be_bytes(buf[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;

                if cursor + 4 > raw_len { // 4b CRC32
                    break;
                }
                let crc = u32::from_be_bytes(buf[cursor..cursor + 4].try_into().unwrap());
                cursor += 4;

                // Validate CRC32
                let computed_crc = Self::compute_crc(&group_id, &topic, partition, offset);
                if crc == computed_crc {
                    offsets.insert((group_id, topic, partition), offset);
                } else {
                    tracing::warn!("Corrupt consumer offset entry encountered during recovery.");
                    break;
                }
            }

            // Truncate trailing partial/corrupt bytes (CORR-05)
            let last_good_pos = cursor as u64;
            if (last_good_pos as usize) < raw_len {
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
        })
    }

    /// Commits a consumer group offset to both RAM state and physical system log
    pub fn commit_offset(&self, group_id: &str, topic: &str, partition: u32, offset: u64) -> IoResult<()> {
        let key = (group_id.to_string(), topic.to_string(), partition);
        self.offsets.insert(key, offset);

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

        let crc = Self::compute_crc(group_id, topic, partition, offset);
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
            let offset = *item.value();

            entry_bytes.put_u8(CONSUMER_OFFSETS_MAGIC);
            let g_bytes = group_id.as_bytes();
            entry_bytes.put_u16(g_bytes.len() as u16);
            entry_bytes.put_slice(g_bytes);

            let t_bytes = topic.as_bytes();
            entry_bytes.put_u16(t_bytes.len() as u16);
            entry_bytes.put_slice(t_bytes);

            entry_bytes.put_u32(*partition);
            entry_bytes.put_u64(offset);

            let crc = Self::compute_crc(group_id, topic, *partition, offset);
            entry_bytes.put_u32(crc);
        }

        let mut lock = self.log_file.lock();
        lock.seek(SeekFrom::Start(0))?;
        lock.set_len(0)?;
        lock.write_all(&entry_bytes)?;
        lock.sync_data()?;
        Ok(())
    }

    /// Fetches the last committed offset for a consumer group
    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: u32) -> Option<u64> {
        let key = (group_id.to_string(), topic.to_string(), partition);
        self.offsets.get(&key).map(|v| *v.value())
    }

    fn compute_crc(group_id: &str, topic: &str, partition: u32, offset: u64) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(group_id.as_bytes());
        hasher.update(topic.as_bytes());
        hasher.update(&partition.to_be_bytes());
        hasher.update(&offset.to_be_bytes());
        hasher.finalize()
    }
}
