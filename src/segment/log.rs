use crate::protocol::{FrameError, RecordFrame, HEADER_SIZE};
use crate::segment::index::{IndexEntry, IndexSegment};
use std::fs::OpenOptions;
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

/// Formats a base offset into a 20-digit zero-padded filename string (e.g. `00000000000000000000`)
pub fn format_segment_filename(base_offset: u64) -> String {
    format!("{:020}", base_offset)
}

/// Active or historical log segment file
#[derive(Debug)]
pub struct LogSegment {
    pub base_offset: u64,
    pub next_offset: u64,
    pub path: PathBuf,
    pub file: std::fs::File,
    pub physical_size: u64, // Actual valid byte offset in log file
    pub is_preallocated: bool,
}

impl LogSegment {
    /// Opens or creates a segment file, performs CRC recovery on startup, truncates partial writes,
    /// and rebuilds missing/corrupt index entries. Handles Windows NTFS file sharing gracefully.
    pub fn open(
        dir: impl AsRef<Path>,
        base_offset: u64,
        max_bytes: u64,
        index_interval: u64,
        preallocate: bool,
        index_segment: &mut IndexSegment,
    ) -> IoResult<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let filename = format_segment_filename(base_offset);
        let log_path = dir.join(format!("{}.log", filename));

        let exists = log_path.exists();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(windows)]
        options.share_mode(7); // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE

        let mut file = options.open(&log_path)?;

        let mut physical_size = 0u64;
        let mut next_offset = base_offset;
        let mut rebuilt_index_entries = Vec::new();
        let mut bytes_since_last_index = 0u64;
        let mut encountered_corruption = false;

        if exists {
            // Perform streaming recovery scan & CRC verification on startup (BUG-08)
            file.seek(SeekFrom::Start(0))?;
            let raw_len = file.metadata()?.len();

            let mut chunk_buf = vec![0u8; 64 * 1024];
            let mut read_buf = Vec::new();
            let mut file_offset = 0u64;

            loop {
                let n = file.read(&mut chunk_buf)?;
                if n == 0 && read_buf.is_empty() {
                    break;
                }
                read_buf.extend_from_slice(&chunk_buf[..n]);

                let mut pos = 0usize;
                let buf_len = read_buf.len();

                while pos < buf_len {
                    if pos + HEADER_SIZE > buf_len {
                        let remaining = &read_buf[pos..];
                        if remaining.iter().all(|&b| b == 0) {
                            break;
                        }
                        if n > 0 {
                            break; // need more data from file
                        } else {
                            tracing::warn!(
                                "Incomplete frame header at byte position {} in segment {}. Truncating.",
                                file_offset + pos as u64,
                                base_offset
                            );
                            encountered_corruption = true;
                            break;
                        }
                    }

                    let slice = &read_buf[pos..];
                    if slice[0] == 0
                        && slice[1..std::cmp::min(HEADER_SIZE, slice.len())]
                            .iter()
                            .all(|&b| b == 0)
                    {
                        break;
                    }

                    match RecordFrame::decode(slice) {
                        Ok((frame, frame_len)) => {
                            let phys_pos = file_offset + pos as u64;
                            if bytes_since_last_index >= index_interval
                                || rebuilt_index_entries.is_empty()
                            {
                                rebuilt_index_entries.push(IndexEntry {
                                    relative_offset: (frame.offset.saturating_sub(base_offset))
                                        as u32,
                                    physical_position: phys_pos as u32,
                                });
                                bytes_since_last_index = 0;
                            }

                            bytes_since_last_index += frame_len as u64;
                            pos += frame_len;
                            next_offset = frame.offset + 1;
                        }
                        Err(FrameError::BufferTooShort { .. }) => {
                            if n > 0 {
                                break; // need more data
                            } else {
                                let remaining = &read_buf[pos..];
                                if remaining.iter().all(|&b| b == 0) {
                                    break;
                                }
                                tracing::warn!(
                                    "Partial payload at byte position {} in segment {}. Truncating.",
                                    file_offset + pos as u64,
                                    base_offset
                                );
                                encountered_corruption = true;
                                break;
                            }
                        }
                        Err(err) => {
                            let remaining = &read_buf[pos..];
                            if remaining.iter().all(|&b| b == 0) {
                                break;
                            }
                            tracing::error!(
                                "Corrupt frame detected at position {} in segment {}: {}. Truncating log.",
                                file_offset + pos as u64,
                                base_offset,
                                err
                            );
                            encountered_corruption = true;
                            break;
                        }
                    }
                }

                file_offset += pos as u64;
                read_buf.drain(..pos);

                if n == 0 || encountered_corruption {
                    break;
                }
            }

            physical_size = file_offset;

            // Only truncate if actual corruption occurred (not clean pre-allocated zeros)
            if encountered_corruption && physical_size != raw_len {
                file.set_len(physical_size)?;
                file.sync_data()?;
            }

            // Sync rebuilt index segment (CORR-01)
            index_segment.truncate_and_rebuild(rebuilt_index_entries)?;
            index_segment.sync()?;
        } else if preallocate {
            // Clean space pre-allocation on new file creation to prevent NTFS fragmentation
            file.set_len(max_bytes)?;
            file.sync_data()?;
        }

        file.seek(SeekFrom::Start(physical_size))?;

        Ok(Self {
            base_offset,
            next_offset,
            path: log_path,
            file,
            physical_size,
            is_preallocated: preallocate && !exists,
        })
    }

    /// Append raw record frame bytes to log file
    pub fn append_bytes(&mut self, bytes: &[u8]) -> IoResult<u64> {
        let written_pos = self.physical_size;
        self.file.seek(SeekFrom::Start(written_pos))?;
        self.file.write_all(bytes)?;
        self.physical_size += bytes.len() as u64;
        Ok(written_pos)
    }

    /// Reads raw frame bytes starting at a physical byte position up to max_bytes
    pub fn read_at(&mut self, physical_pos: u64, max_bytes: usize) -> IoResult<Vec<u8>> {
        if physical_pos >= self.physical_size {
            return Ok(Vec::new());
        }

        self.file.seek(SeekFrom::Start(physical_pos))?;
        let remaining = (self.physical_size - physical_pos) as usize;
        let read_len = std::cmp::min(remaining, max_bytes);

        let mut buf = vec![0u8; read_len];
        let bytes_read = self.file.read(&mut buf)?;
        buf.truncate(bytes_read);
        Ok(buf)
    }

    /// Flushes log segment to physical disk
    pub fn sync(&mut self) -> IoResult<()> {
        self.file.sync_data()
    }

    /// Finalizes segment upon rotation by trimming any unused preallocated trailing space
    pub fn finalize(&mut self) -> IoResult<()> {
        self.file.set_len(self.physical_size)?;
        self.file.sync_data()
    }

    /// Truncates the log file to exactly `physical_size` bytes (Raft-style conflict
    /// truncation: discards a diverging suffix before appending the leader's entries).
    pub fn truncate_to(&mut self, physical_size: u64) -> IoResult<()> {
        self.file.set_len(physical_size)?;
        self.file.sync_data()?;
        self.physical_size = physical_size;
        Ok(())
    }

    /// Returns last modified timestamp in milliseconds since Unix epoch
    pub fn modified_time_ms(&self) -> IoResult<u64> {
        let metadata = self.file.metadata()?;
        let modified = metadata.modified()?;
        Ok(modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64)
    }

    /// Windows Kernel Zero-Copy (Tier 3): Streams bytes directly from OS NTFS page cache to Winsock socket via TransmitFile
    #[cfg(windows)]
    pub async fn transmit_file_zero_copy(
        &self,
        socket: &tokio::net::TcpStream,
        physical_pos: u64,
        max_bytes: u32,
    ) -> IoResult<()> {
        use std::os::windows::io::{AsRawHandle, AsRawSocket};
        use windows_sys::Win32::Networking::WinSock::TransmitFile;
        use windows_sys::Win32::System::IO::OVERLAPPED;

        let raw_socket = socket.as_raw_socket() as usize;
        let raw_file_handle = self.file.as_raw_handle() as isize;

        let remaining = if self.physical_size > physical_pos {
            (self.physical_size - physical_pos) as u32
        } else {
            0
        };

        let bytes_to_send = std::cmp::min(remaining, max_bytes);
        if bytes_to_send == 0 {
            return Ok(());
        }

        tokio::task::spawn_blocking(move || unsafe {
            let mut overlapped: OVERLAPPED = std::mem::zeroed();
            overlapped.Anonymous.Pointer = physical_pos as *mut _;

            let success = TransmitFile(
                raw_socket,
                raw_file_handle,
                bytes_to_send,
                0,
                &mut overlapped as *mut _ as *mut _,
                std::ptr::null_mut(),
                0,
            );

            if success == 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
        .await
        .map_err(std::io::Error::other)?
    }
}
