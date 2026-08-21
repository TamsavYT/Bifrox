use crate::protocol::HEADER_SIZE;
use crate::segment::entry::{decode_entry, LogEntry};
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

/// Sibling path of a `.clean` marker for a given `.log` file path — written by
/// `LogSegment::finalize()` once a segment is rotated out of the active role and never
/// appended to again, and consulted (only for historical, non-active segments) by
/// `LogSegment::open_at_path` to skip the O(N) full-segment CRC recovery scan on startup.
/// See `read_clean_marker`/`write_clean_marker` for the format.
fn clean_marker_path(log_path: &Path) -> PathBuf {
    let mut p = log_path.to_path_buf();
    p.set_extension("clean");
    p
}

/// Writes an 8-byte big-endian file length into `<log_path>.clean`, best-effort (a failure
/// here just means the next startup falls back to a full scan for this segment — never a
/// correctness issue, only a missed optimization).
fn write_clean_marker(log_path: &Path, physical_size: u64) {
    let marker_path = clean_marker_path(log_path);
    let _ = std::fs::write(&marker_path, physical_size.to_be_bytes());
}

/// Reads back the length recorded by `write_clean_marker`, if the marker file exists and
/// is well-formed.
fn read_clean_marker(log_path: &Path) -> Option<u64> {
    let bytes = std::fs::read(clean_marker_path(log_path)).ok()?;
    let arr: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
    Some(u64::from_be_bytes(arr))
}

/// Removes a segment's `.clean` marker, if any — used when a historical segment's content
/// changes out from under a previously-written marker (compaction rewriting the file,
/// Raft-style truncation) so a stale marker can never be trusted again.
pub fn remove_clean_marker(log_path: &Path) {
    let _ = std::fs::remove_file(clean_marker_path(log_path));
}

/// Cross-platform best-effort directory fsync: makes a segment rotation/truncation/
/// compaction's file creations, renames, and deletes durable against a crash, not just the
/// file contents themselves. A failure here is logged but never propagated — losing a
/// just-fsynced rename to a subsequent crash is a rare edge case, not a reason to fail the
/// operation that already succeeded on the file(s) themselves.
pub fn fsync_dir(dir: impl AsRef<Path>) {
    let dir = dir.as_ref();
    #[cfg(unix)]
    {
        match std::fs::File::open(dir) {
            Ok(f) => {
                if let Err(e) = f.sync_all() {
                    tracing::warn!(
                        "fsync_dir: failed to sync directory {}: {}",
                        dir.display(),
                        e
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "fsync_dir: failed to open directory {}: {}",
                    dir.display(),
                    e
                );
            }
        }
    }
    #[cfg(windows)]
    {
        // Directories can't be opened via `std::fs::File::open` on Windows (it lacks
        // FILE_FLAG_BACKUP_SEMANTICS), so this goes through the Win32 API directly.
        // NTFS still allows flushing a directory handle's metadata this way.
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Foundation::{
            CloseHandle, GENERIC_READ, GENERIC_WRITE, INVALID_HANDLE_VALUE,
        };
        use windows_sys::Win32::Storage::FileSystem::{
            CreateFileW, FlushFileBuffers, FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
        };
        let wide: Vec<u16> = dir
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        unsafe {
            let handle = CreateFileW(
                wide.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                std::ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                0, // HANDLE hTemplateFile = NULL
            );
            if handle == INVALID_HANDLE_VALUE {
                tracing::warn!(
                    "fsync_dir: failed to open directory handle for {}: {}",
                    dir.display(),
                    std::io::Error::last_os_error()
                );
                return;
            }
            if FlushFileBuffers(handle) == 0 {
                tracing::warn!(
                    "fsync_dir: FlushFileBuffers failed for {}: {}",
                    dir.display(),
                    std::io::Error::last_os_error()
                );
            }
            CloseHandle(handle);
        }
    }
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
        Self::open_with_trust(
            dir,
            base_offset,
            max_bytes,
            index_interval,
            preallocate,
            index_segment,
            false,
        )
    }

    /// Same as `open`, but lets the caller assert that `index_segment`'s persisted entries
    /// may be trusted without a full re-verification scan, when a `.clean` marker (see
    /// `finalize`) confirms this segment was cleanly finalized and hasn't changed size
    /// since. Only ever safe to pass `true` for a segment that will never be appended to
    /// again (i.e. a historical, already-rotated segment) — the currently-active segment
    /// must always take the full scan path, since it may have been mid-write at the last
    /// shutdown.
    pub fn open_with_trust(
        dir: impl AsRef<Path>,
        base_offset: u64,
        max_bytes: u64,
        index_interval: u64,
        preallocate: bool,
        index_segment: &mut IndexSegment,
        trust_if_clean: bool,
    ) -> IoResult<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let filename = format_segment_filename(base_offset);
        let log_path = dir.join(format!("{}.log", filename));
        Self::open_at_path_with_trust(
            log_path,
            base_offset,
            max_bytes,
            index_interval,
            preallocate,
            index_segment,
            trust_if_clean,
        )
    }

    /// Same as `open`, but at an exact caller-supplied path instead of reconstructing
    /// `dir/{base_offset}.log`. Needed by `SegmentManager::compact_segments`'s
    /// write-to-a-`.compact`-tmp-file-then-rename dance: `open`'s directory-plus-base-offset
    /// reconstruction would otherwise always resolve back to the live original segment's
    /// path, silently writing into it in place instead of the intended tmp file.
    pub fn open_at_path(
        log_path: PathBuf,
        base_offset: u64,
        max_bytes: u64,
        index_interval: u64,
        preallocate: bool,
        index_segment: &mut IndexSegment,
    ) -> IoResult<Self> {
        Self::open_at_path_with_trust(
            log_path,
            base_offset,
            max_bytes,
            index_interval,
            preallocate,
            index_segment,
            false,
        )
    }

    /// Same as `open_at_path`, with the trusted-clean-segment fast path described on
    /// `open_with_trust`.
    #[allow(clippy::too_many_arguments)]
    pub fn open_at_path_with_trust(
        log_path: PathBuf,
        base_offset: u64,
        max_bytes: u64,
        index_interval: u64,
        preallocate: bool,
        index_segment: &mut IndexSegment,
        trust_if_clean: bool,
    ) -> IoResult<Self> {
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

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

        if exists && trust_if_clean {
            let raw_len = file.metadata()?.len();
            if read_clean_marker(&log_path) == Some(raw_len) {
                // Trusted fast path: this segment was cleanly finalized (rotated out of
                // the active role, `set_len`+`sync_data`'d, and marked) and is exactly the
                // size it was then — historical segments are never appended to again after
                // that point (only ever fully rewritten as a new file by compaction, which
                // removes this marker first), so nothing about its content can have changed
                // since. Trust the sparse index already loaded into `index_segment` (no
                // `truncate_and_rebuild` needed) and find `next_offset` by decoding forward
                // from only the *last* index entry instead of the whole segment.
                let fast_path_result = (|| -> IoResult<Option<(u64, u64)>> {
                    match index_segment.last_entry() {
                        Some(last) => {
                            let start_pos = last.physical_position as u64;
                            file.seek(SeekFrom::Start(start_pos))?;
                            let mut tail = Vec::new();
                            file.read_to_end(&mut tail)?;
                            let mut pos = 0usize;
                            let mut last_offset = base_offset;
                            while pos < tail.len() {
                                match decode_entry(&tail[pos..]) {
                                    Ok((LogEntry::Frame(frame), consumed)) => {
                                        last_offset = frame.offset;
                                        pos += consumed;
                                    }
                                    Ok((LogEntry::Batch(batch), consumed)) => {
                                        last_offset =
                                            batch.base_offset + batch.last_offset_delta as u64;
                                        pos += consumed;
                                    }
                                    Err(_) => return Ok(None), // fall back to full scan
                                }
                            }
                            if start_pos + pos as u64 == raw_len {
                                Ok(Some((raw_len, last_offset + 1)))
                            } else {
                                Ok(None)
                            }
                        }
                        None if raw_len == 0 => Ok(Some((0, base_offset))),
                        None => Ok(None),
                    }
                })()?;

                if let Some((trusted_size, trusted_next_offset)) = fast_path_result {
                    file.seek(SeekFrom::Start(trusted_size))?;
                    return Ok(Self {
                        base_offset,
                        next_offset: trusted_next_offset,
                        path: log_path,
                        file,
                        physical_size: trusted_size,
                        is_preallocated: false,
                    });
                }
                tracing::warn!(
                    "Clean marker present but tail verification failed for segment {} — falling back to full scan.",
                    base_offset
                );
            }
        }

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
                                "Incomplete entry header at byte position {} in segment {}. Truncating.",
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

                    match decode_entry(slice) {
                        Ok((entry, entry_len)) => {
                            let phys_pos = file_offset + pos as u64;
                            // A batch spans a range of offsets (`base_offset` through
                            // `base_offset + last_offset_delta`), so it's indexed by its
                            // base offset and advances `next_offset` past its whole range —
                            // a frame is just the range-of-one special case of the same
                            // bookkeeping.
                            let (relative_offset, last_offset_in_entry) = match &entry {
                                LogEntry::Frame(frame) => (
                                    frame.offset.saturating_sub(base_offset) as u32,
                                    frame.offset,
                                ),
                                LogEntry::Batch(batch) => (
                                    batch.base_offset.saturating_sub(base_offset) as u32,
                                    batch.base_offset + batch.last_offset_delta as u64,
                                ),
                            };

                            if bytes_since_last_index >= index_interval
                                || rebuilt_index_entries.is_empty()
                            {
                                rebuilt_index_entries.push(IndexEntry {
                                    relative_offset,
                                    physical_position: phys_pos as u32,
                                });
                                bytes_since_last_index = 0;
                            }

                            bytes_since_last_index += entry_len as u64;
                            pos += entry_len;
                            next_offset = last_offset_in_entry + 1;
                        }
                        Err(err) if err.is_buffer_too_short() => {
                            if n > 0 {
                                break; // need more data
                            } else {
                                let remaining = &read_buf[pos..];
                                if remaining.iter().all(|&b| b == 0) {
                                    break;
                                }
                                tracing::warn!(
                                    "Partial entry at byte position {} in segment {}. Truncating.",
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
                                "Corrupt entry detected at position {} in segment {}: {}. Truncating log.",
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

    /// Finalizes segment upon rotation by trimming any unused preallocated trailing space.
    /// Once finalized, this segment is never appended to again, so a `.clean` marker is
    /// written recording its now-final size — the next startup can trust it and skip the
    /// full-segment recovery scan (see `open_with_trust`).
    pub fn finalize(&mut self) -> IoResult<()> {
        self.file.set_len(self.physical_size)?;
        self.file.sync_data()?;
        write_clean_marker(&self.path, self.physical_size);
        Ok(())
    }

    /// Truncates the log file to exactly `physical_size` bytes (Raft-style conflict
    /// truncation: discards a diverging suffix before appending the leader's entries).
    /// Invalidates any `.clean` marker, since it was only ever safe to trust the untouched
    /// size `finalize()` recorded, not whatever this truncation leaves behind.
    pub fn truncate_to(&mut self, physical_size: u64) -> IoResult<()> {
        self.file.set_len(physical_size)?;
        self.file.sync_data()?;
        self.physical_size = physical_size;
        remove_clean_marker(&self.path);
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
}

/// OS-native zero-copy transmit of `physical_len` bytes starting at `physical_start` in
/// `file`, streamed directly to `socket` by the kernel (Windows `TransmitFile` / Linux
/// `sendfile(2)`) — record payload bytes never pass through a user-space Rust buffer.
///
/// `file` is expected to be an independently-owned handle (e.g. `File::try_clone`'d while
/// briefly holding the segment lock, then used here after that lock has been released), so
/// this function never needs to borrow anything from `SegmentManager`/`LogSegment` across
/// the `.await`.
///
/// Runs `TransmitFile` **synchronously** (`lpOverlapped = NULL`): Tokio's Windows I/O
/// driver already associates every socket it owns with its own IOCP, and issuing an
/// *overlapped* `TransmitFile` on that same socket completes asynchronously via that same
/// IOCP without Tokio ever consuming the completion packet — the send silently races the
/// connection (observed as the client hitting an early EOF / reset, not a clean transmit).
/// A NULL-overlapped call has no such race: the blocking-pool thread this runs on simply
/// doesn't return until the whole range has actually been sent. The file's start position
/// is set explicitly via `SetFilePointerEx` first, since without an `OVERLAPPED` struct
/// `TransmitFile` sends from the handle's current file pointer instead of an arbitrary
/// offset.
#[cfg(windows)]
pub async fn transmit_zero_copy(
    file: &std::fs::File,
    socket: &tokio::net::TcpStream,
    physical_start: u64,
    physical_len: u64,
) -> IoResult<()> {
    use std::os::windows::io::{AsRawHandle, AsRawSocket};
    use windows_sys::Win32::Networking::WinSock::TransmitFile;
    use windows_sys::Win32::Storage::FileSystem::{SetFilePointerEx, FILE_BEGIN};

    if physical_len == 0 {
        return Ok(());
    }

    let raw_socket = socket.as_raw_socket() as usize;
    let raw_file_handle = file.as_raw_handle() as isize;
    let bytes_to_send = physical_len as u32;

    tokio::task::spawn_blocking(move || unsafe {
        if SetFilePointerEx(
            raw_file_handle,
            physical_start as i64,
            std::ptr::null_mut(),
            FILE_BEGIN,
        ) == 0
        {
            return Err(std::io::Error::last_os_error());
        }

        let success = TransmitFile(
            raw_socket,
            raw_file_handle,
            bytes_to_send,
            0,
            std::ptr::null_mut(),
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

/// Linux Kernel Zero-Copy: streams bytes directly from the page cache to the socket via
/// `sendfile(2)`, looping (and waiting for socket writability) across `EWOULDBLOCK` since
/// Tokio sockets are non-blocking and a single `sendfile` call is not guaranteed to send
/// the whole range in one syscall.
#[cfg(target_os = "linux")]
pub async fn transmit_zero_copy(
    file: &std::fs::File,
    socket: &tokio::net::TcpStream,
    physical_start: u64,
    physical_len: u64,
) -> IoResult<()> {
    use std::os::unix::io::AsRawFd;

    if physical_len == 0 {
        return Ok(());
    }

    let raw_file = file.as_raw_fd();
    let raw_socket = socket.as_raw_fd();
    let end = physical_start.checked_add(physical_len).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "zero-copy range overflow")
    })? as libc::off_t;
    let mut offset = physical_start as libc::off_t;

    while offset < end {
        socket.writable().await?;
        let remaining = (end - offset) as usize;

        let (sent, new_offset, io_err) = tokio::task::spawn_blocking(move || {
            let mut off = offset;
            let n = unsafe { libc::sendfile(raw_socket, raw_file, &mut off, remaining) };
            let err = if n < 0 {
                Some(std::io::Error::last_os_error())
            } else {
                None
            };
            (n, off, err)
        })
        .await
        .map_err(std::io::Error::other)?;

        if sent < 0 {
            let err = io_err.expect("negative sendfile return must carry an errno");
            if err.kind() == std::io::ErrorKind::WouldBlock {
                continue;
            }
            return Err(err);
        }
        if sent == 0 {
            break; // peer closed or no more data; caller sees a short transmit as an error via len mismatch
        }
        offset = new_offset;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{BatchCompression, RecordBatch, RecordFrame};
    use bytes::Bytes;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "hermes_log_test_{}_{}_{}",
                label,
                std::process::id(),
                unique
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn frame_bytes(offset: u64, timestamp: u64, payload: &[u8]) -> Vec<u8> {
        let frame = RecordFrame::create(offset, timestamp, Bytes::copy_from_slice(payload));
        let mut buf = Vec::new();
        frame.encode_into(&mut buf);
        buf
    }

    fn batch_bytes(base_offset: u64, records: &[(u64, Bytes)]) -> Vec<u8> {
        let batch = RecordBatch::create(
            base_offset,
            1_700_000_000_000,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            records,
        );
        let mut buf = Vec::new();
        batch.encode_into(&mut buf);
        buf
    }

    fn open_fresh_index(dir: &Path, base_offset: u64) -> IndexSegment {
        let path = dir.join(format!("{}.index", format_segment_filename(base_offset)));
        IndexSegment::open(path, base_offset).unwrap()
    }

    /// A batch as the very first entry in the segment, and a second batch sandwiched
    /// between two frames — the full recovery scan (`open_at_path`, log.rs's site of the
    /// four this stage updates) must decode both, index each by its base offset, and land
    /// on the correct `next_offset` past the last frame, exactly as it would for an
    /// all-frame segment.
    #[test]
    fn full_scan_recovers_batch_as_first_entry_and_between_frames() {
        let dir = TempDir::new("batch_first_and_between");
        let log_path = dir.0.join(format!("{}.log", format_segment_filename(0)));

        let mut raw = Vec::new();
        raw.extend(batch_bytes(
            0,
            &[
                (100, Bytes::from_static(b"a")),
                (110, Bytes::from_static(b"b")),
            ],
        )); // offsets 0-1, FIRST entry
        raw.extend(frame_bytes(2, 200, b"frame2"));
        raw.extend(batch_bytes(3, &[(300, Bytes::from_static(b"c"))])); // offset 3, BETWEEN frames
        raw.extend(frame_bytes(4, 400, b"frame4"));
        std::fs::write(&log_path, &raw).unwrap();

        let mut index_segment = open_fresh_index(&dir.0, 0);
        // index_interval = 0 so every entry gets its own sparse index entry, letting this
        // test check each one individually.
        let seg =
            LogSegment::open_at_path(log_path, 0, 1_000_000, 0, false, &mut index_segment).unwrap();

        assert_eq!(
            seg.next_offset, 5,
            "next_offset must be past the last frame (4)"
        );
        assert_eq!(
            seg.physical_size,
            raw.len() as u64,
            "nothing should be truncated"
        );
        assert_eq!(index_segment.entries_count(), 4);
        assert_eq!(
            index_segment
                .find_nearest_physical_pos(0)
                .unwrap()
                .physical_position,
            0
        );
        assert_eq!(
            index_segment
                .find_nearest_physical_pos(2)
                .unwrap()
                .physical_position as usize,
            batch_bytes(
                0,
                &[
                    (100, Bytes::from_static(b"a")),
                    (110, Bytes::from_static(b"b"))
                ]
            )
            .len()
        );
    }

    /// A batch as the very last entry — nothing after it to lose, but `next_offset` must
    /// still land past its whole offset range, not just past its base offset.
    #[test]
    fn full_scan_recovers_batch_as_last_entry() {
        let dir = TempDir::new("batch_last");
        let log_path = dir.0.join(format!("{}.log", format_segment_filename(0)));

        let mut raw = Vec::new();
        raw.extend(frame_bytes(0, 0, b"frame0"));
        raw.extend(frame_bytes(1, 10, b"frame1"));
        raw.extend(batch_bytes(
            2,
            &[
                (200, Bytes::from_static(b"x")),
                (210, Bytes::from_static(b"y")),
                (220, Bytes::from_static(b"z")),
            ],
        )); // offsets 2-4, LAST entry
        std::fs::write(&log_path, &raw).unwrap();

        let mut index_segment = open_fresh_index(&dir.0, 0);
        let seg =
            LogSegment::open_at_path(log_path, 0, 1_000_000, 0, false, &mut index_segment).unwrap();

        assert_eq!(
            seg.next_offset, 5,
            "next_offset must be past the batch's last offset (4)"
        );
        assert_eq!(seg.physical_size, raw.len() as u64);
    }

    /// A frame-only segment must scan identically to how it always has — this stage adds
    /// batch handling but must not perturb the plain-frame path at all.
    #[test]
    fn full_scan_of_frame_only_segment_is_unaffected() {
        let dir = TempDir::new("frame_only_regression");
        let log_path = dir.0.join(format!("{}.log", format_segment_filename(0)));

        let mut raw = Vec::new();
        for i in 0..5u64 {
            raw.extend(frame_bytes(i, i * 10, format!("payload-{i}").as_bytes()));
        }
        std::fs::write(&log_path, &raw).unwrap();

        let mut index_segment = open_fresh_index(&dir.0, 0);
        let seg =
            LogSegment::open_at_path(log_path, 0, 1_000_000, 0, false, &mut index_segment).unwrap();

        assert_eq!(seg.next_offset, 5);
        assert_eq!(seg.physical_size, raw.len() as u64);
        assert_eq!(index_segment.entries_count(), 5);
    }

    /// A batch cut short at the end of the segment (crash mid-write) must be discarded and
    /// the log truncated back to the last complete entry — exactly the treatment a
    /// truncated frame already gets. Both scenarios are built here side by side so the
    /// comparison is direct: same shape of damage, same recovery outcome.
    #[test]
    fn truncated_batch_at_end_matches_truncated_frame_behavior() {
        let dir = TempDir::new("truncated_entry");

        // Frame-only baseline: one full frame, then a second frame missing its last 10
        // bytes (mid-write crash).
        let frame_log_path = dir.0.join(format!("{}.log", format_segment_filename(0)));
        let good_frame = frame_bytes(0, 0, b"complete-frame");
        let mut frame_raw = good_frame.clone();
        let partial_frame = frame_bytes(1, 10, b"never-finished-writing-this-payload");
        frame_raw.extend(&partial_frame[..partial_frame.len() - 10]);
        std::fs::write(&frame_log_path, &frame_raw).unwrap();

        let mut frame_index = open_fresh_index(&dir.0, 0);
        let frame_seg = LogSegment::open_at_path(
            frame_log_path.clone(),
            0,
            1_000_000,
            0,
            false,
            &mut frame_index,
        )
        .unwrap();

        // Batch equivalent: one full frame, then a batch missing its last 10 bytes.
        let batch_dir = TempDir::new("truncated_entry_batch");
        let batch_log_path = batch_dir
            .0
            .join(format!("{}.log", format_segment_filename(0)));
        let mut batch_raw = good_frame.clone();
        let partial_batch = batch_bytes(
            1,
            &[
                (10, Bytes::from_static(b"one")),
                (20, Bytes::from_static(b"two")),
                (30, Bytes::from_static(b"three")),
            ],
        );
        batch_raw.extend(&partial_batch[..partial_batch.len() - 10]);
        std::fs::write(&batch_log_path, &batch_raw).unwrap();

        let mut batch_index = open_fresh_index(&batch_dir.0, 0);
        let batch_seg = LogSegment::open_at_path(
            batch_log_path.clone(),
            0,
            1_000_000,
            0,
            false,
            &mut batch_index,
        )
        .unwrap();

        // Same recovery outcome in both cases: only the one complete entry survives, and
        // the file on disk is truncated back to exactly its length.
        assert_eq!(frame_seg.next_offset, 1);
        assert_eq!(batch_seg.next_offset, 1);
        assert_eq!(frame_seg.physical_size, good_frame.len() as u64);
        assert_eq!(batch_seg.physical_size, good_frame.len() as u64);
        assert_eq!(
            std::fs::metadata(&frame_log_path).unwrap().len(),
            good_frame.len() as u64,
            "truncated frame tail must be removed from disk"
        );
        assert_eq!(
            std::fs::metadata(&batch_log_path).unwrap().len(),
            good_frame.len() as u64,
            "truncated batch tail must be removed from disk, identically to a truncated frame"
        );
    }

    /// The trusted-clean-segment fast path (`open_at_path_with_trust`'s tail-only decode
    /// from the last sparse index entry) must handle a batch there too, not just a frame —
    /// this is the other of log.rs's two scan sites.
    #[test]
    fn trusted_fast_path_resolves_next_offset_past_a_trailing_batch() {
        let dir = TempDir::new("fast_path_batch_tail");
        let log_path = dir.0.join(format!("{}.log", format_segment_filename(0)));

        let frame0 = frame_bytes(0, 0, b"frame0");
        let frame1 = frame_bytes(1, 10, b"frame1");
        let batch = batch_bytes(
            2,
            &[
                (200, Bytes::from_static(b"x")),
                (210, Bytes::from_static(b"y")),
                (220, Bytes::from_static(b"z")),
            ],
        ); // offsets 2-4, last entry, and where the sparse index's last entry points.

        let mut raw = Vec::new();
        raw.extend(&frame0);
        raw.extend(&frame1);
        let batch_pos = raw.len() as u64;
        raw.extend(&batch);
        std::fs::write(&log_path, &raw).unwrap();

        // Simulate a previous session's sparse index whose last entry points at the
        // batch's start (as `SegmentManager::append_batch` would have left it), and a
        // clean marker matching the file's exact length so the fast path is trusted.
        let mut index_segment = open_fresh_index(&dir.0, 0);
        index_segment.append(0, 0).unwrap();
        index_segment.append(2, batch_pos).unwrap();
        write_clean_marker(&log_path, raw.len() as u64);

        let seg = LogSegment::open_at_path_with_trust(
            log_path,
            0,
            1_000_000,
            0,
            false,
            &mut index_segment,
            true,
        )
        .unwrap();

        assert_eq!(
            seg.next_offset, 5,
            "fast path must decode the trailing batch and land past its last offset (4)"
        );
        assert_eq!(seg.physical_size, raw.len() as u64);
    }
}
