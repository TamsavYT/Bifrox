use crate::protocol::RecordFrame;
use std::fs::File;
use std::io::{Result as IoResult, Write};
use std::time::Instant;

/// High-performance in-memory WAL buffer for batching writes before flushing to disk
#[derive(Debug)]
pub struct WalBuffer {
    buffer: Vec<u8>,
    unflushed_bytes: usize,
    record_count: usize,
    last_flush: Instant,
}

impl WalBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            unflushed_bytes: 0,
            record_count: 0,
            last_flush: Instant::now(),
        }
    }

    /// Append record frame into RAM buffer
    pub fn push(&mut self, frame: &RecordFrame) {
        frame.encode_into(&mut self.buffer);
        self.unflushed_bytes = self.buffer.len();
        self.record_count += 1;
    }

    /// Append pre-encoded record bytes directly into RAM buffer
    pub fn push_encoded(&mut self, encoded_bytes: &[u8]) {
        self.buffer.extend_from_slice(encoded_bytes);
        self.unflushed_bytes = self.buffer.len();
        self.record_count += 1;
    }

    /// Flush accumulated in-memory buffer to physical disk in ONE batch syscall
    pub fn flush_to_file(&mut self, file: &mut File, sync_disk: bool) -> IoResult<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let bytes_written = self.buffer.len();
        file.write_all(&self.buffer)?;

        if sync_disk {
            file.sync_data()?;
        }

        self.buffer.clear();
        self.unflushed_bytes = 0;
        self.record_count = 0;
        self.last_flush = Instant::now();

        Ok(bytes_written)
    }

    pub fn unflushed_bytes(&self) -> usize {
        self.unflushed_bytes
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }

    pub fn time_since_last_flush(&self) -> std::time::Duration {
        self.last_flush.elapsed()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.unflushed_bytes = 0;
        self.record_count = 0;
    }
}
