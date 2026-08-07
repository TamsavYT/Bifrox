pub mod buffer;

pub use buffer::WalBuffer;
use crate::config::FlushPolicy;

#[derive(Debug)]
pub struct WalEngine {
    buffer: WalBuffer,
    flush_policy: FlushPolicy,
}

impl WalEngine {
    pub fn new(flush_policy: FlushPolicy, buffer_capacity: usize) -> Self {
        Self {
            buffer: WalBuffer::new(buffer_capacity),
            flush_policy,
        }
    }

    pub fn buffer_mut(&mut self) -> &mut WalBuffer {
        &mut self.buffer
    }

    pub fn buffer(&self) -> &WalBuffer {
        &self.buffer
    }

    pub fn flush_policy(&self) -> &FlushPolicy {
        &self.flush_policy
    }

    /// Evaluates if dirty RAM buffer should flush based on configured FlushPolicy
    pub fn should_flush(&self) -> bool {
        match &self.flush_policy {
            FlushPolicy::SyncEveryBatch => !self.buffer.is_empty(),
            FlushPolicy::UnbufferedSync => !self.buffer.is_empty(),
            FlushPolicy::AsyncPeriodic { interval, max_bytes } => {
                self.buffer.unflushed_bytes() >= *max_bytes
                    || (!self.buffer.is_empty() && self.buffer.time_since_last_flush() >= *interval)
            }
        }
    }

    /// Returns whether file sync (`sync_data()`) is required for current flush policy
    pub fn requires_sync(&self) -> bool {
        matches!(
            self.flush_policy,
            FlushPolicy::SyncEveryBatch | FlushPolicy::UnbufferedSync
        )
    }
}
