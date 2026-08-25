pub mod batch;
pub mod kafka_adapter;
pub mod wire;

pub use batch::{
    BatchCompression, BatchError, BatchRecord, RecordBatch, BATCH_FRAMING_PREFIX_SIZE,
    BATCH_HEADER_SIZE, BATCH_LENGTH_COVERED_FIXED, BATCH_MAGIC_BYTE,
};

/// Smallest number of bytes any log entry can occupy, used by scans to know when a buffer
/// cannot possibly hold another entry. Every entry is a [`RecordBatch`], so this is the
/// batch header.
pub const HEADER_SIZE: usize = BATCH_HEADER_SIZE;
pub use kafka_adapter::{KafkaHeader, KafkaWireAdapter};
pub use wire::{
    AckBatch, AcknowledgeType, AcquiredRecordBatch, CommandCode, RequestPayload, WireError,
    WireRequest, WireResponse,
};
