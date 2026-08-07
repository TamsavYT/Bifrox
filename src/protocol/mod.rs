pub mod frame;
pub mod wire;

pub use frame::{FrameError, RecordFrame, HEADER_SIZE, MAGIC_BYTE};
pub use wire::{CommandCode, RequestPayload, WireError, WireRequest, WireResponse};
