use bytes::{Buf, BufMut, Bytes};
use crc32fast::Hasher;
use thiserror::Error;

pub const MAGIC_BYTE: u8 = 0xAB;
pub const HEADER_SIZE: usize = 1 + 4 + 8 + 8 + 4; // 25 bytes

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("Buffer too short: needed {required} bytes, available {found} bytes")]
    BufferTooShort { required: usize, found: usize },
    #[error("Invalid magic byte: expected 0x{expected:02X}, got 0x{found:02X}")]
    InvalidMagic { expected: u8, found: u8 },
    #[error("CRC32 checksum corruption: record CRC 0x{expected:08X} != computed 0x{calculated:08X}")]
    CrcMismatch { expected: u32, calculated: u32 },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Disk binary frame representation for an event log entry:
/// `[Magic Byte: 1b] | [CRC32 Checksum: 4b] | [Logical Offset: 8b] | [Timestamp: 8b] | [Payload Len: 4b] | [Payload Bytes]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFrame {
    pub magic: u8,
    pub crc: u32,
    pub offset: u64,
    pub timestamp: u64,
    pub payload_len: u32,
    pub payload: Bytes,
}

impl RecordFrame {
    /// Creates a new RecordFrame, automatically calculating the CRC32 checksum over payload and metadata.
    pub fn create(offset: u64, timestamp: u64, payload: impl Into<Bytes>) -> Self {
        let payload = payload.into();
        let payload_len = payload.len() as u32;
        let crc = Self::calculate_crc(offset, timestamp, &payload);

        Self {
            magic: MAGIC_BYTE,
            crc,
            offset,
            timestamp,
            payload_len,
            payload,
        }
    }

    /// Computes CRC32 checksum over: [Offset | Timestamp | PayloadLen | Payload]
    pub fn calculate_crc(offset: u64, timestamp: u64, payload: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&offset.to_be_bytes());
        hasher.update(&timestamp.to_be_bytes());
        hasher.update(&(payload.len() as u32).to_be_bytes());
        hasher.update(payload);
        hasher.finalize()
    }

    /// Total serialized size on disk in bytes
    pub fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }

    /// Serializes the binary frame into the provided output buffer
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.reserve(self.encoded_size());
        buf.put_u8(self.magic);
        buf.put_u32(self.crc);
        buf.put_u64(self.offset);
        buf.put_u64(self.timestamp);
        buf.put_u32(self.payload_len);
        buf.put_slice(&self.payload);
    }

    /// Decodes a binary frame from a raw byte buffer. Returns decoded frame and total bytes consumed.
    pub fn decode(mut src: &[u8]) -> Result<(Self, usize), FrameError> {
        if src.len() < HEADER_SIZE {
            return Err(FrameError::BufferTooShort {
                required: HEADER_SIZE,
                found: src.len(),
            });
        }

        let magic = src.get_u8();
        if magic != MAGIC_BYTE {
            return Err(FrameError::InvalidMagic {
                expected: MAGIC_BYTE,
                found: magic,
            });
        }

        let crc = src.get_u32();
        let offset = src.get_u64();
        let timestamp = src.get_u64();
        let payload_len = src.get_u32() as usize;

        let total_frame_len = HEADER_SIZE + payload_len;
        if src.len() < payload_len {
            return Err(FrameError::BufferTooShort {
                required: payload_len,
                found: src.len(),
            });
        }

        let payload_bytes = &src[..payload_len];
        let calculated_crc = Self::calculate_crc(offset, timestamp, payload_bytes);
        if crc != calculated_crc {
            return Err(FrameError::CrcMismatch {
                expected: crc,
                calculated: calculated_crc,
            });
        }

        let payload = Bytes::copy_from_slice(payload_bytes);

        Ok((
            Self {
                magic,
                crc,
                offset,
                timestamp,
                payload_len: payload_len as u32,
                payload,
            },
            total_frame_len,
        ))
    }
}
