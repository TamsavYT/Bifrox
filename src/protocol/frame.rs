use bytes::{Buf, BufMut, Bytes};
use crc32fast::Hasher;
use thiserror::Error;

pub const MAGIC_BYTE: u8 = 0xAB;
pub const COMPRESSED_LZ4_MAGIC_BYTE: u8 = 0xAC;
pub const CONTROL_MAGIC_BYTE: u8 = 0xAD;
pub const COMPRESSED_ZSTD_MAGIC_BYTE: u8 = 0xAE;
pub const HEADER_SIZE: usize = 1 + 4 + 8 + 8 + 4; // 25 bytes

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("Buffer too short: needed {required} bytes, available {found} bytes")]
    BufferTooShort { required: usize, found: usize },
    #[error("Invalid magic byte: expected 0x{expected:02X}, got 0x{found:02X}")]
    InvalidMagic { expected: u8, found: u8 },
    #[error(
        "CRC32 checksum corruption: record CRC 0x{expected:08X} != computed 0x{calculated:08X}"
    )]
    CrcMismatch { expected: u32, calculated: u32 },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Disk binary frame representation for an event log entry or control marker:
/// `[Magic Byte: 1b (0xAB data / 0xAD control)] | [CRC32 Checksum: 4b] | [Logical Offset: 8b] | [Timestamp: 8b] | [Payload Len: 4b] | [Payload Bytes]`
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

    /// Creates an LZ4 compressed RecordFrame (magic 0xAC)
    pub fn create_compressed_lz4(offset: u64, timestamp: u64, raw_payload: &[u8]) -> Self {
        let compressed = lz4_flex::compress_prepend_size(raw_payload);
        let payload = Bytes::from(compressed);
        let payload_len = payload.len() as u32;
        let crc = Self::calculate_crc(offset, timestamp, &payload);

        Self {
            magic: COMPRESSED_LZ4_MAGIC_BYTE,
            crc,
            offset,
            timestamp,
            payload_len,
            payload,
        }
    }

    /// Creates a Zstandard compressed RecordFrame (magic 0xAE)
    pub fn create_compressed_zstd(offset: u64, timestamp: u64, raw_payload: &[u8]) -> Self {
        // Level 3 matches zstd's own CLI/library default — a solid speed/ratio balance for
        // per-record compression rather than the (much slower) higher levels meant for
        // one-shot batch archival.
        let compressed = zstd::stream::encode_all(raw_payload, 3)
            .expect("in-memory zstd compression is infallible");
        let payload = Bytes::from(compressed);
        let payload_len = payload.len() as u32;
        let crc = Self::calculate_crc(offset, timestamp, &payload);

        Self {
            magic: COMPRESSED_ZSTD_MAGIC_BYTE,
            crc,
            offset,
            timestamp,
            payload_len,
            payload,
        }
    }

    /// Decompresses frame payload if frame magic indicates a compressed codec (LZ4 or Zstd)
    pub fn decompress_payload(&self) -> Result<Bytes, std::io::Error> {
        match self.magic {
            COMPRESSED_LZ4_MAGIC_BYTE => {
                let decompressed =
                    lz4_flex::decompress_size_prepended(&self.payload).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?;
                Ok(Bytes::from(decompressed))
            }
            COMPRESSED_ZSTD_MAGIC_BYTE => {
                let decompressed =
                    zstd::stream::decode_all(self.payload.as_ref()).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?;
                Ok(Bytes::from(decompressed))
            }
            _ => Ok(self.payload.clone()),
        }
    }

    /// Creates a transaction control marker frame (0x01 = Commit, 0x02 = Abort)
    pub fn create_control_marker(
        offset: u64,
        timestamp: u64,
        control_type: u8,
        producer_id: u64,
        transaction_id: &str,
    ) -> Self {
        let mut buf = Vec::new();
        buf.put_u8(control_type);
        buf.put_u64(producer_id);
        crate::protocol::wire::write_pascal_string(&mut buf, transaction_id);

        let payload: Bytes = buf.into();
        let payload_len = payload.len() as u32;
        let crc = Self::calculate_crc(offset, timestamp, &payload);

        Self {
            magic: CONTROL_MAGIC_BYTE,
            crc,
            offset,
            timestamp,
            payload_len,
            payload,
        }
    }

    /// Returns true if this frame is a transaction control marker (0xAD)
    pub fn is_control_marker(&self) -> bool {
        self.magic == CONTROL_MAGIC_BYTE
    }

    /// Parses control marker payload: returns Option<(control_type: 1b, producer_id: 8b, transaction_id: String)>
    pub fn parse_control_marker(&self) -> Option<(u8, u64, String)> {
        if !self.is_control_marker() || self.payload.len() < 11 {
            return None;
        }
        let mut src = &self.payload[..];
        let control_type = src.get_u8();
        let producer_id = src.get_u64();
        if src.len() < 2 {
            return None;
        }
        let len = src.get_u16() as usize;
        if src.len() < len {
            return None;
        }
        let tx_id = String::from_utf8_lossy(&src[..len]).to_string();
        Some((control_type, producer_id, tx_id))
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

    /// Serializes the binary frame into the provided output buffer. Generic over `BufMut`
    /// (rather than concretely `&mut Vec<u8>`) so callers on a hot path can pass a reused
    /// `BytesMut` scratch buffer instead of allocating a fresh `Vec` per call — every
    /// existing `&mut Vec<u8>` call site keeps compiling unchanged, since `Vec<u8>` already
    /// implements `BufMut`.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
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
        if magic != MAGIC_BYTE
            && magic != COMPRESSED_LZ4_MAGIC_BYTE
            && magic != COMPRESSED_ZSTD_MAGIC_BYTE
            && magic != CONTROL_MAGIC_BYTE
        {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zstd_frame_round_trips_through_encode_decode_and_decompress() {
        let raw =
            b"the quick brown fox jumps over the lazy dog, repeated for compressibility ".repeat(8);
        let frame = RecordFrame::create_compressed_zstd(42, 123_456, &raw);
        assert_eq!(frame.magic, COMPRESSED_ZSTD_MAGIC_BYTE);
        // A reasonably repetitive payload should actually shrink.
        assert!(frame.payload.len() < raw.len());

        let mut encoded = Vec::new();
        frame.encode_into(&mut encoded);
        let (decoded, consumed) = RecordFrame::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.magic, COMPRESSED_ZSTD_MAGIC_BYTE);
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.timestamp, 123_456);

        let decompressed = decoded.decompress_payload().unwrap();
        assert_eq!(decompressed.as_ref(), raw.as_slice());
    }

    #[test]
    fn lz4_frame_round_trips_through_encode_decode_and_decompress() {
        let raw = b"lz4 round trip payload".to_vec();
        let frame = RecordFrame::create_compressed_lz4(1, 2, &raw);
        assert_eq!(frame.magic, COMPRESSED_LZ4_MAGIC_BYTE);

        let mut encoded = Vec::new();
        frame.encode_into(&mut encoded);
        let (decoded, _) = RecordFrame::decode(&encoded).unwrap();
        assert_eq!(
            decoded.decompress_payload().unwrap().as_ref(),
            raw.as_slice()
        );
    }

    #[test]
    fn uncompressed_frame_decompress_is_a_no_op() {
        let raw = b"plain payload".to_vec();
        let frame = RecordFrame::create(5, 6, raw.clone());
        assert_eq!(frame.magic, MAGIC_BYTE);
        assert_eq!(frame.decompress_payload().unwrap().as_ref(), raw.as_slice());
    }
}
