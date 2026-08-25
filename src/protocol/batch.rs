use std::borrow::Cow;

use bytes::{Buf, BufMut, Bytes};
use crc32fast::Hasher;
use thiserror::Error;

/// Magic byte for [`RecordBatch`], the log's only entry format. Distinct from every other
/// magic byte in use: the inter-node magics (`0xAA` replication push,
/// `0xAE`/`0xAF` vote request/response, `0xBB` gRPC replication — `src/replication/mod.rs`,
/// `src/replication/grpc.rs`), the client wire protocol's `0xF1` versioned envelope
/// (`src/protocol/wire.rs`), `0xCE`/`0xCF` (share-group/consumer-offset snapshot magics), the
/// 4-byte `0xCAFEBABE` auth preamble, and `0xB0` (reserved by the parked
/// `inter-node-versioning` branch for a future versioned inter-node frame — avoided so that
/// branch can still land without a collision).
pub const BATCH_MAGIC_BYTE: u8 = 0xC0;

/// Bytes fixed for every batch, from the `magic` byte through `record_count` inclusive —
/// everything before the variable-length `record_data` section.
/// `1(magic) + 4(batch_length) + 4(crc) + 8(base_offset) + 4(last_offset_delta) +
///  8(base_timestamp) + 8(producer_id) + 2(producer_epoch) + 4(base_sequence) +
///  4(leader_epoch) + 2(attributes) + 4(record_count)`
pub const BATCH_HEADER_SIZE: usize = 53;

/// Bytes fixed between the `crc` field and `record_data`, i.e. everything `batch_length`
/// counts (`batch_length` runs from just after itself to the end of the batch, so it covers
/// `crc` too): `BATCH_HEADER_SIZE - 1 (magic) - 4 (batch_length)`.
pub const BATCH_LENGTH_COVERED_FIXED: usize = BATCH_HEADER_SIZE - 1 - 4;

/// Bytes at the head of a batch that *framing* needs — magic, `batch_length`, and the
/// offset range (`base_offset` + `last_offset_delta`) — i.e. everything up to and
/// including byte 21. Deciding where an entry ends and whether its offsets are wanted
/// costs exactly this much of it; the rest is the consumer's business.
pub const BATCH_FRAMING_PREFIX_SIZE: usize = 21;

/// Bytes fixed per record entry inside (decompressed) `record_data`, i.e. present even when
/// both key and value are null: `4(offset_delta) + 8(timestamp_delta) + 4(key_len) +
/// 4(value_len)`.
const RECORD_ENTRY_MIN_SIZE: usize = 20;

const ATTR_COMPRESSION_MASK: u16 = 0x0007;
const ATTR_TRANSACTIONAL_FLAG: u16 = 0x0008;
/// Marks a batch whose records are transaction control markers rather than data. Kafka has
/// the same flag for the same reason: a control record occupies a real offset, so consumers
/// must be able to recognise and skip it without interpreting its contents.
const ATTR_CONTROL_FLAG: u16 = 0x0010;

#[derive(Debug, Error)]
pub enum BatchError {
    #[error("Buffer too short: needed {required} bytes, available {found} bytes")]
    BufferTooShort { required: usize, found: usize },
    #[error("Invalid magic byte: expected 0x{expected:02X}, got 0x{found:02X}")]
    InvalidMagic { expected: u8, found: u8 },
    #[error("Batch length {batch_length} is smaller than the fixed header it must cover ({minimum} bytes)")]
    InvalidBatchLength { batch_length: usize, minimum: usize },
    #[error("Batch CRC32 checksum corruption: batch CRC 0x{expected:08X} != computed 0x{calculated:08X}")]
    CrcMismatch { expected: u32, calculated: u32 },
    #[error("Invalid compression codec in attributes: {value}")]
    InvalidCompressionCodec { value: u16 },
    #[error("Truncated record: buffer ended mid-record while decoding record data")]
    TruncatedRecord,
    #[error(
        "Record count mismatch: {declared} records declared but {trailing} bytes of record data remained unconsumed"
    )]
    TrailingRecordData { declared: u32, trailing: usize },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Compression codec applied to a batch's record data as a whole (never per-record).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchCompression {
    None,
    Lz4,
    Zstd,
}

impl BatchCompression {
    fn to_bits(self) -> u16 {
        match self {
            BatchCompression::None => 0,
            BatchCompression::Lz4 => 1,
            BatchCompression::Zstd => 2,
        }
    }

    fn from_bits(bits: u16) -> Result<Self, BatchError> {
        match bits {
            0 => Ok(BatchCompression::None),
            1 => Ok(BatchCompression::Lz4),
            2 => Ok(BatchCompression::Zstd),
            other => Err(BatchError::InvalidCompressionCodec { value: other }),
        }
    }
}

/// One record decoded out of a batch: its absolute offset and timestamp (base + delta), and
/// its explicit key and value. Both are nullable opaque byte strings — `None` is a genuine
/// null (Kafka-style, distinct from present-but-empty `Some(Bytes::new())`) — and neither is
/// ever decoded or interpreted by the broker; it only ever compares or hashes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRecord {
    pub offset: u64,
    pub timestamp: u64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

/// Disk/wire binary representation of a batch of records sharing one header, one CRC, and
/// (optionally) one compressed payload — for the
/// common case of producing/replicating many records together.
///
/// Layout (all integers big-endian):
/// `[Magic: 1b (0xC0)] | [Batch Length: 4b] | [CRC32: 4b] | [Base Offset: 8b] |
///  [Last Offset Delta: 4b] | [Base Timestamp: 8b] | [Producer Id: 8b] | [Producer Epoch: 2b] |
///  [Base Sequence: 4b] | [Leader Epoch: 4b] | [Attributes: 2b] | [Record Count: 4b] |
///  [Record Data: variable, see below]`
///
/// `Batch Length` counts every byte from `CRC32` to the end of `Record Data` — a reader can
/// read the first 5 bytes, then skip exactly `Batch Length` more bytes to reach the next
/// batch without decoding this one.
///
/// `Attributes` is a bitfield: bits 0-2 hold the compression codec (0 = none, 1 = LZ4,
/// 2 = zstd; 3-7 reserved for future codecs), bit 3 is the transactional flag, bit 4 marks a
/// control batch (transaction markers rather than data), bits 5-15 are reserved.
///
/// `CRC32` covers everything from `Base Offset` through the end of `Record Data` (i.e. the
/// stored, possibly-compressed bytes — corruption of the compressed form is caught even
/// before decompression is attempted).
///
/// `Record Data` holds `Record Count` records back to back, optionally compressed as a
/// single unit per `Attributes` (the surrounding header above is never compressed, so a
/// reader can inspect offsets/length/attributes without decompressing anything). Each
/// decompressed record entry is:
/// `[Offset Delta: 4b] | [Timestamp Delta: 8b, signed] | [Key Len: 4b, signed] | [Key Bytes] |
///  [Value Len: 4b, signed] | [Value Bytes]`
/// — `Offset Delta` added to `Base Offset` and `Timestamp Delta` added to `Base Timestamp`
/// (as signed arithmetic) give the record's absolute offset and timestamp. `Key Len`/
/// `Value Len` are `-1` for null (matching Kafka), distinguishable from present-but-empty
/// (`0`); key and value are opaque bytes the broker never decodes or interprets — only
/// hashed (partitioning) or compared byte-for-byte (compaction).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub magic: u8,
    pub crc: u32,
    pub base_offset: u64,
    pub last_offset_delta: u32,
    pub base_timestamp: u64,
    pub producer_id: u64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub leader_epoch: u32,
    pub attributes: u16,
    pub record_count: u32,
    /// Record data as stored: possibly compressed per `attributes`. Use [`RecordBatch::records`]
    /// to get decoded, decompressed records.
    pub record_data: Bytes,
}

impl RecordBatch {
    /// Builds a batch from a base offset, batch-level metadata, and the records to include.
    /// Each record is `(timestamp, key, value)`, both `key` and `value` nullable opaque
    /// bytes; offsets are assigned sequentially starting at `base_offset` (record `i` gets
    /// offset `base_offset + i`), which is how batches are always produced — there are no
    /// gaps to express within a single batch.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        base_offset: u64,
        base_timestamp: u64,
        leader_epoch: u32,
        producer_id: u64,
        producer_epoch: i16,
        base_sequence: i32,
        transactional: bool,
        codec: BatchCompression,
        records: &[(u64, Option<Bytes>, Option<Bytes>)],
    ) -> Self {
        let record_count = records.len() as u32;
        let last_offset_delta = record_count.saturating_sub(1);

        let mut raw = Vec::new();
        for (i, (timestamp, key, value)) in records.iter().enumerate() {
            raw.put_u32(i as u32);
            raw.put_i64(*timestamp as i64 - base_timestamp as i64);
            encode_opt_bytes(&mut raw, key);
            encode_opt_bytes(&mut raw, value);
        }

        let record_data: Bytes = match codec {
            BatchCompression::None => raw.into(),
            BatchCompression::Lz4 => lz4_flex::compress_prepend_size(&raw).into(),
            BatchCompression::Zstd => zstd::stream::encode_all(raw.as_slice(), 3)
                .expect("in-memory zstd compression is infallible")
                .into(),
        };

        let mut attributes = codec.to_bits() & ATTR_COMPRESSION_MASK;
        if transactional {
            attributes |= ATTR_TRANSACTIONAL_FLAG;
        }

        let crc = Self::calculate_crc(
            base_offset,
            last_offset_delta,
            base_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            leader_epoch,
            attributes,
            record_count,
            &record_data,
        );

        Self {
            magic: BATCH_MAGIC_BYTE,
            crc,
            base_offset,
            last_offset_delta,
            base_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            leader_epoch,
            attributes,
            record_count,
            record_data,
        }
    }

    /// The compression codec this batch's `record_data` is stored under, per `attributes`.
    pub fn compression(&self) -> Result<BatchCompression, BatchError> {
        BatchCompression::from_bits(self.attributes & ATTR_COMPRESSION_MASK)
    }

    /// Whether the transactional attribute bit is set.
    pub fn is_transactional(&self) -> bool {
        self.attributes & ATTR_TRANSACTIONAL_FLAG != 0
    }

    /// Whether this batch carries transaction control markers rather than data.
    pub fn is_control(&self) -> bool {
        self.attributes & ATTR_CONTROL_FLAG != 0
    }

    /// Marks this batch's records as control markers. Used when building a control batch;
    /// the flag lives in the plaintext header, so a reader recognises one without decoding
    /// or decompressing anything.
    pub fn set_control(&mut self) {
        self.attributes |= ATTR_CONTROL_FLAG;
        self.crc = Self::calculate_crc(
            self.base_offset,
            self.last_offset_delta,
            self.base_timestamp,
            self.producer_id,
            self.producer_epoch,
            self.base_sequence,
            self.leader_epoch,
            self.attributes,
            self.record_count,
            &self.record_data,
        );
    }

    /// Builds a batch whose records carry **explicit, possibly non-contiguous** offsets.
    ///
    /// [`Self::create`] assigns offsets sequentially from the base, which is right for a
    /// produce — a produced batch never has gaps. Compaction does: it drops superseded
    /// records from the middle of a batch and keeps the survivors at their original
    /// offsets, so what remains is 5, 9, 12 rather than 0, 1, 2.
    ///
    /// The stored format has always supported this — every record carries its own
    /// `offset_delta`, and `decode_records` reads it back — only `create` could not express
    /// it. `last_offset_delta` is taken from the highest offset present, matching Kafka,
    /// whose compacted batches likewise keep gaps.
    ///
    /// `records` must be sorted ascending by offset and every offset must be at or after
    /// `base_offset`; a record before the base is skipped rather than encoded as a
    /// wrapped-around delta.
    #[allow(clippy::too_many_arguments)]
    pub fn create_with_offsets(
        base_offset: u64,
        base_timestamp: u64,
        leader_epoch: u32,
        producer_id: u64,
        producer_epoch: i16,
        base_sequence: i32,
        transactional: bool,
        codec: BatchCompression,
        records: &[(u64, u64, Option<Bytes>, Option<Bytes>)],
    ) -> Self {
        let mut raw = Vec::new();
        let mut record_count = 0u32;
        let mut last_offset_delta = 0u32;
        for (offset, timestamp, key, value) in records {
            let Some(delta) = offset.checked_sub(base_offset) else {
                continue;
            };
            let delta = delta as u32;
            raw.put_u32(delta);
            raw.put_i64(*timestamp as i64 - base_timestamp as i64);
            encode_opt_bytes(&mut raw, key);
            encode_opt_bytes(&mut raw, value);
            record_count += 1;
            last_offset_delta = last_offset_delta.max(delta);
        }

        let record_data: Bytes = match codec {
            BatchCompression::None => raw.into(),
            BatchCompression::Lz4 => lz4_flex::compress_prepend_size(&raw).into(),
            BatchCompression::Zstd => zstd::stream::encode_all(raw.as_slice(), 3)
                .expect("in-memory zstd compression is infallible")
                .into(),
        };

        let mut attributes = codec.to_bits() & ATTR_COMPRESSION_MASK;
        if transactional {
            attributes |= ATTR_TRANSACTIONAL_FLAG;
        }

        let crc = Self::calculate_crc(
            base_offset,
            last_offset_delta,
            base_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            leader_epoch,
            attributes,
            record_count,
            &record_data,
        );

        Self {
            magic: BATCH_MAGIC_BYTE,
            crc,
            base_offset,
            last_offset_delta,
            base_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            leader_epoch,
            attributes,
            record_count,
            record_data,
        }
    }

    /// Stamps this batch with the offset and leader epoch the broker assigns it, then
    /// recomputes the CRC to match.
    ///
    /// This is what lets the broker accept a batch built and compressed by a producer and
    /// store those bytes as-is: the base offset, leader epoch and CRC all live in the
    /// plaintext 53-byte header, so assigning them touches only the header. `record_data`
    /// is moved through untouched — never decompressed, never re-encoded, never
    /// reallocated — which is the property that keeps a produce cheap and lets the same
    /// stored bytes be handed to followers and consumers later.
    ///
    /// Per-record offsets follow automatically: records carry deltas from the base offset,
    /// so shifting the base shifts every record in the batch without rewriting any of them.
    pub fn assign_base_offset_and_leader_epoch(&mut self, base_offset: u64, leader_epoch: u32) {
        self.base_offset = base_offset;
        self.leader_epoch = leader_epoch;
        self.crc = Self::calculate_crc(
            self.base_offset,
            self.last_offset_delta,
            self.base_timestamp,
            self.producer_id,
            self.producer_epoch,
            self.base_sequence,
            self.leader_epoch,
            self.attributes,
            self.record_count,
            &self.record_data,
        );
    }

    /// Recomputes the CRC32 over the batch's fields and compares it against `self.crc`.
    pub fn verify_crc(&self) -> Result<(), BatchError> {
        let calculated = Self::calculate_crc(
            self.base_offset,
            self.last_offset_delta,
            self.base_timestamp,
            self.producer_id,
            self.producer_epoch,
            self.base_sequence,
            self.leader_epoch,
            self.attributes,
            self.record_count,
            &self.record_data,
        );
        if calculated != self.crc {
            return Err(BatchError::CrcMismatch {
                expected: self.crc,
                calculated,
            });
        }
        Ok(())
    }

    /// Decompresses (if needed) and decodes this batch's records, yielding each record's
    /// absolute offset, absolute timestamp, and opaque payload.
    pub fn records(&self) -> Result<Vec<BatchRecord>, BatchError> {
        let raw: Cow<'_, [u8]> = match self.compression()? {
            BatchCompression::None => Cow::Borrowed(&self.record_data[..]),
            BatchCompression::Lz4 => {
                let decompressed =
                    lz4_flex::decompress_size_prepended(&self.record_data).map_err(|e| {
                        BatchError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e.to_string(),
                        ))
                    })?;
                Cow::Owned(decompressed)
            }
            BatchCompression::Zstd => {
                let decompressed =
                    zstd::stream::decode_all(self.record_data.as_ref()).map_err(|e| {
                        BatchError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e.to_string(),
                        ))
                    })?;
                Cow::Owned(decompressed)
            }
        };

        decode_records(
            self.record_count,
            self.base_offset,
            self.base_timestamp,
            &raw,
        )
    }

    /// Computes CRC32 over: `[BaseOffset | LastOffsetDelta | BaseTimestamp | ProducerId |
    /// ProducerEpoch | BaseSequence | LeaderEpoch | Attributes | RecordCount | RecordData]`.
    #[allow(clippy::too_many_arguments)]
    fn calculate_crc(
        base_offset: u64,
        last_offset_delta: u32,
        base_timestamp: u64,
        producer_id: u64,
        producer_epoch: i16,
        base_sequence: i32,
        leader_epoch: u32,
        attributes: u16,
        record_count: u32,
        record_data: &[u8],
    ) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&base_offset.to_be_bytes());
        hasher.update(&last_offset_delta.to_be_bytes());
        hasher.update(&base_timestamp.to_be_bytes());
        hasher.update(&producer_id.to_be_bytes());
        hasher.update(&producer_epoch.to_be_bytes());
        hasher.update(&base_sequence.to_be_bytes());
        hasher.update(&leader_epoch.to_be_bytes());
        hasher.update(&attributes.to_be_bytes());
        hasher.update(&record_count.to_be_bytes());
        hasher.update(record_data);
        hasher.finalize()
    }

    /// Total serialized size on disk/wire in bytes.
    pub fn encoded_size(&self) -> usize {
        BATCH_HEADER_SIZE + self.record_data.len()
    }

    /// Serializes the batch into the provided output buffer. Generic over `BufMut` (rather
    /// than concretely `&mut Vec<u8>`), so callers on a
    /// hot path can reuse a scratch buffer instead of allocating a fresh `Vec` per call.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        let batch_length = (BATCH_LENGTH_COVERED_FIXED + self.record_data.len()) as u32;
        buf.put_u8(self.magic);
        buf.put_u32(batch_length);
        buf.put_u32(self.crc);
        buf.put_u64(self.base_offset);
        buf.put_u32(self.last_offset_delta);
        buf.put_u64(self.base_timestamp);
        buf.put_u64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_i32(self.base_sequence);
        buf.put_u32(self.leader_epoch);
        buf.put_u16(self.attributes);
        buf.put_u32(self.record_count);
        buf.put_slice(&self.record_data);
    }

    /// Decodes a batch from a raw byte buffer. Returns the decoded batch and total bytes
    /// consumed. Defensive: every length is checked against the remaining buffer before it
    /// is trusted, so a truncated, corrupt, or hostile buffer yields a clean `Err`, never a
    /// panic or an unbounded allocation.
    pub fn decode(mut src: &[u8]) -> Result<(Self, usize), BatchError> {
        const PREFIX: usize = 5; // magic + batch_length
        if src.len() < PREFIX {
            return Err(BatchError::BufferTooShort {
                required: PREFIX,
                found: src.len(),
            });
        }

        let magic = src.get_u8();
        if magic != BATCH_MAGIC_BYTE {
            return Err(BatchError::InvalidMagic {
                expected: BATCH_MAGIC_BYTE,
                found: magic,
            });
        }

        let batch_length = src.get_u32() as usize;
        if batch_length < BATCH_LENGTH_COVERED_FIXED {
            return Err(BatchError::InvalidBatchLength {
                batch_length,
                minimum: BATCH_LENGTH_COVERED_FIXED,
            });
        }
        if src.len() < batch_length {
            return Err(BatchError::BufferTooShort {
                required: batch_length,
                found: src.len(),
            });
        }
        let total_consumed = PREFIX + batch_length;

        let crc = src.get_u32();
        let base_offset = src.get_u64();
        let last_offset_delta = src.get_u32();
        let base_timestamp = src.get_u64();
        let producer_id = src.get_u64();
        let producer_epoch = src.get_i16();
        let base_sequence = src.get_i32();
        let leader_epoch = src.get_u32();
        let attributes = src.get_u16();
        let record_count = src.get_u32();

        let record_data_len = batch_length - BATCH_LENGTH_COVERED_FIXED;
        // Guaranteed by the `src.len() < batch_length` check above: src still held
        // `batch_length` bytes before the fixed fields were consumed, and exactly
        // `BATCH_LENGTH_COVERED_FIXED` of those bytes were just consumed above.
        debug_assert!(src.len() >= record_data_len);
        let record_data = Bytes::copy_from_slice(&src[..record_data_len]);

        let batch = Self {
            magic,
            crc,
            base_offset,
            last_offset_delta,
            base_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            leader_epoch,
            attributes,
            record_count,
            record_data,
        };
        batch.verify_crc()?;

        Ok((batch, total_consumed))
    }
}

/// Encodes a nullable opaque byte string as `[Len: 4b signed][Bytes]`, `-1` signaling null
/// (matching Kafka) so a null key/value stays distinguishable from a present-but-empty one.
fn encode_opt_bytes(buf: &mut impl BufMut, bytes: &Option<Bytes>) {
    match bytes {
        None => buf.put_i32(-1),
        Some(b) => {
            buf.put_i32(b.len() as i32);
            buf.put_slice(b);
        }
    }
}

/// Decodes one `[Len: 4b signed][Bytes]` field, `-1` meaning null. Defensive: the length
/// field is always available-checked before being read, any negative value other than `-1`
/// (never legal) is rejected outright rather than cast to a huge `usize`, and the byte count
/// is checked against the remaining buffer before any copy is attempted.
fn decode_opt_bytes(cursor: &mut &[u8]) -> Result<Option<Bytes>, BatchError> {
    if cursor.remaining() < 4 {
        return Err(BatchError::TruncatedRecord);
    }
    let len = cursor.get_i32();
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(BatchError::TruncatedRecord);
    }
    let len = len as usize;
    if cursor.remaining() < len {
        return Err(BatchError::TruncatedRecord);
    }
    let bytes = Bytes::copy_from_slice(&cursor[..len]);
    cursor.advance(len);
    Ok(Some(bytes))
}

/// Parses `record_count` record entries out of already-decompressed `data`, resolving each
/// record's offset/timestamp deltas against `base_offset`/`base_timestamp`. Defensive: never
/// trusts a key/value length before checking it against the remaining slice, and never
/// pre-allocates based on the untrusted `record_count` alone (capacity is clamped to what the
/// buffer could actually hold).
fn decode_records(
    record_count: u32,
    base_offset: u64,
    base_timestamp: u64,
    data: &[u8],
) -> Result<Vec<BatchRecord>, BatchError> {
    let max_possible_records = data.len() / RECORD_ENTRY_MIN_SIZE;
    let mut records = Vec::with_capacity((record_count as usize).min(max_possible_records));

    let mut cursor = data;
    for _ in 0..record_count {
        if cursor.remaining() < 12 {
            return Err(BatchError::TruncatedRecord);
        }
        let offset_delta = cursor.get_u32();
        let timestamp_delta = cursor.get_i64();
        let key = decode_opt_bytes(&mut cursor)?;
        let value = decode_opt_bytes(&mut cursor)?;

        records.push(BatchRecord {
            offset: base_offset + offset_delta as u64,
            timestamp: (base_timestamp as i64 + timestamp_delta) as u64,
            key,
            value,
        });
    }

    if !cursor.is_empty() {
        return Err(BatchError::TrailingRecordData {
            declared: record_count,
            trailing: cursor.len(),
        });
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(timestamp, key, value)` triples with a null key and the payload as the value —
    /// matching how `SegmentManager::append_batch` populates records today.
    fn sample_records(n: usize) -> Vec<(u64, Option<Bytes>, Option<Bytes>)> {
        (0..n)
            .map(|i| {
                let ts = 1_700_000_000_000u64 + i as u64 * 10;
                let payload = format!(
                    "{{\"user_id\":{},\"event\":\"page_view\",\"path\":\"/home\",\"referrer\":\"https://example.com/search\"}}",
                    i
                );
                (ts, None, Some(Bytes::from(payload)))
            })
            .collect()
    }

    #[test]
    fn round_trip_preserves_records_offsets_and_metadata_uncompressed() {
        let records = sample_records(10);
        let batch = RecordBatch::create(
            1000,
            1_700_000_000_000,
            7,
            42,
            3,
            9,
            false,
            BatchCompression::None,
            &records,
        );

        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, consumed) = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, batch);

        assert_eq!(decoded.base_offset, 1000);
        assert_eq!(decoded.last_offset_delta, 9);
        assert_eq!(decoded.leader_epoch, 7);
        assert_eq!(decoded.producer_id, 42);
        assert_eq!(decoded.producer_epoch, 3);
        assert_eq!(decoded.base_sequence, 9);
        assert!(!decoded.is_transactional());

        let decoded_records = decoded.records().unwrap();
        assert_eq!(decoded_records.len(), records.len());
        for (i, (ts, _key, value)) in records.iter().enumerate() {
            assert_eq!(decoded_records[i].offset, 1000 + i as u64);
            assert_eq!(decoded_records[i].timestamp, *ts);
            assert_eq!(decoded_records[i].key, None);
            assert_eq!(&decoded_records[i].value, value);
        }
    }

    #[test]
    fn round_trip_preserves_records_offsets_and_metadata_lz4() {
        let records = sample_records(10);
        let batch = RecordBatch::create(
            500,
            1_700_000_000_000,
            2,
            1,
            0,
            0,
            false,
            BatchCompression::Lz4,
            &records,
        );
        assert_eq!(batch.compression().unwrap(), BatchCompression::Lz4);

        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, consumed) = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());

        let decoded_records = decoded.records().unwrap();
        assert_eq!(decoded_records.len(), records.len());
        for (i, (ts, _key, value)) in records.iter().enumerate() {
            assert_eq!(decoded_records[i].offset, 500 + i as u64);
            assert_eq!(decoded_records[i].timestamp, *ts);
            assert_eq!(&decoded_records[i].value, value);
        }
    }

    #[test]
    fn round_trip_preserves_records_offsets_and_metadata_zstd() {
        let records = sample_records(10);
        let batch = RecordBatch::create(
            0,
            1_700_000_000_000,
            0,
            99,
            5,
            2,
            true,
            BatchCompression::Zstd,
            &records,
        );
        assert_eq!(batch.compression().unwrap(), BatchCompression::Zstd);
        assert!(batch.is_transactional());

        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, consumed) = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert!(decoded.is_transactional());

        let decoded_records = decoded.records().unwrap();
        assert_eq!(decoded_records.len(), records.len());
        for (i, (ts, _key, value)) in records.iter().enumerate() {
            assert_eq!(decoded_records[i].offset, i as u64);
            assert_eq!(decoded_records[i].timestamp, *ts);
            assert_eq!(&decoded_records[i].value, value);
        }
    }

    #[test]
    fn empty_batch_round_trips() {
        let batch = RecordBatch::create(77, 0, 0, 0, 0, 0, false, BatchCompression::None, &[]);
        assert_eq!(batch.record_count, 0);
        assert_eq!(batch.last_offset_delta, 0);

        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, consumed) = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert!(decoded.records().unwrap().is_empty());
    }

    #[test]
    fn single_record_batch_round_trips() {
        let records = vec![(123u64, None, Some(Bytes::from_static(b"only record")))];
        let batch =
            RecordBatch::create(10, 100, 1, 1, 1, 1, false, BatchCompression::None, &records);
        assert_eq!(batch.record_count, 1);
        assert_eq!(batch.last_offset_delta, 0);

        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, _) = RecordBatch::decode(&encoded).unwrap();
        let decoded_records = decoded.records().unwrap();
        assert_eq!(decoded_records.len(), 1);
        assert_eq!(decoded_records[0].offset, 10);
        assert_eq!(decoded_records[0].timestamp, 123);
        assert_eq!(decoded_records[0].key, None);
        assert_eq!(
            decoded_records[0].value.as_deref(),
            Some(b"only record".as_ref())
        );
    }

    #[test]
    fn last_offset_delta_matches_record_count_minus_one() {
        for n in [0usize, 1, 2, 50] {
            let records = sample_records(n);
            let batch =
                RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
            let expected_delta = if n == 0 { 0 } else { (n - 1) as u32 };
            assert_eq!(batch.last_offset_delta, expected_delta, "n={n}");
            assert_eq!(batch.record_count, n as u32);
        }
    }

    #[test]
    fn crc_detects_corruption_in_header_field() {
        let records = sample_records(5);
        let batch = RecordBatch::create(1, 2, 3, 4, 5, 6, false, BatchCompression::None, &records);
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);

        // Flip a bit inside base_offset (fixed header, after crc).
        encoded[9] ^= 0xFF;
        let err = RecordBatch::decode(&encoded).unwrap_err();
        assert!(matches!(err, BatchError::CrcMismatch { .. }));
    }

    #[test]
    fn crc_detects_corruption_in_record_data() {
        let records = sample_records(5);
        let batch = RecordBatch::create(1, 2, 3, 4, 5, 6, false, BatchCompression::None, &records);
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);

        // Flip a bit inside record_data (after the fixed header).
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let err = RecordBatch::decode(&encoded).unwrap_err();
        assert!(matches!(err, BatchError::CrcMismatch { .. }));
    }

    #[test]
    fn crc_detects_corruption_anywhere_in_batch() {
        let records = sample_records(3);
        let batch = RecordBatch::create(1, 2, 3, 4, 5, 6, false, BatchCompression::None, &records);
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);

        for i in 0..encoded.len() {
            let mut corrupted = encoded.clone();
            corrupted[i] ^= 0x01;
            // Corrupting the magic byte or batch_length is reported as a different error
            // class (InvalidMagic / BufferTooShort / InvalidBatchLength), not CrcMismatch —
            // every other byte must be caught by the CRC. Any `Err` is an acceptable
            // detection of the corruption; only a silent `Ok` with changed content is a bug.
            if let Ok((decoded, _)) = RecordBatch::decode(&corrupted) {
                assert_eq!(decoded, batch, "byte {i} silently changed the batch");
            }
        }
    }

    #[test]
    fn metadata_fields_survive_round_trip() {
        let records = sample_records(3);
        let batch = RecordBatch::create(
            123_456,
            1_700_000_000_000,
            17,
            999_999,
            -5,
            -12345,
            true,
            BatchCompression::Zstd,
            &records,
        );
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, _) = RecordBatch::decode(&encoded).unwrap();

        assert_eq!(decoded.base_offset, 123_456);
        assert_eq!(decoded.base_timestamp, 1_700_000_000_000);
        assert_eq!(decoded.leader_epoch, 17);
        assert_eq!(decoded.producer_id, 999_999);
        assert_eq!(decoded.producer_epoch, -5);
        assert_eq!(decoded.base_sequence, -12345);
        assert!(decoded.is_transactional());
    }

    #[test]
    fn decode_rejects_truncated_prefix() {
        let err = RecordBatch::decode(&[BATCH_MAGIC_BYTE, 0, 0]).unwrap_err();
        assert!(matches!(err, BatchError::BufferTooShort { .. }));
    }

    #[test]
    fn decode_rejects_wrong_magic() {
        let records = sample_records(2);
        let batch = RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        encoded[0] = 0xEE;
        let err = RecordBatch::decode(&encoded).unwrap_err();
        assert!(matches!(err, BatchError::InvalidMagic { .. }));
    }

    #[test]
    fn decode_rejects_corrupt_batch_length_too_small() {
        let records = sample_records(2);
        let batch = RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        // batch_length is bytes [1..5]; set it below the fixed-header minimum.
        encoded[1..5].copy_from_slice(&1u32.to_be_bytes());
        let err = RecordBatch::decode(&encoded).unwrap_err();
        assert!(matches!(err, BatchError::InvalidBatchLength { .. }));
    }

    #[test]
    fn decode_rejects_length_field_claiming_more_than_buffer_holds() {
        let records = sample_records(2);
        let batch = RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        // Claim a batch_length far larger than what actually follows.
        encoded[1..5].copy_from_slice(&u32::MAX.to_be_bytes());
        let err = RecordBatch::decode(&encoded).unwrap_err();
        assert!(matches!(err, BatchError::BufferTooShort { .. }));
    }

    #[test]
    fn decode_rejects_bogus_record_count_too_large() {
        let records = sample_records(2);
        let batch = RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        // record_count is the last 4 bytes of the fixed header, right before record_data.
        let record_count_start = BATCH_HEADER_SIZE - 4;
        encoded[record_count_start..BATCH_HEADER_SIZE].copy_from_slice(&u32::MAX.to_be_bytes());
        // Recompute nothing: this now fails CRC first (attributes/record_count are covered
        // by the CRC), which is itself a clean, non-panicking error — decode either way must
        // not panic or over-allocate.
        let err = RecordBatch::decode(&encoded).unwrap_err();
        assert!(matches!(
            err,
            BatchError::CrcMismatch { .. } | BatchError::TruncatedRecord
        ));
    }

    #[test]
    fn records_rejects_bogus_record_count_too_large_past_crc() {
        // Build a batch, then hand-craft a variant where record_count is inflated but the
        // CRC is recomputed to match, isolating decode_records' own defense (not the CRC's).
        let records = sample_records(2);
        let mut batch =
            RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        batch.record_count = u32::MAX;
        batch.crc = RecordBatch::calculate_crc(
            batch.base_offset,
            batch.last_offset_delta,
            batch.base_timestamp,
            batch.producer_id,
            batch.producer_epoch,
            batch.base_sequence,
            batch.leader_epoch,
            batch.attributes,
            batch.record_count,
            &batch.record_data,
        );
        let err = batch.records().unwrap_err();
        assert!(matches!(err, BatchError::TruncatedRecord));
    }

    #[test]
    fn records_rejects_bogus_record_count_too_small() {
        // record_count under-declares how many records record_data actually holds; leftover
        // bytes after the declared count must be reported, not silently dropped.
        let records = sample_records(3);
        let mut batch =
            RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        batch.record_count = 1;
        batch.crc = RecordBatch::calculate_crc(
            batch.base_offset,
            batch.last_offset_delta,
            batch.base_timestamp,
            batch.producer_id,
            batch.producer_epoch,
            batch.base_sequence,
            batch.leader_epoch,
            batch.attributes,
            batch.record_count,
            &batch.record_data,
        );
        let err = batch.records().unwrap_err();
        assert!(matches!(err, BatchError::TrailingRecordData { .. }));
    }

    #[test]
    fn decode_rejects_corrupt_key_len_field() {
        // Corrupt key_len of the first record entry — layout is
        // offset_delta[0..4] | timestamp_delta[4..12] | key_len[12..16] — to a huge
        // *positive* value (i32::MAX; u32::MAX would read back as the -1 null sentinel, not
        // what this test wants); must be a clean decode error from `records()`, not a panic
        // or an attempt to allocate gigabytes.
        let records = sample_records(2);
        let mut batch =
            RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        let mut raw = batch.record_data.to_vec();
        raw[12..16].copy_from_slice(&i32::MAX.to_be_bytes());
        batch.record_data = Bytes::from(raw);
        batch.crc = RecordBatch::calculate_crc(
            batch.base_offset,
            batch.last_offset_delta,
            batch.base_timestamp,
            batch.producer_id,
            batch.producer_epoch,
            batch.base_sequence,
            batch.leader_epoch,
            batch.attributes,
            batch.record_count,
            &batch.record_data,
        );
        let err = batch.records().unwrap_err();
        assert!(matches!(err, BatchError::TruncatedRecord));
    }

    #[test]
    fn decode_rejects_invalid_compression_codec() {
        let records = sample_records(2);
        let mut batch =
            RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        batch.attributes |= 0x0007; // reserved codec value (7)
        batch.crc = RecordBatch::calculate_crc(
            batch.base_offset,
            batch.last_offset_delta,
            batch.base_timestamp,
            batch.producer_id,
            batch.producer_epoch,
            batch.base_sequence,
            batch.leader_epoch,
            batch.attributes,
            batch.record_count,
            &batch.record_data,
        );
        let err = batch.records().unwrap_err();
        assert!(matches!(err, BatchError::InvalidCompressionCodec { .. }));
    }

    #[test]
    fn decode_never_panics_on_arbitrary_short_buffers() {
        // Fuzz-lite: every truncation length of a valid encoding must decode cleanly or
        // error cleanly, never panic.
        let records = sample_records(4);
        let batch = RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::Zstd, &records);
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);

        for len in 0..encoded.len() {
            let _ = RecordBatch::decode(&encoded[..len]);
        }
    }

    /// Round-trips every combination of null/present key and null/present value, checking
    /// that null stays distinguishable from present-but-empty (`Some(Bytes::new())`) in both
    /// directions — this is the whole point of the `-1` sentinel over a plain length.
    #[test]
    fn key_and_value_nullability_combinations_round_trip_distinctly() {
        let combos: Vec<(Option<Bytes>, Option<Bytes>)> = vec![
            (None, None),
            (None, Some(Bytes::new())),
            (None, Some(Bytes::from_static(b"value"))),
            (Some(Bytes::new()), None),
            (Some(Bytes::new()), Some(Bytes::new())),
            (Some(Bytes::from_static(b"key")), None),
            (
                Some(Bytes::from_static(b"key")),
                Some(Bytes::from_static(b"value")),
            ),
            (Some(Bytes::new()), Some(Bytes::from_static(b"value"))),
            (Some(Bytes::from_static(b"key")), Some(Bytes::new())),
        ];
        let records: Vec<(u64, Option<Bytes>, Option<Bytes>)> = combos
            .iter()
            .enumerate()
            .map(|(i, (k, v))| (1_700_000_000_000 + i as u64, k.clone(), v.clone()))
            .collect();

        let batch = RecordBatch::create(
            0,
            1_700_000_000_000,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &records,
        );
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, _) = RecordBatch::decode(&encoded).unwrap();
        let decoded_records = decoded.records().unwrap();

        assert_eq!(decoded_records.len(), combos.len());
        for (i, (expected_key, expected_value)) in combos.iter().enumerate() {
            assert_eq!(&decoded_records[i].key, expected_key, "combo {i} key");
            assert_eq!(&decoded_records[i].value, expected_value, "combo {i} value");
        }

        // Explicitly pin down null vs. present-but-empty is not conflated anywhere: a null
        // key/value must not equal an empty-but-present one, and vice versa.
        assert_ne!(decoded_records[0].value, decoded_records[1].value); // None vs Some(empty)
        assert_ne!(decoded_records[0].key, decoded_records[3].key); // None vs Some(empty)
        assert_eq!(decoded_records[3].key, Some(Bytes::new()));
        assert_eq!(decoded_records[0].key, None);
    }

    /// A key is opaque bytes, never UTF-8-decoded or otherwise interpreted — a binary key
    /// with no valid UTF-8 interpretation must round-trip byte-identical.
    #[test]
    fn binary_non_utf8_key_round_trips_byte_identical() {
        let binary_key = Bytes::from_static(&[0xFF, 0x00, 0xFE, 0x01, 0x80, 0x81, 0xC0, 0xC1]);
        assert!(
            std::str::from_utf8(&binary_key).is_err(),
            "test key must not be valid UTF-8"
        );

        let records = vec![(
            1_700_000_000_000u64,
            Some(binary_key.clone()),
            Some(Bytes::from_static(b"value")),
        )];
        let batch = RecordBatch::create(
            0,
            1_700_000_000_000,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &records,
        );
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, _) = RecordBatch::decode(&encoded).unwrap();
        let decoded_records = decoded.records().unwrap();

        assert_eq!(decoded_records[0].key, Some(binary_key));
    }

    /// The value is opaque bytes too — never split, parsed, or otherwise interpreted. A JSON
    /// payload containing a `:` (the very character the broker's old payload-sniffing
    /// used to split on) must round-trip byte-identical, proving the broker isn't peeking
    /// inside it.
    #[test]
    fn json_value_round_trips_byte_identical() {
        let json_value = Bytes::from_static(br#"{"a":1,"b":"x:y"}"#);

        let records = vec![(1_700_000_000_000u64, None, Some(json_value.clone()))];
        let batch = RecordBatch::create(
            0,
            1_700_000_000_000,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &records,
        );
        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, _) = RecordBatch::decode(&encoded).unwrap();
        let decoded_records = decoded.records().unwrap();

        assert_eq!(decoded_records[0].key, None);
        assert_eq!(decoded_records[0].value, Some(json_value));
    }

    /// The broker stamps a batch's offset and leader epoch without ever touching the
    /// records. On a compressed batch that is the whole point: `record_data` must come out
    /// byte-identical, so storing a producer's batch costs a header rewrite and nothing
    /// else — no decompress, no re-compress, no re-encode.
    #[test]
    fn assigning_offset_leaves_compressed_record_data_byte_identical() {
        for codec in [
            BatchCompression::Zstd,
            BatchCompression::Lz4,
            BatchCompression::None,
        ] {
            let records = sample_records(6);
            let mut batch =
                RecordBatch::create(0, 1_700_000_000_000, 0, 42, 3, 9, false, codec, &records);
            let data_before = batch.record_data.clone();
            let ptr_before = batch.record_data.as_ptr();
            let crc_before = batch.crc;

            batch.assign_base_offset_and_leader_epoch(5_000, 7);

            // Byte-equality alone would prove nothing here: compression is deterministic,
            // so decompressing and re-compressing yields the same bytes. Pointer identity
            // is the real assertion — `record_data` must be the *same allocation*, moved
            // through rather than rebuilt. `data_before` holds a reference, so the original
            // buffer cannot have been freed and its address coincidentally reused.
            assert_eq!(
                batch.record_data.as_ptr(),
                ptr_before,
                "{codec:?}: record data must be the same allocation, not a rebuilt one"
            );
            assert_eq!(
                batch.record_data, data_before,
                "{codec:?}: record data must be moved through untouched"
            );
            assert_eq!(batch.base_offset, 5_000);
            assert_eq!(batch.leader_epoch, 7);
            assert_ne!(
                batch.crc, crc_before,
                "{codec:?}: the CRC covers the base offset, so it must have changed"
            );
            batch.verify_crc().expect("recomputed CRC must validate");

            // Records carry deltas, so shifting the base shifts all of them.
            let decoded = batch.records().unwrap();
            assert_eq!(decoded.len(), 6);
            for (i, (ts, _k, v)) in records.iter().enumerate() {
                assert_eq!(decoded[i].offset, 5_000 + i as u64, "{codec:?}");
                assert_eq!(decoded[i].timestamp, *ts, "{codec:?}");
                assert_eq!(&decoded[i].value, v, "{codec:?}");
            }
        }
    }

    /// Stamping must not require the record data to be *interpretable* at all. Handing it
    /// a batch whose zstd payload is garbage still has to work and leave the payload
    /// untouched — an implementation that decompressed in order to stamp could not.
    #[test]
    fn assigning_offset_does_not_require_decompressable_record_data() {
        let records = sample_records(3);
        let mut batch = RecordBatch::create(
            0,
            1_700_000_000_000,
            0,
            42,
            3,
            9,
            false,
            BatchCompression::Zstd,
            &records,
        );
        let garbage = Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02]);
        batch.record_data = garbage.clone();
        assert!(
            batch.records().is_err(),
            "precondition: this record data must not be decompressable"
        );

        batch.assign_base_offset_and_leader_epoch(77, 2);

        assert_eq!(
            batch.record_data, garbage,
            "payload must pass through as-is"
        );
        assert_eq!(batch.base_offset, 77);
        assert_eq!(batch.leader_epoch, 2);
        batch
            .verify_crc()
            .expect("CRC must cover the stored bytes, decompressable or not");
    }

    /// Compaction keeps survivors at their original offsets, so a compacted batch has gaps.
    /// The stored format supports that — each record carries its own delta — and this
    /// asserts a batch built that way round-trips with those exact offsets and reports a
    /// `last_offset_delta` taken from the highest offset present, not from the count.
    #[test]
    fn create_with_offsets_preserves_gaps() {
        let records = vec![
            (
                105u64,
                1_700_000_000_000u64,
                Some(Bytes::from_static(b"k1")),
                Some(Bytes::from_static(b"v1")),
            ),
            (
                109,
                1_700_000_000_040,
                Some(Bytes::from_static(b"k2")),
                None,
            ),
            (
                112,
                1_700_000_000_070,
                None,
                Some(Bytes::from_static(b"v3")),
            ),
        ];
        let batch = RecordBatch::create_with_offsets(
            100,
            1_700_000_000_000,
            3,
            42,
            7,
            11,
            false,
            BatchCompression::Zstd,
            &records,
        );
        assert_eq!(batch.record_count, 3);
        assert_eq!(
            batch.last_offset_delta, 12,
            "must come from the highest offset present (112 - 100), not from the count"
        );

        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, _) = RecordBatch::decode(&encoded).unwrap();
        let out = decoded.records().unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(
            out.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![105, 109, 112],
            "gaps must survive the round trip"
        );
        for (i, (_, ts, key, value)) in records.iter().enumerate() {
            assert_eq!(out[i].timestamp, *ts);
            assert_eq!(&out[i].key, key);
            assert_eq!(&out[i].value, value);
        }
    }

    /// A record before the base offset cannot be expressed as an unsigned delta; it must be
    /// skipped rather than wrapping around into a nonsensical offset.
    #[test]
    fn create_with_offsets_skips_records_before_the_base() {
        let records = vec![
            (
                5u64,
                1_700_000_000_000u64,
                None,
                Some(Bytes::from_static(b"before")),
            ),
            (
                11,
                1_700_000_000_000,
                None,
                Some(Bytes::from_static(b"after")),
            ),
        ];
        let batch = RecordBatch::create_with_offsets(
            10,
            1_700_000_000_000,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &records,
        );
        assert_eq!(batch.record_count, 1);
        let out = batch.records().unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].offset, 11);
        assert_eq!(out[0].value.as_deref(), Some(b"after".as_ref()));
    }

    /// A batch that has been stamped and encoded must survive a full round trip through
    /// disk bytes — this is what a follower or consumer will later be handed verbatim.
    #[test]
    fn assigned_batch_round_trips_through_encoded_bytes() {
        let records = sample_records(4);
        let mut batch = RecordBatch::create(
            0,
            1_700_000_000_000,
            0,
            42,
            3,
            9,
            false,
            BatchCompression::Zstd,
            &records,
        );
        batch.assign_base_offset_and_leader_epoch(918, 4);

        let mut encoded = Vec::new();
        batch.encode_into(&mut encoded);
        let (decoded, consumed) = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, batch);
        assert_eq!(decoded.base_offset, 918);
        assert_eq!(decoded.leader_epoch, 4);
        assert_eq!(decoded.records().unwrap()[0].offset, 918);
    }

    #[test]
    fn decode_rejects_key_len_below_null_sentinel() {
        // A key_len of -2 (or any value below the -1 null sentinel) is never legal; it must
        // be a clean decode error, not a cast to a huge usize.
        let records = sample_records(2);
        let mut batch =
            RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &records);
        let mut raw = batch.record_data.to_vec();
        raw[12..16].copy_from_slice(&(-2i32).to_be_bytes());
        batch.record_data = Bytes::from(raw);
        batch.crc = RecordBatch::calculate_crc(
            batch.base_offset,
            batch.last_offset_delta,
            batch.base_timestamp,
            batch.producer_id,
            batch.producer_epoch,
            batch.base_sequence,
            batch.leader_epoch,
            batch.attributes,
            batch.record_count,
            &batch.record_data,
        );
        let err = batch.records().unwrap_err();
        assert!(matches!(err, BatchError::TruncatedRecord));
    }
}
