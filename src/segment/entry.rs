//! Reading a segment's on-disk stream one entry at a time.
//!
//! A segment's `.log` file holds a back-to-back sequence of entries, each a
//! [`RecordBatch`] starting with `BATCH_MAGIC_BYTE` (`0xC0`). There is one entry format:
//! everything written to the log — client produces, cluster metadata, DLQ routing,
//! consumer offsets, transaction state, control markers — is a batch, so every scan
//! decodes the same way and no reader needs a second branch.
//!
//! [`decode_entry`] reads one entry and reports how many bytes it consumed, derived from
//! the batch's own `BatchLength` field validated against the buffer — never trusted
//! blindly — so a caller that only wants to skip an entry can advance cleanly without
//! inspecting its contents.

use crate::protocol::{BatchError, RecordBatch, BATCH_MAGIC_BYTE};
use bytes::Bytes;

/// One decoded entry from a segment's log stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntry {
    Batch(RecordBatch),
}

/// One record read out of the log, whichever entry kind carried it.
///
/// This is what every read path hands back. Two properties matter and are easy to lose:
///
/// - **A null value is not an empty one.** A tombstone marks a key deleted; an empty value
///   is an ordinary record. Compaction acts on the difference — it purges a key on null and
///   retains it on empty — so a reader that cannot see it builds wrong state.
/// - **A key is explicit.** The broker never derives one from a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp: u64,
    /// `None` means the record carries no key. A frame never has one.
    pub key: Option<Bytes>,
    /// `None` is a tombstone on a compacted topic — distinct from `Some(empty)`.
    pub value: Option<Bytes>,
    /// A transaction control marker rather than data. Readers that surface records to
    /// applications skip these.
    pub is_control: bool,
}

/// Decodes every record out of `entries`.
///
/// A frame's payload is compressed per-record, so it is decompressed here — without this,
/// a caller reading records written under a non-default `compression.type` gets compressed
/// bytes and no indication that is what they are. A batch's records are decompressed by
/// `RecordBatch::records` as a unit.
///
/// A batch whose record data fails to decode has its records skipped rather than aborting
/// the scan: `entries` was already parsed into discrete entries, so a bad batch's
/// neighbours are known-good and there is no reason to lose them too.
pub fn records_from_entries(entries: &[LogEntry]) -> Vec<Record> {
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry {
            LogEntry::Batch(batch) => {
                // The control attribute is a batch-level flag in the plaintext header, so
                // every record in a control batch is a marker. Reading it here is what lets
                // `fetch_committed` filter markers and a consumer skip them.
                let is_control = batch.is_control();
                let Ok(records) = batch.records() else {
                    continue;
                };
                for r in records {
                    out.push(Record {
                        offset: r.offset,
                        timestamp: r.timestamp,
                        key: r.key,
                        value: r.value,
                        is_control,
                    });
                }
            }
        }
    }
    out
}

/// Why an entry failed to decode.
#[derive(Debug, thiserror::Error)]
pub enum EntryError {
    #[error(transparent)]
    Batch(#[from] BatchError),
}

impl EntryError {
    /// True when this error means only "the buffer doesn't yet hold the whole entry" — the
    /// caller should read more bytes and retry, not treat this as corruption.
    pub fn is_buffer_too_short(&self) -> bool {
        matches!(self, EntryError::Batch(BatchError::BufferTooShort { .. }))
    }
}

/// Decodes the single entry starting at `src[0]`, dispatching on its magic byte to either
/// `RecordBatch::decode`. Returns the decoded entry and the total
/// number of bytes it consumed — for a batch this comes from `RecordBatch::decode`, which
/// validates the batch's self-reported length against what's actually left in `src` before
/// trusting it, so a caller that only wants to skip past a batch it cannot use can rely on
/// the returned length to advance cleanly, without inspecting the batch's contents.
///
/// An empty `src` is reported as a "buffer too short" error (there is no
/// magic byte yet to dispatch on, and every existing call site's `BufferTooShort` handling
/// already treats that as "need more data", the correct behavior here too).
pub fn decode_entry(src: &[u8]) -> Result<(LogEntry, usize), EntryError> {
    match src.first() {
        Some(&BATCH_MAGIC_BYTE) => {
            let (batch, consumed) = RecordBatch::decode(src)?;
            Ok((LogEntry::Batch(batch), consumed))
        }
        Some(other) => Err(EntryError::Batch(BatchError::InvalidMagic {
            expected: BATCH_MAGIC_BYTE,
            found: *other,
        })),
        None => Err(EntryError::Batch(BatchError::BufferTooShort {
            required: 1,
            found: 0,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::BatchCompression;
    use bytes::Bytes;

    fn sample_batch(base_offset: u64) -> RecordBatch {
        RecordBatch::create(
            base_offset,
            1_700_000_000_000,
            0,
            0,
            0,
            0,
            false,
            BatchCompression::None,
            &[
                (1_700_000_000_000, None, Some(Bytes::from_static(b"one"))),
                (1_700_000_000_010, None, Some(Bytes::from_static(b"two"))),
            ],
        )
    }

    #[test]
    fn dispatches_to_batch_on_batch_magic() {
        let batch = sample_batch(100);
        let mut buf = Vec::new();
        batch.encode_into(&mut buf);

        let (entry, consumed) = decode_entry(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(entry, LogEntry::Batch(batch));
    }

    #[test]
    fn buffer_too_short_is_recognized() {
        let batch = sample_batch(0);
        let mut batch_buf = Vec::new();
        batch.encode_into(&mut batch_buf);
        let err = decode_entry(&batch_buf[..batch_buf.len() - 1]).unwrap_err();
        assert!(err.is_buffer_too_short());
    }

    /// The log holds exactly one entry format. A byte that is not the batch magic is a
    /// corrupt entry, not another kind of entry, and must be reported as such rather than
    /// mistaken for a short buffer the caller should wait for more bytes on.
    #[test]
    fn non_batch_magic_is_an_error_not_a_short_buffer() {
        let batch = sample_batch(0);
        let mut buf = Vec::new();
        batch.encode_into(&mut buf);
        buf[0] = 0xAB; // what a legacy per-record frame used to start with

        let err = decode_entry(&buf).unwrap_err();
        assert!(
            !err.is_buffer_too_short(),
            "a wrong magic is corruption, not an incomplete read"
        );
    }

    #[test]
    fn corrupt_batch_reports_non_buffer_too_short_error() {
        let batch = sample_batch(0);
        let mut buf = Vec::new();
        batch.encode_into(&mut buf);
        // Flip a byte inside the fixed header (past magic+length) to trip the CRC check.
        buf[10] ^= 0xFF;
        let err = decode_entry(&buf).unwrap_err();
        assert!(!err.is_buffer_too_short());
        assert!(matches!(
            err,
            EntryError::Batch(BatchError::CrcMismatch { .. })
        ));
    }
}
