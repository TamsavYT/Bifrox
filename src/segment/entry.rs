//! Shared dispatch point for reading a segment's on-disk stream one entry at a time,
//! without every scan needing to know how many entry encodings exist.
//!
//! A segment's `.log` file holds a back-to-back sequence of entries, each starting with a
//! one-byte magic: a [`RecordFrame`] (one record per entry, magics `0xAB`/`0xAC`/`0xAD`/
//! `0xAE`) or a [`RecordBatch`] (many records sharing one header, magic `0xC0` —
//! `BATCH_MAGIC_BYTE`). Before this module, every scan (`LogSegment::open_at_path_with_trust`'s
//! recovery scan and fast-path tail check, `SegmentPair::read_all_frames`, and
//! `SegmentManager::physical_pos_for_offset`) called `RecordFrame::decode` directly and had
//! no way to recognize a batch except as a decode failure — which, depending on the site,
//! either aborted the whole scan or was mistaken for corruption and truncated the log.
//!
//! [`decode_entry`] fixes that: it looks at the magic byte first and dispatches to the
//! right decoder, so a batch a caller cannot use can still be skipped by its own reported
//! length (`RecordBatch::decode`'s `consumed` return, which is derived from the batch's
//! `BatchLength` field validated against the buffer — never trusted blindly) instead of
//! derailing the scan.

use crate::protocol::{BatchError, FrameError, RecordBatch, RecordFrame, BATCH_MAGIC_BYTE};

/// One decoded entry from a segment's log stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntry {
    Frame(RecordFrame),
    Batch(RecordBatch),
}

/// Either a [`FrameError`] or a [`BatchError`], depending on which decoder
/// [`decode_entry`] dispatched to based on the entry's magic byte.
#[derive(Debug, thiserror::Error)]
pub enum EntryError {
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error(transparent)]
    Batch(#[from] BatchError),
}

impl EntryError {
    /// True when this error means only "the buffer doesn't yet hold the whole entry" —
    /// the caller should read more bytes and retry, not treat this as corruption. Mirrors
    /// `FrameError::BufferTooShort`/`BatchError::BufferTooShort`, which both mean exactly
    /// that for their respective formats.
    pub fn is_buffer_too_short(&self) -> bool {
        matches!(
            self,
            EntryError::Frame(FrameError::BufferTooShort { .. })
                | EntryError::Batch(BatchError::BufferTooShort { .. })
        )
    }
}

/// Decodes the single entry starting at `src[0]`, dispatching on its magic byte to either
/// `RecordFrame::decode` or `RecordBatch::decode`. Returns the decoded entry and the total
/// number of bytes it consumed — for a batch this comes from `RecordBatch::decode`, which
/// validates the batch's self-reported length against what's actually left in `src` before
/// trusting it, so a caller that only wants to skip past a batch it cannot use can rely on
/// the returned length to advance cleanly, without inspecting the batch's contents.
///
/// An empty `src` is reported as `RecordFrame`'s own "buffer too short" error (there is no
/// magic byte yet to dispatch on, and every existing call site's `BufferTooShort` handling
/// already treats that as "need more data", the correct behavior here too).
pub fn decode_entry(src: &[u8]) -> Result<(LogEntry, usize), EntryError> {
    match src.first() {
        Some(&BATCH_MAGIC_BYTE) => {
            let (batch, consumed) = RecordBatch::decode(src)?;
            Ok((LogEntry::Batch(batch), consumed))
        }
        _ => {
            let (frame, consumed) = RecordFrame::decode(src)?;
            Ok((LogEntry::Frame(frame), consumed))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::BatchCompression;
    use bytes::Bytes;

    fn sample_frame(offset: u64) -> RecordFrame {
        RecordFrame::create(offset, offset * 10, Bytes::from_static(b"payload"))
    }

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
                (1_700_000_000_000, Bytes::from_static(b"one")),
                (1_700_000_000_010, Bytes::from_static(b"two")),
            ],
        )
    }

    #[test]
    fn dispatches_to_frame_on_frame_magic() {
        let frame = sample_frame(7);
        let mut buf = Vec::new();
        frame.encode_into(&mut buf);

        let (entry, consumed) = decode_entry(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(entry, LogEntry::Frame(frame));
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
    fn buffer_too_short_is_recognized_for_both_kinds() {
        let frame = sample_frame(1);
        let mut frame_buf = Vec::new();
        frame.encode_into(&mut frame_buf);
        let err = decode_entry(&frame_buf[..frame_buf.len() - 1]).unwrap_err();
        assert!(err.is_buffer_too_short());

        let batch = sample_batch(0);
        let mut batch_buf = Vec::new();
        batch.encode_into(&mut batch_buf);
        let err = decode_entry(&batch_buf[..batch_buf.len() - 1]).unwrap_err();
        assert!(err.is_buffer_too_short());
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
