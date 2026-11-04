use bifrox::protocol::batch::MAX_DECOMPRESSED_BATCH_BYTES;
use bifrox::protocol::{BatchCompression, RecordBatch};
use bytes::Bytes;

fn empty_batch() -> RecordBatch {
    RecordBatch::create(0, 0, 0, 0, 0, 0, false, BatchCompression::None, &[])
}

#[test]
fn oversized_lz4_declaration_is_rejected_before_decompression() {
    let mut batch = empty_batch();
    batch.attributes = 1;
    batch.record_data = Bytes::copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(batch.records().is_err());
}

#[test]
fn zstd_output_cannot_expand_past_limit() {
    // A streaming source avoids allocating the uncompressed input in this test.
    let mut encoded = Vec::new();
    zstd::stream::copy_encode(
        std::io::Read::take(std::io::repeat(0), MAX_DECOMPRESSED_BATCH_BYTES as u64 + 1),
        &mut encoded,
        1,
    )
    .unwrap();
    assert!(encoded.len() < 100_000);
    let mut batch = empty_batch();
    batch.attributes = 2;
    batch.record_data = encoded.into();
    assert!(batch.records().is_err());
}

#[test]
fn supported_compression_round_trips_within_limits() {
    for codec in [
        BatchCompression::None,
        BatchCompression::Lz4,
        BatchCompression::Zstd,
    ] {
        let batch = RecordBatch::create(
            0,
            1,
            0,
            0,
            0,
            0,
            false,
            codec,
            &[(
                1,
                Some(Bytes::from_static(b"key")),
                Some(Bytes::from_static(b"payload")),
            )],
        );
        let records = batch.records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_deref(), Some(b"payload".as_slice()));
    }
}
