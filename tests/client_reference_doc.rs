//! Checks that `docs/BIFROX_CLIENT_CREATOR_REFERENCE.md` still describes the real wire.
//!
//! The doc is the contract a third-party client is built against, and it had drifted badly
//! — it documented a `RecordFrame` type that no longer exists, and a tombstone rule (empty
//! value, with the broker string-parsing a key out of the payload) that had been replaced
//! by null-value semantics. Nothing caught either, because prose has no compiler.
//!
//! These tests are that compiler for the byte-level claims. Each one asserts a specific
//! statement the doc makes, so a change that invalidates the doc fails here rather than
//! silently misleading whoever implements against it.

use bifrox::protocol::{BatchCompression, RecordBatch, BATCH_HEADER_SIZE};
use bytes::Bytes;
use std::str::FromStr;

fn encode(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    batch.encode_into(&mut buf);
    buf
}

fn sample_batch(codec: BatchCompression) -> RecordBatch {
    RecordBatch::create(
        0,
        1_700_000_000_000,
        0,
        0,
        0,
        0,
        false,
        codec,
        &[
            (
                1_700_000_000_000,
                Some(Bytes::from_static(b"k1")),
                Some(Bytes::from_static(b"v1")),
            ),
            (1_700_000_000_005, Some(Bytes::from_static(b"k2")), None),
            (
                1_700_000_000_009,
                Some(Bytes::from_static(b"k3")),
                Some(Bytes::new()),
            ),
        ],
    )
}

/// The doc gives the batch header field by field, with a stated size. A client author will
/// index straight into these offsets, so each one is checked at the position documented.
#[test]
fn the_documented_batch_header_layout_is_the_real_one() {
    assert_eq!(BATCH_HEADER_SIZE, 53, "doc: the fixed header is 53 bytes");

    let encoded = encode(&sample_batch(BatchCompression::Zstd));

    assert_eq!(encoded[0], 0xC0, "doc: magic is 0xC0");
    let batch_length = u32::from_be_bytes(encoded[1..5].try_into().unwrap()) as usize;
    assert_eq!(
        batch_length + 5,
        encoded.len(),
        "doc: batch_length counts everything after itself"
    );
    assert_eq!(
        u64::from_be_bytes(encoded[9..17].try_into().unwrap()),
        0,
        "doc: base_offset is 8 bytes at offset 9"
    );
    assert_eq!(
        u32::from_be_bytes(encoded[17..21].try_into().unwrap()),
        2,
        "doc: last_offset_delta is 4 bytes at offset 17"
    );
    assert_eq!(
        u64::from_be_bytes(encoded[21..29].try_into().unwrap()),
        1_700_000_000_000,
        "doc: base_timestamp is 8 bytes at offset 21"
    );
    assert_eq!(
        u32::from_be_bytes(encoded[49..53].try_into().unwrap()),
        3,
        "doc: record_count is 4 bytes at offset 49"
    );
}

/// The doc's central architectural claim: the header stays plaintext so the broker can
/// serve a batch without decompressing it. If compression ever covered the header, every
/// "the broker never decompresses" statement in the doc becomes false.
#[test]
fn the_batch_header_stays_plaintext_under_every_codec() {
    for codec in [
        BatchCompression::None,
        BatchCompression::Lz4,
        BatchCompression::Zstd,
    ] {
        let batch = sample_batch(codec);
        let encoded = encode(&batch);
        assert_eq!(encoded[0], 0xC0, "{:?}: magic must stay readable", codec);
        assert_eq!(
            u64::from_be_bytes(encoded[9..17].try_into().unwrap()),
            0,
            "{:?}: base_offset must stay readable",
            codec
        );
        assert_eq!(
            u32::from_be_bytes(encoded[17..21].try_into().unwrap()),
            2,
            "{:?}: last_offset_delta must stay readable",
            codec
        );
    }
}

/// The doc tabulates the attribute bits and tells consumers to check bit 4 before treating
/// a batch's contents as records.
#[test]
fn the_documented_attribute_bits_are_the_real_ones() {
    let zstd = encode(&sample_batch(BatchCompression::Zstd));
    let attributes = u16::from_be_bytes(zstd[47..49].try_into().unwrap());
    assert_eq!(attributes & 0x0007, 2, "doc: bits 0-2 codec, 2 = Zstd");
    assert_eq!(
        u16::from_be_bytes(
            encode(&sample_batch(BatchCompression::Lz4))[47..49]
                .try_into()
                .unwrap()
        ) & 0x0007,
        1,
        "doc: 1 = LZ4"
    );
    assert_eq!(
        u16::from_be_bytes(
            encode(&sample_batch(BatchCompression::None))[47..49]
                .try_into()
                .unwrap()
        ) & 0x0007,
        0,
        "doc: 0 = none"
    );
    assert_eq!(attributes & 0x0008, 0, "doc: bit 3 is transactional");
    assert_eq!(attributes & 0x0010, 0, "doc: bit 4 is control");

    let mut control = sample_batch(BatchCompression::None);
    control.set_control();
    assert!(control.is_control());
    assert_eq!(
        u16::from_be_bytes(encode(&control)[47..49].try_into().unwrap()) & 0x0010,
        0x0010,
        "doc: bit 4 set marks a control batch"
    );
}

/// The doc says a batch declares its own length, which is what lets a client walk a fetch
/// response batch by batch without understanding any of them.
#[test]
fn batches_in_a_fetch_response_are_walkable_by_their_declared_length() {
    let mut stream = Vec::new();
    for codec in [
        BatchCompression::None,
        BatchCompression::Lz4,
        BatchCompression::Zstd,
    ] {
        stream.extend_from_slice(&encode(&sample_batch(codec)));
    }

    let mut cursor = 0usize;
    let mut walked = 0usize;
    while cursor < stream.len() {
        let (_, consumed) = RecordBatch::decode(&stream[cursor..]).expect("a whole batch");
        cursor += consumed;
        walked += 1;
    }
    assert_eq!(walked, 3);
    assert_eq!(
        cursor,
        stream.len(),
        "the walk must land exactly at the end"
    );
}

/// The doc's most consequential correction. It used to say an *empty* value was a delete
/// marker; the rule is that only a *null* value is. A client whose value type cannot
/// represent null cannot express a delete at all, which is why the doc now says so
/// explicitly, and why this asserts the distinction survives a real encode/decode.
#[test]
fn a_null_value_stays_distinct_from_an_empty_one_through_the_wire_format() {
    for codec in [
        BatchCompression::None,
        BatchCompression::Lz4,
        BatchCompression::Zstd,
    ] {
        let encoded = encode(&sample_batch(codec));
        let (decoded, consumed) = RecordBatch::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());

        let records = decoded.records().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0].value.as_deref(),
            Some(b"v1".as_slice()),
            "{:?}: an ordinary value",
            codec
        );
        assert_eq!(
            records[1].value, None,
            "{:?}: doc says value_len == -1 is null — a tombstone",
            codec
        );
        assert_eq!(
            records[2].value.as_deref(),
            Some(b"".as_slice()),
            "{:?}: doc says value_len == 0 is present-but-empty, NOT a tombstone",
            codec
        );
    }
}

/// The doc states how a client turns the deltas back into absolute values.
#[test]
fn offsets_and_timestamps_reconstruct_the_way_the_doc_says() {
    let encoded = encode(&sample_batch(BatchCompression::None));
    let (decoded, _) = RecordBatch::decode(&encoded).unwrap();
    let records = decoded.records().unwrap();

    for (i, record) in records.iter().enumerate() {
        assert_eq!(
            record.offset, i as u64,
            "doc: offset = base_offset + offset_delta"
        );
    }
    assert_eq!(
        records[1].timestamp, 1_700_000_000_005,
        "doc: timestamp = base_timestamp + timestamp_delta"
    );
    // Signed, so a record older than its batch's base still reconstructs.
    let backwards = RecordBatch::create(
        0,
        1_000,
        0,
        0,
        0,
        0,
        false,
        BatchCompression::None,
        &[(400, Some(Bytes::from_static(b"k")), Some(Bytes::new()))],
    );
    let (decoded, _) = RecordBatch::decode(&encode(&backwards)).unwrap();
    assert_eq!(
        decoded.records().unwrap()[0].timestamp,
        400,
        "doc: timestamp_delta is signed"
    );
}

/// The doc says `producer` is the default and is not an alias for `none` — the difference
/// being whether the broker imposes a codec on records it did not author.
#[test]
fn compression_type_producer_is_the_default_and_not_an_alias_for_none() {
    use bifrox::config::CompressionCodec;

    assert_eq!(
        CompressionCodec::default(),
        CompressionCodec::Producer,
        "doc: `producer` is the default"
    );
    assert_ne!(
        CompressionCodec::from_str("producer").unwrap(),
        CompressionCodec::from_str("none").unwrap(),
        "doc: `producer` is not an alias for `none`"
    );
    for name in ["producer", "none", "lz4", "zstd"] {
        assert!(
            CompressionCodec::from_str(name).is_ok(),
            "doc lists `{}` as a recognized compression.type",
            name
        );
    }
}
