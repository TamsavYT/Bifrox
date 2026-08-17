//! Criterion micro-benchmarks for Hermes's storage hot paths.
//!
//! These exist to catch performance *regressions* between changes, not to publish
//! headline throughput numbers — absolute results depend heavily on the machine's disk.
//! Run with `cargo bench`; Criterion compares each run against the previous one stored in
//! `target/criterion` and reports whether a change is statistically significant.
//!
//! Benchmarked paths, chosen because they're the ones every produce/fetch request goes
//! through and the ones most likely to regress silently:
//! - `RecordFrame` encode/decode (CRC + header serialization, per record)
//! - `SegmentManager::append_with_codec` across each compression codec
//! - `SegmentManager::fetch` (sparse-index seek + frame decode)
//! - `SegmentManager::compact_segments` (the retention GC's most expensive operation)
//! - `hash_key` (partition routing, called once per keyed produce)

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hermes::config::CompressionCodec;
use hermes::{hash_key, CleanupPolicy, EngineConfig, RecordFrame, SegmentManager};
use std::path::PathBuf;

/// Self-cleaning unique temp directory, so repeated bench runs never accumulate data or
/// read each other's segments.
struct BenchDir(PathBuf);

impl BenchDir {
    fn new(label: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hermes_bench_{}_{}_{}",
            label,
            std::process::id(),
            nanos
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for BenchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn bench_config(dir: &BenchDir) -> EngineConfig {
    EngineConfig {
        data_dir: dir.0.clone(),
        // Large enough that rotation isn't what's being measured in the append benchmarks.
        max_segment_bytes: 256 * 1024 * 1024,
        preallocate_segments: false,
        ..EngineConfig::default()
    }
}

fn bench_frame_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_codec");

    for size in [64usize, 1024, 16 * 1024] {
        let payload = vec![b'x'; size];
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("encode", size), &payload, |b, payload| {
            let frame = RecordFrame::create(42, 1_700_000_000_000, payload.clone());
            b.iter(|| {
                let mut buf = Vec::with_capacity(frame.encoded_size());
                frame.encode_into(&mut buf);
                black_box(buf);
            });
        });

        group.bench_with_input(BenchmarkId::new("decode", size), &payload, |b, payload| {
            let frame = RecordFrame::create(42, 1_700_000_000_000, payload.clone());
            let mut encoded = Vec::with_capacity(frame.encoded_size());
            frame.encode_into(&mut encoded);
            b.iter(|| {
                let (decoded, consumed) = RecordFrame::decode(black_box(&encoded)).unwrap();
                black_box((decoded, consumed));
            });
        });
    }

    group.finish();
}

fn bench_segment_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_append");
    const PAYLOAD_SIZE: usize = 512;
    group.throughput(Throughput::Bytes(PAYLOAD_SIZE as u64));

    for (label, codec) in [
        ("none", CompressionCodec::None),
        ("lz4", CompressionCodec::Lz4),
        ("zstd", CompressionCodec::Zstd),
    ] {
        group.bench_function(BenchmarkId::new("append_with_codec", label), |b| {
            let dir = BenchDir::new(&format!("append_{}", label));
            let mut mgr = SegmentManager::open(&dir.0, bench_config(&dir)).unwrap();
            // Repetitive payload so the compressed codecs have something to actually
            // compress — an incompressible random payload would measure only overhead.
            let payload = Bytes::from(b"hermes-benchmark-record-payload-".repeat(16));
            let mut ts = 1_700_000_000_000u64;
            b.iter(|| {
                ts += 1;
                let frame = mgr
                    .append_with_codec(black_box(payload.clone()), ts, codec)
                    .unwrap();
                black_box(frame);
            });
        });
    }

    group.finish();
}

fn bench_segment_fetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_fetch");

    for record_count in [1_000u64, 10_000] {
        group.bench_function(BenchmarkId::new("fetch_64kb", record_count), |b| {
            let dir = BenchDir::new(&format!("fetch_{}", record_count));
            let mut mgr = SegmentManager::open(&dir.0, bench_config(&dir)).unwrap();
            for i in 0..record_count {
                mgr.append(format!("key{}:value-payload-{}", i, i).as_bytes(), i)
                    .unwrap();
            }

            // Sweep the start offset across the log so the benchmark exercises real
            // sparse-index seeks rather than repeatedly hitting one cached position.
            let mut start = 0u64;
            b.iter(|| {
                start = (start + 97) % record_count;
                let frames = mgr.fetch(black_box(start), 64 * 1024).unwrap();
                black_box(frames);
            });
        });
    }

    group.finish();
}

fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");
    // Compaction rewrites whole segments, so keep the sample count low — the default 100
    // samples would make this benchmark run for minutes.
    group.sample_size(10);

    group.bench_function("compact_segments_50pct_dirty", |b| {
        b.iter_batched(
            || {
                // Setup (not timed): 2000 records over 1000 distinct keys, so exactly half
                // the records are superseded and the segment clears the default 0.5
                // min_cleanable_dirty_ratio gate.
                let dir = BenchDir::new("compaction");
                let config = EngineConfig {
                    cleanup_policy: CleanupPolicy::Compact,
                    min_cleanable_dirty_ratio: 0.0,
                    ..bench_config(&dir)
                };
                let mut mgr = SegmentManager::open(&dir.0, config).unwrap();
                for round in 0..2u64 {
                    for key in 0..1_000u64 {
                        mgr.append(
                            format!("key{}:value-round-{}", key, round).as_bytes(),
                            round * 1_000 + key,
                        )
                        .unwrap();
                    }
                }
                mgr.rotate_segment().unwrap();
                (dir, mgr)
            },
            |(dir, mut mgr)| {
                let compacted = mgr.compact_segments().unwrap();
                black_box(compacted);
                // Keep `dir` alive until after the timed closure so its Drop (directory
                // removal) isn't what's being measured.
                drop(mgr);
                drop(dir);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_partition_routing(c: &mut Criterion) {
    c.bench_function("hash_key_12_partitions", |b| {
        let keys: Vec<Vec<u8>> = (0..256)
            .map(|i| format!("user-id-{}", i).into_bytes())
            .collect();
        let mut idx = 0usize;
        b.iter(|| {
            idx = (idx + 1) % keys.len();
            black_box(hash_key(black_box(&keys[idx]), 12));
        });
    });
}

criterion_group!(
    benches,
    bench_frame_codec,
    bench_segment_append,
    bench_segment_fetch,
    bench_compaction,
    bench_partition_routing,
);
criterion_main!(benches);
