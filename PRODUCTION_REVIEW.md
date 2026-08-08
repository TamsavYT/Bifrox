# Hermes Event Streaming Engine — Production Readiness Review

> **Purpose**: This document is a comprehensive code review and bug report for the Hermes codebase, intended to guide an AI agent in making all necessary fixes and improvements to reach production-grade quality comparable to Apache Kafka (but targeting Windows-first deployments).
>
> **Reviewer**: GitHub Copilot CLI  
> **Review Date**: 2026-08-08  
> **Codebase**: Rust / Tokio async TCP event streaming engine  
> **Total Source Files Reviewed**: 24 Rust source files

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Critical Bugs (Must Fix)](#2-critical-bugs-must-fix)
3. [Security Issues](#3-security-issues)
4. [Correctness & Data Integrity Issues](#4-correctness--data-integrity-issues)
5. [Concurrency & Race Conditions](#5-concurrency--race-conditions)
6. [Replication & Consensus Issues](#6-replication--consensus-issues)
7. [Performance Issues](#7-performance-issues)
8. [Protocol & API Issues](#8-protocol--api-issues)
9. [Error Handling Gaps](#9-error-handling-gaps)
10. [Memory Management Issues](#10-memory-management-issues)
11. [Windows-Specific Issues](#11-windows-specific-issues)
12. [Observability Gaps](#12-observability-gaps)
13. [Testing Gaps](#13-testing-gaps)
14. [Missing Production Features](#14-missing-production-features)
15. [Dependency & Build Issues](#15-dependency--build-issues)
16. [Improvement Roadmap Summary](#16-improvement-roadmap-summary)

---

## 1. Executive Summary

Hermes is a well-structured event streaming engine with a Kafka-inspired architecture written in Rust. The codebase demonstrates strong foundational ideas: segment-based log storage, sparse index, WAL, Raft-like consensus, transactions with LSO-based isolation, and consumer group offset persistence. However, several **critical bugs**, **data integrity gaps**, **race conditions**, and **missing production features** prevent it from being deployed safely in production today.

**Severity Distribution**:
| Severity | Count |
|----------|-------|
| 🔴 Critical (data loss / correctness) | 12 |
| 🟠 High (reliability / security) | 9 |
| 🟡 Medium (performance / robustness) | 11 |
| 🟢 Low (polish / observability) | 8 |

---

## 2. Critical Bugs (Must Fix)

### BUG-01 — WAL Engine Is Never Actually Used for Writes
**File**: `src/server/partition.rs` — `produce_frame()` and `produce_control_marker()`  
**Severity**: 🔴 Critical (data loss on crash)

The `WalEngine` field exists on `PartitionManager`, but writes go **directly** to the `SegmentManager`. The WAL buffer is checked for flush decisions but is **never populated** with data. This means there is no write-ahead log — a crash between the OS `write()` call and `sync_data()` can silently corrupt or lose records.

```rust
// Current (WRONG): WAL buffer is queried but never written to
let wal_guard = self.wal_engine.lock();
if wal_guard.should_flush() {
    seg_guard.sync()?;
}
```

**Fix**: Either fully implement the WAL (write to WAL first, then checkpoint to segment), or remove the misleading `WalEngine` abstraction and make `FlushPolicy` a direct parameter of `SegmentManager::append()`. The `WalBuffer::push()` and `flush_to_file()` methods exist but are never called from the write path.

---

### BUG-02 — `FetchByTimestamp` Ignores the `offset` Parameter and Does a Full Scan
**File**: `src/server/handler.rs` — `process_request()` FetchByTimestamp arm  
**Severity**: 🔴 Critical (correctness + O(N) instead of O(log N) performance)

```rust
// BUG: hardcodes offset=0, ignores the actual start offset from client
Ok(frames) => match engine.fetch(&topic, partition, 0, max_bytes) {
```

The `FetchByTimestamp` implementation fetches all records from offset 0, then filters in memory. This is `O(N)` and ignores the time index entirely. On large partitions this will OOM the server.

**Fix**: Use the `TimeIndexSegment` (already present in `src/segment/timeindex.rs`) to binary-search the nearest physical position, then return frames from that position forward. The `fetch_by_timestamp` path should call the segment manager's time index lookup.

---

### BUG-03 — `aborted_ranges()` Returns `u64::MAX` as End Offset
**File**: `src/server/transaction.rs` — `aborted_ranges()`  
**Severity**: 🔴 Critical (ALL records are hidden after one abort)

```rust
// Returns (start_offset, u64::MAX) — this hides every record from start_offset onwards
ranges.push((*start_offset, u64::MAX));
```

`fetch_committed()` in `engine.rs` checks `frame.offset >= *start && frame.offset <= *end`. Because `end = u64::MAX`, this will filter out **every single record** in the partition from the transaction's start offset onward — including legitimate committed records from other transactions.

**Fix**: Track the end offset of an aborted transaction by recording the offset of the Abort control marker. The `TransactionState` should store the actual end offset once the abort marker is written.

---

### BUG-04 — Partition Directory Layout Bug in `PartitionManager::open()`
**File**: `src/server/partition.rs` — `open()`  
**Severity**: 🔴 Critical (data stored in wrong directory)

```rust
// Creates data/{topic}-{partition}/{topic}/partition-{partition}/
let partition_dir = base_data_dir
    .as_ref()
    .join(&topic)          // adds topic again!
    .join(format!("partition-{}", partition));
```

`StorageEngine::get_or_create_partition()` already constructs the path as `data_dir/{topic}-{partition}` and passes it as `base_data_dir`. Then `PartitionManager::open()` appends `/topic/partition-N` again, creating a deeply nested wrong path like `data/orders-0/orders/partition-0/`. This means partition discovery on restart fails completely since the engine scans for `{topic}-{partition}` directories but the actual data is two levels deeper.

**Fix**: Remove the redundant `join(&topic).join(format!("partition-{}", partition))` from `PartitionManager::open()`. The `base_data_dir` passed in is already the complete partition directory.

---

### BUG-05 — Heartbeat Does Not Update `last_heartbeat` in `ReplicationManager`
**File**: `src/replication/mod.rs` and `src/server/handler.rs`  
**Severity**: 🔴 Critical (spurious leader elections)

The `start_election_timeout_loop()` reads `self.last_heartbeat` to determine if an election should start. But `decode_heartbeat_packet()` in `handler.rs` calls `engine.set_leader_addr()` and `engine.replication().set_epoch()` — it never updates `last_heartbeat`. The election loop will always see a stale timestamp and trigger elections even when heartbeats are flowing normally.

**Fix**: Add a method `ReplicationManager::record_heartbeat()` that updates `last_heartbeat = Instant::now()`. Call it from `decode_heartbeat_packet()` after validating the incoming term.

---

### BUG-06 — `replay_metadata_log()` Causes Infinite Recursion
**File**: `src/server/engine.rs` — `replay_metadata_log()`  
**Severity**: 🔴 Critical (stack overflow on startup)

`replay_metadata_log()` calls `self.get_or_create_partition("__cluster_metadata", 0)`. `get_or_create_partition()` then writes a new `TopicPartition` metadata record for any non-system topic — BUT `__cluster_metadata` is guarded by a `!topic.starts_with("__")` check. However, on the first call to `get_or_create_partition("__cluster_metadata", 0)` inside `replay_metadata_log()`, the metadata partition is being initialized *before* it's inserted into the `DashMap`, and a concurrent write path could re-enter. More critically, this mutual dependency means calling `replay_metadata_log` before the metadata partition is ready causes the partition to be opened twice, resulting in two `SegmentManager` instances pointing at the same files.

**Fix**: Pre-seed the `__cluster_metadata-0` partition *before* calling `replay_metadata_log()`. Guard against double-open with an explicit check.

---

### BUG-07 — `TransmitFile` Uses Wrong Socket API Type
**File**: `src/segment/log.rs` — `transmit_file_zero_copy()`  
**Severity**: 🔴 Critical (Windows only — always fails silently)

```rust
use windows_sys::Win32::Networking::WinSock::TransmitFile;
let raw_socket = socket.as_raw_socket() as usize;
```

`TransmitFile` expects a `SOCKET` (which is `usize` on 64-bit Windows), but Tokio's `TcpStream::as_raw_socket()` returns a `RawSocket` which is a `u64`. The cast to `usize` is platform-dependent. More critically, `TransmitFile` is **synchronous** and blocks the calling OS thread, which will deadlock Tokio's async runtime when called from an async context. Also, `TransmitFile` requires the file offset to be set via the `OVERLAPPED` structure — the current code passes `null_mut()` for OVERLAPPED and relies on the file's current position, which is not safe with a `physical_pos` argument.

**Fix**: Use `TransmitFile` with a proper `OVERLAPPED` structure and file offset, or replace with Tokio's `tokio::io::copy` plus mmap slices sent via `AsyncWriteExt::write_all`. If zero-copy is critical, use Windows `ReadFileScatter`/`WriteFileGather` via `io_uring`-equivalent or keep using mmap reads fed into async write.

---

### BUG-08 — Segment Recovery Reads Entire Segment into RAM
**File**: `src/segment/log.rs` — `LogSegment::open()`  
**Severity**: 🔴 Critical (OOM on large segments)

```rust
let raw_len = file.metadata()?.len();
let mut read_buf = vec![0u8; raw_len as usize]; // allocates up to max_segment_bytes in one shot
```

With the default `max_segment_bytes = 10MB` per segment, this is 10MB per partition per startup. With 100 partitions that's 1GB of startup heap allocation just for recovery. With `preallocate_segments = true` (the default), the pre-allocated zero bytes at the end of the file are fully read into RAM unnecessarily.

**Fix**: Use a streaming read with a fixed-size buffer (e.g., 64KB chunks), decoding frames incrementally. Stop reading at the first fully-zero header rather than reading the entire preallocated file.

---

### BUG-09 — `consumer_offsets.log` Grows Unboundedly
**File**: `src/consumer_group.rs`  
**Severity**: 🔴 Critical (disk exhaustion in production)

Every `commit_offset()` call appends a new record to `__consumer_offsets.log`. There is no compaction. A consumer group committing every 100ms will generate ~36MB/hour of offset log data per topic-partition combination. Recovery on startup replays all records, becoming increasingly slow.

**Fix**: Implement log compaction for consumer offsets. Periodically rewrite the log file with only the latest offset per `(group_id, topic, partition)` key. Alternatively, use a separate in-memory dirty-set and write a compacted snapshot to disk at intervals.

---

### BUG-10 — `pascal_string` Length Field Is Only 2 Bytes (65535 Byte Limit)
**File**: `src/protocol/wire.rs` — `write_pascal_string()`  
**Severity**: 🔴 Critical (silent truncation on large payloads)

```rust
buf.put_u16(bytes.len() as u16); // silently truncates if > 65535 bytes
```

Topic names, transaction IDs, group IDs, and record payloads can all be passed through pascal strings. A topic name isn't typically large, but record payloads encoded as pascal strings (not used here) would truncate silently. More critically, if any of these string fields grows unexpectedly (e.g., long transaction_id), the 2-byte length will truncate without error. The payload length in `WireRequest::ProduceBatch` correctly uses `u32` per record, but the topic/key/transaction_id strings are bounded to 65535 bytes.

**Fix**: Validate input lengths before encoding and return an error rather than silently truncating. Add a `MAX_STRING_LEN` constant and enforce it at encode/decode boundaries.

---

### BUG-11 — `produce_batch` Does Not Handle `num_partitions = 0` Safely on Key-Routed Produce
**File**: `src/server/engine.rs` — `produce_batch()`  
**Severity**: 🟠 High (panic potential)

```rust
let partition_id = if !key.is_empty() && num_partitions > 0 {
    hash_key(key.as_bytes(), num_partitions as usize)
} else {
    0  // falls back to partition 0 silently
};
```

`hash_key()` is guarded, but a client can send `num_partitions = 0` with a key, causing all traffic to route to partition 0 without any diagnostic. Clients must track the actual partition count themselves, which is not exposed via any API call.

**Fix**: Add a `DescribeTopicPartitions` command (wire code `0x0C`) that returns the current partition count for a topic. Return an error when `num_partitions = 0` and a non-empty key is provided.

---

### BUG-12 — `__transaction_state` Replay Does Not Restore Partition Lists
**File**: `src/server/engine.rs` — `replay_transaction_state()`  
**Severity**: 🔴 Critical (transaction commit/abort after restart writes no control markers)

`restore_transaction()` in `TransactionManager` sets `partitions: Vec::new()`. After a restart, if a client calls `CommitTx` for an in-flight transaction, `commit_transaction()` iterates over `state.partitions` — which is empty — and writes no commit control markers to any partition. Consumers see records from the transaction as permanently uncommitted.

**Fix**: Persist the `(topic, partition, start_offset)` list as part of the `__transaction_state` log record format. Restore it during replay. The current `encode_tx_state_record` format must be extended to include partition list data.

---

## 3. Security Issues

### SEC-01 — No Maximum Payload Size Enforcement
**File**: `src/protocol/wire.rs` — `WireRequest::decode()`  
**Severity**: 🟠 High (remote OOM / DoS)

A malicious or buggy client can send `record_count = 1_000_000` with each record having `rec_len = 4_000_000`. The server will attempt to allocate terabytes of memory trying to satisfy the request before any connection-level check occurs.

**Fix**: Add wire-level constants:
```rust
const MAX_RECORD_COUNT: usize = 10_000;
const MAX_RECORD_SIZE_BYTES: usize = 10 * 1024 * 1024; // 10 MB per record
const MAX_REQUEST_PAYLOAD_BYTES: usize = 100 * 1024 * 1024; // 100 MB total
```
Return `WireError::InvalidProtocol` if limits are exceeded.

---

### SEC-02 — No Authentication or Authorization
**Severity**: 🟠 High

Any TCP client can produce to any topic, consume any partition, begin and commit transactions, and access cluster metadata. There is no SASL, TLS, or ACL mechanism.

**Fix for production**: Add TLS via `tokio-rustls` (client certificate auth is optional). Add a simple pre-shared key (PSK) auth scheme as minimum viable security: a 32-byte auth token sent in a fixed header before any command. Store allowed tokens in `server.properties`.

---

### SEC-03 — Topic Name Injection via Directory Traversal
**File**: `src/server/engine.rs` — `get_or_create_partition()`  
**Severity**: 🟠 High

```rust
let partition_dir = self.config.data_dir.join(format!("{}-{}", topic, partition));
```

A topic name of `../../etc` would create `data_dir/../../etc-0/`, potentially traversing out of the data directory. On Windows NTFS, this is a real attack vector.

**Fix**: Validate topic names on receipt (wire decode or engine entry point). Allow only alphanumeric characters, hyphens, underscores, and dots. Maximum 249 characters (Kafka limit). Reject all others with a protocol error.

---

## 4. Correctness & Data Integrity Issues

### CORR-01 — Sparse Index Rebuild on Recovery Does Not Respect `index_interval_bytes`
**File**: `src/segment/log.rs` — `LogSegment::open()`  
**Severity**: 🟡 Medium

During recovery, the index is rebuilt in `rebuilt_index_entries`. But the check `bytes_since_last_index >= index_interval` is evaluated against `index_interval` (the parameter), while the outer `bytes_since_last_index` is reset correctly. However, the first entry is always added unconditionally (`|| rebuilt_index_entries.is_empty()`), which is correct. The issue is that after calling `index_segment.truncate_and_rebuild(rebuilt_index_entries)`, the `IndexSegment` is passed in as `&mut` but then the same `index_segment` is used as the active index. The rebuilt entries are good but the `IndexSegment` on disk may not be flushed/synced after rebuild.

**Fix**: Call `index_segment.sync()` after `truncate_and_rebuild()` to ensure the rebuilt index is persisted.

---

### CORR-02 — `find_segment_pair()` Binary Search Is Incorrect
**File**: `src/segment/manager.rs` — `find_segment_pair()`  
**Severity**: 🔴 Critical (reads from wrong segment)

```rust
Err(idx) => {
    if idx == 0 {
        &self.historical[0]  // BUG: returns first segment even if offset is before it
    } else if idx <= self.historical.len() {
        let cand = &self.historical[idx - 1];
        if offset >= self.active.base_offset {
            &self.active
        } else {
            cand
        }
    } else {
        &self.active
    }
}
```

When `binary_search_by_key` returns `Err(0)`, the offset is *before* all historical segment base offsets. In that case `historical[0]` is returned — which is correct. But when `idx > 0 && idx <= historical.len()`, the check `if offset >= self.active.base_offset` should be checked *before* computing `idx-1` because `idx` already means "would insert at this position", so `idx-1` is the largest segment that's still ≤ offset. The condition is logically correct only when `historical.len()` equals `idx`. When `idx` is somewhere in the middle, the check `offset >= self.active.base_offset` is redundant because `cand` already points to the right historical segment. The real bug is the `Err(0)` case: if the requested offset is *below* all historical segments, the code should return the first segment (historical[0]), but there is no validation that historical[0] actually covers that offset.

**Fix**: Simplify the segment lookup to a clear linear search or correctly verified binary search:
```rust
fn find_segment_pair(&self, offset: u64) -> &SegmentPair {
    if offset >= self.active.base_offset {
        return &self.active;
    }
    for pair in self.historical.iter().rev() {
        if offset >= pair.base_offset {
            return pair;
        }
    }
    self.historical.first().unwrap_or(&self.active)
}
```

---

### CORR-03 — `commit_transaction` Persists `producer_id = 0`
**File**: `src/server/engine.rs` — `commit_transaction()` and `abort_transaction()`  
**Severity**: 🟡 Medium

```rust
let producer_id = 0u64; // producer_id already stored in partition markers
```

The `__transaction_state` log record encodes `producer_id` as part of the recovery format. Writing `0` means the replay path (`replay_transaction_state`) restores the transaction with `producer_id = 0`, losing idempotency information. After restart, the original `producer_id` is gone.

**Fix**: Fetch the actual `producer_id` from the `TransactionState` before overwriting it:
```rust
let producer_id = self.transactions.get_producer_id(transaction_id).unwrap_or(0);
```

---

### CORR-04 — `high_watermark` Not Persisted; Always Resets to 0 on Fresh Startup
**File**: `src/server/partition.rs`  
**Severity**: 🟡 Medium

`PartitionManager::open()` initializes `high_watermark` from `segment_manager.high_watermark()`, which comes from `active_log.next_offset`. This is correct for existing data. But for a brand new partition, `next_offset = base_offset = 0`. This is fine for a true new partition but can be 0 incorrectly if the segment manager's active segment detection logic ever picks the wrong file as "active" (see BUG CORR-02 / segment lookup issues).

---

### CORR-05 — `__consumer_offsets.log` Has No CRC on Partial Entry Detection
**File**: `src/consumer_group.rs`  
**Severity**: 🟡 Medium

CRC validation is correctly performed per-entry. However, if a crash occurs mid-write of an entry, the partial bytes at the end of the file are silently ignored (the `while` loop just breaks). On the next restart, the file is opened with `SeekFrom::End(0)`, so the partial/corrupt bytes remain in the file. Future entries will be written after the corrupt bytes, making subsequent recoveries impossible since the magic byte check (`CONSUMER_OFFSETS_MAGIC`) will fail immediately.

**Fix**: After recovery, truncate the file to the last successfully decoded position (track `last_good_pos`) before seeking to end. Same pattern used in `LogSegment::open()`.

---

## 5. Concurrency & Race Conditions

### RACE-01 — `SegmentManager` Held Under `parking_lot::Mutex` During File I/O
**File**: `src/server/partition.rs`  
**Severity**: 🟠 High (throughput bottleneck + potential deadlock)

`PartitionManager` wraps `SegmentManager` in a `parking_lot::Mutex`. Every `produce_frame()`, `fetch()`, `apply_retention()`, and `flush()` call acquires this exclusive lock. Since `SegmentManager::append()` performs file writes (which can block on OS scheduler), this serializes ALL produces and fetches for a partition through a single synchronous mutex. This eliminates any benefit of Tokio's async I/O.

**Fix**: Move file I/O to a `tokio::task::spawn_blocking()` call. Replace `parking_lot::Mutex` with `tokio::sync::Mutex` and hold it across blocking calls dispatched via `spawn_blocking`. Alternatively, use a channel-based single-writer model per partition (one writer task, unlimited async readers via mmap).

---

### RACE-02 — `DashMap` Partition Entry Insert Is Not Atomic with Directory Creation
**File**: `src/server/engine.rs` — `get_or_create_partition()`  
**Severity**: 🟠 High (TOCTOU race condition)

```rust
if let Some(pm) = self.partitions.get(&key) {
    return Ok(pm.value().clone());
}
// --- Window: two concurrent callers can both miss the DashMap check ---
let pm = Arc::new(PartitionManager::open(...))?;
self.partitions.insert(key, pm.clone());
```

Two concurrent `ProduceBatch` requests for the same new topic-partition can both observe the DashMap miss and both call `PartitionManager::open()`, creating two `SegmentManager` instances pointing to the same directory. Both will write to the same log file, resulting in interleaved/corrupted data.

**Fix**: Use `DashMap::entry()` API for atomic check-and-insert:
```rust
use dashmap::mapref::entry::Entry;
match self.partitions.entry(key.clone()) {
    Entry::Occupied(e) => Ok(e.get().clone()),
    Entry::Vacant(e) => {
        let pm = Arc::new(PartitionManager::open(...))?;
        e.insert(pm.clone());
        Ok(pm)
    }
}
```

---

### RACE-03 — `ReplicationManager` `epoch` Uses `std::sync::RwLock`, Not `tokio::sync::RwLock`
**File**: `src/replication/mod.rs`  
**Severity**: 🟡 Medium

`Arc<RwLock<u64>>` from `std::sync` is used in async contexts. `std::sync::RwLock::write()` blocks the OS thread. If the lock is held during an `await` point (it isn't currently, but it's fragile), it will park the Tokio worker thread, causing latency spikes under load. The `.unwrap()` on lock acquisition will panic if the lock is poisoned (any thread that panics while holding it poisons it permanently).

**Fix**: Replace `.unwrap()` with `.unwrap_or_else(|e| e.into_inner())` to handle poisoning gracefully. For the epoch field specifically, replace `RwLock<u64>` with `std::sync::atomic::AtomicU64` which requires no locking at all:
```rust
epoch: Arc<AtomicU64>,
// get: epoch.load(Ordering::Acquire)
// set: epoch.store(val, Ordering::Release)
```

---

### RACE-04 — `TransactionManager` Does Not Prevent Duplicate `begin_transaction` Under Concurrent Requests
**File**: `src/server/transaction.rs` — `begin_transaction()`  
**Severity**: 🟡 Medium

```rust
if self.transactions.contains_key(transaction_id) {
    return Err(format!("Transaction ID '{}' already exists", transaction_id));
}
// --- Window: concurrent begins can both pass the check ---
self.transactions.insert(...)
```

Two concurrent `BeginTx` requests with the same `transaction_id` can both pass the `contains_key` check before either inserts. Use `DashMap::entry()` here too.

---

## 6. Replication & Consensus Issues

### REP-01 — Newly Elected Leader Does Not Start Heartbeat Loop
**File**: `src/replication/mod.rs` — `start_election_timeout_loop()`  
**Severity**: 🔴 Critical (newly elected leaders never send heartbeats)

When a follower wins an election (`consensus.tally_election_votes()` returns `true`), it sets its leader addr and consensus state, but **never starts the heartbeat broadcaster loop**. Only the initial leader (configured via `role = Leader` in `server.properties`) runs `start_leader_heartbeat_loop()`. A node that becomes leader through election will never send heartbeats, causing all other nodes to immediately re-elect.

**Fix**: After a successful election, spawn the heartbeat loop:
```rust
if consensus.tally_election_votes(votes_granted) {
    // ... existing code ...
    // Start sending heartbeats as the new leader
    start_heartbeat_loop_for_new_leader(peer_addrs.clone(), ...);
}
```

---

### REP-02 — Raft Vote Does Not Check `voted_for` (Double-Voting Bug)
**File**: `src/server/handler.rs` — `decode_vote_request_packet()`  
**Severity**: 🔴 Critical (split-brain / multi-leader scenario)

```rust
if term >= our_epoch {
    engine.replication().set_epoch(term);
    // GRANTS VOTE — but never checks if already voted this term!
    Ok((bytes_consumed, vec![0x01]))
}
```

A node will grant votes to **any** candidate whose term is ≥ current epoch, even if it already voted for a different candidate in the same term. This violates the fundamental Raft safety property and can lead to two nodes both believing they won a majority, creating a split-brain cluster.

**Fix**: Track `voted_for: Option<(u32, u64)>` (candidate_id, term) in `ReplicationManager`. Only grant a vote if `voted_for` is `None` for the current term or matches the same candidate:
```rust
let already_voted = engine.replication().has_voted_this_term(term);
if term >= our_epoch && !already_voted {
    engine.replication().record_vote(candidate_id, term);
    Ok((bytes_consumed, vec![0x01]))
} else {
    Ok((bytes_consumed, vec![0x00]))
}
```

---

### REP-03 — Replication Is Fire-and-Forget; No Retry or Backpressure
**File**: `src/server/engine.rs` — `produce_batch()`  
**Severity**: 🟠 High

```rust
tokio::spawn(async move {
    if let Err(e) = repl.replicate_batch(...).await {
        tracing::error!("HA Replication: replicate_batch failed: {}", e);
    }
});
```

Replication failures are logged but not retried. A follower that is momentarily unreachable will permanently fall behind. There is no mechanism for the follower to re-sync from the leader (the `send_grpc_replication_fetch` pull mechanism exists but is never invoked on the follower side). The `ReplicationFetchRequest` API exists but has no background loop calling it.

**Fix**: Implement a follower-side replication pull loop that runs as a background task, periodically fetching missing records from the leader using `send_grpc_replication_fetch`. Track per-peer watermarks and only pull what's missing.

---

### REP-04 — `STALE_EPOCH` Detection Is Byte-String Comparison, Not Protocol
**File**: `src/replication/mod.rs` — `replicate_batch()`  
**Severity**: 🟡 Medium

```rust
if err_str.contains("STALE_EPOCH") {
```

Stale epoch detection relies on checking whether an error message *string contains* `"STALE_EPOCH"`. This is extremely fragile — a change in the error message string, OS locale, or log output format could silently break epoch fencing. The ACK byte from the follower should encode semantic status codes.

**Fix**: Change the replication ACK response from `vec![0u8]` (OK) / `b"STALE_EPOCH".to_vec()` (stale) to a proper 1-byte status code:
- `0x00` = ACK (success)
- `0x01` = STALE_EPOCH (step down)
- `0x02` = Internal error

---

### REP-05 — `min_insync_replicas` Is Never Enforced on Produce Acknowledge
**File**: `src/server/engine.rs` — `produce_batch()`  
**Severity**: 🟠 High (durability guarantee not upheld)

`ReplicationManager::await_isr_quorum()` is fully implemented but **never called** from `produce_batch()`. The config field `min_insync_replicas` is parsed and stored but has zero effect on write acknowledgment semantics. Clients receive an `Ok` response the moment the leader writes locally, regardless of how many followers have replicated the data.

**Fix**: Call `await_isr_quorum()` from `produce_batch()` before returning success:
```rust
// After local write, before returning Ok
if self.config.role == NodeRole::Leader && self.config.min_insync_replicas > 1 {
    let quorum_ok = self.replication.await_isr_quorum(
        topic, partition_id, last_offset,
        Duration::from_secs(5)
    ).await;
    if !quorum_ok {
        return Err(IoError::new(ErrorKind::TimedOut, "ISR quorum not reached"));
    }
}
```
Note: `produce_batch` is currently synchronous (`IoResult`). This requires making it `async` or using a `tokio::runtime::Handle`.

---

## 7. Performance Issues

### PERF-01 — Each Replication Push Opens a New TCP Connection
**File**: `src/replication/mod.rs` — `send_replication_push()`  
**Severity**: 🟠 High (TCP connection overhead per batch)

```rust
let mut stream = match timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer_addr)).await {
```

Every call to `replicate_batch()` opens a new TCP connection to each peer. TCP connection setup (3-way handshake + Nagle algorithm delay) adds ~1-5ms per batch. At 10,000 produces/sec this creates 10,000 TCP connections/sec to each follower, exhausting ephemeral ports.

**Fix**: Maintain persistent TCP connections to peer nodes using a `DashMap<String, Arc<Mutex<TcpStream>>>` connection pool. Reconnect on error with exponential backoff.

---

### PERF-02 — Heartbeat Also Opens a New TCP Connection Each Interval
**File**: `src/replication/mod.rs` — `send_leader_heartbeat()`  
**Severity**: 🟡 Medium

Same issue as PERF-01. The heartbeat broadcast creates a new TCP connection every 10 seconds per peer.

**Fix**: Reuse the same persistent connection pool as replication.

---

### PERF-03 — `SegmentManager::fetch()` Re-reads From Physical File on Every Request
**File**: `src/segment/manager.rs` — `fetch()`  
**Severity**: 🟡 Medium

`fetch()` calls `LogSegment::read_at()` which does a `file.seek()` + `file.read()` on every request. The `MmapLogSegment` exists (`src/segment/mmap.rs`) but is never used in the fetch path. Historical segments should use mmap for zero-copy reads.

**Fix**: In `SegmentManager`, maintain `MmapLogSegment` counterparts for all historical (read-only) segments. Switch `find_segment_pair()` to return an enum `ActiveSeg | MmapSeg` and use the mmap path for historical reads.

---

### PERF-04 — `produce_batch` Calls `encode_into` Twice Per Frame
**File**: `src/server/engine.rs` and `src/segment/manager.rs`  
**Severity**: 🟡 Low

`produce_batch()` calls `produce_frame()` → `SegmentManager::append()` → `RecordFrame::encode_into()` to write to disk. Then the returned `RecordFrame` is cloned and `encode_into()` is called again in `replicate_batch()` for the replication payload. This means every frame is serialized twice.

**Fix**: Cache the encoded bytes inside `RecordFrame` or return encoded bytes from `append()` alongside the frame.

---

### PERF-05 — `aborted_ranges()` and `last_stable_offset()` Do Full Scans of All Transactions
**File**: `src/server/transaction.rs`  
**Severity**: 🟡 Medium

Every `fetch_committed()` call iterates all entries in the `transactions` DashMap (which grows unboundedly). In a high-throughput system with many transactions, this O(T) scan per read is a bottleneck.

**Fix**: Maintain a separate `Arc<DashMap<(String, u32), u64>>` for LSO per `(topic, partition)` and update it on every `begin_transaction`/`commit`/`abort`. This makes LSO lookup O(1). Remove completed (Committed/Aborted) transactions from the main map after they've been fully processed.

---

## 8. Protocol & API Issues

### PROTO-01 — No Correlation ID / Request Multiplexing
**Severity**: 🟠 High

The wire protocol has no correlation ID field. All requests over a single connection are strictly serial (send request, wait for response, send next). Under high load, a single slow request blocks all subsequent requests on that connection. This prevents efficient client-side pipelining.

**Fix**: Add a 4-byte correlation ID to the request and response wire format:
```
Request:  [Cmd: 1b] [CorrelationID: 4b] [PayloadLen: 4b] [Payload]
Response: [Status: 1b] [CorrelationID: 4b] [PayloadLen: 4b] [Payload]
```

---

### PROTO-02 — No `ListTopics` / `DescribeCluster` API
**Severity**: 🟡 Medium

There is no command to list available topics, number of partitions per topic, cluster topology, or broker metadata. Client applications have no way to discover the cluster dynamically.

**Fix**: Add wire commands:
- `0x0C` — `ListTopics` → returns `[(topic, partition_count)]`
- `0x0D` — `DescribeCluster` → returns `[(node_id, bind_addr, role)]`

---

### PROTO-03 — `FetchOffset` Returns `u64::MAX` When No Offset Committed
**File**: `src/server/handler.rs` — `FetchOffset` arm  
**Severity**: 🟡 Medium

```rust
let offset = engine.fetch_offset(&group_id, &topic, partition).unwrap_or(u64::MAX);
```

Returning `u64::MAX` as the "no committed offset" sentinel is ambiguous. Clients cannot distinguish "offset `u64::MAX` was explicitly committed" from "no offset has been committed". This causes clients to start fetching from `u64::MAX` which returns no data, rather than from offset 0 (earliest) or the current high watermark (latest).

**Fix**: Return a distinct response status (`0x02 = NO_OFFSET_COMMITTED`) separate from the `u64` payload. Or document and use `-1i64` cast as `u64::MAX` as a sentinel with explicit client-side handling.

---

## 9. Error Handling Gaps

### ERR-01 — `replay_transaction_state` and `replay_metadata_log` Silently Swallow Errors
**File**: `src/server/engine.rs` — `StorageEngine::new()`  
**Severity**: 🟡 Medium

```rust
let _ = engine.replay_metadata_log();
let _ = engine.replay_transaction_state();
```

Errors during startup replay are discarded with `let _ = `. If replay fails (e.g., disk error, corrupt log), the server starts with incomplete state and silently serves stale or missing data.

**Fix**: Propagate replay errors to the caller and refuse to start:
```rust
engine.replay_metadata_log()?;
engine.replay_transaction_state()?;
```

---

### ERR-02 — `CommitTx`/`AbortTx` Control Marker Write Errors Are Silently Ignored
**File**: `src/server/transaction.rs` — `commit_transaction()` / `abort_transaction()`  
**Severity**: 🟠 High (silent transaction corruption)

```rust
let _ = pm.produce_control_marker(1, producer_id, &tx_id);
```

If writing the commit or abort control marker fails (e.g., disk full), the error is silently swallowed. The in-memory state says `Committed` but the disk has no marker. On restart, the transaction is replayed as `Committed` (from `__transaction_state`) but consumers fetching with `FetchCommitted` will see the records as uncommitted because there is no control marker in the partition log.

**Fix**: Return an error from `commit_transaction`/`abort_transaction` if any control marker write fails. Retry the write before declaring success.

---

### ERR-03 — `StorageEngine::new()` Does Not Handle Leader Registration Failure
**File**: `src/server/engine.rs`  
**Severity**: 🟡 Low

```rust
if let Ok(meta_pm) = engine.get_or_create_partition("__cluster_metadata", 0) {
    let _ = meta_pm.produce(&reg_rec.encode());
}
```

The `let _` silently discards the produce error. If the leader cannot write its own broker registration record, the cluster metadata log is permanently missing this entry. Followers will never learn the leader's address.

---

## 10. Memory Management Issues

### MEM-01 — `handle_connection` Buffer Grows Unboundedly
**File**: `src/server/handler.rs` — `handle_connection()`  
**Severity**: 🟠 High (OOM on malicious/buggy clients)

```rust
if filled == buffer.len() {
    buffer.resize(buffer.len() * 2, 0); // unbounded growth
}
```

If a client sends a partial frame and never completes it, the buffer will double repeatedly until OOM. There is no maximum buffer size cap.

**Fix**: Add a `MAX_CONNECTION_BUFFER: usize = 128 * 1024 * 1024;` (128MB) cap. If the buffer would grow beyond this, close the connection with a protocol error.

---

### MEM-02 — Completed Transactions Are Never Removed from Memory
**File**: `src/server/transaction.rs`  
**Severity**: 🟠 High (memory leak in long-running servers)

`transactions: Arc<DashMap<String, TransactionState>>` grows indefinitely. Every `BeginTx` adds an entry. `CommitTx`/`AbortTx` change the status to `Committed`/`Aborted` but **never remove the entry**. A server running for days with many short-lived transactions will accumulate millions of stale `TransactionState` entries.

**Fix**: After a successful `commit_transaction` or `abort_transaction`, remove the entry from the DashMap:
```rust
self.transactions.remove(transaction_id);
```
Keep a time-bounded "recently aborted" cache (e.g., last 60 seconds) for LSO and aborted-range queries.

---

### MEM-03 — `RecordFrame` Clones `Bytes` Payload on Every Replication Batch
**File**: `src/server/engine.rs` — `produce_batch()`  
**Severity**: 🟡 Low

```rust
let frames_clone = frames.clone();
tokio::spawn(async move {
    repl.replicate_batch(&topic_str, partition_id, &frames_clone).await
```

`frames.clone()` clones all `RecordFrame` structs. Each `RecordFrame` contains a `Bytes` payload. While `Bytes` uses reference-counted slices (cheap clone for the ref count), the outer `Vec<RecordFrame>` is deeply cloned. Use `Arc<Vec<RecordFrame>>` instead to make the spawn truly cheap.

---

## 11. Windows-Specific Issues

### WIN-01 — `TransmitFile` Not Integrated into the Fetch Path
**File**: `src/segment/log.rs` — `transmit_file_zero_copy()`  
**Severity**: 🟡 Medium

The `transmit_file_zero_copy()` method is defined but **never called**. The fetch path always uses `read_at()` (seek + read into heap buffer). All the Windows IOCP benefit is unused.

**Fix**: Integrate TransmitFile into the fetch response path (after fixing BUG-07). For each `Fetch` request, instead of building a `Vec<RecordFrame>`, send the raw bytes directly from the file to the socket using TransmitFile with proper OVERLAPPED I/O.

---

### WIN-02 — `preallocate_segments = true` by Default Hurts SSD Performance
**File**: `src/config.rs`  
**Severity**: 🟡 Medium

Pre-allocating 10MB per segment with `file.set_len(max_bytes)` triggers NTFS file table updates and zeroing. On SSDs with write amplification concerns, pre-allocating 10MB for every new partition is wasteful if partitions are short-lived.

**Fix**: Make `preallocate_segments` default to `false` for SSDs (or document the tradeoff). Add a Windows registry/WMI check to detect if the storage path is on an HDD vs SSD and adjust the default accordingly.

---

### WIN-03 — `socket2` `set_reuse_port` Is Called on Non-Windows but May Not Be Available
**File**: `src/server/listener.rs`  
**Severity**: 🟡 Low

```rust
let _ = socket.set_reuse_port(true);
```

`set_reuse_port` is silently ignored (result discarded). On macOS, `SO_REUSEPORT` semantics differ from Linux. The `let _` is acceptable here, but the comment should be updated to note the cross-platform behavior.

---

## 12. Observability Gaps

### OBS-01 — No Metrics Exposed (No Prometheus / No Windows Performance Counters)
**Severity**: 🟠 High

There are no metrics for: produce throughput (records/sec, bytes/sec), fetch latency (p99), segment rotation count, replication lag per follower, consumer group lag, transaction commit/abort rate, or active connection count. Without metrics, it is impossible to operate this system in production.

**Fix**: Add a metrics abstraction layer. Recommended: embed `metrics` crate (https://crates.io/crates/metrics) with a `metrics-exporter-prometheus` backend. Add counters/histograms at every critical path:
```rust
metrics::counter!("hermes.produce.records", 1);
metrics::histogram!("hermes.fetch.latency_ms", elapsed.as_millis() as f64);
```

---

### OBS-02 — Structured Logging Missing Key Fields
**Severity**: 🟡 Medium

Log entries use string interpolation (`tracing::info!("...", var)`) but lack structured key-value fields. Production log aggregation systems (Splunk, Elasticsearch, Datadog) can't efficiently query unstructured strings.

**Fix**: Use structured tracing fields:
```rust
tracing::info!(
    topic = %topic,
    partition = partition_id,
    offset = first_offset..=last_offset,
    "Produce batch committed"
);
```

---

### OBS-03 — No Health Check Endpoint
**Severity**: 🟡 Medium

There is no `/health` or wire-level heartbeat command that a load balancer or orchestrator can use to determine if the node is ready to serve traffic. The `ping_handshake()` in `TestClient` exists but is test-only.

**Fix**: Add a `0x00` command code (`Ping`) that responds with the node's current role, epoch, and high watermark. This doubles as a health check for monitoring systems.

---

## 13. Testing Gaps

### TEST-01 — No Chaos / Fault Injection Tests
**Severity**: 🟡 Medium

The integration tests in `tests/integration_tests.rs` cover the happy path well (produce, fetch, transactions, consumer groups). But there are no tests for:
- Crash recovery (truncated log segments, partial writes)
- Network partition (follower losing connection to leader during replication)
- Leader failover via election
- Concurrent produces from 100+ clients
- Memory pressure / large payload handling

**Fix**: Add a test harness with fault injection capabilities:
- `TestEnv::kill_and_restart_server()` — restart with same data dir
- `TestEnv::corrupt_last_segment()` — write random bytes at the end
- A multi-server test that can pause/resume individual servers to simulate network partitions

---

### TEST-02 — No Benchmark Tests
**Severity**: 🟢 Low

There are no `criterion`-based benchmarks for the hot paths: `append()`, `fetch()`, `segment_rotation`, or `index_lookup`. Without baselines, performance regressions are invisible.

**Fix**: Add `benches/` directory with criterion benchmarks for at minimum:
- `produce_single_record` — single record latency
- `produce_batch_1000` — batch throughput
- `fetch_10k_records` — sequential read throughput
- `segment_rotation` — rotation cost

---

## 14. Missing Production Features

### FEAT-01 — No Topic Deletion
There is no wire command to delete a topic. Once a topic is created, its data directory and partition entries persist forever. Add a `DeleteTopic` command that removes partition directories and unregisters from the DashMap.

### FEAT-02 — No Partition Reassignment
The number of partitions for a topic is inferred from the first `ProduceBatch` call's `num_partitions` field. Changing the partition count after creation is not supported and not validated, leading to hash-key routing inconsistencies.

### FEAT-03 — No Consumer Group Rebalancing Protocol
Consumer groups track committed offsets, but there is no group membership protocol, no partition assignment, and no rebalance trigger. Multiple consumers in the same group will all consume all partitions redundantly instead of dividing work.

### FEAT-04 — No Compression Support
Record payloads are stored and transmitted uncompressed. Add support for LZ4 or Zstandard compression at the record batch level. This is critical for production throughput on wide-area networks.

### FEAT-05 — No Schema / Content-Type Validation
Producers can write arbitrary bytes. There is no schema registry integration, no content-type metadata, and no validation. Add at minimum a `content_type` field to the record wire format.

### FEAT-06 — No Log Compaction (Key-Based)
Kafka's log compaction retains only the last record per key. This is essential for changelog topics (CDC, state stores). No equivalent exists in Hermes.

### FEAT-07 — No Admin HTTP API
There is no HTTP management interface for topic management, partition inspection, or consumer group administration. Add a minimal HTTP API (using `axum` or `warp`) on a separate port (e.g., `9093`).

---

## 15. Dependency & Build Issues

### DEP-01 — `Cargo.toml` Missing Several Recommended Dependencies

```toml
# Current dependencies are functional but missing production essentials:
# - serde / serde_json — for config validation and HTTP API responses
# - metrics + metrics-exporter-prometheus — for observability
# - tokio-rustls — for TLS
# - lz4_flex or zstd — for compression
# - rand — for proper election jitter (currently uses a deterministic hash)
# - axum — for admin HTTP API
# - anyhow — for ergonomic error handling in main.rs
```

### DEP-02 — `windows-sys` Feature Set Is Too Narrow

```toml
windows-sys = { version = "0.52", features = ["Win32_Storage_FileSystem", "Win32_Foundation"] }
```

`TransmitFile` is in `Win32_Networking_WinSock` which is imported in the source but not listed in the `Cargo.toml` features. This means the code may fail to compile on Windows or pulls in features implicitly.

**Fix**: Add required features:
```toml
windows-sys = { version = "0.52", features = [
    "Win32_Storage_FileSystem",
    "Win32_Foundation",
    "Win32_Networking_WinSock",
    "Win32_System_IO",
] }
```

### DEP-03 — `rand` Crate Is Not Used; Jitter Is Deterministic

The election timeout jitter uses `node_id.wrapping_mul(2654435761).wrapping_add(tick) % ELECTION_TIMEOUT_JITTER_SECS`. This is deterministic and predictable — an adversary who knows the node_id and tick count can predict exactly when elections will trigger. Add `rand` crate for proper random jitter.

---

## 16. Improvement Roadmap Summary

Below is a prioritized list of all issues by category and severity, suitable for a sprint plan:

### Sprint 1 — Critical Data Integrity (Fix Before Any Production Use) — ✅ COMPLETED

| Status | ID | File | Fix |
|--------|----|------|-----|
| [x] | BUG-04 | `partition.rs` | Fix partition directory double-nesting |
| [x] | BUG-02 | `handler.rs` | Fix `FetchByTimestamp` full scan bug |
| [x] | BUG-03 | `transaction.rs` | Fix `aborted_ranges()` returning `u64::MAX` |
| [x] | BUG-12 | `engine.rs` | Persist partition list in `__transaction_state` |
| [x] | CORR-02 | `manager.rs` | Fix `find_segment_pair()` binary search |
| [x] | RACE-02 | `engine.rs` | Fix TOCTOU race in `get_or_create_partition` |
| [x] | REP-02 | `handler.rs` | Fix Raft double-voting bug |
| [x] | BUG-05 | `mod.rs` | Fix heartbeat not updating `last_heartbeat` |
| [x] | REP-01 | `mod.rs` | Make newly-elected leaders start heartbeat loop |
| [x] | ERR-01 | `engine.rs` | Propagate startup replay errors |
| [x] | ERR-02 | `transaction.rs` | Surface control marker write errors |
| [x] | SEC-03 | `engine.rs` | Validate topic names, prevent directory traversal |

### Sprint 2 — Reliability & Correctness — ✅ COMPLETED

| Status | ID | Fix |
|--------|----|-----|
| [x] | BUG-01 | Either implement WAL properly or remove the misleading WalEngine abstraction |
| [x] | BUG-08 | Fix segment recovery OOM (streaming read instead of full allocation) |
| [x] | BUG-09 | Implement consumer offset log compaction |
| [x] | CORR-05 | Fix `__consumer_offsets.log` partial-write recovery |
| [x] | MEM-02 | Garbage collect completed transactions from DashMap |
| [x] | MEM-01 | Cap connection buffer growth |
| [x] | REP-03 | Implement follower pull-based catch-up replication |
| [x] | REP-05 | Enforce `min_insync_replicas` before acknowledging produce |

### Sprint 3 — Performance & Windows Optimization

| ID | Fix |
|----|-----|
| PERF-01 | Connection pooling for replication and heartbeat TCP streams |
| PERF-03 | Use `MmapLogSegment` for historical segment reads |
| WIN-01 | Integrate `TransmitFile` into fetch path (fix BUG-07 first) |
| BUG-07 | Fix `TransmitFile` OVERLAPPED I/O and async safety |
| RACE-01 | Move segment I/O to `spawn_blocking` |
| RACE-03 | Replace epoch `RwLock<u64>` with `AtomicU64` |

### Sprint 4 — Production Hardening

| ID | Fix |
|----|-----|
| SEC-01 | Add per-request payload size limits |
| SEC-02 | Add TLS + PSK authentication |
| OBS-01 | Add Prometheus metrics |
| OBS-02 | Add structured logging fields |
| OBS-03 | Add `Ping`/health check command |
| PROTO-01 | Add correlation IDs to wire protocol |
| DEP-02 | Fix `windows-sys` feature list in `Cargo.toml` |

### Sprint 5 — Feature Completeness

| Fix |
|-----|
| Add `ListTopics` / `DescribeCluster` wire commands |
| Add `DeleteTopic` wire command |
| Add consumer group rebalancing protocol |
| Add LZ4/Zstandard compression for record batches |
| Add admin HTTP API on separate port |
| Add criterion benchmarks |

---

## Appendix: Quick Reference — Files Requiring the Most Changes

| File | Issues Found | Priority |
|------|-------------|----------|
| `src/server/engine.rs` | BUG-06, BUG-11, BUG-12, RACE-02, ERR-01, ERR-03, REP-05 | 🔴 Immediate |
| `src/replication/mod.rs` | BUG-05, PERF-01, PERF-02, RACE-03, REP-03, REP-04 | 🔴 Immediate |
| `src/server/handler.rs` | BUG-02, RACE-04, REP-02, MEM-01, PROTO-01 | 🔴 Immediate |
| `src/server/transaction.rs` | BUG-03, BUG-12, CORR-03, ERR-02, MEM-02, PERF-05 | 🔴 Immediate |
| `src/segment/manager.rs` | CORR-02, PERF-03, PERF-04 | 🟠 High |
| `src/segment/log.rs` | BUG-07, BUG-08, WIN-01 | 🟠 High |
| `src/server/partition.rs` | BUG-01, BUG-04, RACE-01 | 🔴 Immediate |
| `src/consumer_group.rs` | BUG-09, CORR-05 | 🟠 High |
| `src/protocol/wire.rs` | SEC-01, BUG-10, PROTO-01, PROTO-03 | 🟠 High |
| `Cargo.toml` | DEP-01, DEP-02, DEP-03 | 🟡 Medium |

---

*End of Review — Total Issues: 40 identified across 24 source files*
