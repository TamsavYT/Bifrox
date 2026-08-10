# Hermes Security & Bug Audit Findings
Date: 2026-08-10

## Summary

| # | Severity | File | Line | Issue |
|---|----------|------|------|-------|
| 1 | Critical | src/server/handler.rs | 433 | Disk failure silently ACKed as success — silent data loss |
| 2 | Critical | src/server/handler.rs | 307 | Dead vote-peer validation — always evaluates false |
| 3 | Critical | src/server/handler.rs | 426 | Replication partial-parse duplicates records on TCP fragment |
| 4 | Critical | src/server/listener.rs | 89  | Zero client authentication |
| 5 | High | src/replication/mod.rs | 451 | STALE_EPOCH fencing broken end-to-end — split-brain |
| 6 | High | src/server/handler.rs | 559 | Heartbeat whitelist bypass / SSRF via 0.0.0.0 bind |
| 7 | High | src/server/handler.rs | 220 | forward_to_leader missing read timeout |
| 8 | High | src/server/handler.rs | 757 | DeleteTopic skips validate_topic_name |
| 9 | High | src/protocol/wire.rs | 385 | write_pascal_string silently truncates strings >65535 bytes |
| 10 | High | src/replication/mod.rs | 340 | Heartbeat task leak on every Raft election win |
| 11 | High | src/replication/grpc.rs | 127 | Follower pull has no connect timeout |
| 12 | Medium | src/client.rs | 110 | Client panics on short server responses via unwrap() |
| 13 | Medium | src/consumer_group.rs | 182 | compact_log non-atomic — crash destroys all committed offsets |
| 14 | Medium | src/server/engine.rs | 471 | delete_topic prefix match deletes sibling topics |
| 15 | Medium | src/consumer_group.rs | 41 | Consumer-offsets recovery: no size cap, OOM risk |
| 16 | Medium | src/server/engine.rs | 455 | TOCTOU race in delete_topic vs get_or_create_partition |
| 17 | Medium | src/server/transaction.rs | 57 | producer_sequences DashMap grows without bound |

---

## Critical Findings (Detailed)

### C1 — Silent Data Loss on Replication Disk Failure
**File:** `src/server/handler.rs` (write-pass section)
**Status:** Fixed (2026-08-10)

If any `produce_frame` call fails mid-batch (disk full, I/O error), the error is logged
and the loop continues. After all iterations, the function still returns a zero (success)
ACK. The leader advances its replication watermark and counts this follower as in-sync,
but the follower silently dropped one or more records with no NACK, no reconnect, and
no recovery path.

**Fix:** Propagate `produce_frame` errors immediately out of the decode loop and return
a non-zero ACK so the leader retries or removes this follower from ISR.

---

### C2 — Dead Vote-Peer Validation
**File:** `src/server/handler.rs:307`
**Status:** Fixed (2026-08-10)

```rust
// BUGGY — any(|_| true) is a tautology; the rejection block is unreachable
let known_peer = engine.config().peer_addrs.iter().any(|_| true);
if !known_peer && !engine.config().peer_addrs.is_empty() { ... }
```

`Iterator::any(|_| true)` returns true whenever the iterator is non-empty, so
`known_peer` is always true when `peer_addrs` is non-empty. The condition
`!known_peer && !peer_addrs.is_empty()` is therefore always false. Any external node
that knows the `cluster_id` (logged at startup) can send a VoteRequest and have it
granted, manipulating Raft leader elections.

**Fix:** Actually compare the candidate's node_id against known peer addresses.

---

### C3 — Replication Partial-Parse Duplicates Records on TCP Fragment
**File:** `src/server/handler.rs:426`
**Status:** Fixed (2026-08-10)

`decode_replication_packet` writes each decoded frame to disk inside the parse loop
before the full batch has arrived. If the TCP stream delivers the packet in fragments,
the function hits `NeedMoreData` partway through and returns without advancing `consumed`.
On the next call it receives the same bytes-plus-more, replays already-written frames
at new offsets, producing permanent leader/follower divergence.

**Fix:** Separate parsing from writing — collect all frames into a Vec in a first pass,
verify completeness, then apply all writes in a second pass.

---

### C4 — Zero Client Authentication
**Files:** `src/config.rs`, `src/server/handler.rs`
**Status:** Fixed (2026-08-10)

The TCP listener accepts every connection and dispatches it directly to the command
handler with no authentication, TLS, token, or IP allowlist. All commands including
`DeleteTopic`, `AbortTx`, and writes to internal partitions are reachable by any
anonymous peer.

**Fix:** Added a simple shared-secret authentication handshake at connection open.
The client must send a fixed-length token matching `config.auth_token` before any
command is processed. Connections that fail authentication are immediately closed.
If `auth_token` is not configured the check is skipped (backward-compatible default).

---

---

### N7 — Transaction ID Reuse Blocked After Restart (replay inserts committed/aborted entries)
**File:** `src/server/engine.rs:232`
**Status:** Fixed (2026-08-10)

`replay_transaction_state` called `restore_transaction` for every log entry, including
Committed and Aborted ones. After a restart, any producer that reused a previously
committed transaction ID got `Err("Transaction ID '...' already exists")` from
`begin_transaction`. Aborted entries also caused `aborted_ranges` to return stale
ranges, incorrectly filtering reads for unrelated producers on the same partitions.

**Fix:** Only restore `Ongoing` transactions during replay; skip Committed/Aborted entries
since their effects are already baked into the partition log data.

---

### N8 — forward_to_leader Skips Auth Handshake — All Forwards Fail With auth_token Set
**File:** `src/server/handler.rs:241`
**Status:** Fixed (2026-08-10)

Regression introduced by the C4 auth fix: `forward_to_leader` opened a TCP connection
to the leader and sent the raw `WireRequest` without first sending the auth magic + token.
The leader's `handle_connection` rejected the first bytes as a failed auth handshake and
closed the connection. All follower→leader produce forwards returned an error to clients.

**Fix:** `forward_to_leader` now accepts `auth_token: Option<&str>` and, when set, writes
`[0xCA 0xFE 0xBA 0xBE][token]` before the request payload.

---

## High Findings (Detailed)

### H5 — STALE_EPOCH Epoch Fencing Never Triggers Leader Step-Down
**File:** `src/replication/mod.rs:451`, `src/server/handler.rs`
**Status:** Fixed (2026-08-10)

Three-part bug:
1. Follower returns 11-byte literal `"STALE_EPOCH"` to socket.
2. Leader reads only 1 byte ACK (`'S'` = 0x53 ≠ 0), builds error `"Replication ACK failed"`.
3. Step-down check tests `err_str.contains("STALE_EPOCH")` — always false.

Epoch fencing is completely non-functional; stale leaders never step down.

---

### H6 — Heartbeat Whitelist Bypass / SSRF via 0.0.0.0 Bind
**File:** `src/server/handler.rs:559`
**Status:** Fixed (2026-08-10)

Two failure modes:
1. **Self-loop attack:** attacker sends heartbeat with `leader_bind_addr` equal to the
   follower's own bind address → follower marks itself as leader, forwarded produces loop back.
2. **Wildcard bypass:** when all nodes bind to `0.0.0.0:9092`, every follower's `bind_addr`
   equals `"0.0.0.0:9092"`, so any peer with the `cluster_id` can advertise an arbitrary
   leader address.

---

### H7 — forward_to_leader Missing Read Timeout
**File:** `src/server/handler.rs:220`
**Status:** Fixed (2026-08-10)

`FORWARD_TIMEOUT` wraps only `TcpStream::connect`, not the subsequent `read_exact` calls.
A stalled leader that accepts the connection but never responds pins the Tokio task forever,
exhausting worker threads.

---

### H8 — DeleteTopic Skips validate_topic_name
**File:** `src/server/handler.rs:757`
**Status:** Fixed (2026-08-10)

`ProduceBatch` calls `validate_topic_name` before proceeding; `DeleteTopic` does not.
Combined with zero authentication (C4), any client can delete system partitions
(`__transaction_state`, `__consumer_offsets`) via `remove_dir_all`.

---

### H9 — write_pascal_string Silently Truncates Strings >65535 Bytes
**File:** `src/protocol/wire.rs:385`
**Status:** Fixed (2026-08-10)

`bytes.len() as u16` wraps silently for strings longer than 65535 bytes, corrupting the
u16 length field and misaligning every subsequent field in the same wire message.

---

### H10 — Heartbeat Task Leak on Every Raft Election Win
**File:** `src/replication/mod.rs:340`
**Status:** Fixed (2026-08-10)

On every election win a new permanent heartbeat loop is spawned with no handle stored and
no cancellation token. Repeated Follower→Leader transitions accumulate unbounded tasks,
eventually exhausting TCP connections and Tokio workers.

---

### H11 — Follower Pull Has No Connect Timeout
**File:** `src/replication/grpc.rs:127`
**Status:** Fixed (2026-08-10)

`TcpStream::connect` in the follower pull loop has no timeout wrapper. If the leader is
unreachable, the OS TCP SYN timeout (up to 127s on Linux) blocks the Tokio task, stalling
follower catch-up for minutes per cycle. The push path already does this correctly.

---

### N5 — Corruption Recovery Truncates at Chunk Boundary, Not Corruption Point
**File:** `src/segment/log.rs:155–163`
**Status:** Open

When corruption is detected inside the inner parse loop, `pos` is overwritten to `buf_len`
(the full 64 KB read-chunk size) before `file_offset += pos`. This advances `physical_size`
past the corrupt frame by up to one full 64 KB chunk. The file is then truncated to that
inflated value, keeping the corrupt frames. On the next startup, `physical_size == raw_len`
so no truncation fires and the corrupt frames survive permanently.

**Fix:** Capture the corruption position before overwriting `pos`, then use that captured
value for the `file_offset` advancement.

---

### N6 — `find_offset_for_timestamp` Always Returns From Oldest Segment
**File:** `src/segment/manager.rs:339`
**Status:** Open

`TimeIndexSegment::find_offset_for_timestamp` returns `Some(entries[0].logical_offset)`
even when `target_timestamp` is before the segment's first entry (the `Err(0)` arm of
`binary_search`). Any historical segment with at least one time-index entry therefore
matches any timestamp, causing `FetchByTimestamp` to always return records from the oldest
segment. Post-filter removes them all so the client silently receives zero records.

**Fix:** Search from newest historical segment backward and skip segments where
`target_timestamp` is before the segment's first time-index entry timestamp.

---

## Medium Findings (Detailed)

### M12 — Client Panics on Short Server Responses
**File:** `src/client.rs:110, 159, 256, 291, 327`
**Status:** Open

Multiple response-parsing paths index fixed-offset slices without verifying `payload_len`
is sufficient. A server sending `status=0` with a short or empty payload crashes the client
process via out-of-bounds `unwrap()`.

---

### M13 — compact_log Non-Atomic — Crash Destroys All Committed Offsets
**File:** `src/consumer_group.rs:182`
**Status:** Open

`compact_log` calls `file.set_len(0)` then `write_all`. A crash or write failure between
these two operations leaves the consumer-offsets file permanently empty with no recovery.

---

### M14 — delete_topic Prefix Match Deletes Sibling Topics
**File:** `src/server/engine.rs:471`
**Status:** Open

`name.starts_with(&format!("{}-", topic))` incorrectly matches other topics sharing a
name prefix. Deleting topic `"logs"` also removes directories for `"logs-archive"`.

---

### M15 — Consumer-Offsets Recovery: No Size Cap, OOM Risk
**File:** `src/consumer_group.rs:41`
**Status:** Open

`vec![0u8; raw_len]` allocates the entire consumer-offsets file into RAM at startup with
no upper bound. A multi-GB file causes OOM before the server becomes available.

---

### M16 — TOCTOU Race in delete_topic vs get_or_create_partition
**File:** `src/server/engine.rs:455`
**Status:** Open

Between DashMap removal and `remove_dir_all`, a concurrent `get_or_create_partition` call
re-inserts the partition into the map before the directory is deleted, leaving an in-memory
`PartitionManager` writing to a non-existent path.

---

### M17 — producer_sequences DashMap Grows Without Bound
**File:** `src/server/transaction.rs:57`
**Status:** Open

`cleanup_completed_transaction` removes the transaction entry but not the sequence entry.
Any client that calls `BeginTx` with unique `producer_id` values per request causes
unbounded DashMap growth and eventual OOM.
