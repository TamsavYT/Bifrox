# Hermes Client Creator Reference

This document is for anyone building a Hermes client, SDK, automation agent, or test
harness.

## 1. First rule: Hermes uses a custom protocol

Hermes is **not** a Kafka wire-compatible broker. Build against Hermes behavior and
Hermes commands directly.

Start from:

- [`src/protocol/wire.rs`](../src/protocol/wire.rs) for request/response layouts
- [`src/client.rs`](../src/client.rs) for a working client implementation
- [`tests/integration_tests.rs`](../tests/integration_tests.rs) for end-to-end examples

## 2. Connection model

Typical session flow:

1. Open a TCP connection.
2. If the deployment uses legacy `auth.token` without SASL, send the auth preface first.
3. If the deployment uses `SASL_*`, run SASL handshake/authentication.
4. Optionally send `SetClientId`.
5. Start produce, fetch, group, transaction, or admin commands.

## 3. Wire envelope

### Requests

Every request uses:

- `1 byte` command code
- `4 bytes` payload length
- payload bytes

### Responses

Every response uses:

- `1 byte` status
  - `0` = success
  - `1` = error
- `4 bytes` payload length
- payload bytes

On errors, Hermes usually returns a UTF-8 error message in the payload.

## 4. Authentication options

### SASL PLAIN

- Send `SaslHandshake`
- Then `SaslAuthenticate`
- The auth payload is standard null-delimited PLAIN content

### SASL SCRAM-SHA-256

Hermes supports the SCRAM flow:

1. `SaslHandshake("SCRAM-SHA-256")`
2. client-first message
3. server-first challenge
4. client-final with proof
5. server-final verification

Server-side SCRAM state is stored as persistent verifier material, so your client should
assume credentials survive broker restart.

### Legacy token auth

Only relevant for non-SASL deployments that use `auth.token`. New clients should prefer
SASL where possible.

## 5. Logical client identity

Use `SetClientId` early after connect if your SDK or agent has a stable logical name.

Why it matters:

- better quota separation
- clearer operational identity
- behavior closer to Kafka's `client.id`

## 6. Core command groups

### Produce and fetch

- `ProduceBatch`
- `Fetch`
- `FetchCommitted`
- `FetchByTimestamp`
- `Seek`
- `LatestOffset`

Behavior notes:

- Partition routing is key-based when you provide a key and partition count.
- Followers can forward produce requests to the correct partition leader.
- `FetchCommitted` hides uncommitted and aborted transactional data.
- LZ4- or Zstd-compressed payloads (`RecordFrame.magic == 0xAC` / `0xAE`) may need
  decompression on the client side if you implement your own raw decoder instead of
  reusing Hermes helpers (`RecordFrame::decompress_payload()` handles both transparently).
- Plain `Fetch` (unlike `FetchCommitted`) does **not** filter out transaction
  commit/abort control markers (`RecordFrame.magic == 0xAD` / `is_control_marker()`) —
  matching real Kafka, where the server exposes raw control batches at the wire level and
  every client library is expected to recognize and skip them. If you're building a
  general-purpose consumer on top of plain `Fetch`, check `is_control_marker()` (or
  `magic`) yourself before treating a frame's payload as a real record — Hermes's own
  `hermes_cli` consume commands do this filtering client-side for exactly this reason.

### Consumer groups

- `JoinGroup` — response is `[member_id][generation_id][is_leader][protocol_name]`. Save
  `generation_id` for every subsequent `SyncGroup`/`Heartbeat` call in this
  generation, and check `is_leader` to know whether you're responsible for computing the
  group's assignment.
- `SyncGroup` — **every** member calls this, not just the leader. The leader's call
  carries a non-empty `assignments` list (the assignor's output for the whole group);
  every other member calls it with an empty `assignments` list and just retrieves its own
  slice. The response is always the calling member's own `[(topic, partitions)]` — the
  server never exposes another member's assignment to you.
- `Heartbeat`
- `LeaveGroup`
- `OffsetCommit`
- `OffsetFetch`
- legacy `CommitOffset`
- legacy `FetchOffset`

Behavior notes:

- The `protocols` list you pass to `JoinGroup` should list your preferred assignor name
  first (e.g. `"cooperative-sticky"` or `"range"`/`"roundrobin"`) — the first member to
  (re)form the group from empty picks the group's protocol for that generation.
- **Eager groups** (protocol name without "cooperative" in it): a `Heartbeat`/`SyncGroup`
  call at a stale `generation_id` is a hard failure — you must call `JoinGroup` again.
- **Cooperative groups** (protocol name containing "cooperative", matching Kafka's
  `cooperative-sticky` convention): a `Heartbeat`/`SyncGroup` call at a stale
  `generation_id` returns a distinguishable, retryable error containing the string
  `"REBALANCE_IN_PROGRESS"` instead of a hard failure — you were not kicked, you just
  haven't rejoined the current generation yet, and can keep processing the partitions you
  already own (they're never revoked out from under you server-side) until you get around
  to calling `JoinGroup` again. This is the actual behavioral difference KIP-429
  cooperative rebalancing is about; computing a minimal reassignment diff (rather than
  Hermes forcing one) is still the client-side assignor's job.
- Session heartbeats matter; do not join and then go silent.
- Group offsets are durably persisted.

### Transactions and idempotence

- `InitProducerId`
- `AddPartitionsToTxn`
- `EndTxn`
- legacy `BeginTx`
- legacy `CommitTx`
- legacy `AbortTx`

Important behavior:

- `transactional.id` is durable.
- Re-initializing an existing `transactional.id` reuses the producer ID and bumps the
  producer epoch.
- Old producers are fenced.
- Transaction partition registration is durably tracked for restart recovery.
- For EOS-style clients, always carry `producer_id`, `producer_epoch`, and sequence data.

### Topic and cluster admin

- `Ping`
- `ListTopics`
- `DescribeCluster`
- `CreateTopic`
- `DescribeTopic`
- `DeleteTopic`
- `RegisterBroker`
- `UnregisterBroker`

### Dynamic topic configs

- `DescribeConfigs` — returns the topic's current config overrides as key/value pairs.
  Keys not present in the response are simply unset (falling back to the broker's global
  default), not an error.
- `AlterConfigs` — full-replace: the given config map entirely replaces the topic's
  stored overrides.
- `IncrementalAlterConfigs` — merge semantics: upserts are set/overwritten, deletes are
  removed, everything else already stored is left untouched.

Recognized keys with real runtime effect: `cleanup.policy` (`delete`/`compact`/
`compact,delete`), `compression.type` (`none`/`lz4`/`zstd`), `retention.ms`, `retention.bytes`,
`min.insync.replicas`, `delete.retention.ms`, `min.cleanable.dirty.ratio`. Unrecognized
keys are stored and returned by `DescribeConfigs` but have no effect — useful for
client-side metadata, but don't rely on Hermes enforcing config keys it doesn't recognize.

For `cleanup.policy=compact` topics specifically:

- **Tombstones**: a record with an empty value for a given key (`"key:"`, `"key="`, or a
  length-prefixed record whose value portion is zero-length) marks that key for deletion —
  Hermes's equivalent of Kafka's null-value delete marker. A record whose key can't be
  unambiguously parsed out of the payload is never treated as a tombstone, even if the
  whole payload happens to be short.
- **`delete.retention.ms`** (default 24h) controls how long a tombstone is kept as the
  latest record for its key — matching Kafka, this exists so slow/lagging consumers still
  get a chance to observe the delete before it's fully purged. Once a tombstone is older
  than this, the next compaction pass erases the key entirely, including the tombstone
  itself. `0` (or a very small value) purges tombstones on the next compaction tick.
- **`min.cleanable.dirty.ratio`** (default `0.5`) is the minimum fraction of a segment's
  bytes that must be superseded before compaction rewrites that segment — segments with
  only a few stale keys are left alone until they cross this threshold, avoiding low-value
  rewrite I/O.

### Security admin

- `DescribeAcls`
- `CreateAcls`
- `DeleteAcls`
- `UpsertScramUser`
- `DeleteScramUser`

### Share groups (queue-style consumption, KIP-932-like)

Share groups are Hermes's cooperative "queue" consumption model: any member of the group
can be handed any available record from a partition (no per-consumer partition ownership,
unlike classic consumer groups), and delivery is lease-based rather than offset-commit-based.

- `ShareFetch` — acquires up to `max_records`/`max_bytes` available records for
  `member_id`, leased for `lock_timeout_ms`. May also carry piggybacked
  `acknowledgements` from the previous batch so a client can ack-and-fetch in one round
  trip. Response is a list of `AcquiredRecordBatch { first_offset, last_offset,
  delivery_count, records }`.
- `ShareAcknowledge` — resolves previously-acquired offset ranges with one of four
  `AcknowledgeType`s per `AckBatch { first_offset, last_offset, ack_type }`:
  - `Accept` (1) — done, advances the group's watermark once the range is contiguous.
  - `Release` (2) — put it back as available immediately for redelivery to any member.
  - `Reject` (3) — permanently failed; routed to that topic's `-dlq` topic.
  - `Renew` (4) — extend the lock lease without resolving it yet (long-processing records).
- `ShareGroupHeartbeat` — keeps `member_id` registered as active in the group; members
  silent for 60s are dropped from `ShareGroupDescribe`'s membership list.
- `ShareGroupDescribe` — returns current group membership/state.

Behavior notes:

- Unacknowledged leases expire automatically (server-side sweep, ~500ms resolution) and
  are redelivered to any available member; after enough failed delivery attempts a record
  is archived to the DLQ automatically, same as an explicit `Reject`.
- Because delivery is not partition-ownership-based, don't assume ordering across members
  the way a classic consumer group guarantees per-partition ordering to a single owner.
- A client should always send `ShareAcknowledge` (or piggyback acks on the next
  `ShareFetch`) for every record it acquires — an acquired-but-never-acknowledged record
  just sits leased until timeout, wasting delivery attempts.

## 7. Quotas and throttling

Hermes throttles instead of hard-failing when quotas are exceeded.

A client should:

- tolerate slower responses under load
- avoid treating delay as an automatic failure
- identify itself with `SetClientId` if multiple logical clients may share one source IP

## 8. Error-handling expectations

A good Hermes client should:

- surface server error payloads directly
- retry transient network failures
- re-resolve cluster metadata when topology changes
- reconnect after disconnect
- treat producer fencing as a hard session reset for that transactional producer

## 9. Suggested client features

If you are building a production Hermes client, prioritize:

1. connection + reconnect handling
2. SASL PLAIN and SCRAM-SHA-256
3. `SetClientId`
4. produce/fetch
5. consumer-group flows
6. EOS producer flows
7. admin APIs
8. metrics-friendly logging and tracing
9. share-group flows (only if your users need queue-style, non-ordered cooperative
   consumption — most clients should start with classic consumer groups)

## 10. Windows-specific guidance

If your users run Hermes on Windows:

- use conservative socket timeouts
- assume service restarts can happen during upgrades
- test reconnect and metadata refresh paths
- avoid file-lock assumptions outside the broker

For broker-side Windows deployment notes, see
[packaging/windows/README.md](../packaging/windows/README.md).

## 11. Fast implementation checklist

- [ ] parse Hermes request/response envelopes correctly
- [ ] implement SASL handshake + authentication
- [ ] send `SetClientId`
- [ ] handle produce success and broker-forwarded behavior
- [ ] support read-committed fetch when needed
- [ ] persist consumer-group state on the client side as needed
- [ ] handle transactional fencing errors cleanly
- [ ] expose useful logs for operator debugging

## 12. Best reference code

Use these files as the primary truth:

- [`src/client.rs`](../src/client.rs)
- [`src/protocol/wire.rs`](../src/protocol/wire.rs)
- [`src/server/handler.rs`](../src/server/handler.rs)
- [`tests/integration_tests.rs`](../tests/integration_tests.rs)
