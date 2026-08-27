# Bifrox Client Creator Reference

This document is for anyone building a Bifrox client, SDK, automation agent, or test
harness.

## 1. First rule: Bifrox uses a custom protocol

Bifrox speaks its own binary protocol. Build against Bifrox behavior and
Bifrox commands directly.

Start from:

- [`src/protocol/wire.rs`](../src/protocol/wire.rs) for request/response layouts
- [`src/protocol/batch.rs`](../src/protocol/batch.rs) for the record batch format — a
  client owns both encoding and decoding of this, so it is the file to read first
- [`src/client.rs`](../src/client.rs) for a working client implementation
- [`tests/integration_tests.rs`](../tests/integration_tests.rs) for end-to-end examples

## 2. Connection model

Typical session flow:

1. Open a TCP connection.
2. If the deployment uses legacy `auth.token` without SASL, send the auth preface first.
3. If the deployment uses `SASL_*`, run SASL handshake/authentication.
4. Optionally send `NegotiateProtocol` to learn which envelope versions and commands this
   broker supports.
5. Optionally send `SetClientId`.
6. Start produce, fetch, group, transaction, or admin commands.

TLS connections are supported and change nothing above the socket.

## 3. Wire envelope

There are two framings. **Legacy** is the original bare form; **versioned** wraps it and is
what a new client should send. The broker answers in whichever framing the request arrived
in, so the two can be mixed freely on one connection.

### Legacy framing

Request:

```
[command code: 1b] [payload_len: 4b] [payload]
```

Response:

```
[status: 1b] [payload_len: 4b] [payload]
```

`status` is `0` for success, `1` for error. On an error the payload is a UTF-8 message.

### Versioned framing (preferred)

Request:

```
[0xF1] [api_version: 2b] [correlation_id: 4b]
[tagged_count: 1b] ( [tag: 1b] [len: 2b] [value: len] )*
[command code: 1b] [payload_len: 4b] [payload]
```

Response:

```
[0xF1] [correlation_id: 4b] [status: 1b] [payload_len: 4b] [payload]
```

Two things this buys you:

- **`correlation_id` is echoed back**, so you can have more than one request in flight on a
  single connection instead of a strict request/response lockstep. Match replies to
  requests by this value, not by arrival order.
- **Tagged fields** carry optional per-request settings. A broker acts on the tags it
  knows and *skips* the ones it doesn't, so this is where new per-request options appear
  without any existing layout changing.

`api_version` must currently be `1`. A version outside the supported range is refused with
an explicit error rather than parsed on a guess — call `NegotiateProtocol` first if you
want to check before sending.

### Tagged fields

| Tag | Name | Value | Meaning |
|-----|------|-------|---------|
| `0x01` | `ISOLATION_LEVEL` | 1 byte | `0` read-uncommitted (default), `1` read-committed. See [§6](#produce-and-fetch). |
| `0x02` | `FORWARDED` | empty | Broker-internal; clients should not send it. |
| `0x03` | `SESSION_TIMEOUT_MS` | `u32` BE | Requested `session.timeout.ms` on `JoinGroup`. The coordinator clamps it into a sane range. |
| `0x04` | `GROUP_MEMBER` | `[group_id][member_id]`, two pascal strings | Attributes a `Fetch` to a group member so the coordinator can see it is making progress. |
| `0x05` | `MAX_WAIT_MS` | `u32` BE | `fetch.max.wait.ms`. Absent means 0 — answer immediately. |
| `0x06` | `MIN_BYTES` | `u32` BE | `fetch.min.bytes`. Absent means 1. Only meaningful alongside `MAX_WAIT_MS`. |
| `0x07` | `COOPERATIVE_ROUND_TWO` | empty | Marks a `JoinGroup` as a cooperative rebalance's second round. See [consumer groups](#consumer-groups). |

### `NegotiateProtocol`

Response payload:

```
[version_min: 2b] [version_max: 2b] [command_count: 2b] [command codes: 1b each]
```

Use it to discover whether a command exists on this broker in advance, rather than sending
it and trying to interpret the error — which is indistinguishable from the command failing
for an ordinary reason.

### Strings

`pascal string` throughout this document means `[len: 2b][UTF-8 bytes]`.

## 4. Authentication options

### SASL PLAIN

- Send `SaslHandshake`
- Then `SaslAuthenticate`
- The auth payload is standard null-delimited PLAIN content

### SASL SCRAM-SHA-256

Bifrox supports the SCRAM flow:

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
- behavior closer to `client.id`

## 6. Core command groups

### Produce and fetch

- `ProduceBatch`
- `Fetch`
- `FetchCommitted`
- `FetchByTimestamp`
- `Seek`
- `LatestOffset`

#### The one thing to understand first

**The broker never compresses or decompresses your records.** A producer builds a record
batch, compresses it, and sends the bytes; the broker stores them exactly as they arrived
and hands those same bytes back on fetch. Decompressing and decoding is the *consumer's*
job.

That means a Bifrox client owns both halves of the record batch format below. It is not
optional — there is no server-side mode that hands you pre-decoded records.

(The one exception is a compacted topic, where the broker must decompress on produce to
check that every record has a key. Everything else stays untouched.)

#### `ProduceBatch`

Request payload:

```
[topic: pascal] [key: pascal] [transaction_id: pascal]
[num_partitions: 4b] [batch_len: 4b] [batch bytes]
```

Response payload:

```
[assigned_partition: 4b] [first_offset: 8b] [last_offset: 8b]
```

`key` selects the partition when `num_partitions > 0`; an empty key means partition 0.
`transaction_id` is empty for a non-transactional produce. `batch bytes` is one encoded
record batch (below) — you assign no offsets, the broker stamps them.

#### `Fetch` / `FetchCommitted` / `FetchByTimestamp`

`Fetch` request payload:

```
[topic: pascal] [partition: 4b] [offset: 8b] [max_bytes: 4b]
```

Response payload, for all three:

```
[last_stable_offset: 8b]
[aborted_count: 4b] ( [start: 8b] [end: 8b] )*
[entries_len: 4b] [entry bytes]
```

`entry bytes` is a run of whole record batches, exactly as stored. Walk it batch by batch;
each batch declares its own length.

Two rules that will bite you if you miss them:

- **A batch is atomic.** If your `offset` lands in the middle of a batch you get the
  *whole* batch, including records before your offset. Filter them out client-side.
- **A batch larger than `max_bytes` is still returned whole**, alone. Without this a
  record bigger than your fetch size would be unreachable forever.

#### Read-committed

Set the `ISOLATION_LEVEL` tagged field to `1` on an ordinary `Fetch`. (The separate
`FetchCommitted` command still works and is always read-committed, but isolation is really
a per-request property and the tag is the better way to express it.)

The broker does **not** filter aborted records out for you — it cannot, without
decompressing your batches. Instead it reports what you need to filter with:

- `last_stable_offset` — ignore any record at or above this offset.
- The aborted ranges — drop records whose offset falls in `[start, end)`.
- **Control batches** — a batch with the control bit set in its attributes holds
  transaction commit/abort markers, not data. They occupy real offsets, so you must
  recognise and skip them rather than treating their contents as records.

This is the only thing that works once batches are compressed. Do this filtering in your consumer
after decompressing, or you will surface aborted records as if they were committed.

#### Long-polling fetch

Set `MAX_WAIT_MS` (and optionally `MIN_BYTES`) and the broker parks the request until data
arrives instead of answering an idle partition empty. This cuts request volume *and* lowers
delivery latency — a plain polling loop waits on average half its poll interval for a
record that already landed, whereas a parked fetch returns as soon as one commits.

#### Record batch format

```
[magic: 1b = 0xC0]
[batch_length: 4b]        counts everything after this field
[crc: 4b]                 CRC32 over base_offset .. end of record_data
[base_offset: 8b]
[last_offset_delta: 4b]
[base_timestamp: 8b]
[producer_id: 8b]
[producer_epoch: 2b]
[base_sequence: 4b]
[leader_epoch: 4b]
[attributes: 2b]
[record_count: 4b]
[record_data]             compressed as a whole, per `attributes`
```

The fixed header is 53 bytes and is always **plaintext** — compression covers
`record_data` only. That is what lets the broker route, index and serve batches without
decompressing them.

`attributes` bits:

| Bits | Meaning |
|------|---------|
| 0–2 | Codec: `0` none, `1` LZ4, `2` Zstd |
| 3 | Transactional |
| 4 | Control batch (markers, not data — skip these) |

Inside `record_data`, once decompressed, each record is:

```
[offset_delta: 4b] [timestamp_delta: 8b, signed]
[key_len: 4b, signed] [key] [value_len: 4b, signed] [value]
```

A length of `-1` means **null** and no bytes follow. A length of `0` means present but
empty. **These are different**, and the difference is load-bearing on compacted topics —
see [tombstones](#dynamic-topic-configs). Absolute offset is `base_offset + offset_delta`;
absolute timestamp is `base_timestamp + timestamp_delta`.

#### Other behavior notes

- Partition routing is key-based when you provide a key and partition count.
- Followers can forward produce requests to the correct partition leader.
- Whatever codec you compress with is the codec the broker stores, unless the topic's
  `compression.type` says otherwise — see [dynamic topic configs](#dynamic-topic-configs).

### Consumer groups

- `JoinGroup` — request payload is
  `[group_id: pascal] [member_id: pascal] [protocol_count: 4b] [protocol: pascal]*
  [group_instance_id: pascal, optional trailing]`. Response is
  `[member_id][generation_id][is_leader][protocol_name]`. Save `generation_id` for every
  subsequent `SyncGroup`/`Heartbeat` call in this generation, and check `is_leader` to know
  whether you're responsible for computing the group's assignment.
- `SyncGroup` — **every** member calls this, not just the leader. The leader's call
  carries a non-empty `assignments` list (the assignor's output for the whole group);
  every other member calls it with an empty `assignments` list and just retrieves its own
  slice. The response is always the calling member's own `[(topic, partitions)]` — the
  server never exposes another member's assignment to you.
- `Heartbeat`
- `LeaveGroup`
- `OffsetCommit`
- `OffsetFetch`
- `ListGroups` / `DescribeGroup` — group introspection
- legacy `CommitOffset`
- legacy `FetchOffset`

Behavior notes:

- The `protocols` list you pass to `JoinGroup` should list your preferred assignor name
  first (e.g. `"cooperative-sticky"` or `"range"`/`"roundrobin"`) — the first member to
  (re)form the group from empty picks the group's protocol for that generation.
- **Eager groups** (protocol name without "cooperative" in it): a `Heartbeat`/`SyncGroup`
  call at a stale `generation_id` is a hard failure — you must call `JoinGroup` again.
- **Cooperative groups** (protocol name containing "cooperative", matching the
  `cooperative-sticky` convention): a `Heartbeat`/`SyncGroup` call at a stale
  `generation_id` returns a distinguishable, retryable error containing the string
  `"REBALANCE_IN_PROGRESS"` instead of a hard failure — you were not kicked, you just
  haven't rejoined the current generation yet, and can keep processing the partitions you
  already own (they're never revoked out from under you server-side) until you get around
  to calling `JoinGroup` again. This is the actual behavioral difference incremental
  cooperative rebalancing is about; computing a minimal reassignment diff (rather than
  Bifrox forcing one) is still the client-side assignor's job.
- **Static membership**: pass a stable `group_instance_id` on `JoinGroup` and a
  restarting member reclaims its own slot instead of triggering a rebalance — it keeps its
  existing member id and the group's generation does not advance. Use it for consumers with
  stable identity (a StatefulSet pod, a named worker); omit it and the member is dynamic,
  where a restart is indistinguishable from a new member joining.
- **Cooperative round two**: after a cooperative rebalance's first round has revoked what
  needs revoking, the leader opens the second round by sending `JoinGroup` again with the
  `COOPERATIVE_ROUND_TWO` tagged field set. Say it explicitly rather than relying on the
  coordinator to infer it — the inference cannot distinguish a deliberate second round from
  a leader that rejoined for some other reason, and it never fired at all for a static
  leader.
- Ask for a `session.timeout.ms` with the `SESSION_TIMEOUT_MS` tagged field. The
  coordinator clamps it into a sane range, so you cannot request one so short you get
  evicted on ordinary jitter.
- Send the `GROUP_MEMBER` tagged field on your `Fetch` calls. Without it the coordinator
  only sees your heartbeats, and a member that heartbeats but has stopped consuming looks
  identical to a healthy one.
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
`compact,delete`), `compression.type` (`producer`/`none`/`lz4`/`zstd`), `retention.ms`,
`retention.bytes`, `min.insync.replicas`, `delete.retention.ms`,
`min.cleanable.dirty.ratio`. Unrecognized keys are stored and returned by
`DescribeConfigs` but have no effect — useful for client-side metadata, but don't rely on
Bifrox enforcing config keys it doesn't recognize.

#### `compression.type`

`producer` is the default and it is **not** an alias for `none`: it means the broker stores
whatever codec you sent, untouched. That is the behavior you want in almost every case —
it is what keeps produce and fetch free of any broker-side compression work.

Setting an explicit codec (`none`/`lz4`/`zstd`) only affects records the *broker* itself
authors for that topic. Your batches are still stored as you sent them.

#### Compacted topics

- **Every record must have a key.** A produce to a `cleanup.policy=compact` topic
  containing a record with a null key is **rejected** — a keyless record can never be
  superseded and can never be compacted away, so it would sit in the log forever and
  surface later as unexplained growth. Such a produce is rejected outright.
- **Tombstones are null-valued records**: `value_len == -1` in the record encoding. A record with a **present but empty** value (`value_len == 0`) is an
  ordinary record and does *not* delete anything.

  > If you are porting from an older Bifrox note: there is no string-parsing rule here.
  > The broker does not look inside a payload for `"key:"` or `"key="` separators, and an
  > empty value is not a delete marker. Only a genuine null value is a tombstone.

  Your client must therefore keep null and empty distinguishable end to end. If your API
  models a value as "bytes, possibly empty" with no null case, you cannot express a delete.
- **Keys are compared byte-for-byte.** The broker never parses a payload looking for a key
  — it uses the key field you wrote.
- **`delete.retention.ms`** (default 24h) controls how long a tombstone is kept as the
  latest record for its key —, this exists so slow/lagging consumers still
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

### Share groups (queue-style consumption-like)

Share groups are Bifrox's cooperative "queue" consumption model: any member of the group
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

Bifrox throttles instead of hard-failing when quotas are exceeded.

A client should:

- tolerate slower responses under load
- avoid treating delay as an automatic failure
- identify itself with `SetClientId` if multiple logical clients may share one source IP

## 8. Error-handling expectations

A good Bifrox client should:

- surface server error payloads directly
- retry transient network failures
- re-resolve cluster metadata when topology changes
- reconnect after disconnect
- treat producer fencing as a hard session reset for that transactional producer

## 9. Suggested client features

If you are building a production Bifrox client, prioritize:

1. connection + reconnect handling
2. SASL PLAIN and SCRAM-SHA-256
3. `SetClientId`
4. record batch encode/decode, with LZ4 and Zstd
5. produce/fetch
6. consumer-group flows
7. EOS producer flows
8. admin APIs
9. metrics-friendly logging and tracing
10. share-group flows (only if your users need queue-style, non-ordered cooperative
   consumption — most clients should start with classic consumer groups)

## 10. Windows-specific guidance

If your users run Bifrox on Windows:

- use conservative socket timeouts
- assume service restarts can happen during upgrades
- test reconnect and metadata refresh paths
- avoid file-lock assumptions outside the broker

For broker-side Windows deployment notes, see
[packaging/windows/README.md](../packaging/windows/README.md).

## 11. Fast implementation checklist

- [ ] parse both request/response envelopes, and match replies by `correlation_id`
- [ ] **encode record batches, including compression** — the broker will not do it for you
- [ ] **decode and decompress record batches on fetch**, walking whole batches
- [ ] keep null and empty values distinguishable end to end (tombstones depend on it)
- [ ] skip control batches, and filter aborted ranges under read-committed
- [ ] return a whole batch's records even when the fetch offset lands mid-batch
- [ ] implement SASL handshake + authentication
- [ ] send `SetClientId`
- [ ] handle produce success and broker-forwarded behavior
- [ ] persist consumer-group state on the client side as needed
- [ ] handle transactional fencing errors cleanly
- [ ] expose useful logs for operator debugging

## 12. Best reference code

Use these files as the primary truth:

- [`src/client.rs`](../src/client.rs)
- [`src/protocol/wire.rs`](../src/protocol/wire.rs)
- [`src/protocol/batch.rs`](../src/protocol/batch.rs)
- [`src/server/handler.rs`](../src/server/handler.rs)
- [`tests/integration_tests.rs`](../tests/integration_tests.rs)
