<div align="center">

# Bifrox

**A distributed event streaming broker, written in Rust.**

Queue's semantics — partitioned append-only logs, consumer groups, exactly-once
transactions, log compaction — on a small, auditable codebase and a protocol designed to
be implemented, not reverse-engineered.

[![Build & Test](https://github.com/TamsavYT/Bifrox/actions/workflows/ci.yml/badge.svg)](https://github.com/TamsavYT/Bifrox/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org)
[![Platforms](https://img.shields.io/badge/platforms-Linux%20%7C%20Windows%20%7C%20macOS-lightgrey.svg)](#platform-support)

*Pronounced **BY-frox**, after Bifröst — the bridge joining separate realms.*

</div>

---

## Table of contents

- [What Bifrox is](#what-bifrox-is) · [and what it is not](#what-bifrox-is-not)
- [Quickstart](#quickstart)
- [Design principles](#design-principles)
- [Feature status](#feature-status)
- [Repository layout](#repository-layout)
- [Configuration](#configuration)
- [Operating Bifrox](#operating-bifrox)
- [Building a client](#building-a-client)
- [Project status](#project-status)
- [Development](#development)

---

## What Bifrox is

A single Rust binary that stores records in partitioned, append-only logs and serves them
to consumers, with strict per-partition ordering and durability backed by replication —
in roughly 30k lines you can read in an afternoon.

It implements the parts of that model that matter in production:

| | |
|---|---|
| **Storage** | Segmented logs, sparse offset indexes, time indexes, retention, log compaction with tombstones |
| **Delivery** | Consumer groups with eager and cooperative rebalancing, static membership, long-polling fetch, share groups for queue-style consumption |
| **Correctness** | Idempotent producers, exactly-once transactions with producer fencing, read-committed isolation |
| **Cluster** | Raft-replicated metadata, leader/follower replication, ISR tracking, `min.insync.replicas` |
| **Security** | TLS, SASL `PLAIN` and `SCRAM-SHA-256`, ACLs, per-principal quotas |

## What Bifrox is not

**It is not a drop-in replacement for an existing broker.** Bifrox speaks only its own
protocol, and there is no compatibility adapter — a client written for another system will
not connect. That is a deliberate choice, not a gap waiting to be filled: mature streaming
protocols carry decades of accumulated versions, and reimplementing one faithfully is a
larger project than this broker itself.

What Bifrox offers instead is a small, versioned binary protocol that is fully documented
and designed for clients to be written against. See
**[Building a client](#building-a-client)**.

---

## Quickstart

Everything below is copy-pasteable and was run end to end against this revision.

**Prerequisites:** Rust stable. No other dependencies.

### 1. Start a broker

```bash
cat > server.properties <<'EOF'
cluster.id=bifrox-quickstart
node.id=1
role=leader
listeners=PLAINTEXT://127.0.0.1:9092
log.dirs=./data
logs.dir=./logs
EOF

cargo run --release --bin bifrox -- server.properties
```

```
INFO bifrox: Cluster ID: bifrox-quickstart
INFO bifrox: Starting TCP Listener Loop. Send SIGINT (Ctrl+C) or SIGTERM to stop.
```

### 2. Produce and consume

In a second terminal:

```bash
alias bfx='cargo run --quiet --bin bifrox_cli -- --server 127.0.0.1:9092'

bfx ping
bfx create-topic --topic orders --partitions 3
bfx produce --topic orders --key user-42 --message '{"id":1,"total":99}' --partitions 3
```

```
✅ PONG received from server 127.0.0.1:9092
✅ Created topic 'orders' with 3 partition(s)
  Assigned Partition: 1
  Logical Offset:     0
```

> **Note the assigned partition.** Records with a key are routed by hashing it, so
> `user-42` landed on partition **1**, not 0. Every record with that key will follow it
> there — that is what gives you per-key ordering. Consume the partition the producer
> reported:

```bash
bfx consume --topic orders --partition 1 --from-beginning
```

```
📥 Fetched 1 record frame(s) [Topic 'orders' Partition 1]:
  [000] Offset: 0 | Timestamp: 1787818111313 | Payload: '{"id":1,"total":99}'
```

### 3. Consume as a group

```bash
bfx group-consume --group billing --topic orders
```

Joins the group, receives an assignment, consumes its partitions, and auto-commits
offsets. Start a second one and the partitions redistribute between them.

### 4. Watch it

```bash
# add to server.properties, then restart:
#   metrics.bind.addr=127.0.0.1:9644
curl -s http://127.0.0.1:9644/metrics | grep '^bifrox_'
```

```
bifrox_produce_bytes_total 78
bifrox_produce_records_total 1
bifrox_topics_count 1
bifrox_active_brokers_count 1
```

<details>
<summary><b>More CLI commands</b></summary>

```
produce-batch    Produce many records in one request (--messages a,b,c | --stdin)
perf-produce     Producer benchmark (--num-records, --record-size, --throughput)
perf-consume     Consumer benchmark
latest-offset    Partition high watermark
commit-offset    Commit a consumer group offset
fetch-offset     Read a committed consumer group offset
seek             Resolve the physical byte position of an offset
list-topics      List topics
```

Run `cargo run --bin bifrox_cli -- --help` for the full list.

</details>

---

## Design principles

Three decisions shape most of the codebase. They are worth understanding before reading
any of it.

### The broker never compresses or decompresses

A producer builds a record batch, compresses it, and sends the bytes. Bifrox stores them
**exactly as they arrived** and hands the same bytes back on fetch. Decompression is the
consumer's job.

This is why `compression.type=producer` is the default, and it is not a synonym for
`none` — it means the broker imposes no codec of its own. The payoff is that produce and
fetch cost no CPU for compression at all, and a follower's log ends up byte-identical to
its leader's rather than a re-encoded copy.

The one exception is a compacted topic, where the broker must decompress on produce to
verify every record has a key.

### Fetches are served by the kernel

Because stored bytes are exactly what goes on the wire, a fetch can be handed to
`sendfile(2)` (`TransmitFile` on Windows) and streamed from the page cache straight to
the socket — record payloads never enter a user-space buffer.

Deciding *which* bytes to send reads only batch headers, through a bounded sliding window,
so framing a multi-megabyte fetch costs a handful of small reads. The buffered and
zero-copy paths derive their byte range from one function, so their responses are
byte-identical by construction rather than by two implementations agreeing.

### Both protocols are versioned and length-delimited

The client protocol wraps requests in a versioned envelope carrying a correlation id — so
multiple requests can be in flight on one connection — plus tagged fields for per-request
options. Unknown tags are skipped, so new options are added without breaking old brokers.

Inter-node traffic uses the same idea: every frame between brokers carries a magic,
version, type and **length**. The length is what makes change safe — a version tells a
receiver how to read a frame it understands, a length tells it how far to skip past one it
does not.

---

## Feature status

Everything listed is implemented and covered by the test suite. Nothing here is aspirational.

<details open>
<summary><b>Storage and retention</b></summary>

- Segmented append-only logs with sparse `.index`, `.timeindex` and `.txnindex` sidecars
- Crash-safe restart: clean-shutdown markers, full-scan recovery fallback, truncation
- Size- and time-based retention (`retention.bytes`, `retention.ms`)
- **Log compaction** — dedup by key, null-value tombstones with `delete.retention.ms`
  grace, dirty-ratio gating, crash-safe segment swap
- Compacted topics reject records without a key, by design
- LZ4 and Zstd, applied by the producer and preserved verbatim

</details>

<details open>
<summary><b>Producers and consumers</b></summary>

- Key-hashed partition routing; produce forwarding to the current partition leader
- **Long-polling fetch** (`fetch.max.wait.ms` / `fetch.min.bytes`) — an idle consumer parks
  on the broker instead of spinning
- Consumer groups: join, sync, heartbeat, leave, offset commit and fetch
- **Cooperative rebalancing** — a stale generation is a retryable signal,
  not an eviction, and members keep processing partitions they still own
- **Static membership** — a restarting member reclaims its slot without
  triggering a rebalance
- Stalled-consumer detection: a member that heartbeats but stops consuming is evicted
- **Share groups** — queue-style consumption with lease-based delivery, four
  acknowledgement types, automatic redelivery and dead-letter routing

</details>

<details open>
<summary><b>Transactions and correctness</b></summary>

- Idempotent producer sequencing
- `InitProducerId` / `AddPartitionsToTxn` / `EndTxn`, plus legacy `BeginTx` / `CommitTx` / `AbortTx`
- Durable `transactional.id → producer_id/epoch` state; stale producers fenced after restart
- Read-committed isolation via `last_stable_offset` and aborted-range reporting
- Control batches marked in the batch header so consumers can skip them

</details>

<details open>
<summary><b>Cluster and replication</b></summary>

- Raft leader election over a replicated `__cluster_metadata` log
- Follower-pull replication for data partitions; leader-push for metadata
- ISR tracking with `min.insync.replicas` enforcement on write
- High watermark propagation — consumers never see uncommitted data
- Dynamic broker registration and discovery through the heartbeat round trip

</details>

<details open>
<summary><b>Security and operations</b></summary>

- `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`
- SASL `PLAIN` and `SCRAM-SHA-256` with persistent verifier storage
- ACLs across topic, group, transactional-id and cluster resources
- Quota throttling by principal, logical `client_id`, then source IP — clients are
  *delayed*, never hard-rejected
- Prometheus `/metrics`, optionally IP-allowlisted and token-guarded
- Graceful SIGINT/SIGTERM shutdown

</details>

---

## Repository layout

| Path | Purpose |
|---|---|
| [`src/main.rs`](src/main.rs) | Broker entry point |
| [`src/bin/bifrox_cli.rs`](src/bin/bifrox_cli.rs) | Command-line client |
| [`src/client.rs`](src/client.rs) | Reference client implementation |
| [`src/protocol/`](src/protocol/) | Wire protocol, versioned envelope, record batch format |
| [`src/segment/`](src/segment/) | Log segments, indexes, compaction, zero-copy transmit |
| [`src/server/`](src/server/) | Handler, engine, coordinators, ACLs, quotas, share groups |
| [`src/replication/`](src/replication/) | Raft, ISR, inter-node envelope, metadata log |
| [`docs/`](docs/) | Client authoring reference |
| [`tests/`](tests/) | 99 end-to-end scenarios + protocol-contract tests |

---

## Configuration

Bifrox reads a `.properties` file, or environment variables when no file is
given. Conventional broker config names are accepted where the concept matches.

<details>
<summary><b>Common settings</b></summary>

```properties
# Identity
cluster.id=bifrox-prod-01
node.id=1
role=leader
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://broker1.internal:9092

# Storage
log.dirs=/var/lib/bifrox/data
logs.dir=/var/log/bifrox
max.segment.bytes=1073741824
index.interval.bytes=4096
log.retention.ms=604800000
log.retention.bytes=53687091200

# Compaction
log.cleanup.policy=delete            # or: compact / compact,delete
min.cleanable.dirty.ratio=0.5
delete.retention.ms=86400000

# Durability
min.insync.replicas=2
replica.peer.addresses=10.0.0.2:9092,10.0.0.3:9092
log.flush.interval.messages=10000
log.flush.interval.ms=1000

# Compression — `producer` (default) stores what the client sent, untouched
compression.type=producer

# Quotas (bytes/sec)
producer.byte.rate=10485760
consumer.byte.rate=10485760

# Observability
metrics.bind.addr=127.0.0.1:9644
metrics.allowed.ips=127.0.0.1,10.0.0.0/8
```

</details>

<details>
<summary><b>Security settings</b></summary>

```properties
security.protocol=SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-256
acls.enabled=true
super.users=User:admin

ssl.cert.path=/etc/bifrox/server.crt
ssl.key.path=/etc/bifrox/server.key
ssl.ca.path=/etc/bifrox/ca.crt
ssl.client.auth=required

# Bootstrap seed user — imported into the persistent SCRAM store on first start
sasl.user.admin=change-me
```

SCRAM credentials are stored as verifier material in the replicated metadata log, so they
survive restart and are consistent across the cluster.

</details>

<details>
<summary><b>Environment variables</b></summary>

```bash
export BIFROX_BIND_ADDR=0.0.0.0:9092
export BIFROX_DATA_DIR=/var/lib/bifrox
export BIFROX_LOG_DIR=/var/log/bifrox
export BIFROX_CONFIG=/etc/bifrox/server.properties
cargo run --release --bin bifrox
```

</details>

---

## Operating Bifrox

### Metrics

| Metric | Meaning |
|---|---|
| `bifrox_produce_bytes_total` / `bifrox_produce_records_total` | Ingest volume |
| `bifrox_fetch_bytes_total` | Delivery volume |
| `bifrox_produce_latency_ms` / `bifrox_fetch_latency_ms` | Broker-side latency histograms |
| `bifrox_active_connections` | Open client connections |
| `bifrox_topics_count` / `bifrox_active_brokers_count` | Cluster shape |
| `bifrox_quota_throttled_clients_total` | Clients currently being delayed |
| `bifrox_acl_denied_requests_total` | Authorization failures |

Per-topic variants (`bifrox_topic_*`) are exported alongside the totals.

### Platform support

Linux, Windows and macOS, with Linux and Windows both covered by CI on every commit.

Windows is a first-class target, not an afterthought: segment files are opened with share
modes that permit concurrent rename and delete (so compaction and retention do not trip
over live readers), and zero-copy transmit uses `TransmitFile` with the synchronous
calling convention that avoids racing Tokio's IOCP driver. Service packaging notes are in
[`packaging/windows/`](packaging/windows/README.md).

---

## Building a client

Bifrox's protocol is documented to be implemented from scratch:

### 📖 **[Client Creator Reference](docs/BIFROX_CLIENT_CREATOR_REFERENCE.md)**

It specifies the byte layouts you need — both request framings, the tagged-field table,
produce and fetch request/response shapes, and the record batch format field by field —
along with the semantics that are easy to get wrong:

- A batch is **atomic**: a fetch from an offset inside one returns the *whole* batch,
  including earlier records. Filter client-side.
- A batch larger than your `max_bytes` is still returned whole, alone — otherwise that
  offset would be unreachable forever.
- **Null and empty values are different.** `value_len == -1` is a tombstone;
  `value_len == 0` is an ordinary empty record. If your client's value type cannot
  represent null, it cannot express a delete.
- Control batches occupy real offsets and must be skipped.
- Under read-committed, the broker reports `last_stable_offset` and aborted ranges but
  does not filter — it cannot, without decompressing your batches. Your consumer applies them.

The doc's byte-level claims are enforced by
[`tests/client_reference_doc.rs`](tests/client_reference_doc.rs), so it cannot silently
drift from the implementation.

---

## Project status

**Pre-1.0 and under active development.** The core is well tested — 217 unit, 101
integration and 7 protocol-contract tests run on Linux and Windows for every commit, and
the integration suite covers replication, failover, transactions, compaction, security,
quotas and restart recovery as end-to-end scenarios against real brokers over real
sockets.

What that does *not* yet mean:

- **No production deployments exist.** It has not been run at scale or under adversarial load.
- **No stability guarantee.** With no deployed clusters, breaking changes are still taken
  when they make the design better — the versioned protocols exist so that stops being
  true once there is something to be compatible with.
- **No published crate yet.** Build from source.

If you are evaluating Bifrox: read it, run it, benchmark it, and open issues. It is not
ready to hold data you cannot lose.

---

## Development

```bash
cargo build --all-targets          # build everything
cargo test                         # 325 tests
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo bench                        # storage benchmarks
```

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Security issues:
[SECURITY.md](SECURITY.md).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
