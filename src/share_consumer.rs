//! Client-side API for share groups: broker-side, lease-based, queue-style consumption
//! from a partition — multiple members acquire disjoint offset ranges from the *same*
//! partition concurrently, resolving each one (Accept/Release/Reject) instead of committing
//! an offset. See `crate::server::share` for the broker half this wraps.
//!
//! [`ShareConsumer`] mirrors [`crate::consumer::GroupConsumer`]'s shape — construct with
//! [`ShareConsumer::join`], loop on [`ShareConsumer::poll`], resolve what you read, call
//! [`ShareConsumer::leave`] when done — but the two protocols underneath are different
//! enough that mirroring the shape is where the similarity ends; see the doc comment on
//! [`ShareConsumer::join`] for the biggest divergence.

use crate::client::TestClient;
use crate::protocol::{AckBatch, AcknowledgeType};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

/// Disambiguates member ids generated in the same nanosecond. `SystemTime`'s resolution is
/// coarser than a nanosecond on some platforms, and two `ShareConsumer::join` calls in a
/// tight loop (a test spinning up several members, say) can land on the same reading — the
/// counter, not the timestamp, is what actually guarantees two such ids never collide.
static MEMBER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Configuration for [`ShareConsumer::join`].
#[derive(Debug, Clone)]
pub struct ShareConsumerConfig {
    pub group_id: String,
    pub topic: String,
    /// Partitions this member works, in order. [`ShareConsumer::poll`] rotates its
    /// starting point across these on every call so the first entry cannot starve the
    /// rest.
    pub partitions: Vec<u32>,
    /// Client-chosen member id. Empty means "generate one" — see [`ShareConsumer::join`].
    pub member_id: String,
    pub max_records: u32,
    pub max_bytes: u32,
    /// How long the broker leases an acquired record to this member before it is eligible
    /// for redelivery to someone else. Sent as `lock_timeout_ms` on every `ShareFetch`.
    pub lock_timeout: Duration,
    /// How often the background heartbeat task sends `ShareGroupHeartbeat`, keeping this
    /// member visible to `ShareGroupDescribe` between polls.
    pub heartbeat_interval: Duration,
}

impl Default for ShareConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            topic: String::new(),
            partitions: Vec::new(),
            member_id: String::new(),
            max_records: 100,
            max_bytes: 1024 * 1024,
            // Matches `SharePartition::default_lock_timeout` on the broker side.
            lock_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
        }
    }
}

impl ShareConsumerConfig {
    /// Rejects a config that would misbehave in a way construction-time validation can
    /// catch outright, rather than surfacing later as a confusing runtime failure. Mirrors
    /// `GroupConsumerConfig::validate`.
    pub fn validate(&self) -> IoResult<()> {
        if self.group_id.is_empty() {
            return Err(std::io::Error::other(
                "ShareConsumerConfig::group_id must not be empty",
            ));
        }
        if self.topic.is_empty() {
            return Err(std::io::Error::other(
                "ShareConsumerConfig::topic must not be empty",
            ));
        }
        if self.partitions.is_empty() {
            return Err(std::io::Error::other(
                "ShareConsumerConfig::partitions must not be empty — a share consumer with \
                 no partitions to fetch from would poll forever and never return a record; \
                 pass at least one partition number",
            ));
        }
        if self.max_records == 0 {
            return Err(std::io::Error::other(
                "ShareConsumerConfig::max_records must be greater than zero, e.g. 100",
            ));
        }
        if self.max_bytes == 0 {
            return Err(std::io::Error::other(
                "ShareConsumerConfig::max_bytes must be greater than zero, e.g. 1048576 (1 MiB)",
            ));
        }
        if self.lock_timeout.is_zero() {
            return Err(std::io::Error::other(
                "ShareConsumerConfig::lock_timeout must be greater than zero, e.g. 30s \
                 (matching the broker's default_lock_timeout)",
            ));
        }
        if self.heartbeat_interval.is_zero() {
            return Err(std::io::Error::other(
                "ShareConsumerConfig::heartbeat_interval must be greater than zero, e.g. 3s",
            ));
        }
        if self.heartbeat_interval >= self.lock_timeout {
            return Err(std::io::Error::other(format!(
                "ShareConsumerConfig::heartbeat_interval ({:?}) must be less than lock_timeout \
                 ({:?}) — a member whose heartbeat is slower than its own lease is \
                 misconfigured; try a heartbeat_interval around a tenth of lock_timeout",
                self.heartbeat_interval, self.lock_timeout
            )));
        }
        Ok(())
    }
}

/// One record delivered to a [`ShareConsumer`] — an acquired lease, not a committed read.
/// It stays "in flight" (see [`ShareConsumer::in_flight`]) until resolved with
/// [`ShareConsumer::accept`], [`ShareConsumer::release`], or [`ShareConsumer::reject`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareRecord {
    pub partition: u32,
    pub offset: u64,
    pub timestamp: u64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    /// 1 on first delivery. Higher means this record was released, rejected-but-under-the-
    /// limit, or had its lease expire before. A caller can use it to route a repeatedly
    /// failing record differently before the broker gives up on it and sends it to the DLQ.
    pub delivery_count: u16,
}

/// A consumer that belongs to a share group.
///
/// Where [`crate::consumer::GroupConsumer`] gets an assignment negotiated by the group
/// coordinator, a `ShareConsumer` always works exactly the partitions listed in
/// `ShareConsumerConfig::partitions` — there is no rebalance protocol on this side to hand
/// out or take away a partition. What "belonging to the group" buys instead is disjoint,
/// lease-based delivery from those partitions: see `crate::server::share::SharePartition`
/// for how the broker keeps two members from ever acquiring the same offset range at once.
///
/// The background heartbeat task runs on its own connection to the broker (opened via
/// `TestClient::addr`), independent of whatever the application is doing in
/// [`Self::poll`] — same reasoning as `GroupConsumer`'s: a caller stuck on a slow batch
/// should not go quiet on liveness. But it is a good deal simpler here, because a
/// share-group member id never changes after `join()` — see [`Self::join`].
pub struct ShareConsumer {
    client: TestClient,
    config: ShareConsumerConfig,
    member_id: String,
    /// Index into `config.partitions` that the next `poll()` starts from. Advances by one
    /// every call regardless of how many partitions actually got visited, so the rotation
    /// keeps moving even when a poll fills up on the very first partition it tries.
    next_partition_idx: usize,
    /// Records leased to this member and not yet resolved: partition -> offset ->
    /// delivery_count. A `BTreeMap` per partition so `accept`/`release`/`reject`/`renew`
    /// can be built directly on top of `coalesce_acks`, which depends on ascending order.
    in_flight: HashMap<u32, BTreeMap<u64, u16>>,
    /// Resolutions not yet sent to the broker: partition -> offset -> ack type. Piggybacked
    /// on the next `ShareFetch` for that partition, or sent explicitly via
    /// [`Self::flush_acks`].
    staged_acks: HashMap<u32, BTreeMap<u64, AcknowledgeType>>,
    heartbeat_task: Option<JoinHandle<()>>,
}

/// Runs on its own connection to `addr`, sending `ShareGroupHeartbeat` for the fixed
/// `member_id` every `interval` until aborted.
///
/// Simpler than `GroupConsumer`'s `run_heartbeat_task` in one specific way: there is no
/// `watch` channel threading in a changing identity, and no `needs_rejoin` flag for
/// `poll()` to check afterward. A share-group member id is client-chosen at `join()` and
/// never changes — there is no generation to go stale and fence against, so there is
/// nothing a heartbeat rejection could mean that would need recovering from on the poll
/// side. A failed heartbeat here is just logged and retried next tick.
async fn run_heartbeat_task(
    addr: SocketAddr,
    group_id: String,
    member_id: String,
    interval: Duration,
) {
    let mut client: Option<TestClient> = None;
    loop {
        tokio::time::sleep(interval).await;

        if client.is_none() {
            client = TestClient::connect(addr).await.ok();
        }
        let Some(active) = client.as_mut() else {
            // Broker unreachable right now; try again next tick rather than busy-looping.
            continue;
        };

        if let Err(e) = active.share_group_heartbeat(&group_id, &member_id).await {
            tracing::debug!(
                group_id,
                member_id,
                error = %e,
                "background share-group heartbeat failed"
            );
            // Reconnect fresh next tick — the failure may be connection-level, and a
            // poisoned stream should not be reused either way.
            client = None;
        }
    }
}

/// Merges consecutive per-offset acknowledgements of the same type into ranged
/// [`AckBatch`]es.
///
/// The wire representation is a range (`first_offset..=last_offset`, one `ack_type`) — see
/// `SharePartition::acknowledge` — not one entry per record, so acknowledging N contiguous
/// offsets and then sending N separate `AckBatch`es would multiply the request size by N
/// for no benefit; the broker already treats a contiguous range as a single unit. Walking
/// `acks` in ascending offset order (hence a `BTreeMap` in every caller, never a `HashMap`)
/// and extending the current run while the next offset is exactly `prev + 1` and the ack
/// type is unchanged is what turns four individual `Accept`s at offsets 0..=3 into the one
/// `AckBatch { first_offset: 0, last_offset: 3, .. }` the wire format was designed to carry.
pub(crate) fn coalesce_acks(acks: &BTreeMap<u64, AcknowledgeType>) -> Vec<AckBatch> {
    let mut batches = Vec::new();
    let mut iter = acks.iter();
    let Some((&first_offset, &first_type)) = iter.next() else {
        return batches;
    };

    let mut current_first = first_offset;
    let mut current_last = first_offset;
    let mut current_type = first_type;

    for (&offset, &ack_type) in iter {
        if offset == current_last + 1 && ack_type == current_type {
            current_last = offset;
        } else {
            batches.push(AckBatch {
                first_offset: current_first,
                last_offset: current_last,
                ack_type: current_type,
            });
            current_first = offset;
            current_last = offset;
            current_type = ack_type;
        }
    }
    batches.push(AckBatch {
        first_offset: current_first,
        last_offset: current_last,
        ack_type: current_type,
    });
    batches
}

impl ShareConsumer {
    /// Validates `config`, resolves this member's id, and starts the background heartbeat
    /// task. Unlike `GroupConsumer::join`, there is no join/sync handshake to run first —
    /// share groups have no `JoinGroup`/`SyncGroup` at all. The member id is entirely
    /// client-chosen (generated here if `config.member_id` is empty) and the broker only
    /// ever learns of a member as a side effect of its first `ShareFetch` or
    /// `ShareGroupHeartbeat` — there is no coordinator round trip that has to succeed
    /// before this returns. This is the single most surprising thing about `ShareConsumer`
    /// coming from `GroupConsumer`: there is no assignment to wait for, no generation id,
    /// and no rebalance that could ever invalidate one — [`Self::poll`] can be called the
    /// instant this returns.
    pub async fn join(client: TestClient, config: ShareConsumerConfig) -> IoResult<Self> {
        config.validate()?;
        let addr = client.addr();
        let heartbeat_interval = config.heartbeat_interval;
        let group_id = config.group_id.clone();

        let member_id = if config.member_id.is_empty() {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let seq = MEMBER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("{}-{}-{}", config.group_id, nanos, seq)
        } else {
            config.member_id.clone()
        };

        let heartbeat_task = tokio::spawn(run_heartbeat_task(
            addr,
            group_id,
            member_id.clone(),
            heartbeat_interval,
        ));

        Ok(Self {
            client,
            config,
            member_id,
            next_partition_idx: 0,
            in_flight: HashMap::new(),
            staged_acks: HashMap::new(),
            heartbeat_task: Some(heartbeat_task),
        })
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn partitions(&self) -> &[u32] {
        &self.config.partitions
    }

    pub fn client_mut(&mut self) -> &mut TestClient {
        &mut self.client
    }

    /// Records currently leased to this member and not yet acknowledged.
    pub fn in_flight(&self) -> usize {
        self.in_flight.values().map(BTreeMap::len).sum()
    }

    /// One poll round: fetches across `config.partitions` starting at the rotating cursor
    /// (`next_partition_idx`), visiting each partition at most once and stopping once
    /// `max_records` total records have been collected. Advancing the cursor by one every
    /// call — not only on a round that actually reaches the later partitions — is what
    /// keeps partition 0 from permanently starving the rest whenever it alone is enough to
    /// fill `max_records`.
    ///
    /// Any acknowledgements staged for a partition (via `accept`/`release`/`reject`) are
    /// piggybacked on that partition's `ShareFetch` and cleared only once the request
    /// succeeds — a fetch that fails leaves them staged rather than dropping them, and a
    /// partition this round never reaches simply keeps its staged acks for next time.
    ///
    /// # Errors and empty results
    ///
    /// `Ok(vec![])` means genuinely idle: every partition was reached and none had
    /// anything to deliver. `Err` means nothing could be fetched from *any* partition this
    /// round — the broker is unreachable, say — so the caller must not treat it as idle. A
    /// partition that fails after others already yielded records still returns `Ok` with
    /// whatever was collected; the failed partition's staged acks stay staged and it is
    /// simply retried on the next call, same as one this round never got to.
    ///
    /// This differs from [`crate::consumer::GroupConsumer::poll`], which also swallows a
    /// per-partition fetch error and reconnects. That is safe there because
    /// `GroupConsumer` has a `needs_rejoin` flag: a broker-side rejection sets it, and the
    /// *next* `poll()` sees it and returns early instead of silently spinning. A
    /// `ShareConsumer` has no such flag — there is no rejoin handshake for a share-group
    /// member at all (see [`Self::join`]) — so swallowing every error here would make a
    /// broker outage indistinguishable from idle partitions and a consume loop would spin
    /// silently forever. Surfacing the first error once nothing else came back is the only
    /// way this type can make a total failure visible.
    pub async fn poll(&mut self) -> IoResult<Vec<ShareRecord>> {
        let mut collected = Vec::new();
        let mut first_error: Option<std::io::Error> = None;
        let mut reconnected = false;
        let partition_count = self.config.partitions.len();

        for step in 0..partition_count {
            if collected.len() >= self.config.max_records as usize {
                break;
            }
            let idx = (self.next_partition_idx + step) % partition_count;
            let partition = self.config.partitions[idx];
            let remaining = self.config.max_records - collected.len() as u32;

            let acks: Vec<AckBatch> = self
                .staged_acks
                .get(&partition)
                .map(coalesce_acks)
                .unwrap_or_default();

            match self
                .client
                .share_fetch(
                    &self.config.group_id,
                    &self.member_id,
                    &self.config.topic,
                    partition,
                    remaining,
                    self.config.max_bytes,
                    self.config.lock_timeout.as_millis() as u32,
                    &acks,
                )
                .await
            {
                Ok(batches) => {
                    // Only clear now that the broker has actually applied them — a fetch
                    // that never reached the broker (see the Err arm) must not lose them.
                    if !acks.is_empty() {
                        self.staged_acks.remove(&partition);
                    }
                    let in_flight = self.in_flight.entry(partition).or_default();
                    for batch in batches {
                        for record in batch.records {
                            in_flight.insert(record.offset, batch.delivery_count);
                            collected.push(ShareRecord {
                                partition,
                                offset: record.offset,
                                timestamp: record.timestamp,
                                key: record.key,
                                value: record.value,
                                delivery_count: batch.delivery_count,
                            });
                        }
                    }
                }
                Err(e) => {
                    // An unreachable broker must not lose records already collected from
                    // other partitions this round, and must leave this partition's staged
                    // acks in place for the next poll to retry — move on to the next
                    // partition. But the failure itself must not be thrown away: remember
                    // the first one so it can be surfaced below if nothing at all came
                    // back this round (see the doc comment above).
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                    // Reconnect at most once per `poll()` call — a round over several dead
                    // partitions must not reconnect once per partition.
                    if !reconnected {
                        reconnected = true;
                        let _ = self.client.reconnect().await;
                    }
                }
            }
        }

        self.next_partition_idx = (self.next_partition_idx + 1) % partition_count.max(1);

        if collected.is_empty() {
            if let Some(e) = first_error {
                return Err(e);
            }
        }
        Ok(collected)
    }

    /// Stages `partition`/`offset` as `Accept` and drops it from the in-flight set.
    /// Synchronous and non-network — see [`Self::flush_acks`] for when it actually reaches
    /// the broker.
    pub fn accept(&mut self, partition: u32, offset: u64) -> IoResult<()> {
        self.stage_ack(partition, offset, AcknowledgeType::Accept)
    }

    /// Stages `partition`/`offset` as `Release`, making it immediately eligible for
    /// redelivery to any member instead of waiting out the lease.
    pub fn release(&mut self, partition: u32, offset: u64) -> IoResult<()> {
        self.stage_ack(partition, offset, AcknowledgeType::Release)
    }

    /// Stages `partition`/`offset` as `Reject`, routing it to the topic's DLQ.
    pub fn reject(&mut self, partition: u32, offset: u64) -> IoResult<()> {
        self.stage_ack(partition, offset, AcknowledgeType::Reject)
    }

    /// Removes `offset` from the in-flight set for `partition` and stages `ack_type` for
    /// it. Errors — rather than silently sending it anyway — if `offset` was never leased
    /// to this member on this partition: acknowledging a record this consumer does not
    /// hold is a caller bug, and the broker should never be asked to resolve a lease this
    /// side has no record of acquiring.
    fn stage_ack(
        &mut self,
        partition: u32,
        offset: u64,
        ack_type: AcknowledgeType,
    ) -> IoResult<()> {
        let removed = self
            .in_flight
            .get_mut(&partition)
            .is_some_and(|offsets| offsets.remove(&offset).is_some());
        if !removed {
            return Err(std::io::Error::other(format!(
                "cannot acknowledge partition {partition} offset {offset}: it is not \
                 currently in flight for member {} — this consumer was never leased that \
                 record, or already resolved it",
                self.member_id
            )));
        }
        self.staged_acks
            .entry(partition)
            .or_default()
            .insert(offset, ack_type);
        Ok(())
    }

    /// Sends every staged acknowledgement now, instead of waiting for the next `poll` to
    /// piggyback it. Partitions with nothing staged are skipped — no empty requests. A
    /// partition's staged acks are cleared only once its `share_acknowledge` call succeeds.
    pub async fn flush_acks(&mut self) -> IoResult<()> {
        let partitions: Vec<u32> = self
            .staged_acks
            .iter()
            .filter(|(_, acks)| !acks.is_empty())
            .map(|(&partition, _)| partition)
            .collect();

        for partition in partitions {
            let batches = match self.staged_acks.get(&partition) {
                Some(acks) => coalesce_acks(acks),
                None => continue,
            };
            self.client
                .share_acknowledge(
                    &self.config.group_id,
                    &self.member_id,
                    &self.config.topic,
                    partition,
                    &batches,
                )
                .await?;
            self.staged_acks.remove(&partition);
        }
        Ok(())
    }

    /// Extends the lease on everything currently in flight for `partition`, for a caller
    /// whose processing legitimately outruns `lock_timeout`. Coalesces the *in-flight*
    /// offsets (not the staged acks — a renew must not resolve anything) into `Renew`
    /// batches by building a temporary map and reusing [`coalesce_acks`], the same way
    /// `flush_acks` does for staged acks. In-flight state is unchanged: a renew extends a
    /// lease, it does not accept, release, or reject the record. A no-op when nothing is
    /// in flight for `partition`.
    pub async fn renew(&mut self, partition: u32) -> IoResult<()> {
        let renew_map: BTreeMap<u64, AcknowledgeType> = match self.in_flight.get(&partition) {
            Some(offsets) if !offsets.is_empty() => offsets
                .keys()
                .map(|&offset| (offset, AcknowledgeType::Renew))
                .collect(),
            _ => return Ok(()),
        };
        let batches = coalesce_acks(&renew_map);
        self.client
            .share_acknowledge(
                &self.config.group_id,
                &self.member_id,
                &self.config.topic,
                partition,
                &batches,
            )
            .await
    }

    /// Releases everything still in flight, then stops heartbeating. Releasing first is the
    /// entire point of leaving explicitly rather than just dropping this value: it hands
    /// every still-leased record back to the group immediately, instead of leaving it
    /// locked to a member id no one holds anymore until `lock_timeout` eventually expires
    /// it — see the note on [`Drop`] below.
    pub async fn leave(&mut self) -> IoResult<()> {
        for (&partition, offsets) in self.in_flight.iter() {
            if offsets.is_empty() {
                continue;
            }
            let staged = self.staged_acks.entry(partition).or_default();
            for &offset in offsets.keys() {
                staged.insert(offset, AcknowledgeType::Release);
            }
        }
        self.in_flight.clear();

        let result = self.flush_acks().await;
        self.stop_heartbeat_task();
        result
    }

    /// Aborts the background heartbeat task, if it's still running. Idempotent — safe to
    /// call from both `leave()` and `Drop`.
    fn stop_heartbeat_task(&mut self) {
        if let Some(handle) = self.heartbeat_task.take() {
            handle.abort();
        }
    }
}

impl Drop for ShareConsumer {
    /// Guarantees the background heartbeat task does not outlive this consumer, exactly as
    /// `GroupConsumer`'s `Drop` does. `Drop` cannot `.await`, so it cannot run the release
    /// [`Self::leave`] does — anything still in flight when a `ShareConsumer` is simply
    /// dropped stays locked to this member id until the broker's own lease expiry reclaims
    /// it after `lock_timeout`. An explicit `leave()` is what gets that work redistributed
    /// promptly; relying on `Drop` alone means waiting out the lock.
    fn drop(&mut self) {
        self.stop_heartbeat_task();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> ShareConsumerConfig {
        ShareConsumerConfig {
            group_id: "test-group".to_string(),
            topic: "test-topic".to_string(),
            partitions: vec![0, 1],
            member_id: "test-member".to_string(),
            max_records: 100,
            max_bytes: 1024,
            lock_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
        }
    }

    // -- coalesce_acks -------------------------------------------------------------

    #[test]
    fn coalesce_empty_map_yields_empty_vec() {
        let acks: BTreeMap<u64, AcknowledgeType> = BTreeMap::new();
        let batches = coalesce_acks(&acks);
        assert!(
            batches.is_empty(),
            "an empty ack map must coalesce to no batches, got {batches:?}"
        );
    }

    #[test]
    fn coalesce_single_offset_yields_one_batch_with_equal_bounds() {
        let mut acks = BTreeMap::new();
        acks.insert(5, AcknowledgeType::Accept);
        let batches = coalesce_acks(&acks);
        assert_eq!(
            batches,
            vec![AckBatch {
                first_offset: 5,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept,
            }],
            "a single offset must produce one batch whose first and last offset are equal"
        );
    }

    #[test]
    fn coalesce_contiguous_same_type_merges_into_one_batch() {
        let mut acks = BTreeMap::new();
        for offset in 0..=3u64 {
            acks.insert(offset, AcknowledgeType::Accept);
        }
        let batches = coalesce_acks(&acks);
        assert_eq!(
            batches,
            vec![AckBatch {
                first_offset: 0,
                last_offset: 3,
                ack_type: AcknowledgeType::Accept,
            }],
            "four contiguous same-type acks must merge into exactly one batch, got {batches:?}"
        );
    }

    #[test]
    fn coalesce_gap_splits_into_two_batches() {
        let mut acks = BTreeMap::new();
        acks.insert(0, AcknowledgeType::Accept);
        acks.insert(1, AcknowledgeType::Accept);
        acks.insert(3, AcknowledgeType::Accept);
        let batches = coalesce_acks(&acks);
        assert_eq!(
            batches,
            vec![
                AckBatch {
                    first_offset: 0,
                    last_offset: 1,
                    ack_type: AcknowledgeType::Accept,
                },
                AckBatch {
                    first_offset: 3,
                    last_offset: 3,
                    ack_type: AcknowledgeType::Accept,
                },
            ],
            "a gap at offset 2 must split the run into two batches, got {batches:?}"
        );
    }

    #[test]
    fn coalesce_type_change_splits_even_when_contiguous() {
        let mut acks = BTreeMap::new();
        acks.insert(0, AcknowledgeType::Accept);
        acks.insert(1, AcknowledgeType::Release);
        let batches = coalesce_acks(&acks);
        assert_eq!(
            batches,
            vec![
                AckBatch {
                    first_offset: 0,
                    last_offset: 0,
                    ack_type: AcknowledgeType::Accept,
                },
                AckBatch {
                    first_offset: 1,
                    last_offset: 1,
                    ack_type: AcknowledgeType::Release,
                },
            ],
            "contiguous offsets of different ack types must not merge, got {batches:?}"
        );
    }

    #[test]
    fn coalesce_mixed_run_produces_exactly_three_batches() {
        let mut acks = BTreeMap::new();
        acks.insert(0, AcknowledgeType::Accept);
        acks.insert(1, AcknowledgeType::Accept);
        acks.insert(2, AcknowledgeType::Release);
        acks.insert(3, AcknowledgeType::Release);
        acks.insert(5, AcknowledgeType::Accept);
        let batches = coalesce_acks(&acks);
        assert_eq!(
            batches,
            vec![
                AckBatch {
                    first_offset: 0,
                    last_offset: 1,
                    ack_type: AcknowledgeType::Accept,
                },
                AckBatch {
                    first_offset: 2,
                    last_offset: 3,
                    ack_type: AcknowledgeType::Release,
                },
                AckBatch {
                    first_offset: 5,
                    last_offset: 5,
                    ack_type: AcknowledgeType::Accept,
                },
            ],
            "a run mixing two type changes and a gap must yield exactly three batches, got \
             {batches:?}"
        );
    }

    #[test]
    fn coalesce_renew_run_merges_like_any_other_type() {
        let mut acks = BTreeMap::new();
        for offset in 10..=12u64 {
            acks.insert(offset, AcknowledgeType::Renew);
        }
        let batches = coalesce_acks(&acks);
        assert_eq!(
            batches,
            vec![AckBatch {
                first_offset: 10,
                last_offset: 12,
                ack_type: AcknowledgeType::Renew,
            }],
            "a contiguous Renew run must coalesce exactly like Accept/Release/Reject, got \
             {batches:?}"
        );
    }

    // -- ShareConsumerConfig::validate ----------------------------------------------

    #[test]
    fn validate_accepts_a_fully_populated_config() {
        assert!(
            valid_config().validate().is_ok(),
            "a fully populated, internally consistent config must validate"
        );
    }

    #[test]
    fn validate_rejects_empty_group_id() {
        let mut config = valid_config();
        config.group_id = String::new();
        assert!(
            config.validate().is_err(),
            "an empty group_id must be rejected"
        );
    }

    #[test]
    fn validate_rejects_empty_topic() {
        let mut config = valid_config();
        config.topic = String::new();
        assert!(
            config.validate().is_err(),
            "an empty topic must be rejected"
        );
    }

    #[test]
    fn validate_rejects_empty_partitions() {
        let mut config = valid_config();
        config.partitions = Vec::new();
        assert!(
            config.validate().is_err(),
            "an empty partitions list must be rejected — it would poll forever and never \
             return a record"
        );
    }

    #[test]
    fn validate_rejects_zero_max_records() {
        let mut config = valid_config();
        config.max_records = 0;
        assert!(
            config.validate().is_err(),
            "max_records of zero must be rejected"
        );
    }

    #[test]
    fn validate_rejects_zero_max_bytes() {
        let mut config = valid_config();
        config.max_bytes = 0;
        assert!(
            config.validate().is_err(),
            "max_bytes of zero must be rejected"
        );
    }

    #[test]
    fn validate_rejects_zero_lock_timeout() {
        let mut config = valid_config();
        config.lock_timeout = Duration::ZERO;
        assert!(
            config.validate().is_err(),
            "a zero lock_timeout must be rejected"
        );
    }

    #[test]
    fn validate_rejects_zero_heartbeat_interval() {
        let mut config = valid_config();
        config.heartbeat_interval = Duration::ZERO;
        assert!(
            config.validate().is_err(),
            "a zero heartbeat_interval must be rejected"
        );
    }

    #[test]
    fn validate_rejects_heartbeat_interval_greater_than_lock_timeout() {
        let mut config = valid_config();
        config.lock_timeout = Duration::from_secs(1);
        config.heartbeat_interval = Duration::from_secs(2);
        let err = config
            .validate()
            .expect_err("heartbeat_interval > lock_timeout must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("heartbeat_interval") && message.contains("lock_timeout"),
            "the error should name both fields so the misconfiguration is self-evident, got: \
             {message}"
        );
    }

    #[test]
    fn validate_rejects_heartbeat_interval_equal_to_lock_timeout() {
        let mut config = valid_config();
        config.lock_timeout = Duration::from_secs(1);
        config.heartbeat_interval = Duration::from_secs(1);
        let err = config
            .validate()
            .expect_err("heartbeat_interval == lock_timeout must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("heartbeat_interval") && message.contains("lock_timeout"),
            "the error should name both fields so the misconfiguration is self-evident, got: \
             {message}"
        );
    }
}
