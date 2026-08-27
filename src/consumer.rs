use crate::client::TestClient;
use crate::protocol::wire::MemberAssignment;
use crate::segment::Record;
use std::collections::HashMap;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// range assignor: divides `partition_count` partitions evenly across
/// `member_ids`, giving the first `partition_count % member_ids.len()` members one extra
/// partition each so every partition still lands somewhere. Members are sorted first so
/// every member (and the leader submitting on everyone's behalf) computes the same answer
/// from the same inputs — this must be deterministic, since only the leader actually runs
/// it and every follower has to trust the result without recomputing it.
///
/// Partitions are handed out in contiguous blocks (member 0 gets `[0, k)`, member 1 gets
/// `[k, 2k)`, ...), a contiguous-range convention rather than an interleaved
/// round-robin.
///
/// Every member id passed in is present in the output, even one that lands past the
/// partition count and so gets an empty `Vec` — the caller (a `SyncGroup` submission) needs
/// an explicit empty assignment for such a member, not its absence from the list. An empty
/// `member_ids`, or a `partition_count` of zero, produces an empty result rather than
/// panicking.
pub fn assign_range(partition_count: u32, member_ids: &[String]) -> Vec<(String, Vec<u32>)> {
    if member_ids.is_empty() {
        return Vec::new();
    }

    let mut sorted_ids: Vec<String> = member_ids.to_vec();
    sorted_ids.sort();

    let member_count = sorted_ids.len() as u32;
    let base = partition_count / member_count;
    let remainder = partition_count % member_count;

    let mut result = Vec::with_capacity(sorted_ids.len());
    let mut next_partition = 0u32;
    for (i, member_id) in sorted_ids.into_iter().enumerate() {
        let share = base + u32::from((i as u32) < remainder);
        let partitions: Vec<u32> = (next_partition..next_partition + share).collect();
        next_partition += share;
        result.push((member_id, partitions));
    }
    result
}

/// round-robin assignor: deals `partition_count` partitions out one at a time
/// across `member_ids`, in order, wrapping back to the first member after the last. Members
/// are sorted first for the same reason as [`assign_range`] — only the leader runs this, and
/// every follower has to trust the result without recomputing it, so the answer must be
/// deterministic from the same inputs.
///
/// Unlike `assign_range`, a member's partitions are scattered rather than a contiguous
/// block: member 0 gets `0, k, 2k, ...`, member 1 gets `1, k+1, 2k+1, ...`, and so on. This
/// spreads load more evenly when partitions vary in size, at the cost of losing the key
/// locality a contiguous range gives.
///
/// Every member id passed in is present in the output, even one that lands past the
/// partition count and so gets an empty `Vec` — same contract as `assign_range`. An empty
/// `member_ids`, or a `partition_count` of zero, produces an empty result rather than
/// panicking.
pub fn assign_roundrobin(partition_count: u32, member_ids: &[String]) -> Vec<(String, Vec<u32>)> {
    if member_ids.is_empty() {
        return Vec::new();
    }

    let mut sorted_ids: Vec<String> = member_ids.to_vec();
    sorted_ids.sort();

    let mut result: Vec<(String, Vec<u32>)> = sorted_ids
        .into_iter()
        .map(|member_id| (member_id, Vec::new()))
        .collect();

    let member_count = result.len();
    for partition in 0..partition_count {
        result[partition as usize % member_count].1.push(partition);
    }
    result
}

/// Sticky assignor, minus the cooperative-protocol half of it): unlike
/// [`assign_range`] and [`assign_roundrobin`], which derive every member's share purely
/// from its *position* in a sorted member list, this one takes the group's *previous*
/// assignment into account so that a membership change moves as few partitions as possible.
///
/// Position-based assignors reshuffle nearly everything on any membership change: adding or
/// removing one member shifts almost every other member's index in the sorted list, and with
/// it almost every partition it owns. Every partition that moves discards whatever local
/// state a consumer built for it (in-memory aggregates, warmed caches, ...) even though most
/// members had no reason to lose anything. This assignor instead keeps a still-present
/// member's previously-owned partitions in place and only reshuffles what it must to stay
/// balanced.
///
/// Algorithm:
/// 1. **Keep**: for each member still in the group, keep whichever of its previously-owned
///    partitions still exist in the topic (a partition owned by a member who has since left,
///    or that no longer exists, is not kept by anyone).
/// 2. **Collect the remainder**: every partition not kept by its previous owner — new
///    partitions, and partitions orphaned by a departed member — goes into an unassigned
///    pool.
/// 3. **Fill**: hand the pool out to members with the fewest partitions first, so members
///    who kept little get topped up before members who kept a lot get more.
/// 4. **Level**: while any member holds more than `ceil(partition_count / member_count)`,
///    move one partition from it to a member below `floor(partition_count / member_count)`.
///    A partition the receiving member did not previously own is preferred, so a partition
///    is not thrashed back to an owner it was just taken from purely to hit the target count.
///
/// Every step sorts its inputs, so — like [`assign_range`] and [`assign_roundrobin`] — the
/// result is deterministic: the same `(partition_count, member_ids, previous_assignment)`
/// always produces the same output, which matters here more than for the position-based
/// assignors since the leader is folding in state (the previous assignment) rather than
/// just recomputing from scratch.
///
/// Same empty-input contract as [`assign_range`]: an empty `member_ids` or zero
/// `partition_count` produces an empty result, and a member past the partition count still
/// appears in the output with an empty `Vec` rather than being omitted.
pub fn assign_sticky(
    partition_count: u32,
    member_ids: &[String],
    previous_assignment: &HashMap<String, Vec<u32>>,
) -> Vec<(String, Vec<u32>)> {
    if member_ids.is_empty() {
        return Vec::new();
    }

    let mut sorted_ids: Vec<String> = member_ids.to_vec();
    sorted_ids.sort();
    let member_set: std::collections::HashSet<&str> =
        sorted_ids.iter().map(String::as_str).collect();

    // Step 1: keep whichever previously-owned partitions are still valid — still exist, and
    // still owned by a member still in the group.
    let mut owned: HashMap<String, Vec<u32>> = HashMap::with_capacity(sorted_ids.len());
    for member_id in &sorted_ids {
        owned.insert(member_id.clone(), Vec::new());
    }
    let mut claimed = vec![false; partition_count as usize];
    // Iterate previous owners in sorted order so which duplicate claim (if the input were
    // ever inconsistent) wins is deterministic rather than HashMap-iteration-order dependent.
    let mut previous_owners: Vec<&String> = previous_assignment.keys().collect();
    previous_owners.sort();
    for member_id in previous_owners {
        if !member_set.contains(member_id.as_str()) {
            continue;
        }
        let mut kept: Vec<u32> = previous_assignment[member_id]
            .iter()
            .copied()
            .filter(|&p| p < partition_count && !claimed[p as usize])
            .collect();
        kept.sort_unstable();
        for &p in &kept {
            claimed[p as usize] = true;
        }
        owned.insert(member_id.clone(), kept);
    }

    // Step 2: collect the unassigned remainder, in order.
    let mut remainder: Vec<u32> = (0..partition_count)
        .filter(|&p| !claimed[p as usize])
        .collect();

    // Step 3: fill — hand the remainder to whichever member currently has the fewest
    // partitions, breaking ties by member id so the outcome is deterministic.
    remainder.sort_unstable();
    for partition in remainder {
        let target = sorted_ids
            .iter()
            .min_by_key(|id| (owned[id.as_str()].len(), id.as_str()))
            .expect("sorted_ids is non-empty")
            .clone();
        owned.get_mut(&target).unwrap().push(partition);
    }

    // Step 4: level — while the most- and least-loaded members are more than one
    // partition apart, move one from the former to the latter. Ties are broken by member
    // id (both the max and the min searches below run over `sorted_ids`, already sorted),
    // so this converges to a deterministic balanced state: no member ends up holding more
    // than `ceil(partition_count / member_count)`, and none holds fewer than
    // `floor(partition_count / member_count)`.
    //
    // Comparing counts directly (rather than each against a precomputed ceiling/floor) is
    // what makes this correct for e.g. a brand-new member with zero partitions joining a
    // group where everyone else already sits exactly at the ceiling: such a member is
    // never "over" the ceiling, but the gap between it and everyone else is still > 1 and
    // must be closed.
    if !sorted_ids.is_empty() {
        loop {
            // `sorted_ids` is sorted ascending, so `find`ing the first id at the max (or
            // min) count — rather than `max_by_key`/`min_by_key`, which break ties toward
            // the *last* matching element — ties both searches to the smallest id, the
            // same tie-break `fill` above uses.
            let max_count = sorted_ids
                .iter()
                .map(|id| owned[id.as_str()].len())
                .max()
                .unwrap();
            let min_count = sorted_ids
                .iter()
                .map(|id| owned[id.as_str()].len())
                .min()
                .unwrap();
            if max_count <= min_count + 1 {
                break;
            }
            let over = sorted_ids
                .iter()
                .find(|id| owned[id.as_str()].len() == max_count)
                .cloned()
                .unwrap();
            let under = sorted_ids
                .iter()
                .find(|id| owned[id.as_str()].len() == min_count)
                .cloned()
                .unwrap();

            let over_partitions = owned.get_mut(&over).unwrap();
            // Prefer moving a partition `under` did not previously own, so a partition is
            // not moved only to be moved straight back to where it started.
            let previously_under = previous_assignment
                .get(&under)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let idx = over_partitions
                .iter()
                .position(|p| !previously_under.contains(p))
                .unwrap_or(0);
            let moved = over_partitions.remove(idx);
            owned.get_mut(&under).unwrap().push(moved);
        }
    }

    sorted_ids
        .into_iter()
        .map(|id| {
            let mut partitions = owned.remove(&id).unwrap_or_default();
            partitions.sort_unstable();
            (id, partitions)
        })
        .collect()
}

/// Which partition assignment strategy the group negotiated, per [`assign_range`],
/// [`assign_roundrobin`], and [`assign_sticky`].
///
/// This mirrors [`crate::protocol::IsolationLevel`]'s house style: a small `Copy` enum, a
/// permissive `from_*` constructor that never fails on an unrecognised input, and a default
/// that matches the strategy this crate always used before the strategy became selectable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AssignmentStrategy {
    /// Contiguous blocks per member. The historical, and still default, behavior — see
    /// [`assign_range`].
    #[default]
    Range,
    /// Partitions dealt out one at a time across members — see [`assign_roundrobin`].
    RoundRobin,
    /// Keeps each member's previous partitions where possible — see [`assign_sticky`].
    Sticky,
    /// Same underlying computation as [`Sticky`](Self::Sticky) — see [`assign_sticky`] —
    /// but applied incrementally: the leader hands out only the
    /// intersection of a member's current and target partitions in a first round, and
    /// only the partitions that actually have to move go through a revoke-then-reassign
    /// second round. See `GroupConsumer::rejoin`'s leader branch and
    /// [`cooperative_round_one`] for the two-round mechanics this drives; the assignment
    /// *algorithm* itself is untouched, only how its output is rolled out.
    CooperativeSticky,
}

impl AssignmentStrategy {
    /// Maps a negotiated `protocol_name` (as returned by `JoinGroup` /
    /// `DescribeGroup`) to a strategy.
    ///
    /// An empty or unrecognised name — an older client, a newer client offering a strategy
    /// this build does not know, or a group that has not negotiated one yet — falls back to
    /// `Range` rather than failing the join outright, mirroring how
    /// `IsolationLevel::from_byte` treats an unknown byte.
    pub fn from_protocol_name(name: &str) -> Self {
        match name {
            "roundrobin" => AssignmentStrategy::RoundRobin,
            "range" => AssignmentStrategy::Range,
            "sticky" => AssignmentStrategy::Sticky,
            // the conventional cooperative sticky assignor name — matching
            // it exactly is what lets `ConsumerGroup::is_cooperative`'s "contains
            // cooperative" check recognise a group that negotiated this without any
            // wire-visible change.
            "cooperative-sticky" => AssignmentStrategy::CooperativeSticky,
            other => {
                if !other.is_empty() {
                    tracing::warn!(
                        protocol = other,
                        "unrecognised partition assignment protocol, defaulting to range"
                    );
                }
                AssignmentStrategy::Range
            }
        }
    }

    /// Runs this strategy's assignment function.
    ///
    /// `previous_assignment` carries the group's prior `member_id -> Vec<partition>` mapping
    /// for the subscribed topic (empty for a brand-new group, or when the negotiated
    /// strategy has no use for it). `Range` and `RoundRobin` ignore it entirely — they are
    /// purely position-based and must keep producing exactly the assignment they always
    /// have; only `Sticky` consults it.
    pub fn assign(
        self,
        partition_count: u32,
        member_ids: &[String],
        previous_assignment: &HashMap<String, Vec<u32>>,
    ) -> Vec<(String, Vec<u32>)> {
        match self {
            AssignmentStrategy::Range => assign_range(partition_count, member_ids),
            AssignmentStrategy::RoundRobin => assign_roundrobin(partition_count, member_ids),
            AssignmentStrategy::Sticky | AssignmentStrategy::CooperativeSticky => {
                assign_sticky(partition_count, member_ids, previous_assignment)
            }
        }
    }

    /// Whether this strategy is applied incrementally rather than
    /// stop-the-world. Only [`CooperativeSticky`](Self::CooperativeSticky) is — `Sticky`
    /// computes the same *target*, per [`assign_sticky`], but still hands it out in one
    /// shot, same as `Range`/`RoundRobin` always have.
    ///
    /// Named to match [`crate::server::coordinator::ConsumerGroup::is_cooperative`] (same
    /// question, asked from the coordinator's side of the protocol name instead of a
    /// parsed strategy), which this must stay consistent with: both ultimately key off
    /// the same negotiated `protocol_name` containing "cooperative".
    pub fn is_cooperative(self) -> bool {
        matches!(self, AssignmentStrategy::CooperativeSticky)
    }
}

/// Round one of a cooperative rebalance: for each member, narrows its freshly
/// computed `target` down to the intersection with what it already owned per
/// `previous_assignment` — literally `target(m) ∩ previous(m)`, nothing cleverer. A
/// partition in a member's target that it did not already own itself — whether it's
/// moving over from a different member, or was never owned by anyone in
/// `previous_assignment` at all (a brand-new group, or brand-new partitions) — is left out
/// of the member's keep-set entirely, not handed to it early.
///
/// That last case is a deliberate simplification, not an oversight: a genuinely unowned
/// partition would actually be *safe* to hand out immediately (nobody needs to revoke it
/// first), but distinguishing "unowned by anyone" from "owned by someone else" here would
/// mean computing a second, cleverer notion of "safe to hand out now" beyond the plain
/// per-member intersection. Plain intersection is what round one submits; the cost is that
/// a brand-new group's very first assignment — or any addition of new partitions — costs
/// one extra (harmless, self-contained) round to hand out, same as an actual conflict
/// would.
///
/// Returns `(keep_sets, needs_second_round)`. `keep_sets` is submitted via `SyncGroup`
/// instead of `target` — see `GroupConsumer::rejoin`'s leader branch.
/// `needs_second_round` is true the moment any member's keep-set came out smaller than its
/// target, i.e. something had to be held back (given up by its old owner, or newly
/// available to a new one) — exactly the condition under which a second round is required
/// to actually deliver it.
pub fn cooperative_round_one(
    target: &[(String, Vec<u32>)],
    previous_assignment: &HashMap<String, Vec<u32>>,
) -> (Vec<(String, Vec<u32>)>, bool) {
    let mut needs_second_round = false;
    let keep_sets = target
        .iter()
        .map(|(member_id, target_partitions)| {
            let previously_owned = previous_assignment
                .get(member_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let kept: Vec<u32> = target_partitions
                .iter()
                .copied()
                .filter(|p| previously_owned.contains(p))
                .collect();
            if kept.len() != target_partitions.len() {
                needs_second_round = true;
            }
            (member_id.clone(), kept)
        })
        .collect();
    (keep_sets, needs_second_round)
}

/// Configuration for a [`GroupConsumer`].
#[derive(Debug, Clone)]
pub struct GroupConsumerConfig {
    pub group_id: String,
    pub topic: String,
    /// `group.instance.id` static membership). `None` means this member joins
    /// dynamically — every process start is a new arrival and the group rebalances around
    /// it. See `TestClient::join_group_static` for what setting this buys.
    pub instance_id: Option<String>,
    /// Assignment protocols this member offers, most preferred first. The group as a whole
    /// negotiates one name (see `GroupCoordinator::join_group`) — offering `"range"`,
    /// `"roundrobin"`, and `"sticky"` here, rather than only the default, lets a group
    /// actually pick any of them instead of `"range"` being the sole option a member could
    /// ever propose.
    pub protocols: Vec<String>,
    pub fetch_max_bytes: u32,
    /// How long a follower keeps retrying a `REBALANCE_IN_PROGRESS` `SyncGroup` response
    /// before giving up and returning an error.
    ///
    /// This wait does not itself send a heartbeat, but it no longer needs to: the
    /// background heartbeat task (see [`GroupConsumer`]) already learned this rejoin's new
    /// generation before `SyncGroup` was ever called (`rejoin` publishes it as soon as
    /// `JoinGroup` returns) and keeps heartbeating on its own connection throughout this
    /// wait. Before issue #53 this comment warned that `sync_retry_timeout` had to stay
    /// comfortably under the session timeout or the member would be pruned out from under
    /// itself; that is no longer the constraint, since liveness during the wait comes from
    /// the background task rather than from this loop finishing quickly.
    pub sync_retry_timeout: Duration,
    pub sync_retry_interval: Duration,
    /// `session.timeout.ms`: how long the group coordinator will wait for a heartbeat
    /// before considering this member dead and evicting it
    /// (`GroupCoordinator::prune_expired_members`). Sent on `JoinGroup` as the
    /// `SESSION_TIMEOUT_MS` tagged field and honored by the coordinator — clamped to its
    /// own sane range (`MIN_SESSION_TIMEOUT`..=`MAX_SESSION_TIMEOUT`) rather than trusted
    /// outright — instead of the historical hardcoded 10s. Also sizes `heartbeat_interval`
    /// against, per [`GroupConsumerConfig::validate`].
    pub session_timeout: Duration,
    /// How often the background heartbeat task (see [`GroupConsumer`]) sends a heartbeat,
    /// independent of whether the application is calling [`GroupConsumer::poll`]. Must be
    /// meaningfully shorter than `session_timeout` — [`GroupConsumerConfig::validate`]
    /// enforces this at construction time rather than letting a too-long interval silently
    /// guarantee eviction.
    pub heartbeat_interval: Duration,
}

impl Default for GroupConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            topic: String::new(),
            instance_id: None,
            protocols: vec![
                "range".to_string(),
                "roundrobin".to_string(),
                "sticky".to_string(),
            ],
            fetch_max_bytes: 64 * 1024,
            sync_retry_timeout: Duration::from_secs(5),
            sync_retry_interval: Duration::from_millis(50),
            session_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(3),
        }
    }
}

impl GroupConsumerConfig {
    /// Checks `heartbeat_interval` against `session_timeout` before a [`GroupConsumer`] is
    /// built from this config.
    ///
    /// An interval greater than or equal to the timeout guarantees eviction — the
    /// coordinator prunes a member once `now - last_heartbeat > session_timeout`
    /// (`GroupCoordinator::prune_expired_members`), so a heartbeat that lands no more often
    /// than the timeout itself can never land in time. This requires a comfortable margin
    /// rather than merely `<`: `heartbeat_interval <= session_timeout / 3`, the common rule
    /// of thumb (a common default ratio), so a couple of missed or delayed
    /// heartbeats in a row still don't cost the member its slot. A bad value fails loudly
    /// here rather than being silently clamped into something that happens to work.
    pub fn validate(&self) -> IoResult<()> {
        if self.session_timeout.is_zero() {
            return Err(std::io::Error::other(
                "GroupConsumerConfig::session_timeout must be greater than zero",
            ));
        }
        if self.heartbeat_interval * 3 > self.session_timeout {
            return Err(std::io::Error::other(format!(
                "GroupConsumerConfig::heartbeat_interval ({:?}) must be at most a third of \
                 session_timeout ({:?}), or a live member risks eviction for heartbeating \
                 too rarely",
                self.heartbeat_interval, self.session_timeout
            )));
        }
        Ok(())
    }
}

/// A consumer that belongs to a consumer group.
///
/// Which partitions this consumer reads from `config.topic` comes from the group's
/// membership and the negotiated assignment — never from a partition number the caller
/// picked. That is the point of this type: see `GroupCoordinator` in
/// `src/server/coordinator.rs` for the join/sync/heartbeat protocol it drives, and
/// `TestClient` for the wire calls it wraps.
///
/// Heartbeating runs on a dedicated background task, on its own connection to the broker
/// (opened via `TestClient::addr`), independent of whatever the application is doing with
/// [`Self::poll`]. This is issue #53: previously a heartbeat only went out as a side effect
/// of the application calling `poll()`, so a caller stuck on a slow batch — or blocked in
/// `rejoin`'s `SyncGroup` retry wait — stopped heartbeating and could be evicted from a
/// group it was still healthy in. Reporting liveness on its own schedule, decoupled from
/// application pace, is what lets `session_timeout` be set short enough to detect an
/// actually-dead consumer quickly without evicting a live one that is merely busy.
///
/// The task cannot safely share the consumer's own `TestClient`: a single connection is
/// strictly request-then-response, so a heartbeat queued behind an in-flight fetch would
/// still arrive late — exactly the failure this exists to fix. It also cannot own
/// `generation_id`/`member_id` directly (those live behind `&mut self` on the consumer, and
/// change on every [`Self::rejoin`]), so the current identity is threaded to it over a
/// `tokio::sync::watch` channel and a heartbeat rejection (stale generation, fenced member)
/// is reported back via `needs_rejoin` rather than being handled — or swallowed — inside
/// the task itself. [`Self::poll`] checks that flag and drives `rejoin()` on the consumer's
/// own connection when it's set.
///
/// The task is stopped — via `heartbeat_task`'s `JoinHandle::abort` — on [`Self::leave`]
/// and on `Drop`, so a consumer that goes away without an explicit `leave()` doesn't keep
/// heartbeating for a member no one holds anymore.
pub struct GroupConsumer {
    client: TestClient,
    config: GroupConsumerConfig,
    member_id: String,
    generation_id: u32,
    is_leader: bool,
    /// Partitions of `config.topic` this member currently owns, sorted.
    assignment: Vec<u32>,
    next_offsets: HashMap<u32, u64>,
    /// Highest offset consumed but not yet committed, per partition.
    pending_commits: HashMap<u32, u64>,
    /// Current (member_id, generation_id), published for the background heartbeat task to
    /// read. Updated by `rejoin` as soon as `JoinGroup` returns — before `SyncGroup` even
    /// runs — so the task heartbeats under the new generation for the rest of the rejoin,
    /// not the stale one that got it here.
    identity_tx: watch::Sender<HeartbeatIdentity>,
    /// Set by the background heartbeat task when a heartbeat is rejected for a stale
    /// generation or a fenced member. `poll()` checks and clears this before fetching, and
    /// drives `rejoin()` when it finds it set — this is the only path a rejection reaches
    /// the consume loop by; it is never left to just fail silently in the background.
    needs_rejoin: Arc<AtomicBool>,
    /// The background heartbeat task. `None` only in the brief window inside `join()`
    /// before the first `rejoin()` has produced a member id/generation to heartbeat.
    heartbeat_task: Option<JoinHandle<()>>,
}

/// The identity a heartbeat is sent under. Threaded from [`GroupConsumer`] to its
/// background heartbeat task over a `watch` channel — see the task-lifecycle notes on
/// [`GroupConsumer`] for why a plain snapshot at spawn time isn't enough.
#[derive(Debug, Clone, Default)]
struct HeartbeatIdentity {
    member_id: String,
    generation_id: u32,
}

/// Runs on its own connection to `addr`, independent of the consumer's own `TestClient`.
/// Sends a heartbeat every `interval`, always for whatever `(member_id, generation_id)`
/// `identity_rx` currently holds rather than a value captured at spawn time. A rejected
/// heartbeat sets `needs_rejoin` for the consume loop to act on and drops the connection so
/// the next tick reconnects — covering both a stale-generation rejection and a plain
/// connection failure with the same recovery.
async fn run_heartbeat_task(
    addr: SocketAddr,
    group_id: String,
    interval: Duration,
    mut identity_rx: watch::Receiver<HeartbeatIdentity>,
    needs_rejoin: Arc<AtomicBool>,
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

        let identity = identity_rx.borrow_and_update().clone();
        if let Err(e) = active
            .heartbeat(&group_id, identity.generation_id, &identity.member_id)
            .await
        {
            tracing::debug!(
                group_id,
                member_id = identity.member_id,
                error = %e,
                "background heartbeat failed; flagging for rejoin"
            );
            needs_rejoin.store(true, Ordering::Release);
            // Reconnect fresh next tick — the failure may be connection-level rather than
            // a rejection, and a poisoned stream should not be reused either way.
            client = None;
        }
    }
}

impl GroupConsumer {
    /// Constructs a `GroupConsumer`, runs one full join+sync cycle, and starts the
    /// background heartbeat task — so the returned value already has a member id,
    /// generation and assignment, and stays a live member on its own from this point on
    /// without needing [`Self::poll`] to be called on any particular schedule.
    pub async fn join(client: TestClient, config: GroupConsumerConfig) -> IoResult<Self> {
        config.validate()?;
        let addr = client.addr();
        let heartbeat_interval = config.heartbeat_interval;
        let group_id = config.group_id.clone();

        let (identity_tx, identity_rx) = watch::channel(HeartbeatIdentity::default());
        let needs_rejoin = Arc::new(AtomicBool::new(false));

        let mut consumer = Self {
            client,
            config,
            member_id: String::new(),
            generation_id: 0,
            is_leader: false,
            assignment: Vec::new(),
            next_offsets: HashMap::new(),
            pending_commits: HashMap::new(),
            identity_tx,
            needs_rejoin: needs_rejoin.clone(),
            heartbeat_task: None,
        };
        consumer.rejoin().await?;

        consumer.heartbeat_task = Some(tokio::spawn(run_heartbeat_task(
            addr,
            group_id,
            heartbeat_interval,
            identity_rx,
            needs_rejoin,
        )));

        Ok(consumer)
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn generation_id(&self) -> u32 {
        self.generation_id
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    pub fn assignment(&self) -> &[u32] {
        &self.assignment
    }

    pub fn client_mut(&mut self) -> &mut TestClient {
        &mut self.client
    }

    /// Runs one join+sync cycle: `JoinGroup`, then `SyncGroup` (computing and submitting
    /// the assignment if this member turns out to be the leader), then refreshes
    /// `next_offsets` for the resulting assignment.
    async fn rejoin(&mut self) -> IoResult<()> {
        // Commit whatever is still pending before giving up ownership of these partitions
        // — this is the last moment this member unambiguously owns them, and no one else
        // has been handed them yet. Without it, whoever inherits a partition after this
        // rebalance resumes from a stale commit and re-processes everything this member
        // already handed back, on every single rebalance. Other systems commit in
        // `onPartitionsRevoked` for the same reason.
        //
        // Best-effort: a rejoin is often triggered by a broken connection in the first
        // place, so failing the whole rejoin over a failed commit would trade a bounded
        // replay for an outright failure to rejoin at all. Cleared either way — a commit
        // that failed is no more retryable here than one that succeeded.
        let pending: Vec<(u32, u64)> = self
            .pending_commits
            .iter()
            .map(|(&partition, &offset)| (partition, offset))
            .collect();
        for (partition, offset) in pending {
            let _ = self
                .client
                .commit_offset(&self.config.group_id, &self.config.topic, partition, offset)
                .await;
        }
        self.pending_commits.clear();

        let protocol_refs: Vec<&str> = self.config.protocols.iter().map(String::as_str).collect();
        // Pass the member id currently held (empty string on the very first join) so a
        // dynamic member rejoins as itself rather than as a brand new arrival.
        //
        // `config.session_timeout` rides along as the `SESSION_TIMEOUT_MS` tagged field —
        // the coordinator now actually uses it (clamped to its own sane range) as this
        // member's eviction threshold, rather than the historical hardcoded 10s.
        let join = self
            .client
            .join_group_with_session_timeout(
                &self.config.group_id,
                &self.member_id,
                self.config.instance_id.as_deref(),
                &protocol_refs,
                Some(self.config.session_timeout),
            )
            .await?;
        self.member_id = join.member_id;
        self.generation_id = join.generation_id;
        self.is_leader = join.is_leader;

        // Publish the new identity to the background heartbeat task right away, before
        // `SyncGroup` even runs — not at the end of `rejoin`. The task then heartbeats
        // under the current generation for the rest of this rejoin (including the
        // follower `SyncGroup` retry wait below, which does not itself heartbeat), rather
        // than continuing to send a now-stale generation on its own connection until this
        // whole method returns. Also clears any pending rejoin signal: it's being acted on
        // right now.
        let _ = self.identity_tx.send(HeartbeatIdentity {
            member_id: self.member_id.clone(),
            generation_id: self.generation_id,
        });
        self.needs_rejoin.store(false, Ordering::Release);

        let assignment_map: HashMap<String, Vec<u32>> = if self.is_leader {
            // JoinGroup's response carries no member list, so the leader learns the
            // group's shape from DescribeGroup + DescribeTopic instead. That is
            // deliberate — propagating subscriptions through JoinGroup is a wire change
            // and belongs to a later issue.
            let (_, members) = self.client.describe_group(&self.config.group_id).await?;
            // `DescribeGroup` already carries each member's current assignment
            // (`assigned_partitions`, populated server-side from its last `SyncGroup`) —
            // across every topic that member is subscribed to. Sticky assignment needs
            // that as its starting point, so pull out this subscription's slice before the
            // member list collapses down to bare ids below.
            let mut previous_assignment: HashMap<String, Vec<u32>> =
                HashMap::with_capacity(members.len());
            let member_ids: Vec<String> = members
                .into_iter()
                .map(|m| {
                    let mut partitions: Vec<u32> = m
                        .assigned_partitions
                        .into_iter()
                        .filter(|(topic, _)| topic == &self.config.topic)
                        .map(|(_, partition)| partition)
                        .collect();
                    partitions.sort_unstable();
                    previous_assignment.insert(m.member_id.clone(), partitions);
                    m.member_id
                })
                .collect();
            let (_, partitions) = self.client.describe_topic(&self.config.topic).await?;
            let partition_count = partitions.len() as u32;

            // `join.protocol_name` is what the group actually negotiated (the first
            // protocol offered by the first member to join the generation — see
            // `GroupCoordinator::join_group`), not necessarily the first entry in this
            // member's own `protocols` list. Using it here, rather than always calling
            // `assign_range`, is the point of this method: a group that negotiated
            // `roundrobin` must actually get round-robin, one that negotiated `sticky`
            // must actually get sticky assignment fed with `previous_assignment` above,
            // and one that negotiated `cooperative-sticky` must roll that same target out
            // incrementally rather than handing it out in one shot — see below.
            let strategy = AssignmentStrategy::from_protocol_name(&join.protocol_name);
            let target = strategy.assign(partition_count, &member_ids, &previous_assignment);

            // Eager strategies (`Range`, `RoundRobin`, plain `Sticky`) submit `target`
            // directly, in one round, exactly as this always has — `is_cooperative()` is
            // false for every one of them, so `round_one` is just `target` and
            // `needs_second_round` is always false; nothing below changes their
            // behavior. Only `CooperativeSticky` narrows round one down to each member's
            // keep-set (`target ∩ previous`) instead.
            let (round_one, needs_second_round) = if strategy.is_cooperative() {
                cooperative_round_one(&target, &previous_assignment)
            } else {
                (target.clone(), false)
            };

            let round_one_map = self.submit_assignment(&round_one).await?;

            if needs_second_round {
                // Round one's SyncGroup above just made every withheld partition
                // ownerless — no member's `assigned_partitions` includes them any more
                // (each kept only its share of `round_one`). Getting them to their new
                // owners needs a second generation, which this asks for explicitly with
                // the `COOPERATIVE_ROUND_TWO` tagged field: the coordinator bumps the
                // generation and reopens the join barrier so every other member notices
                // — via its own heartbeat rejection — and rejoins before this round
                // closes.
                //
                // Stated rather than inferred. The coordinator used to read "a known
                // member called JoinGroup while the group was Stable" as this request,
                // which fired on an unrelated leader reconnect too (#70) and never fired
                // at all for a static leader, whose rejoin short-circuits earlier (#69).
                // Eager groups never reach this branch (`needs_second_round` is always
                // false for them), so it never runs for one regardless.
                // Recomputed rather than reusing the outer `protocol_refs` — that borrow
                // of `self.config.protocols` would otherwise have to stay alive across
                // the mutable `self.submit_assignment` call above, which the borrow
                // checker rejects.
                let protocol_refs_round_two: Vec<&str> =
                    self.config.protocols.iter().map(String::as_str).collect();
                let round_two_join = self
                    .client
                    .join_group_round_two(
                        &self.config.group_id,
                        &self.member_id,
                        self.config.instance_id.as_deref(),
                        &protocol_refs_round_two,
                        Some(self.config.session_timeout),
                    )
                    .await?;
                self.generation_id = round_two_join.generation_id;
                self.is_leader = round_two_join.is_leader;
                // Same reasoning as the round-one publish above: the background
                // heartbeat task must heartbeat under round two's generation for the
                // rest of this call, not round one's.
                let _ = self.identity_tx.send(HeartbeatIdentity {
                    member_id: self.member_id.clone(),
                    generation_id: self.generation_id,
                });
                self.needs_rejoin.store(false, Ordering::Release);

                // Round two submits `target` as-is, not recomputed. Round one already
                // reduced every member's holdings to a subset of `target` and nothing
                // outside it, so `target` is now fully achievable without taking
                // anything away from anyone: every partition a member doesn't yet own
                // is either freshly freed by round one or was never anyone's to begin
                // with. Recomputing instead — running the assignor's leveling step
                // again against round one's partial state — could, depending on
                // tie-breaks, reassign a partition a member is still actively holding
                // straight to someone else, defeating the revoke-before-reassign
                // ordering this whole scheme exists to guarantee. Reusing `target` is
                // also what bounds this to exactly two rounds: there is nothing left to
                // disagree with, so a third round is never triggered.
                self.submit_assignment(&target).await?
            } else {
                round_one_map
            }
        } else {
            let deadline = Instant::now() + self.config.sync_retry_timeout;
            loop {
                match self
                    .client
                    .sync_group(
                        &self.config.group_id,
                        self.generation_id,
                        &self.member_id,
                        &[],
                    )
                    .await
                {
                    Ok(assignment) => break assignment.into_iter().collect(),
                    Err(e) if e.to_string().contains("REBALANCE_IN_PROGRESS") => {
                        if Instant::now() >= deadline {
                            return Err(std::io::Error::other(format!(
                                "group '{}': SyncGroup kept returning REBALANCE_IN_PROGRESS \
                                 past the retry deadline",
                                self.config.group_id
                            )));
                        }
                        tokio::time::sleep(self.config.sync_retry_interval).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        };

        let mut new_assignment: Vec<u32> = assignment_map
            .get(&self.config.topic)
            .cloned()
            .unwrap_or_default();
        new_assignment.sort_unstable();

        // Drop the offset bookkeeping for partitions no longer owned. `pending_commits` is
        // not touched here — it was already committed and cleared above, before the
        // assignment changed.
        self.next_offsets.retain(|p, _| new_assignment.contains(p));

        // A partition still owned across the rebalance keeps its existing offset — the
        // in-memory position is ahead of the last commit, so re-reading it would replay
        // records this member already consumed. Only a newly owned partition needs its
        // starting offset looked up.
        for &partition in &new_assignment {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                self.next_offsets.entry(partition)
            {
                let committed = self
                    .client
                    .fetch_offset(&self.config.group_id, &self.config.topic, partition)
                    .await?;
                let start = if committed == u64::MAX {
                    0
                } else {
                    committed + 1
                };
                entry.insert(start);
            }
        }

        self.assignment = new_assignment;
        Ok(())
    }

    /// Submits `assignments` via `SyncGroup` under the current generation and member id,
    /// returning this member's own resulting `topic -> partitions` map. Leader-only —
    /// only the leader's `SyncGroup` call carries a payload — and shared between a
    /// cooperative rebalance's two rounds and an eager rebalance's single one, so every
    /// round goes over the wire the exact same way.
    async fn submit_assignment(
        &mut self,
        assignments: &[(String, Vec<u32>)],
    ) -> IoResult<HashMap<String, Vec<u32>>> {
        let member_assignments: Vec<MemberAssignment> = assignments
            .iter()
            .map(|(member_id, partitions)| MemberAssignment {
                member_id: member_id.clone(),
                topic: self.config.topic.clone(),
                partitions: partitions.clone(),
            })
            .collect();
        Ok(self
            .client
            .sync_group(
                &self.config.group_id,
                self.generation_id,
                &self.member_id,
                &member_assignments,
            )
            .await?
            .into_iter()
            .collect())
    }

    /// One poll round: fetches every owned partition in order. Liveness itself no longer
    /// rides on this call — the background heartbeat task (see [`GroupConsumer`]) keeps the
    /// session alive on its own schedule.
    ///
    /// If the background task observed a heartbeat rejected — the generation went stale, or
    /// this member was fenced — it set `needs_rejoin`, and this calls `rejoin()` and
    /// returns an empty result for the round rather than fetching under a stale generation;
    /// the caller is expected to call `poll()` again.
    pub async fn poll(&mut self) -> IoResult<Vec<(u32, Record)>> {
        if self.needs_rejoin.swap(false, Ordering::AcqRel) {
            self.rejoin().await?;
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        let partitions = self.assignment.clone();
        for partition in partitions {
            let next_offset = *self.next_offsets.get(&partition).unwrap_or(&0);
            // Tagged with this member's identity (issue #54) so the coordinator can tell
            // a member that's genuinely still consuming from one that's merely still
            // heartbeating — see `GroupCoordinator::record_progress`.
            match self
                .client
                .fetch_as_member(
                    &self.config.topic,
                    partition,
                    next_offset,
                    self.config.fetch_max_bytes,
                    &self.config.group_id,
                    &self.member_id,
                )
                .await
            {
                Ok(frames) => {
                    for frame in frames {
                        // Offsets advance past every frame — control markers occupy real
                        // offsets too — but only non-control frames are handed back.
                        self.next_offsets.insert(partition, frame.offset + 1);
                        self.pending_commits.insert(partition, frame.offset);
                        if !frame.is_control {
                            records.push((partition, frame));
                        }
                    }
                }
                Err(_) => {
                    // An unreachable broker must not lose the frames already collected from
                    // other partitions this round — reconnect and move on.
                    let _ = self.client.reconnect().await;
                }
            }
        }

        Ok(records)
    }

    /// Commits the pending offset for each owned partition and clears the pending set.
    pub async fn commit(&mut self) -> IoResult<()> {
        let pending: Vec<(u32, u64)> = self
            .pending_commits
            .iter()
            .map(|(&partition, &offset)| (partition, offset))
            .collect();
        for (partition, offset) in pending {
            self.client
                .commit_offset(&self.config.group_id, &self.config.topic, partition, offset)
                .await?;
        }
        self.pending_commits.clear();
        Ok(())
    }

    /// Leaves the group and stops the background heartbeat task. Stopping it here (rather
    /// than only on `Drop`) matters for a *static* member: `LeaveGroup` retires the
    /// instance's reservation, and a heartbeat that kept landing afterward on a member id
    /// the coordinator no longer knows about would just fail harmlessly — but stopping it
    /// promptly is still the right thing to do rather than leaving it to fail forever until
    /// the `GroupConsumer` itself is dropped.
    pub async fn leave(&mut self) -> IoResult<()> {
        self.stop_heartbeat_task();
        self.client
            .leave_group_static(
                &self.config.group_id,
                &self.member_id,
                self.config.instance_id.as_deref(),
            )
            .await
    }

    /// Aborts the background heartbeat task, if it's still running. Idempotent — safe to
    /// call from both `leave()` and `Drop`.
    fn stop_heartbeat_task(&mut self) {
        if let Some(handle) = self.heartbeat_task.take() {
            handle.abort();
        }
    }
}

impl Drop for GroupConsumer {
    /// Guarantees the background heartbeat task does not outlive this consumer. Without
    /// this, a `GroupConsumer` dropped without an explicit `leave()` — the common case on
    /// an error path, a panic unwind, or just going out of scope — would leak a task that
    /// keeps heartbeating for a member no one holds anymore, which is the one outcome this
    /// whole feature exists to avoid: a departed member's slot staying artificially alive.
    fn drop(&mut self) {
        self.stop_heartbeat_task();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn even_division_splits_partitions_equally() {
        let members = ids(&["a", "b", "c", "d"]);
        let result = assign_range(8, &members);
        assert_eq!(
            result,
            vec![
                ("a".to_string(), vec![0, 1]),
                ("b".to_string(), vec![2, 3]),
                ("c".to_string(), vec![4, 5]),
                ("d".to_string(), vec![6, 7]),
            ]
        );
    }

    #[test]
    fn uneven_remainder_goes_to_the_first_members() {
        let members = ids(&["a", "b", "c"]);
        let result = assign_range(7, &members);
        assert_eq!(
            result,
            vec![
                ("a".to_string(), vec![0, 1, 2]),
                ("b".to_string(), vec![3, 4]),
                ("c".to_string(), vec![5, 6]),
            ]
        );
    }

    #[test]
    fn more_members_than_partitions_leaves_some_empty() {
        let members = ids(&["a", "b", "c", "d", "e"]);
        let result = assign_range(2, &members);
        assert_eq!(
            result,
            vec![
                ("a".to_string(), vec![0]),
                ("b".to_string(), vec![1]),
                ("c".to_string(), vec![]),
                ("d".to_string(), vec![]),
                ("e".to_string(), vec![]),
            ]
        );
        assert_eq!(result.len(), 5, "every member must still be present");
    }

    #[test]
    fn single_member_takes_everything() {
        let members = ids(&["only"]);
        let result = assign_range(5, &members);
        assert_eq!(result, vec![("only".to_string(), vec![0, 1, 2, 3, 4])]);
    }

    #[test]
    fn every_partition_is_assigned_exactly_once() {
        let members = ids(&["m1", "m2", "m3", "m4", "m5", "m6", "m7"]);
        let partition_count = 23;
        let result = assign_range(partition_count, &members);

        let mut all_partitions: Vec<u32> = result
            .iter()
            .flat_map(|(_, partitions)| partitions.clone())
            .collect();
        all_partitions.sort_unstable();

        let expected: Vec<u32> = (0..partition_count).collect();
        assert_eq!(
            all_partitions, expected,
            "every partition must be assigned to exactly one member"
        );
    }

    #[test]
    fn empty_member_ids_produces_empty_result() {
        assert_eq!(assign_range(10, &[]), Vec::new());
    }

    #[test]
    fn zero_partitions_produces_empty_assignments_for_every_member() {
        let members = ids(&["a", "b"]);
        let result = assign_range(0, &members);
        assert_eq!(
            result,
            vec![("a".to_string(), vec![]), ("b".to_string(), vec![])]
        );
    }

    // --- assign_roundrobin ---

    #[test]
    fn roundrobin_deals_partitions_one_at_a_time() {
        let members = ids(&["a", "b", "c"]);
        let result = assign_roundrobin(7, &members);
        assert_eq!(
            result,
            vec![
                ("a".to_string(), vec![0, 3, 6]),
                ("b".to_string(), vec![1, 4]),
                ("c".to_string(), vec![2, 5]),
            ]
        );
    }

    #[test]
    fn roundrobin_more_members_than_partitions_leaves_some_empty() {
        let members = ids(&["a", "b", "c", "d", "e"]);
        let result = assign_roundrobin(2, &members);
        assert_eq!(
            result,
            vec![
                ("a".to_string(), vec![0]),
                ("b".to_string(), vec![1]),
                ("c".to_string(), vec![]),
                ("d".to_string(), vec![]),
                ("e".to_string(), vec![]),
            ]
        );
        assert_eq!(result.len(), 5, "every member must still be present");
    }

    #[test]
    fn roundrobin_every_partition_is_assigned_exactly_once() {
        let members = ids(&["m1", "m2", "m3", "m4", "m5", "m6", "m7"]);
        let partition_count = 23;
        let result = assign_roundrobin(partition_count, &members);

        let mut all_partitions: Vec<u32> = result
            .iter()
            .flat_map(|(_, partitions)| partitions.clone())
            .collect();
        all_partitions.sort_unstable();

        let expected: Vec<u32> = (0..partition_count).collect();
        assert_eq!(
            all_partitions, expected,
            "every partition must be assigned to exactly one member"
        );
    }

    #[test]
    fn roundrobin_empty_member_ids_produces_empty_result() {
        assert_eq!(assign_roundrobin(10, &[]), Vec::new());
    }

    #[test]
    fn roundrobin_zero_partitions_produces_empty_assignments_for_every_member() {
        let members = ids(&["a", "b"]);
        let result = assign_roundrobin(0, &members);
        assert_eq!(
            result,
            vec![("a".to_string(), vec![]), ("b".to_string(), vec![])]
        );
    }

    #[test]
    fn roundrobin_membership_change_redistributes_partitions() {
        let before = assign_roundrobin(6, &ids(&["a", "b", "c"]));
        assert_eq!(before[0].1, vec![0, 3]);

        // "d" joins: every member's slice changes shape, proving a join redistributes
        // rather than only handing the newcomer whatever was left over.
        let after = assign_roundrobin(6, &ids(&["a", "b", "c", "d"]));
        assert_eq!(
            after,
            vec![
                ("a".to_string(), vec![0, 4]),
                ("b".to_string(), vec![1, 5]),
                ("c".to_string(), vec![2]),
                ("d".to_string(), vec![3]),
            ]
        );
    }

    // --- assign_sticky ---

    /// Builds a `member_id -> partitions` previous-assignment map from `(id, partitions)`
    /// pairs, the shape [`assign_sticky`] takes.
    fn prev(pairs: &[(&str, &[u32])]) -> HashMap<String, Vec<u32>> {
        pairs
            .iter()
            .map(|(id, partitions)| (id.to_string(), partitions.to_vec()))
            .collect()
    }

    /// Counts partitions whose owner differs between two assignment results covering the
    /// same partition range — a direct measure of how much a rebalance churned.
    fn moved_partitions(before: &[(String, Vec<u32>)], after: &[(String, Vec<u32>)]) -> usize {
        let mut before_owner: HashMap<u32, &str> = HashMap::new();
        for (id, partitions) in before {
            for &p in partitions {
                before_owner.insert(p, id.as_str());
            }
        }
        let mut after_owner: HashMap<u32, &str> = HashMap::new();
        for (id, partitions) in after {
            for &p in partitions {
                after_owner.insert(p, id.as_str());
            }
        }
        after_owner
            .iter()
            .filter(|(p, owner)| before_owner.get(*p) != Some(*owner))
            .count()
    }

    #[test]
    fn sticky_stable_membership_reshuffles_nothing() {
        let previous = prev(&[("a", &[0, 1, 2]), ("b", &[3, 4, 5]), ("c", &[6, 7, 8])]);
        let members = ids(&["a", "b", "c"]);
        let result = assign_sticky(9, &members, &previous);
        assert_eq!(
            result,
            vec![
                ("a".to_string(), vec![0, 1, 2]),
                ("b".to_string(), vec![3, 4, 5]),
                ("c".to_string(), vec![6, 7, 8]),
            ],
            "an unchanged membership must not reshuffle anything"
        );
    }

    #[test]
    fn sticky_member_joining_moves_far_fewer_partitions_than_range() {
        // Steady state: "b", "c", "d" hold three partitions each.
        let previous = prev(&[("b", &[0, 1, 2]), ("c", &[3, 4, 5]), ("d", &[6, 7, 8])]);
        let before: Vec<(String, Vec<u32>)> = vec![
            ("b".to_string(), vec![0, 1, 2]),
            ("c".to_string(), vec![3, 4, 5]),
            ("d".to_string(), vec![6, 7, 8]),
        ];

        // "a" joins, sorting before every existing member — the worst case for a
        // position-based assignor, since it shifts every existing member's index by one.
        let members = ids(&["a", "b", "c", "d"]);

        let sticky_after = assign_sticky(9, &members, &previous);
        let range_after = assign_range(9, &members);

        let sticky_moved = moved_partitions(&before, &sticky_after);
        let range_moved = moved_partitions(&before, &range_after);

        assert!(
            sticky_moved <= 3,
            "sticky should move only a handful of partitions when one member joins, moved {sticky_moved}"
        );
        assert!(
            range_moved >= 6,
            "range reshuffles nearly everything on this transition, moved {range_moved}"
        );
        assert!(
            sticky_moved < range_moved,
            "sticky ({sticky_moved} moved) must move strictly fewer partitions than range \
             ({range_moved} moved) for the same membership change"
        );

        let mut all: Vec<u32> = sticky_after.iter().flat_map(|(_, p)| p.clone()).collect();
        all.sort_unstable();
        assert_eq!(all, (0..9).collect::<Vec<u32>>());
    }

    #[test]
    fn sticky_member_leaving_redistributes_without_moving_stayers() {
        let previous = prev(&[
            ("a", &[0, 1]),
            ("b", &[2, 3]),
            ("c", &[4, 5]),
            ("d", &[6, 7, 8]),
        ]);
        let members = ids(&["a", "c", "d"]); // "b" has left

        let result = assign_sticky(9, &members, &previous);
        let owned: HashMap<&str, &Vec<u32>> =
            result.iter().map(|(id, p)| (id.as_str(), p)).collect();

        // Every partition "a", "c", and "d" already owned is still theirs — nothing moves
        // off a member who did not leave.
        assert!(owned["a"].contains(&0) && owned["a"].contains(&1));
        assert!(owned["c"].contains(&4) && owned["c"].contains(&5));
        assert!(
            owned["d"].contains(&6) && owned["d"].contains(&7) && owned["d"].contains(&8),
            "\"d\"'s partitions must be untouched by a departure that has nothing to do with it"
        );

        // "b"'s orphaned partitions (2, 3) are redistributed, not dropped.
        let mut all: Vec<u32> = result.iter().flat_map(|(_, p)| p.clone()).collect();
        all.sort_unstable();
        assert_eq!(all, (0..9).collect::<Vec<u32>>());
    }

    #[test]
    fn sticky_is_deterministic() {
        let previous = prev(&[("a", &[0, 2, 4]), ("b", &[1, 3]), ("d", &[5, 6, 7, 8])]);
        let members = ids(&["a", "b", "c", "d"]);

        let first = assign_sticky(9, &members, &previous);
        for _ in 0..20 {
            assert_eq!(
                assign_sticky(9, &members, &previous),
                first,
                "same inputs must always produce the same assignment"
            );
        }
    }

    #[test]
    fn sticky_cold_start_with_no_previous_assignment_is_balanced() {
        // A brand-new group has nothing to stay sticky to, but the result still must not be
        // degenerate — e.g. everything piled onto one member.
        let members = ids(&["a", "b", "c"]);
        let result = assign_sticky(6, &members, &HashMap::new());
        assert_eq!(
            result,
            assign_roundrobin(6, &members),
            "with no prior assignment, sticky should fall back to an evenly spread split"
        );
    }

    #[test]
    fn sticky_every_partition_is_assigned_exactly_once() {
        let members = ids(&["m1", "m2", "m3", "m4", "m5", "m6", "m7"]);
        let partition_count = 23;
        // "m9" is not a current member — its old partitions must be reclaimed, not dropped.
        let previous = prev(&[("m1", &[0, 1, 2, 3]), ("m9", &[4, 5])]);
        let result = assign_sticky(partition_count, &members, &previous);

        let mut all_partitions: Vec<u32> = result
            .iter()
            .flat_map(|(_, partitions)| partitions.clone())
            .collect();
        all_partitions.sort_unstable();

        let expected: Vec<u32> = (0..partition_count).collect();
        assert_eq!(
            all_partitions, expected,
            "every partition must be assigned to exactly one member"
        );
    }

    #[test]
    fn sticky_more_members_than_partitions_leaves_some_empty() {
        let members = ids(&["a", "b", "c", "d", "e"]);
        let result = assign_sticky(2, &members, &HashMap::new());
        assert_eq!(
            result,
            vec![
                ("a".to_string(), vec![0]),
                ("b".to_string(), vec![1]),
                ("c".to_string(), vec![]),
                ("d".to_string(), vec![]),
                ("e".to_string(), vec![]),
            ]
        );
        assert_eq!(result.len(), 5, "every member must still be present");
    }

    #[test]
    fn sticky_empty_member_ids_produces_empty_result() {
        let previous = prev(&[("a", &[0, 1])]);
        assert_eq!(assign_sticky(10, &[], &previous), Vec::new());
    }

    #[test]
    fn sticky_zero_partitions_and_empty_group_do_not_panic() {
        assert_eq!(assign_sticky(0, &[], &HashMap::new()), Vec::new());
        let members = ids(&["a", "b"]);
        assert_eq!(
            assign_sticky(0, &members, &HashMap::new()),
            vec![("a".to_string(), vec![]), ("b".to_string(), vec![])]
        );
    }

    #[test]
    fn sticky_result_is_balanced() {
        let members = ids(&["m1", "m2", "m3", "m4", "m5", "m6", "m7"]);
        let partition_count = 23;
        // A deliberately lopsided prior state — one member holding far more than its share
        // — to prove leveling actually kicks in rather than only ever preserving state.
        let previous = prev(&[("m1", &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]);
        let result = assign_sticky(partition_count, &members, &previous);

        let counts: Vec<usize> = result.iter().map(|(_, p)| p.len()).collect();
        let min = *counts.iter().min().unwrap();
        let max = *counts.iter().max().unwrap();
        assert!(
            max - min <= 1,
            "no member should hold more than one partition above the minimum, got counts {counts:?}"
        );
    }

    // --- AssignmentStrategy ---

    #[test]
    fn strategy_defaults_to_range() {
        assert_eq!(AssignmentStrategy::default(), AssignmentStrategy::Range);
    }

    #[test]
    fn strategy_from_protocol_name_recognises_range() {
        assert_eq!(
            AssignmentStrategy::from_protocol_name("range"),
            AssignmentStrategy::Range
        );
    }

    #[test]
    fn strategy_from_protocol_name_recognises_roundrobin() {
        assert_eq!(
            AssignmentStrategy::from_protocol_name("roundrobin"),
            AssignmentStrategy::RoundRobin
        );
    }

    #[test]
    fn strategy_from_protocol_name_recognises_sticky() {
        assert_eq!(
            AssignmentStrategy::from_protocol_name("sticky"),
            AssignmentStrategy::Sticky
        );
    }

    #[test]
    fn strategy_from_protocol_name_falls_back_to_range_for_unknown_or_empty() {
        // "sticky" used to stand in here as an example of an unrecognised name, but it is
        // now a real, recognised strategy — use a name that actually is unrecognised.
        assert_eq!(
            AssignmentStrategy::from_protocol_name("fancy-new-protocol"),
            AssignmentStrategy::Range
        );
        assert_eq!(
            AssignmentStrategy::from_protocol_name(""),
            AssignmentStrategy::Range
        );
    }

    #[test]
    fn negotiated_roundrobin_protocol_name_selects_roundrobin_assignment() {
        // Simulates what consumer.rs does at the SyncGroup call site: take the
        // negotiated `protocol_name` from the join response and use it to pick the
        // strategy, rather than always calling `assign_range`.
        let negotiated_protocol_name = "roundrobin".to_string();
        let members = ids(&["a", "b", "c"]);
        let no_previous = HashMap::new();

        let strategy = AssignmentStrategy::from_protocol_name(&negotiated_protocol_name);
        let result = strategy.assign(7, &members, &no_previous);

        assert_eq!(result, assign_roundrobin(7, &members));
        assert_ne!(
            result,
            assign_range(7, &members),
            "a negotiated roundrobin protocol must not silently produce a range assignment"
        );
    }

    #[test]
    fn negotiated_sticky_protocol_name_selects_sticky_assignment() {
        let negotiated_protocol_name = "sticky".to_string();
        let members = ids(&["a", "b", "c"]);
        let mut previous = HashMap::new();
        previous.insert("a".to_string(), vec![0, 1, 2]);
        previous.insert("b".to_string(), vec![3, 4]);
        previous.insert("c".to_string(), vec![5, 6]);

        let strategy = AssignmentStrategy::from_protocol_name(&negotiated_protocol_name);
        let result = strategy.assign(7, &members, &previous);

        assert_eq!(result, assign_sticky(7, &members, &previous));
    }

    #[test]
    fn unrecognised_negotiated_protocol_name_falls_back_to_range_assignment() {
        let negotiated_protocol_name = "fancy-new-protocol".to_string();
        let members = ids(&["a", "b", "c"]);
        let no_previous = HashMap::new();

        let strategy = AssignmentStrategy::from_protocol_name(&negotiated_protocol_name);
        let result = strategy.assign(7, &members, &no_previous);

        assert_eq!(
            result,
            assign_range(7, &members),
            "an unrecognised protocol name must default rather than error"
        );
    }

    #[test]
    fn strategy_from_protocol_name_recognises_cooperative_sticky() {
        assert_eq!(
            AssignmentStrategy::from_protocol_name("cooperative-sticky"),
            AssignmentStrategy::CooperativeSticky
        );
    }

    #[test]
    fn is_cooperative_is_true_only_for_cooperative_sticky() {
        assert!(!AssignmentStrategy::Range.is_cooperative());
        assert!(!AssignmentStrategy::RoundRobin.is_cooperative());
        assert!(
            !AssignmentStrategy::Sticky.is_cooperative(),
            "plain sticky computes the same target as cooperative-sticky but still rolls \
             it out eagerly — it must not be treated as cooperative"
        );
        assert!(AssignmentStrategy::CooperativeSticky.is_cooperative());
    }

    #[test]
    fn negotiated_cooperative_sticky_protocol_name_computes_the_same_target_as_sticky() {
        // Constraint: cooperative-sticky must not change what the assignor computes,
        // only how the leader rolls the result out (see `cooperative_round_one`).
        let negotiated_protocol_name = "cooperative-sticky".to_string();
        let members = ids(&["a", "b", "c"]);
        let mut previous = HashMap::new();
        previous.insert("a".to_string(), vec![0, 1, 2]);
        previous.insert("b".to_string(), vec![3, 4]);
        previous.insert("c".to_string(), vec![5, 6]);

        let strategy = AssignmentStrategy::from_protocol_name(&negotiated_protocol_name);
        assert_eq!(strategy, AssignmentStrategy::CooperativeSticky);
        let result = strategy.assign(7, &members, &previous);

        assert_eq!(result, assign_sticky(7, &members, &previous));
    }

    // --- cooperative_round_one ---

    #[test]
    fn cooperative_round_one_keeps_only_the_intersection_with_target() {
        // "a" is losing partition 1 (to "b") and keeping 0; "b" is gaining 1 but already
        // had 2 and keeps it too.
        let target = vec![("a".to_string(), vec![0]), ("b".to_string(), vec![1, 2])];
        let previous = prev(&[("a", &[0, 1]), ("b", &[2])]);

        let (keep_sets, needs_second_round) = cooperative_round_one(&target, &previous);

        let owned: HashMap<&str, &Vec<u32>> =
            keep_sets.iter().map(|(id, p)| (id.as_str(), p)).collect();
        assert_eq!(
            owned["a"],
            &vec![0],
            "\"a\" must keep exactly what it already owned and is still targeted for"
        );
        assert_eq!(
            owned["b"],
            &vec![2],
            "\"b\" must not receive partition 1 in round one — it never owned it, so it \
             hasn't been revoked by anyone yet"
        );
        assert!(
            needs_second_round,
            "partition 1 had to be withheld from everyone, so a second round is required"
        );
    }

    #[test]
    fn cooperative_round_one_needs_no_second_round_when_nothing_moves() {
        let target = vec![("a".to_string(), vec![0, 1]), ("b".to_string(), vec![2, 3])];
        let previous = prev(&[("a", &[0, 1]), ("b", &[2, 3])]);

        let (keep_sets, needs_second_round) = cooperative_round_one(&target, &previous);

        assert_eq!(
            keep_sets, target,
            "an unchanged target must be handed out in full"
        );
        assert!(
            !needs_second_round,
            "nothing had to be withheld, so no second round should be requested"
        );
    }

    #[test]
    fn cooperative_round_one_gives_a_new_member_nothing_until_round_two() {
        // "c" just joined and has no previous assignment at all.
        let target = vec![
            ("a".to_string(), vec![0]),
            ("b".to_string(), vec![1]),
            ("c".to_string(), vec![2]),
        ];
        let previous = prev(&[("a", &[0, 2]), ("b", &[1])]);

        let (keep_sets, needs_second_round) = cooperative_round_one(&target, &previous);

        let owned: HashMap<&str, &Vec<u32>> =
            keep_sets.iter().map(|(id, p)| (id.as_str(), p)).collect();
        assert_eq!(
            owned["a"],
            &vec![0],
            "\"a\" keeps only what's still its own"
        );
        assert_eq!(
            owned["b"],
            &vec![1],
            "\"b\" is unaffected, keeps its only partition"
        );
        assert_eq!(
            owned["c"],
            &Vec::<u32>::new(),
            "a brand-new member must receive nothing in round one"
        );
        assert!(needs_second_round);
    }

    #[test]
    fn cooperative_round_one_revoked_set_is_exactly_the_difference_not_everything() {
        // Only partition 1 is actually moving (from "a" to "b"); "a"'s partition 0 and
        // "b"'s partition 2 are untouched. The key claim under test: round one's keep-set
        // for "a" must equal target(a) minus the moved partition, not an empty set — an
        // eager (stop-the-world) implementation would produce an empty keep-set for every
        // member instead.
        let target = vec![("a".to_string(), vec![0]), ("b".to_string(), vec![1, 2])];
        let previous = prev(&[("a", &[0, 1]), ("b", &[2])]);

        let (keep_sets, _) = cooperative_round_one(&target, &previous);
        let owned: HashMap<&str, &Vec<u32>> =
            keep_sets.iter().map(|(id, p)| (id.as_str(), p)).collect();

        let a_current = previous["a"].clone();
        let a_revoked: Vec<u32> = a_current
            .iter()
            .copied()
            .filter(|p| !owned["a"].contains(p))
            .collect();
        assert_eq!(
            a_revoked,
            vec![1],
            "\"a\" must revoke exactly partition 1, the one actually moving — not \
             partition 0 too"
        );
        assert!(
            owned["a"].contains(&0),
            "\"a\"'s non-moving partition must stay in its round-one keep-set"
        );
    }

    #[test]
    fn cooperative_round_one_bootstrap_with_no_previous_assignment_withholds_everything() {
        // A deliberate consequence of the plain `target ∩ previous` definition: a
        // brand-new group's very first assignment has no previous owners at all, so
        // nothing can be kept and a second round is always needed to actually hand
        // anything out — same treatment as a genuine conflict, not a special case.
        let target = vec![("a".to_string(), vec![0, 1])];
        let previous = HashMap::new();

        let (keep_sets, needs_second_round) = cooperative_round_one(&target, &previous);

        assert_eq!(keep_sets, vec![("a".to_string(), Vec::new())]);
        assert!(needs_second_round);
    }

    // --- GroupConsumerConfig::validate ---

    #[test]
    fn validate_accepts_the_default_config() {
        GroupConsumerConfig::default()
            .validate()
            .expect("default heartbeat_interval/session_timeout must satisfy the 1/3 rule");
    }

    #[test]
    fn validate_accepts_an_interval_exactly_at_a_third_of_the_timeout() {
        let config = GroupConsumerConfig {
            session_timeout: Duration::from_secs(9),
            heartbeat_interval: Duration::from_secs(3),
            ..GroupConsumerConfig::default()
        };
        config
            .validate()
            .expect("interval == timeout / 3 is the boundary and must pass");
    }

    #[test]
    fn validate_rejects_an_interval_equal_to_the_timeout() {
        let config = GroupConsumerConfig {
            session_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(10),
            ..GroupConsumerConfig::default()
        };
        let err = config
            .validate()
            .expect_err("interval == timeout guarantees eviction and must be rejected");
        assert!(err.to_string().contains("heartbeat_interval"));
    }

    #[test]
    fn validate_rejects_an_interval_greater_than_the_timeout() {
        let config = GroupConsumerConfig {
            session_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(30),
            ..GroupConsumerConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_an_interval_just_over_a_third_of_the_timeout() {
        let config = GroupConsumerConfig {
            session_timeout: Duration::from_secs(9),
            heartbeat_interval: Duration::from_secs(3) + Duration::from_millis(1),
            ..GroupConsumerConfig::default()
        };
        assert!(
            config.validate().is_err(),
            "the 1/3 margin must be enforced strictly, not just interval < timeout"
        );
    }

    #[test]
    fn validate_rejects_a_zero_session_timeout() {
        let config = GroupConsumerConfig {
            session_timeout: Duration::ZERO,
            heartbeat_interval: Duration::ZERO,
            ..GroupConsumerConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
