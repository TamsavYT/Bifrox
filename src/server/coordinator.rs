use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Lower bound the coordinator will honor for a member's requested `session.timeout.ms`
/// (see `crate::protocol::wire::tags::SESSION_TIMEOUT_MS`), no matter how low the client
/// asks. A shorter value would evict a perfectly live member on nothing more than ordinary
/// heartbeat jitter — scheduling delay on the client, a slow network round trip — rather
/// than an actual failure.
pub const MIN_SESSION_TIMEOUT: Duration = Duration::from_millis(200);

/// Upper bound the coordinator will honor for a requested session timeout. Past this, the
/// timeout stops doing the job it exists for: a member that has genuinely died keeps its
/// partitions — and blocks the group from redistributing them — for however long this is
/// set to. Kept well under an hour for the same reason; five minutes is already a long time
/// for a dead member to sit on live partitions.
pub const MAX_SESSION_TIMEOUT: Duration = Duration::from_secs(300);

/// Session timeout used when a `JoinGroup` carries no `SESSION_TIMEOUT_MS` tag at all — a
/// legacy client, or any request built without it. Matches the coordinator's historical
/// hardcoded value exactly, so a caller that doesn't ask for anything sees no behavior
/// change from before this was configurable.
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(10);

/// Default `max.poll.interval.ms` — how long a member may go without making fetch progress
/// before it is evicted for stalling even though it keeps heartbeating (issue #54). Matches
/// Kafka's own default of five minutes.
pub const DEFAULT_MAX_POLL_INTERVAL: Duration = Duration::from_secs(300);

/// Why a member was pruned from its group. Kept distinct from a plain string so a caller
/// (currently: the eviction log line, and `GroupCoordinator::recent_evictions` for tests)
/// can tell the two failure modes apart without parsing text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionReason {
    /// No heartbeat arrived within `session.timeout.ms` — the member is presumed dead.
    SessionTimeout,
    /// Heartbeats kept arriving on schedule, but no fetch was attributed to this member
    /// within `max.poll.interval.ms` (issue #54) — the member is alive but has stopped
    /// making progress: deadlocked, stuck on a poisoned record, blocked on a downstream
    /// call, or otherwise wedged in a way that heartbeating alone can't detect.
    StalledConsumption,
}

/// One pruning event, kept around briefly so an operator (or a test) can see *why* a
/// member disappeared rather than just that it did.
#[derive(Debug, Clone)]
pub struct EvictionRecord {
    pub member_id: String,
    pub reason: EvictionReason,
    pub at: Instant,
}

/// How many `EvictionRecord`s a group keeps before dropping the oldest. Bounded so a group
/// under churn can't grow this without limit; generous enough that an operator (or a test)
/// polling occasionally still finds what it's looking for.
const MAX_RECENT_EVICTIONS: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

#[derive(Debug, Clone)]
pub struct MemberState {
    pub member_id: String,
    pub client_id: String,
    pub session_timeout: Duration,
    pub assigned_partitions: HashMap<String, Vec<u32>>,
    pub last_heartbeat: Instant,
    /// When this member last made fetch progress (issue #54) — i.e. a `Fetch` tagged with
    /// this member's identity (`crate::protocol::wire::tags::GROUP_MEMBER`) was served.
    /// Seeded to the join/rejoin time rather than left at some zero value, so a member
    /// that simply hasn't fetched *yet* gets a full `max.poll.interval` grace period
    /// before it can be judged stalled, matching how `last_heartbeat` is seeded.
    pub last_progress: Instant,
    /// Set when this member declared a `group.instance.id` — i.e. it is a *static* member
    /// whose slot survives its own process. Kept here so pruning can retire the
    /// instance-to-member mapping along with the member itself.
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub state: GroupState,
    pub generation_id: u32,
    pub leader: Option<String>,
    pub members: HashMap<String, MemberState>,
    /// The protocol name negotiated when the group was last (re)formed from Empty (e.g.
    /// `"range"`, `"roundrobin"`, or a cooperative assignor like `"cooperative-sticky"`).
    /// Kafka's KIP-429 cooperative rebalancing is fundamentally a client-side assignor
    /// behavior (revoke-then-assign vs. incremental diff), but the coordinator still has
    /// one real server-side responsibility: not treating an in-progress rebalance as a
    /// hard failure for members that haven't rejoined yet when the group opted into a
    /// cooperative protocol. See `is_cooperative`/`heartbeat`.
    pub protocol_name: Option<String>,
    /// When the currently-open join window closes. While this is `Some(deadline)` in the
    /// future, the group is collecting joiners into a single generation instead of forming
    /// a new one per arrival — see `GroupCoordinator::join_group`.
    pub rebalance_deadline: Option<Instant>,
    /// `group.instance.id` -> the member id that instance currently holds (KIP-345 static
    /// membership).
    ///
    /// A consumer that declares an instance id keeps its slot — and therefore its
    /// assignment and the group's generation — across a restart, because the coordinator
    /// recognises the returning process as the member already in the group rather than as
    /// a new arrival. Without this, every restart of every member is an arrival and a
    /// departure, so a rolling bounce of an N-member group costs 2N rebalances and stops
    /// the group's progress each time; with it, a bounce that completes inside
    /// `session.timeout` costs none.
    pub static_members: HashMap<String, String>,
    /// Monotonic per-group counter behind static members' generated member ids. See
    /// `GroupCoordinator::join_group` for why a returning static member is issued a *new*
    /// member id rather than handed back its previous one.
    pub member_id_seq: u64,
    /// Recent prunings, most recent last, capped at `MAX_RECENT_EVICTIONS` — see
    /// `GroupCoordinator::recent_evictions`.
    pub recent_evictions: Vec<EvictionRecord>,
}

impl ConsumerGroup {
    pub fn new(group_id: String) -> Self {
        Self {
            group_id,
            state: GroupState::Empty,
            generation_id: 0,
            leader: None,
            members: HashMap::new(),
            protocol_name: None,
            rebalance_deadline: None,
            static_members: HashMap::new(),
            member_id_seq: 0,
            recent_evictions: Vec::new(),
        }
    }

    /// Records a pruning event, dropping the oldest once `MAX_RECENT_EVICTIONS` is
    /// exceeded — a bookkeeping detail kept on `ConsumerGroup` itself rather than inline
    /// in `GroupCoordinator::prune_expired_members`, so the cap is enforced in exactly one
    /// place no matter which caller records an eviction.
    fn record_eviction(&mut self, member_id: String, reason: EvictionReason, at: Instant) {
        self.recent_evictions.push(EvictionRecord {
            member_id,
            reason,
            at,
        });
        if self.recent_evictions.len() > MAX_RECENT_EVICTIONS {
            let overflow = self.recent_evictions.len() - MAX_RECENT_EVICTIONS;
            self.recent_evictions.drain(0..overflow);
        }
    }

    /// True while this group's join window is still open.
    pub fn join_window_open(&self) -> bool {
        self.rebalance_deadline
            .map(|deadline| Instant::now() < deadline)
            .unwrap_or(false)
    }

    /// True if the group negotiated a cooperative assignor (name containing
    /// "cooperative", matching Kafka's `cooperative-sticky` convention) rather than an
    /// eager one (`range`, `roundrobin`, etc.).
    pub fn is_cooperative(&self) -> bool {
        self.protocol_name
            .as_deref()
            .map(|p| p.to_ascii_lowercase().contains("cooperative"))
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub struct GroupCoordinator {
    groups: Mutex<HashMap<String, ConsumerGroup>>,
    /// How long a join window stays open for additional members to arrive
    /// (`group.initial.rebalance.delay.ms`).
    initial_rebalance_delay: Duration,
    /// `max.poll.interval.ms` — how long a member may go without fetch progress before
    /// it's evicted for stalling even while it keeps heartbeating (issue #54). See
    /// `EvictionReason::StalledConsumption`.
    max_poll_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub group_id: String,
    pub state_str: String,
    pub members: Vec<crate::protocol::wire::DescribedGroupMember>,
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupCoordinator {
    /// Removes members that have failed one of two independent liveness checks, tagging
    /// each removal with *which* check it failed (`EvictionReason`) so the two failure
    /// modes stay distinguishable downstream (logging, `recent_evictions`) rather than
    /// collapsing into one generic "member gone".
    ///
    /// A member is checked for `SessionTimeout` first — if it hasn't heartbeated in time
    /// it's presumed dead outright, and whether it also happened to stop fetching is
    /// moot. Only a member that's still heartbeating on schedule is then checked for
    /// `StalledConsumption` (issue #54): heartbeating alone no longer proves a member is
    /// doing useful work once heartbeats run on their own background schedule (#53),
    /// decoupled from whatever the application is actually doing with `poll()`.
    fn prune_expired_members(&self, group: &mut ConsumerGroup) {
        let now = Instant::now();
        let expired: Vec<(String, EvictionReason)> = group
            .members
            .iter()
            .filter_map(|(member_id, member)| {
                if now.duration_since(member.last_heartbeat) > member.session_timeout {
                    Some((member_id.clone(), EvictionReason::SessionTimeout))
                } else if now.duration_since(member.last_progress) > self.max_poll_interval {
                    Some((member_id.clone(), EvictionReason::StalledConsumption))
                } else {
                    None
                }
            })
            .collect();

        for (member_id, reason) in expired {
            // A static member is not exempt from either check — it just gets its slot
            // back for free if it returns inside one. Past that the instance is genuinely
            // gone, so drop its reservation too; keeping it would let a member that has
            // been down for hours reclaim an assignment the group has since redistributed.
            if let Some(member) = group.members.remove(&member_id) {
                if let Some(instance_id) = member.group_instance_id {
                    group.static_members.remove(&instance_id);
                }
            }
            if group.leader.as_deref() == Some(&member_id) {
                group.leader = group.members.keys().next().cloned();
            }

            match reason {
                EvictionReason::SessionTimeout => tracing::warn!(
                    group_id = group.group_id,
                    member_id,
                    reason = "session_timeout",
                    "consumer group member evicted: no heartbeat within its session timeout"
                ),
                EvictionReason::StalledConsumption => tracing::warn!(
                    group_id = group.group_id,
                    member_id,
                    reason = "stalled_consumption",
                    max_poll_interval_ms = self.max_poll_interval.as_millis() as u64,
                    "consumer group member evicted: heartbeating but no fetch progress \
                     within max.poll.interval"
                ),
            }
            group.record_eviction(member_id, reason, now);
        }

        if group.members.is_empty() {
            group.state = GroupState::Empty;
            group.leader = None;
        }
    }

    /// Hands an instance's existing slot to a returning static member, or `None` if this
    /// instance id isn't one the group is holding a slot for.
    ///
    /// The slot — its assignment, its leadership, and the group's generation — is left
    /// exactly as it was, which is the entire point: the group does not rebalance for a
    /// member it never considered gone.
    ///
    /// What does change is the member id. The returning process is issued a fresh one and
    /// the old one stops existing, which fences a predecessor that is still running: a
    /// half-dead process whose replacement has already started would otherwise keep
    /// heartbeating and consuming under an identity two processes now share, and the two
    /// would double-consume the same partitions while each believed it owned them.
    /// Rotating costs the returning member nothing, since it is told its member id in the
    /// join response either way.
    ///
    /// Note that a subscription change cannot be detected here and so cannot force the
    /// rebalance it should: `JoinGroup` carries assignor names, not the member's topic
    /// list, so the coordinator has nothing to compare. A static member that returns
    /// subscribed to different topics keeps its previous assignment until something else
    /// rebalances the group.
    /// Clamps a client-requested `session.timeout.ms` into
    /// `[MIN_SESSION_TIMEOUT, MAX_SESSION_TIMEOUT]`, logging when the requested value gets
    /// adjusted. `None` — no `SESSION_TIMEOUT_MS` tag at all — yields
    /// `DEFAULT_SESSION_TIMEOUT` unchanged rather than being clamped: a legacy client
    /// asking for nothing is not "asking for something out of range", and clamping it
    /// would just be a confusing way of writing the same default.
    fn resolve_session_timeout(
        requested_ms: Option<u32>,
        group_id: &str,
        member_id: &str,
    ) -> Duration {
        let Some(ms) = requested_ms else {
            return DEFAULT_SESSION_TIMEOUT;
        };
        let requested = Duration::from_millis(ms as u64);
        let clamped = requested.clamp(MIN_SESSION_TIMEOUT, MAX_SESSION_TIMEOUT);
        if clamped != requested {
            tracing::warn!(
                group_id,
                member_id,
                requested_ms = ms,
                clamped_ms = clamped.as_millis() as u64,
                min_ms = MIN_SESSION_TIMEOUT.as_millis() as u64,
                max_ms = MAX_SESSION_TIMEOUT.as_millis() as u64,
                "session.timeout.ms clamped to the coordinator's allowed range"
            );
        }
        clamped
    }

    fn rejoin_static(
        group: &mut ConsumerGroup,
        instance_id: &str,
        session_timeout: Duration,
    ) -> Option<(String, u32, bool, String)> {
        let previous_member_id = group.static_members.get(instance_id)?.clone();
        let mut member = group.members.remove(&previous_member_id)?;

        group.member_id_seq = group.member_id_seq.saturating_add(1);
        let new_member_id = format!("{}-{}", instance_id, group.member_id_seq);

        member.member_id = new_member_id.clone();
        member.client_id = new_member_id.clone();
        let now = Instant::now();
        member.last_heartbeat = now;
        // A restarting process hasn't fetched anything yet either, so this rejoin gets the
        // same fresh `max.poll.interval` grace period a brand-new member would — not the
        // stale progress timestamp its predecessor left behind.
        member.last_progress = now;
        // This JoinGroup's requested timeout applies from here on, in case it changed
        // since the instance last joined (e.g. a config change shipped with the restart).
        member.session_timeout = session_timeout;
        group.members.insert(new_member_id.clone(), member);
        group
            .static_members
            .insert(instance_id.to_string(), new_member_id.clone());

        let is_leader = group.leader.as_deref() == Some(previous_member_id.as_str());
        if is_leader {
            group.leader = Some(new_member_id.clone());
        }

        Some((
            new_member_id,
            group.generation_id,
            is_leader,
            group.protocol_name.clone().unwrap_or_default(),
        ))
    }

    pub fn new() -> Self {
        Self::with_rebalance_delay(Duration::from_millis(3_000))
    }

    pub fn with_rebalance_delay(initial_rebalance_delay: Duration) -> Self {
        Self::with_config(initial_rebalance_delay, DEFAULT_MAX_POLL_INTERVAL)
    }

    pub fn with_config(initial_rebalance_delay: Duration, max_poll_interval: Duration) -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            initial_rebalance_delay,
            max_poll_interval,
        }
    }

    pub fn initial_rebalance_delay(&self) -> Duration {
        self.initial_rebalance_delay
    }

    pub fn max_poll_interval(&self) -> Duration {
        self.max_poll_interval
    }

    /// How long until this group's open join window closes, or `None` if it is already
    /// closed. Drives `StorageEngine::join_group`'s wait without holding the group lock
    /// across an await.
    pub fn join_window_remaining(&self, group_id: &str) -> Option<Duration> {
        let groups = self.groups.lock().unwrap();
        let group = groups.get(group_id)?;
        let deadline = group.rebalance_deadline?;
        let now = Instant::now();
        if now < deadline {
            Some(deadline - now)
        } else {
            None
        }
    }

    /// Closes an elapsed join window and moves the group on to assignment. Idempotent.
    pub fn close_join_window(&self, group_id: &str) {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            if group
                .rebalance_deadline
                .map(|d| Instant::now() >= d)
                .unwrap_or(false)
            {
                group.rebalance_deadline = None;
                if group.state == GroupState::PreparingRebalance {
                    group.state = GroupState::CompletingRebalance;
                }
            }
        }
    }

    /// Returns the group's current generation and whether `member_id` leads it — read
    /// after the join window closes, so every member of the window reports the same
    /// generation.
    pub fn join_result(&self, group_id: &str, member_id: &str) -> Option<(u32, bool, String)> {
        let groups = self.groups.lock().unwrap();
        let group = groups.get(group_id)?;
        Some((
            group.generation_id,
            group.leader.as_deref() == Some(member_id),
            group.protocol_name.clone().unwrap_or_default(),
        ))
    }

    /// Returns `(member_id, generation_id, is_leader, protocol_name)` on success. The
    /// caller needs `generation_id` to make correct `SyncGroup`/`Heartbeat` calls, and
    /// `is_leader` to know whether it's the one responsible for computing/submitting the
    /// group's assignment via `SyncGroup` — both were previously not returned at all
    /// (callers had to hardcode generation 1 and had no way to discover leadership),
    /// which meant this protocol only actually worked for a group's very first join.
    ///
    /// `group_instance_id` opts the caller into static membership: see `rejoin_static`.
    /// `session_timeout_ms` is the client's requested `session.timeout.ms` — the
    /// `SESSION_TIMEOUT_MS` tagged field off the request envelope, or `None` if the
    /// request carried no such tag. Resolved (and clamped) via `resolve_session_timeout`
    /// before it's used as this member's eviction threshold.
    pub fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocols: Vec<String>,
        session_timeout_ms: Option<u32>,
    ) -> Result<(String, u32, bool, String), String> {
        let mut groups = self.groups.lock().unwrap();
        let group = groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_id.to_string()));
        self.prune_expired_members(group);

        let session_timeout =
            Self::resolve_session_timeout(session_timeout_ms, group_id, member_id);

        // A known instance id short-circuits everything below: the caller is a member the
        // group already has, so it takes its slot back without the group rebalancing.
        if let Some(instance_id) = group_instance_id {
            if let Some(result) = Self::rejoin_static(group, instance_id, session_timeout) {
                return Ok(result);
            }
        }

        let m_id = if let Some(instance_id) = group_instance_id {
            // First sighting of this instance. Name its member id after the instance so a
            // static member is identifiable in `describe_group` output, and so the
            // sequence below can hand out a fresh one on every rejoin.
            group.member_id_seq = group.member_id_seq.saturating_add(1);
            format!("{}-{}", instance_id, group.member_id_seq)
        } else if member_id.is_empty() {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_string()
        } else {
            member_id.to_string()
        };

        let was_empty = group.state == GroupState::Empty;
        let mut rebalance_needed = false;
        if !group.members.contains_key(&m_id) {
            let now = Instant::now();
            group.members.insert(
                m_id.clone(),
                MemberState {
                    member_id: m_id.clone(),
                    client_id: m_id.clone(), // Simplify for now
                    session_timeout,
                    assigned_partitions: HashMap::new(),
                    last_heartbeat: now,
                    // Seeded to the join time, not left at some zero value — a member
                    // that simply hasn't fetched yet gets a full `max.poll.interval`
                    // grace period before it can be judged stalled (issue #54).
                    last_progress: now,
                    group_instance_id: group_instance_id.map(str::to_string),
                },
            );
            if let Some(instance_id) = group_instance_id {
                group
                    .static_members
                    .insert(instance_id.to_string(), m_id.clone());
            }
            rebalance_needed = true;
        } else if let Some(member) = group.members.get_mut(&m_id) {
            member.last_heartbeat = Instant::now();
            // Refreshed on every rejoin, in case the client's requested timeout changed
            // — same reasoning as `rejoin_static`. `last_progress` is deliberately left
            // alone here: this is an existing member rejoining under its own id (e.g.
            // recovering from a stale generation), not a fresh process, so it must not
            // get a free pass on the stalled-consumption check it hasn't earned.
            member.session_timeout = session_timeout;
        }

        // Negotiate the protocol when the group is (re)forming from Empty — the first
        // member to (re)start the group picks it, matching Kafka's convention of clients
        // listing their preferred assignor first.
        if was_empty {
            group.protocol_name = protocols.into_iter().next();
        }

        // Join barrier (`group.initial.rebalance.delay.ms`).
        //
        // A new member only forms a NEW generation if no join window is currently open;
        // otherwise it joins the window already in progress. Without this, every arrival
        // formed its own generation and immediately bumped `generation_id`, which
        // invalidated the assignment just handed to the previous joiner and forced it to
        // rejoin — so a group of N members starting together produced ~N rebalances and
        // could fail to converge at all under churn, leaving partitions unassigned.
        //
        // The window is opened by the first joiner of a rebalance and *extended* by each
        // subsequent one (capped below), so a group whose members trickle in still settles
        // into a single generation. The caller waits for the window to close before
        // reporting the result — see `StorageEngine::join_group`.
        if rebalance_needed || was_empty {
            let now = Instant::now();
            // Each straggler buys the window a little more time, not a full fresh delay.
            let extension = self.initial_rebalance_delay / 4;
            match group.rebalance_deadline {
                // A window is already open: this member joins that generation. Extend the
                // deadline a little so a straggler still lands in the same generation,
                // but never past `max_window` — otherwise a steady arrival rate could keep
                // pushing the deadline out and the group would never stabilize.
                Some(deadline) if now < deadline => {
                    let max_window = now + self.initial_rebalance_delay * 2;
                    group.rebalance_deadline = Some((deadline + extension).min(max_window));
                }
                // No open window: start one and form the new generation.
                _ => {
                    group.state = GroupState::PreparingRebalance;
                    group.generation_id = group.generation_id.saturating_add(1);
                    group.rebalance_deadline = Some(now + self.initial_rebalance_delay);
                }
            }
        }
        if group.leader.is_none() {
            group.leader = Some(m_id.clone());
        }

        let is_leader = group.leader.as_deref() == Some(m_id.as_str());
        let protocol_name = group.protocol_name.clone().unwrap_or_default();
        Ok((m_id, group.generation_id, is_leader, protocol_name))
    }

    /// Returns the CALLING member's own assignment (topic -> partitions) on success.
    ///
    /// Previously this only ever succeeded for the group leader and returned nothing
    /// useful even then (`Result<(), String>`) — every non-leader member's `SyncGroup`
    /// call hard-failed with "Only group leader can sync assignments", so there was no
    /// wire-protocol path for a follower to ever learn its own assignment at all. Real
    /// Kafka has every member call `SyncGroup`; only the leader's call carries the
    /// computed assignment payload, but every member's call retrieves its own slice of
    /// it once the leader has submitted.
    ///
    /// If the leader hasn't synced yet (group still mid-rebalance), a follower's call
    /// returns `Err("REBALANCE_IN_PROGRESS")` — a distinguishable, retryable signal
    /// rather than a fatal error, so a client can poll briefly instead of giving up.
    pub fn sync_group(
        &self,
        group_id: &str,
        generation_id: u32,
        member_id: &str,
        assignments: Vec<crate::protocol::wire::MemberAssignment>,
    ) -> Result<HashMap<String, Vec<u32>>, String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            self.prune_expired_members(group);
            if generation_id != group.generation_id {
                return Err(format!(
                    "Generation mismatch: expected {}, got {}",
                    group.generation_id, generation_id
                ));
            }
            if !group.members.contains_key(member_id) {
                return Err("Member not found in group".to_string());
            }

            if group.leader.as_deref() == Some(member_id) {
                for assign in assignments {
                    if let Some(member) = group.members.get_mut(&assign.member_id) {
                        member
                            .assigned_partitions
                            .insert(assign.topic, assign.partitions);
                    }
                }
                group.state = GroupState::Stable;
            } else if group.state != GroupState::Stable {
                // The leader hasn't submitted the assignment for this generation yet —
                // ask the follower to retry rather than failing it outright.
                return Err("REBALANCE_IN_PROGRESS".to_string());
            }

            let assignment = group
                .members
                .get(member_id)
                .map(|m| {
                    m.assigned_partitions
                        .iter()
                        .map(|(t, p)| (t.clone(), p.clone()))
                        .collect()
                })
                .unwrap_or_default();
            return Ok(assignment);
        }
        Err("Group not found".to_string())
    }

    pub fn heartbeat(
        &self,
        group_id: &str,
        generation_id: u32,
        member_id: &str,
    ) -> Result<(), String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            self.prune_expired_members(group);
            if generation_id != group.generation_id {
                // Both branches below signal the same underlying event — this member's
                // generation is stale and it needs to call JoinGroup — via the same
                // recognizable "REBALANCE_IN_PROGRESS" prefix real Kafka clients key off
                // of, so a client never has to guess from free-form text whether an error
                // means "rejoin" versus something fatal. What differs is what the member
                // may do in the meantime, which is the actual "stop the world" difference
                // KIP-429 cooperative rebalancing is about:
                //
                // Under a cooperative assignor, a member that hasn't rejoined for the
                // current generation yet is not a fatal condition — it can keep
                // processing the partitions it already owns from its last stable
                // assignment (which is still sitting untouched in `assigned_partitions`,
                // since rebalancing here never revokes it up front) until it gets around
                // to calling JoinGroup/SyncGroup again, so its heartbeat is refreshed
                // rather than left to expire.
                if group.is_cooperative() && group.state != GroupState::Stable {
                    if let Some(member) = group.members.get_mut(member_id) {
                        member.last_heartbeat = Instant::now();
                        return Err("REBALANCE_IN_PROGRESS".to_string());
                    }
                }
                // Eager (default) groups: no grace period, and the heartbeat is
                // deliberately NOT refreshed — if this member doesn't rejoin promptly, it
                // expires and is pruned like normal, rather than being kept alive
                // indefinitely on a generation it's no longer part of.
                return Err(format!(
                    "REBALANCE_IN_PROGRESS: generation mismatch, expected {}, got {}",
                    group.generation_id, generation_id
                ));
            }
            if let Some(member) = group.members.get_mut(member_id) {
                member.last_heartbeat = Instant::now();
                return Ok(());
            }
        }
        Err("Group or member not found".to_string())
    }

    /// Records that `member_id` in `group_id` made fetch progress just now — the signal
    /// used to detect a member that is heartbeating but has stopped consuming (issue #54).
    /// See [`MemberState::last_progress`].
    ///
    /// A `group_id`/`member_id` that doesn't currently resolve to a live member (unknown
    /// group, wrong or already-evicted member id) is silently ignored: the fetch itself is
    /// still served normally by the caller regardless of whether the attribution lands,
    /// same as an unrecognised tag anywhere else in the envelope.
    pub fn record_progress(&self, group_id: &str, member_id: &str) {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            if let Some(member) = group.members.get_mut(member_id) {
                member.last_progress = Instant::now();
            }
        }
    }

    /// The group's most recent prunings, oldest first, capped at `MAX_RECENT_EVICTIONS`.
    /// Exists so an eviction's cause (`EvictionReason`) is checkable in-process — by a
    /// test, or eventually an operator-facing surface — rather than only ever visible in
    /// the `tracing::warn!` line `prune_expired_members` emits.
    pub fn recent_evictions(&self, group_id: &str) -> Vec<EvictionRecord> {
        let groups = self.groups.lock().unwrap();
        groups
            .get(group_id)
            .map(|g| g.recent_evictions.clone())
            .unwrap_or_default()
    }

    /// Removes a member from the group.
    ///
    /// `group_instance_id`, when given, identifies the member instead of `member_id` —
    /// a static member's id is rotated on every rejoin, so a caller that has restarted (or
    /// an operator retiring an instance from outside) has no way to name it otherwise.
    /// Leaving this way is what actually retires the instance: it releases the reservation
    /// that would otherwise let the instance return and reclaim its assignment.
    pub fn leave_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<(), String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            let member_id = match group_instance_id {
                Some(instance_id) => match group.static_members.remove(instance_id) {
                    Some(resolved) => resolved,
                    // The instance holds no slot here. Fall back to the member id given,
                    // so an explicit id still works even if the instance id is stale.
                    None => member_id.to_string(),
                },
                None => member_id.to_string(),
            };
            let member_id = member_id.as_str();

            if let Some(member) = group.members.remove(member_id) {
                if let Some(instance_id) = member.group_instance_id {
                    group.static_members.remove(&instance_id);
                }
            }
            if group.members.is_empty() {
                group.state = GroupState::Empty;
                group.leader = None;
            } else {
                group.state = GroupState::PreparingRebalance;
                if group.leader.as_deref() == Some(member_id) {
                    group.leader = group.members.keys().next().cloned();
                }
            }
            return Ok(());
        }
        Err("Group not found".to_string())
    }

    pub fn list_groups(&self) -> Vec<String> {
        let mut groups = self.groups.lock().unwrap();
        for group in groups.values_mut() {
            self.prune_expired_members(group);
        }
        groups.keys().cloned().collect()
    }

    pub fn describe_group(&self, group_id: &str) -> Option<GroupDescription> {
        let mut groups = self.groups.lock().unwrap();
        let group = groups.get_mut(group_id)?;
        self.prune_expired_members(group);
        let state_str = format!("{:?}", group.state);
        let mut members = Vec::new();
        for (m_id, member) in &group.members {
            let mut assigned_partitions = Vec::new();
            for (topic, parts) in &member.assigned_partitions {
                for p in parts {
                    assigned_partitions.push((topic.clone(), *p));
                }
            }
            members.push(crate::protocol::wire::DescribedGroupMember {
                member_id: m_id.clone(),
                assigned_partitions,
            });
        }
        Some(GroupDescription {
            group_id: group_id.to_string(),
            state_str,
            members,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- resolve_session_timeout ---

    #[test]
    fn resolve_session_timeout_defaults_when_absent() {
        assert_eq!(
            GroupCoordinator::resolve_session_timeout(None, "g", "m"),
            DEFAULT_SESSION_TIMEOUT,
            "no SESSION_TIMEOUT_MS tag must keep the coordinator's historical default"
        );
    }

    #[test]
    fn resolve_session_timeout_clamps_below_minimum() {
        let requested_ms = (MIN_SESSION_TIMEOUT.as_millis() as u32).saturating_sub(50);
        assert_eq!(
            GroupCoordinator::resolve_session_timeout(Some(requested_ms), "g", "m"),
            MIN_SESSION_TIMEOUT,
            "a timeout below the floor must be clamped up to it, not honored as-is"
        );
    }

    #[test]
    fn resolve_session_timeout_clamps_above_maximum() {
        let requested_ms = (MAX_SESSION_TIMEOUT.as_millis() as u32) + 60_000;
        assert_eq!(
            GroupCoordinator::resolve_session_timeout(Some(requested_ms), "g", "m"),
            MAX_SESSION_TIMEOUT,
            "a timeout above the ceiling must be clamped down to it, not honored as-is"
        );
    }

    #[test]
    fn resolve_session_timeout_passes_through_in_range_value() {
        assert_eq!(
            GroupCoordinator::resolve_session_timeout(Some(5_000), "g", "m"),
            Duration::from_millis(5_000),
            "an in-range request must be honored exactly, not silently adjusted"
        );
    }

    // --- `join_group` actually applies the resolved timeout to the member ---

    fn member_session_timeout(
        coordinator: &GroupCoordinator,
        group_id: &str,
        member_id: &str,
    ) -> Duration {
        coordinator
            .groups
            .lock()
            .unwrap()
            .get(group_id)
            .and_then(|g| g.members.get(member_id))
            .expect("member must exist")
            .session_timeout
    }

    #[test]
    fn join_group_honors_an_in_range_requested_session_timeout() {
        let coordinator = GroupCoordinator::with_rebalance_delay(Duration::ZERO);
        let (member_id, ..) = coordinator
            .join_group("g", "m1", None, vec!["range".to_string()], Some(2_000))
            .unwrap();
        assert_eq!(
            member_session_timeout(&coordinator, "g", &member_id),
            Duration::from_millis(2_000)
        );
    }

    #[test]
    fn join_group_clamps_a_too_low_requested_session_timeout() {
        let coordinator = GroupCoordinator::with_rebalance_delay(Duration::ZERO);
        let (member_id, ..) = coordinator
            .join_group("g", "m1", None, vec!["range".to_string()], Some(1))
            .unwrap();
        assert_eq!(
            member_session_timeout(&coordinator, "g", &member_id),
            MIN_SESSION_TIMEOUT,
            "a 1ms request must not be honored as-is — it would evict the member on \
             ordinary jitter"
        );
    }

    #[test]
    fn join_group_clamps_a_too_high_requested_session_timeout() {
        let coordinator = GroupCoordinator::with_rebalance_delay(Duration::ZERO);
        let (member_id, ..) = coordinator
            .join_group("g", "m1", None, vec!["range".to_string()], Some(3_600_000))
            .unwrap();
        assert_eq!(
            member_session_timeout(&coordinator, "g", &member_id),
            MAX_SESSION_TIMEOUT,
            "an hour-long request must not be honored as-is — it would defeat failure \
             detection almost entirely"
        );
    }

    #[test]
    fn join_group_without_the_tag_keeps_the_default_session_timeout() {
        let coordinator = GroupCoordinator::with_rebalance_delay(Duration::ZERO);
        let (member_id, ..) = coordinator
            .join_group("g", "m1", None, vec!["range".to_string()], None)
            .unwrap();
        assert_eq!(
            member_session_timeout(&coordinator, "g", &member_id),
            DEFAULT_SESSION_TIMEOUT,
            "a legacy caller sending no tag must see exactly the old hardcoded behavior"
        );
    }

    #[test]
    fn rejoin_static_refreshes_the_session_timeout_too() {
        let coordinator = GroupCoordinator::with_rebalance_delay(Duration::ZERO);
        coordinator
            .join_group(
                "g",
                "",
                Some("instance-a"),
                vec!["range".to_string()],
                Some(1_000),
            )
            .unwrap();
        // Same instance rejoins with a different requested timeout — simulating a
        // restarted process shipped with a new config.
        let (member_id, ..) = coordinator
            .join_group(
                "g",
                "",
                Some("instance-a"),
                vec!["range".to_string()],
                Some(9_000),
            )
            .unwrap();
        assert_eq!(
            member_session_timeout(&coordinator, "g", &member_id),
            Duration::from_millis(9_000),
            "a static member's rejoin must pick up its newly requested timeout"
        );
    }

    // --- eviction-cause distinguishability (issue #54) ---

    /// Back-dates a member's liveness timestamps directly, rather than sleeping in the
    /// test — deterministic and instant instead of racing real wall-clock thresholds.
    fn backdate(
        coordinator: &GroupCoordinator,
        group_id: &str,
        member_id: &str,
        heartbeat_age: Duration,
        progress_age: Duration,
    ) {
        let mut groups = coordinator.groups.lock().unwrap();
        let member = groups
            .get_mut(group_id)
            .unwrap()
            .members
            .get_mut(member_id)
            .unwrap();
        member.last_heartbeat = Instant::now().checked_sub(heartbeat_age).unwrap();
        member.last_progress = Instant::now().checked_sub(progress_age).unwrap();
    }

    #[test]
    fn a_dead_member_is_evicted_for_session_timeout_not_stalled_consumption() {
        let coordinator = GroupCoordinator::with_config(Duration::ZERO, Duration::from_secs(600));
        let (member_id, ..) = coordinator
            .join_group("g", "dead", None, vec!["range".to_string()], Some(1_000))
            .unwrap();
        // Neither heartbeat nor fetch for well past the 1s session timeout, but nowhere
        // near the group's very generous 600s max_poll_interval — only the session-timeout
        // check can be what fires here.
        backdate(
            &coordinator,
            "g",
            &member_id,
            Duration::from_secs(5),
            Duration::from_secs(5),
        );

        assert!(coordinator.describe_group("g").unwrap().members.is_empty());

        let evictions = coordinator.recent_evictions("g");
        assert_eq!(evictions.len(), 1);
        assert_eq!(evictions[0].member_id, member_id);
        assert_eq!(evictions[0].reason, EvictionReason::SessionTimeout);
    }

    #[test]
    fn a_heartbeating_member_that_stops_fetching_is_evicted_for_stalled_consumption() {
        let coordinator = GroupCoordinator::with_config(Duration::ZERO, Duration::from_millis(500));
        let (member_id, ..) = coordinator
            .join_group(
                "g",
                "stalled",
                None,
                vec!["range".to_string()],
                Some(60_000),
            )
            .unwrap();
        // Heartbeat is fresh — session timeout cannot be why this gets pruned — but fetch
        // progress is stale well past max_poll_interval.
        backdate(
            &coordinator,
            "g",
            &member_id,
            Duration::from_millis(10),
            Duration::from_secs(2),
        );

        assert!(coordinator.describe_group("g").unwrap().members.is_empty());

        let evictions = coordinator.recent_evictions("g");
        assert_eq!(evictions.len(), 1);
        assert_eq!(evictions[0].member_id, member_id);
        assert_eq!(evictions[0].reason, EvictionReason::StalledConsumption);
    }

    #[test]
    fn record_progress_prevents_a_heartbeating_members_stalled_eviction() {
        let coordinator = GroupCoordinator::with_config(Duration::ZERO, Duration::from_millis(500));
        let (member_id, ..) = coordinator
            .join_group("g", "active", None, vec!["range".to_string()], Some(60_000))
            .unwrap();
        backdate(
            &coordinator,
            "g",
            &member_id,
            Duration::from_millis(10),
            Duration::from_secs(2),
        );

        // Refresh progress right before the next prune-triggering call, exactly as a real
        // tagged `Fetch` would via `GroupCoordinator::record_progress`.
        coordinator.record_progress("g", &member_id);

        assert!(
            !coordinator.describe_group("g").unwrap().members.is_empty(),
            "a member whose progress was just refreshed must not be evicted for stalling"
        );
        assert!(coordinator.recent_evictions("g").is_empty());
    }

    #[test]
    fn both_eviction_causes_are_recorded_distinctly_in_the_same_group() {
        let coordinator = GroupCoordinator::with_config(Duration::ZERO, Duration::from_millis(500));
        let (dead, ..) = coordinator
            .join_group("g", "dead", None, vec!["range".to_string()], Some(200))
            .unwrap();
        let (stalled, ..) = coordinator
            .join_group(
                "g",
                "stalled",
                None,
                vec!["range".to_string()],
                Some(60_000),
            )
            .unwrap();
        let (healthy, ..) = coordinator
            .join_group(
                "g",
                "healthy",
                None,
                vec!["range".to_string()],
                Some(60_000),
            )
            .unwrap();

        backdate(
            &coordinator,
            "g",
            &dead,
            Duration::from_secs(5),
            Duration::from_secs(5),
        );
        backdate(
            &coordinator,
            "g",
            &stalled,
            Duration::from_millis(10),
            Duration::from_secs(2),
        );
        backdate(
            &coordinator,
            "g",
            &healthy,
            Duration::from_millis(10),
            Duration::from_millis(10),
        );

        let described = coordinator.describe_group("g").unwrap();
        let remaining: Vec<String> = described
            .members
            .iter()
            .map(|m| m.member_id.clone())
            .collect();
        assert_eq!(
            remaining,
            vec![healthy],
            "only the actively healthy member should remain"
        );

        let evictions = coordinator.recent_evictions("g");
        assert_eq!(evictions.len(), 2);
        let dead_record = evictions.iter().find(|e| e.member_id == dead).unwrap();
        let stalled_record = evictions.iter().find(|e| e.member_id == stalled).unwrap();
        assert_eq!(dead_record.reason, EvictionReason::SessionTimeout);
        assert_eq!(stalled_record.reason, EvictionReason::StalledConsumption);
        assert_ne!(
            dead_record.reason, stalled_record.reason,
            "the two eviction causes must be distinguishable from one another"
        );
    }
}
