use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

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
    fn prune_expired_members(group: &mut ConsumerGroup) {
        let now = Instant::now();
        let expired: Vec<String> = group
            .members
            .iter()
            .filter(|(_, member)| {
                now.duration_since(member.last_heartbeat) > member.session_timeout
            })
            .map(|(member_id, _)| member_id.clone())
            .collect();

        for member_id in expired {
            group.members.remove(&member_id);
            if group.leader.as_deref() == Some(&member_id) {
                group.leader = group.members.keys().next().cloned();
            }
        }

        if group.members.is_empty() {
            group.state = GroupState::Empty;
            group.leader = None;
        }
    }

    pub fn new() -> Self {
        Self::with_rebalance_delay(Duration::from_millis(3_000))
    }

    pub fn with_rebalance_delay(initial_rebalance_delay: Duration) -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            initial_rebalance_delay,
        }
    }

    pub fn initial_rebalance_delay(&self) -> Duration {
        self.initial_rebalance_delay
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
    pub fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        protocols: Vec<String>,
    ) -> Result<(String, u32, bool, String), String> {
        let mut groups = self.groups.lock().unwrap();
        let group = groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_id.to_string()));
        Self::prune_expired_members(group);

        let m_id = if member_id.is_empty() {
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
            group.members.insert(
                m_id.clone(),
                MemberState {
                    member_id: m_id.clone(),
                    client_id: m_id.clone(), // Simplify for now
                    session_timeout: Duration::from_secs(10),
                    assigned_partitions: HashMap::new(),
                    last_heartbeat: Instant::now(),
                },
            );
            rebalance_needed = true;
        } else if let Some(member) = group.members.get_mut(&m_id) {
            member.last_heartbeat = Instant::now();
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
            Self::prune_expired_members(group);
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
            Self::prune_expired_members(group);
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

    pub fn leave_group(&self, group_id: &str, member_id: &str) -> Result<(), String> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(group) = groups.get_mut(group_id) {
            group.members.remove(member_id);
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
            Self::prune_expired_members(group);
        }
        groups.keys().cloned().collect()
    }

    pub fn describe_group(&self, group_id: &str) -> Option<GroupDescription> {
        let mut groups = self.groups.lock().unwrap();
        let group = groups.get_mut(group_id)?;
        Self::prune_expired_members(group);
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
