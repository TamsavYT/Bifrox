use crate::client::TestClient;
use crate::protocol::wire::MemberAssignment;
use crate::protocol::RecordFrame;
use std::collections::HashMap;
use std::io::Result as IoResult;
use std::time::{Duration, Instant};

/// Kafka-style range assignor: divides `partition_count` partitions evenly across
/// `member_ids`, giving the first `partition_count % member_ids.len()` members one extra
/// partition each so every partition still lands somewhere. Members are sorted first so
/// every member (and the leader submitting on everyone's behalf) computes the same answer
/// from the same inputs — this must be deterministic, since only the leader actually runs
/// it and every follower has to trust the result without recomputing it.
///
/// Partitions are handed out in contiguous blocks (member 0 gets `[0, k)`, member 1 gets
/// `[k, 2k)`, ...), matching Kafka's `RangeAssignor` convention rather than an interleaved
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

/// Kafka-style round-robin assignor: deals `partition_count` partitions out one at a time
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

/// Which partition assignment strategy the group negotiated, per [`assign_range`] vs.
/// [`assign_roundrobin`].
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
    pub fn assign(self, partition_count: u32, member_ids: &[String]) -> Vec<(String, Vec<u32>)> {
        match self {
            AssignmentStrategy::Range => assign_range(partition_count, member_ids),
            AssignmentStrategy::RoundRobin => assign_roundrobin(partition_count, member_ids),
        }
    }
}

/// Configuration for a [`GroupConsumer`].
#[derive(Debug, Clone)]
pub struct GroupConsumerConfig {
    pub group_id: String,
    pub topic: String,
    /// `group.instance.id` (KIP-345 static membership). `None` means this member joins
    /// dynamically — every process start is a new arrival and the group rebalances around
    /// it. See `TestClient::join_group_static` for what setting this buys.
    pub instance_id: Option<String>,
    /// Assignment protocols this member offers, most preferred first. The group as a whole
    /// negotiates one name (see `GroupCoordinator::join_group`) — offering both `"range"`
    /// and `"roundrobin"` here, rather than only the default, lets a group actually pick
    /// either instead of `"range"` being the sole option a member could ever propose.
    pub protocols: Vec<String>,
    pub fetch_max_bytes: u32,
    /// How long a follower keeps retrying a `REBALANCE_IN_PROGRESS` `SyncGroup` response
    /// before giving up and returning an error.
    ///
    /// This wait does not heartbeat, and `GroupCoordinator::sync_group` does not refresh
    /// the member's liveness either — so it must stay comfortably under the group's
    /// session timeout (a fixed 10s, coordinator-side), or a member that waits out the
    /// full retry window gets pruned for inactivity at almost the same moment, turning a
    /// retryable wait into a fatal "Member not found in group".
    pub sync_retry_timeout: Duration,
    pub sync_retry_interval: Duration,
}

impl Default for GroupConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            topic: String::new(),
            instance_id: None,
            protocols: vec!["range".to_string(), "roundrobin".to_string()],
            fetch_max_bytes: 64 * 1024,
            sync_retry_timeout: Duration::from_secs(5),
            sync_retry_interval: Duration::from_millis(50),
        }
    }
}

/// A consumer that belongs to a Kafka-style consumer group.
///
/// Which partitions this consumer reads from `config.topic` comes from the group's
/// membership and the negotiated assignment — never from a partition number the caller
/// picked. That is the point of this type: see `GroupCoordinator` in
/// `src/server/coordinator.rs` for the join/sync/heartbeat protocol it drives, and
/// `TestClient` for the wire calls it wraps.
///
/// Heartbeating happens inline, on whatever task calls [`Self::poll`] — there is no
/// background task keeping the session alive between polls. A caller that stops polling for
/// longer than the group's session timeout gets evicted and discovers it the next time it
/// calls `poll()`, same as a real Kafka consumer that stops calling `poll()`. Moving the
/// heartbeat to a background task is issue #53; left out of this change to keep it small.
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
}

impl GroupConsumer {
    /// Constructs a `GroupConsumer` and runs one full join+sync cycle, so the returned
    /// value already has a member id, generation and assignment.
    pub async fn join(client: TestClient, config: GroupConsumerConfig) -> IoResult<Self> {
        let mut consumer = Self {
            client,
            config,
            member_id: String::new(),
            generation_id: 0,
            is_leader: false,
            assignment: Vec::new(),
            next_offsets: HashMap::new(),
            pending_commits: HashMap::new(),
        };
        consumer.rejoin().await?;
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
        // already handed back, on every single rebalance. Kafka commits in
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
        let join = self
            .client
            .join_group_static(
                &self.config.group_id,
                &self.member_id,
                self.config.instance_id.as_deref(),
                &protocol_refs,
            )
            .await?;
        self.member_id = join.member_id;
        self.generation_id = join.generation_id;
        self.is_leader = join.is_leader;

        let assignment_map: HashMap<String, Vec<u32>> = if self.is_leader {
            // JoinGroup's response carries no member list, so the leader learns the
            // group's shape from DescribeGroup + DescribeTopic instead. That is
            // deliberate — propagating subscriptions through JoinGroup is a wire change
            // and belongs to a later issue.
            let (_, members) = self.client.describe_group(&self.config.group_id).await?;
            let member_ids: Vec<String> = members.into_iter().map(|m| m.member_id).collect();
            let (_, partitions) = self.client.describe_topic(&self.config.topic).await?;
            let partition_count = partitions.len() as u32;

            // `join.protocol_name` is what the group actually negotiated (the first
            // protocol offered by the first member to join the generation — see
            // `GroupCoordinator::join_group`), not necessarily the first entry in this
            // member's own `protocols` list. Using it here, rather than always calling
            // `assign_range`, is the point of this method: a group that negotiated
            // `roundrobin` must actually get round-robin.
            let strategy = AssignmentStrategy::from_protocol_name(&join.protocol_name);
            let assignments = strategy.assign(partition_count, &member_ids);
            let member_assignments: Vec<MemberAssignment> = assignments
                .into_iter()
                .map(|(member_id, partitions)| MemberAssignment {
                    member_id,
                    topic: self.config.topic.clone(),
                    partitions,
                })
                .collect();

            self.client
                .sync_group(
                    &self.config.group_id,
                    self.generation_id,
                    &self.member_id,
                    &member_assignments,
                )
                .await?
                .into_iter()
                .collect()
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

    /// One poll round: heartbeats, then fetches every owned partition in order.
    ///
    /// If the heartbeat fails — the generation went stale, or this member was fenced — this
    /// calls `rejoin()` and returns an empty result for the round rather than fetching under
    /// a stale generation; the caller is expected to call `poll()` again.
    pub async fn poll(&mut self) -> IoResult<Vec<(u32, RecordFrame)>> {
        if self
            .client
            .heartbeat(&self.config.group_id, self.generation_id, &self.member_id)
            .await
            .is_err()
        {
            self.rejoin().await?;
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        let partitions = self.assignment.clone();
        for partition in partitions {
            let next_offset = *self.next_offsets.get(&partition).unwrap_or(&0);
            match self
                .client
                .fetch(
                    &self.config.topic,
                    partition,
                    next_offset,
                    self.config.fetch_max_bytes,
                )
                .await
            {
                Ok(frames) => {
                    for frame in frames {
                        // Offsets advance past every frame — control markers occupy real
                        // offsets too — but only non-control frames are handed back.
                        self.next_offsets.insert(partition, frame.offset + 1);
                        self.pending_commits.insert(partition, frame.offset);
                        if !frame.is_control_marker() {
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

    pub async fn leave(&mut self) -> IoResult<()> {
        self.client
            .leave_group_static(
                &self.config.group_id,
                &self.member_id,
                self.config.instance_id.as_deref(),
            )
            .await
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
    fn strategy_from_protocol_name_falls_back_to_range_for_unknown_or_empty() {
        assert_eq!(
            AssignmentStrategy::from_protocol_name("sticky"),
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

        let strategy = AssignmentStrategy::from_protocol_name(&negotiated_protocol_name);
        let result = strategy.assign(7, &members);

        assert_eq!(result, assign_roundrobin(7, &members));
        assert_ne!(
            result,
            assign_range(7, &members),
            "a negotiated roundrobin protocol must not silently produce a range assignment"
        );
    }

    #[test]
    fn unrecognised_negotiated_protocol_name_falls_back_to_range_assignment() {
        let negotiated_protocol_name = "sticky".to_string();
        let members = ids(&["a", "b", "c"]);

        let strategy = AssignmentStrategy::from_protocol_name(&negotiated_protocol_name);
        let result = strategy.assign(7, &members);

        assert_eq!(
            result,
            assign_range(7, &members),
            "an unrecognised protocol name must default rather than error"
        );
    }
}
