use crate::protocol::wire::{AckBatch, AcknowledgeType};
use crate::server::partition::PartitionManager;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareRecordState {
    Available,
    Acquired,
    Acknowledged,
    Archived,
}

#[derive(Debug, Clone)]
pub struct InFlightBatch {
    pub first_offset: u64,
    pub last_offset: u64,
    pub state: ShareRecordState,
    pub acquired_by: Option<String>,
    pub acquired_at: Option<Instant>,
    pub lock_timeout: Duration,
    pub delivery_count: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ExpiryTimer {
    expire_at: Instant,
    first_offset: u64,
    last_offset: u64,
    member_id: String,
}

/// Orders primarily by `expire_at` (so `timers.first()`/`pop_first()` on the `BTreeSet`
/// below still gives the earliest-expiring timer first, same min-heap behavior as before),
/// tie-broken by the remaining fields so the ordering is a genuine total order consistent
/// with the derived `Eq` — required for `BTreeSet` correctness: two *distinct* timers that
/// happen to share the exact same `Instant` (plausible, since `Instant`s from back-to-back
/// `acquire_records` calls under a fixed `lock_timeout` can collide) must never compare
/// `Equal`, or the set would silently keep only one of them and the other's batch would
/// never get reaped on expiry.
impl Ord for ExpiryTimer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expire_at
            .cmp(&other.expire_at)
            .then_with(|| self.first_offset.cmp(&other.first_offset))
            .then_with(|| self.last_offset.cmp(&other.last_offset))
            .then_with(|| self.member_id.cmp(&other.member_id))
    }
}

impl PartialOrd for ExpiryTimer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct SharePartition {
    pub topic: String,
    pub partition: u32,
    pub group_id: String,
    pub default_lock_timeout: Duration,
    pub max_delivery_attempts: u16,
    pub start_offset: AtomicU64,
    pub next_fetch_offset: AtomicU64,
    batches: RwLock<BTreeMap<u64, InFlightBatch>>,
    /// Pending lock-expiry timers, earliest-first. A `BTreeSet` rather than a
    /// `BinaryHeap` so a timer whose batch is fully acknowledged/released/rejected before
    /// it naturally expires can be removed outright (`acknowledge` below) instead of
    /// lingering in the structure as dead weight until its `expire_at` eventually passes
    /// and `sweep_timers_internal` discards it as a no-op.
    timers: RwLock<BTreeSet<ExpiryTimer>>,
}

pub struct AcquiredRecordInfo {
    pub offset: u64,
    pub delivery_count: u16,
    pub frame: crate::segment::Record,
}

impl SharePartition {
    pub fn new(
        topic: String,
        partition: u32,
        group_id: String,
        default_lock_timeout: Duration,
        max_delivery_attempts: u16,
        initial_start_offset: u64,
    ) -> Self {
        Self {
            topic,
            partition,
            group_id,
            default_lock_timeout,
            max_delivery_attempts,
            start_offset: AtomicU64::new(initial_start_offset),
            next_fetch_offset: AtomicU64::new(initial_start_offset),
            batches: RwLock::new(BTreeMap::new()),
            timers: RwLock::new(BTreeSet::new()),
        }
    }

    /// Fetches and acquires up to `max_records` from the partition for `member_id`.
    /// 1. First satisfies from existing `Available` batches (fast reuse without reading fresh offsets).
    /// 2. Satisfies remaining records from disk log starting at `next_fetch_offset`.
    pub fn acquire_records(
        &self,
        member_id: &str,
        max_records: usize,
        lock_timeout: Option<Duration>,
        partition_manager: &PartitionManager,
    ) -> Result<Vec<AcquiredRecordInfo>, String> {
        if max_records == 0 {
            return Ok(Vec::new());
        }

        let timeout = lock_timeout.unwrap_or(self.default_lock_timeout);
        let now = Instant::now();
        let expire_at = now + timeout;

        // Process any expired locks lazily in O(1) peek
        self.sweep_timers_inline(now);

        let mut batches = self.batches.write();
        let mut timers = self.timers.write();
        let mut acquired_infos = Vec::with_capacity(max_records);
        let mut new_acquired_ranges: Vec<(u64, u64)> = Vec::new();

        // 1. Acquire from existing Available batches
        let available_keys: Vec<u64> = batches
            .iter()
            .filter(|(_, b)| b.state == ShareRecordState::Available)
            .map(|(&k, _)| k)
            .collect();

        for key in available_keys {
            if acquired_infos.len() >= max_records {
                break;
            }
            if let Some(mut batch) = batches.remove(&key) {
                if batch.state != ShareRecordState::Available {
                    batches.insert(key, batch);
                    continue;
                }

                let needed = max_records - acquired_infos.len();
                let batch_len = (batch.last_offset - batch.first_offset + 1) as usize;

                if batch_len <= needed {
                    // Entire batch acquired
                    batch.state = ShareRecordState::Acquired;
                    batch.acquired_by = Some(member_id.to_string());
                    batch.acquired_at = Some(now);
                    batch.lock_timeout = timeout;
                    batch.delivery_count += 1;

                    // Fetch records from partition manager
                    if let Ok(frames) =
                        partition_manager.fetch(batch.first_offset, 10 * 1024 * 1024)
                    {
                        for frame in frames {
                            if frame.offset >= batch.first_offset
                                && frame.offset <= batch.last_offset
                            {
                                acquired_infos.push(AcquiredRecordInfo {
                                    offset: frame.offset,
                                    delivery_count: batch.delivery_count,
                                    frame,
                                });
                            }
                        }
                    }

                    new_acquired_ranges.push((batch.first_offset, batch.last_offset));
                    batches.insert(batch.first_offset, batch);
                } else {
                    // Split batch: take prefix `needed`, leave remainder `Available`
                    let split_offset = batch.first_offset + needed as u64 - 1;

                    let acquired_batch = InFlightBatch {
                        first_offset: batch.first_offset,
                        last_offset: split_offset,
                        state: ShareRecordState::Acquired,
                        acquired_by: Some(member_id.to_string()),
                        acquired_at: Some(now),
                        lock_timeout: timeout,
                        delivery_count: batch.delivery_count + 1,
                    };

                    let remainder_batch = InFlightBatch {
                        first_offset: split_offset + 1,
                        last_offset: batch.last_offset,
                        state: ShareRecordState::Available,
                        acquired_by: None,
                        acquired_at: None,
                        lock_timeout: batch.lock_timeout,
                        delivery_count: batch.delivery_count,
                    };

                    if let Ok(frames) =
                        partition_manager.fetch(acquired_batch.first_offset, 10 * 1024 * 1024)
                    {
                        for frame in frames {
                            if frame.offset >= acquired_batch.first_offset
                                && frame.offset <= acquired_batch.last_offset
                            {
                                acquired_infos.push(AcquiredRecordInfo {
                                    offset: frame.offset,
                                    delivery_count: acquired_batch.delivery_count,
                                    frame,
                                });
                            }
                        }
                    }

                    new_acquired_ranges
                        .push((acquired_batch.first_offset, acquired_batch.last_offset));
                    batches.insert(acquired_batch.first_offset, acquired_batch);
                    batches.insert(remainder_batch.first_offset, remainder_batch);
                }
            }
        }

        // 2. If more records needed, fetch fresh offsets from log
        if acquired_infos.len() < max_records {
            let next_offset = self.next_fetch_offset.load(Ordering::SeqCst);
            let needed = max_records - acquired_infos.len();

            if let Ok(frames) = partition_manager.fetch(next_offset, 10 * 1024 * 1024) {
                let mut valid_frames = Vec::new();
                for frame in frames {
                    if frame.offset >= next_offset && valid_frames.len() < needed {
                        valid_frames.push(frame);
                    }
                }

                if !valid_frames.is_empty() {
                    let first_offset = valid_frames.first().unwrap().offset;
                    let last_offset = valid_frames.last().unwrap().offset;

                    batches.insert(
                        first_offset,
                        InFlightBatch {
                            first_offset,
                            last_offset,
                            state: ShareRecordState::Acquired,
                            acquired_by: Some(member_id.to_string()),
                            acquired_at: Some(now),
                            lock_timeout: timeout,
                            delivery_count: 1,
                        },
                    );

                    for frame in valid_frames {
                        let off = frame.offset;
                        acquired_infos.push(AcquiredRecordInfo {
                            offset: off,
                            delivery_count: 1,
                            frame,
                        });
                    }

                    new_acquired_ranges.push((first_offset, last_offset));
                    self.next_fetch_offset
                        .store(last_offset + 1, Ordering::SeqCst);
                }
            }
        }

        // Register timers for newly acquired ranges
        for (f, l) in new_acquired_ranges {
            timers.insert(ExpiryTimer {
                expire_at,
                first_offset: f,
                last_offset: l,
                member_id: member_id.to_string(),
            });
        }

        Ok(acquired_infos)
    }

    /// Applies acknowledgements across offset ranges (ACCEPT, RELEASE, REJECT, RENEW).
    /// Uses interval range splitting and merging for O(log B) efficiency.
    pub fn acknowledge(
        &self,
        member_id: &str,
        ack_batches: &[AckBatch],
    ) -> Result<Vec<u64>, String> {
        let mut batches = self.batches.write();
        let mut timers = self.timers.write();
        let now = Instant::now();
        let mut dlq_offsets = Vec::new();

        for ack in ack_batches {
            let target_first = ack.first_offset;
            let target_last = ack.last_offset;

            // Find overlapping batches
            let matching_keys: Vec<u64> = batches
                .range(..=target_last)
                .filter(|(_, b)| b.last_offset >= target_first)
                .map(|(&k, _)| k)
                .collect();

            for key in matching_keys {
                if let Some(batch) = batches.remove(&key) {
                    // Only the owner member can acknowledge an active acquisition
                    if batch.state != ShareRecordState::Acquired
                        || batch.acquired_by.as_deref() != Some(member_id)
                    {
                        batches.insert(key, batch);
                        continue;
                    }

                    // Range intersection
                    let overlap_start = batch.first_offset.max(target_first);
                    let overlap_end = batch.last_offset.min(target_last);

                    // The batch popped above was `Acquired`, so it has a pending timer
                    // entry covering its *original, full* [first_offset, last_offset]
                    // range. That range is about to be superseded by up to three pieces
                    // (left remainder / acked middle / right remainder) below, so cancel
                    // the original timer outright — instead of leaving it in the set to
                    // linger until it eventually expires and gets discarded as a no-op —
                    // and re-register fresh timers (same `expire_at`, narrower ranges) for
                    // whichever remainder pieces are still `Acquired` after the split.
                    let original_expire_at = batch.acquired_at.map(|at| at + batch.lock_timeout);
                    if let (Some(expire_at), Some(owner)) = (original_expire_at, &batch.acquired_by)
                    {
                        timers.remove(&ExpiryTimer {
                            expire_at,
                            first_offset: batch.first_offset,
                            last_offset: batch.last_offset,
                            member_id: owner.clone(),
                        });
                    }

                    // 1. Left non-overlapping slice
                    if batch.first_offset < overlap_start {
                        batches.insert(
                            batch.first_offset,
                            InFlightBatch {
                                first_offset: batch.first_offset,
                                last_offset: overlap_start - 1,
                                state: batch.state,
                                acquired_by: batch.acquired_by.clone(),
                                acquired_at: batch.acquired_at,
                                lock_timeout: batch.lock_timeout,
                                delivery_count: batch.delivery_count,
                            },
                        );
                        if let (Some(expire_at), Some(owner)) =
                            (original_expire_at, &batch.acquired_by)
                        {
                            timers.insert(ExpiryTimer {
                                expire_at,
                                first_offset: batch.first_offset,
                                last_offset: overlap_start - 1,
                                member_id: owner.clone(),
                            });
                        }
                    }

                    // 2. Middle overlapping slice (apply ACK)
                    let new_state = match ack.ack_type {
                        AcknowledgeType::Accept => ShareRecordState::Acknowledged,
                        AcknowledgeType::Release => ShareRecordState::Available,
                        AcknowledgeType::Reject => {
                            for off in overlap_start..=overlap_end {
                                dlq_offsets.push(off);
                            }
                            ShareRecordState::Archived
                        }
                        AcknowledgeType::Renew => {
                            timers.insert(ExpiryTimer {
                                expire_at: now + batch.lock_timeout,
                                first_offset: overlap_start,
                                last_offset: overlap_end,
                                member_id: member_id.to_string(),
                            });
                            ShareRecordState::Acquired
                        }
                    };

                    batches.insert(
                        overlap_start,
                        InFlightBatch {
                            first_offset: overlap_start,
                            last_offset: overlap_end,
                            state: new_state,
                            acquired_by: if new_state == ShareRecordState::Acquired {
                                Some(member_id.to_string())
                            } else {
                                None
                            },
                            acquired_at: if new_state == ShareRecordState::Acquired {
                                Some(now)
                            } else {
                                None
                            },
                            lock_timeout: batch.lock_timeout,
                            delivery_count: batch.delivery_count,
                        },
                    );

                    // 3. Right non-overlapping slice
                    if batch.last_offset > overlap_end {
                        batches.insert(
                            overlap_end + 1,
                            InFlightBatch {
                                first_offset: overlap_end + 1,
                                last_offset: batch.last_offset,
                                state: batch.state,
                                acquired_by: batch.acquired_by.clone(),
                                acquired_at: batch.acquired_at,
                                lock_timeout: batch.lock_timeout,
                                delivery_count: batch.delivery_count,
                            },
                        );
                        if let (Some(expire_at), Some(owner)) =
                            (original_expire_at, &batch.acquired_by)
                        {
                            timers.insert(ExpiryTimer {
                                expire_at,
                                first_offset: overlap_end + 1,
                                last_offset: batch.last_offset,
                                member_id: owner.clone(),
                            });
                        }
                    }
                }
            }
        }

        self.advance_watermark(&mut batches);
        Ok(dlq_offsets)
    }

    /// Fast O(1) Timer Min-Heap peek to reap expired leases.
    pub fn check_lock_timeouts(&self) -> Vec<u64> {
        let now = Instant::now();
        self.sweep_timers_internal(now)
    }

    fn sweep_timers_inline(&self, now: Instant) {
        // Quick lock-free or read-lock check first
        let should_sweep = {
            let timers = self.timers.read();
            timers.first().map(|t| t.expire_at <= now).unwrap_or(false)
        };
        if should_sweep {
            self.sweep_timers_internal(now);
        }
    }

    fn sweep_timers_internal(&self, now: Instant) -> Vec<u64> {
        let mut timers = self.timers.write();
        let mut batches = self.batches.write();
        let mut dlq_offsets = Vec::new();

        while let Some(timer) = timers.first() {
            if timer.expire_at > now {
                break; // Earliest timer has not expired — O(1) early return!
            }

            let expired = timers.pop_first().unwrap();
            let target_first = expired.first_offset;
            let target_last = expired.last_offset;

            let matching_keys: Vec<u64> = batches
                .range(..=target_last)
                .filter(|(_, b)| b.last_offset >= target_first)
                .map(|(&k, _)| k)
                .collect();

            for key in matching_keys {
                if let Some(mut batch) = batches.remove(&key) {
                    if batch.state == ShareRecordState::Acquired
                        && batch.acquired_by.as_deref() == Some(&expired.member_id)
                    {
                        if let Some(acquired_at) = batch.acquired_at {
                            if now.duration_since(acquired_at) >= batch.lock_timeout {
                                if batch.delivery_count >= self.max_delivery_attempts {
                                    batch.state = ShareRecordState::Archived;
                                    batch.acquired_by = None;
                                    batch.acquired_at = None;
                                    for off in batch.first_offset..=batch.last_offset {
                                        dlq_offsets.push(off);
                                    }
                                } else {
                                    batch.state = ShareRecordState::Available;
                                    batch.acquired_by = None;
                                    batch.acquired_at = None;
                                }
                            }
                        }
                    }
                    batches.insert(key, batch);
                }
            }
        }

        self.advance_watermark(&mut batches);
        dlq_offsets
    }

    /// Advances `start_offset` watermark in O(1) per batch.
    fn advance_watermark(&self, batches: &mut BTreeMap<u64, InFlightBatch>) {
        let mut current_start = self.start_offset.load(Ordering::SeqCst);
        while let Some((&first, batch)) = batches.first_key_value() {
            if first == current_start
                && (batch.state == ShareRecordState::Acknowledged
                    || batch.state == ShareRecordState::Archived)
            {
                current_start = batch.last_offset + 1;
                batches.pop_first();
            } else {
                break;
            }
        }
        self.start_offset.store(current_start, Ordering::SeqCst);
    }

    pub fn inflight_count(&self) -> usize {
        let batches = self.batches.read();
        batches
            .values()
            .map(|b| (b.last_offset - b.first_offset + 1) as usize)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::protocol::wire::AckBatch;
    use std::path::PathBuf;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "hermes_share_partition_test_{}_{}_{}",
                label,
                std::process::id(),
                unique
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Produces `count` records into a fresh `PartitionManager` and returns it alongside
    /// the owning `TempDir` (which must stay alive for the partition's files to persist).
    fn partition_with_records(label: &str, count: u64) -> (TempDir, PartitionManager) {
        let dir = TempDir::new(label);
        let pm =
            PartitionManager::open(&dir.0, "share-test-topic", 0, EngineConfig::default()).unwrap();
        for i in 0..count {
            pm.produce(format!("record-{}", i).as_bytes()).unwrap();
        }
        (dir, pm)
    }

    #[test]
    fn full_range_accept_cancels_pending_timer() {
        let (_dir, pm) = partition_with_records("full_accept", 3);
        let sp = SharePartition::new(
            "share-test-topic".to_string(),
            0,
            "group-a".to_string(),
            Duration::from_secs(30),
            5,
            0,
        );

        let acquired = sp.acquire_records("member-1", 3, None, &pm).unwrap();
        assert_eq!(acquired.len(), 3);
        assert_eq!(
            sp.timers.read().len(),
            1,
            "one timer should be registered for the whole acquired batch"
        );

        let dlq = sp
            .acknowledge(
                "member-1",
                &[AckBatch {
                    first_offset: 0,
                    last_offset: 2,
                    ack_type: AcknowledgeType::Accept,
                }],
            )
            .unwrap();
        assert!(dlq.is_empty());
        assert_eq!(
            sp.timers.read().len(),
            0,
            "acknowledging the entire acquired range must cancel its pending timer, not \
             leave it to linger until it separately expires"
        );
        assert_eq!(sp.start_offset.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn partial_range_ack_still_expires_remaining_acquired_slices() {
        let (_dir, pm) = partition_with_records("partial_ack", 5);
        let sp = SharePartition::new(
            "share-test-topic".to_string(),
            0,
            "group-b".to_string(),
            Duration::from_millis(20),
            5,
            0,
        );

        let acquired = sp.acquire_records("member-1", 5, None, &pm).unwrap();
        assert_eq!(acquired.len(), 5);

        // Acknowledge only the middle offset — the timer for the original full [0,4]
        // range gets cancelled and re-split into two fresh timers for the surviving
        // Acquired left ([0,1]) and right ([3,4]) remainders.
        sp.acknowledge(
            "member-1",
            &[AckBatch {
                first_offset: 2,
                last_offset: 2,
                ack_type: AcknowledgeType::Accept,
            }],
        )
        .unwrap();
        assert_eq!(
            sp.timers.read().len(),
            2,
            "splitting an acquired batch must preserve timer coverage for both remainders"
        );

        std::thread::sleep(Duration::from_millis(50));
        let dlq = sp.check_lock_timeouts();
        // Neither remainder has hit max_delivery_attempts, so both should be released
        // back to Available (re-acquirable), not archived to the DLQ.
        assert!(dlq.is_empty());
        assert_eq!(sp.timers.read().len(), 0);

        // Both remainders must still be independently acquirable after release.
        let reacquired = sp.acquire_records("member-2", 10, None, &pm).unwrap();
        let mut offsets: Vec<u64> = reacquired.iter().map(|r| r.offset).collect();
        offsets.sort_unstable();
        assert_eq!(offsets, vec![0, 1, 3, 4]);
    }

    #[test]
    fn check_lock_timeouts_archives_after_max_delivery_attempts() {
        let (_dir, pm) = partition_with_records("max_delivery", 1);
        let sp = SharePartition::new(
            "share-test-topic".to_string(),
            0,
            "group-c".to_string(),
            Duration::from_millis(10),
            1, // max_delivery_attempts
            0,
        );

        sp.acquire_records("member-1", 1, None, &pm).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        let dlq = sp.check_lock_timeouts();
        assert_eq!(
            dlq,
            vec![0],
            "delivery limit exceeded — should route to DLQ"
        );
        assert_eq!(sp.timers.read().len(), 0);
    }
}
