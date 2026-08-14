use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusState {
    Follower,
    Candidate,
    Leader,
}

/// Caduceus Consensus Controller (Raft-based quorum state machine for Hermes nodes)
#[derive(Debug, Clone)]
pub struct HermesConsensus {
    inner: Arc<Mutex<ControllerInner>>,
}

#[derive(Debug)]
struct ControllerInner {
    node_id: u32,
    cluster_size: usize,
    state: ConsensusState,
    current_term: u64,
    /// (term, candidate_id) this node voted for — the single source of truth for vote
    /// bookkeeping. Previously `ReplicationManager` kept a second, separate `voted_for`
    /// that was never synchronized with this one, so the two could disagree about
    /// whether a vote had already been cast in a given term.
    voted_for: Option<(u64, u32)>,
    last_heartbeat: Instant,
    election_timeout: Duration,
}

impl HermesConsensus {
    pub fn new(node_id: u32, cluster_size: usize) -> Self {
        Self::new_with_state(node_id, cluster_size, ConsensusState::Follower)
    }

    /// Create consensus with a specific initial state.
    /// Used when a node is configured as Leader at startup so it begins accepting
    /// produce requests immediately without waiting for a full election round.
    pub fn new_with_state(
        node_id: u32,
        cluster_size: usize,
        initial_state: ConsensusState,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ControllerInner {
                node_id,
                cluster_size,
                state: initial_state,
                current_term: 0,
                voted_for: if initial_state == ConsensusState::Leader {
                    Some((0, node_id))
                } else {
                    None
                },
                last_heartbeat: Instant::now(),
                election_timeout: Duration::from_millis(1500 + (node_id as u64 * 100)),
            })),
        }
    }

    pub fn state(&self) -> ConsensusState {
        self.inner.lock().state
    }

    pub fn current_term(&self) -> u64 {
        self.inner.lock().current_term
    }

    /// Forces candidate state transition for immediate testing / election trigger
    pub fn force_candidate_state(&self) {
        let mut lock = self.inner.lock();
        lock.state = ConsensusState::Candidate;
        lock.current_term += 1;
        let self_id = lock.node_id;
        let term = lock.current_term;
        lock.voted_for = Some((term, self_id));
    }

    /// Handles incoming heartbeat ping from active Leader node
    pub fn handle_leader_heartbeat(&self, leader_id: u32, term: u64) -> bool {
        let mut lock = self.inner.lock();
        if term >= lock.current_term {
            lock.current_term = term;
            lock.state = ConsensusState::Follower;
            lock.voted_for = Some((term, leader_id));
            lock.last_heartbeat = Instant::now();
            true
        } else {
            false
        }
    }

    /// Checks heartbeat expiry and triggers automated Hermes consensus election if Leader missed heartbeat
    pub fn check_election_timeout(&self) -> bool {
        let mut lock = self.inner.lock();
        if lock.state != ConsensusState::Leader
            && lock.last_heartbeat.elapsed() >= lock.election_timeout
        {
            // Transition to Candidate & increment election term
            lock.state = ConsensusState::Candidate;
            lock.current_term += 1;
            let self_id = lock.node_id;
            let term = lock.current_term;
            lock.voted_for = Some((term, self_id));
            lock.last_heartbeat = Instant::now();
            tracing::info!(
                "Hermes Consensus: Node {} missed leader heartbeat. Starting election for term {}.",
                self_id,
                lock.current_term
            );
            return true;
        }
        false
    }

    /// Evaluates vote tallies to determine if candidate achieved quorum majority
    pub fn tally_election_votes(&self, votes: usize) -> bool {
        let mut lock = self.inner.lock();
        let quorum = (lock.cluster_size / 2) + 1;
        if lock.state == ConsensusState::Candidate && votes >= quorum {
            lock.state = ConsensusState::Leader;
            tracing::info!(
                "Hermes Consensus: Node {} achieved quorum ({}/{} votes). Promoted to Leader for term {}.",
                lock.node_id,
                votes,
                quorum,
                lock.current_term
            );
            true
        } else {
            false
        }
    }

    /// Atomically decides whether to grant a vote to `candidate_id` for `term` and, if so,
    /// records it — replacing the old two-call `can_vote_for` + `record_vote` sequence
    /// (previously split across `ReplicationManager`'s own separate `voted_for`), which
    /// left a check-then-act gap where two concurrent VoteRequests in the same term could
    /// both observe "not yet voted" and both be granted.
    ///
    /// This only covers the term/voted-for bookkeeping half of the Raft vote rule; the
    /// caller is responsible for the separate log-completeness check (§5.4.1) — this
    /// module doesn't own the metadata log, so it can't compare log lengths itself.
    pub fn try_record_vote(&self, candidate_id: u32, term: u64) -> bool {
        let mut lock = self.inner.lock();
        if term < lock.current_term {
            return false;
        }
        if term > lock.current_term {
            // A higher term always resets our vote for that (new) term, and demotes us
            // to Follower — we can't still be the leader/candidate of a term we've fallen
            // behind on.
            lock.current_term = term;
            lock.state = ConsensusState::Follower;
            lock.voted_for = None;
        }
        match lock.voted_for {
            Some((voted_term, voted_candidate)) if voted_term == term => {
                voted_candidate == candidate_id
            }
            _ => {
                lock.voted_for = Some((term, candidate_id));
                true
            }
        }
    }

    /// Steps down from Leader/Candidate to Follower when a higher epoch is observed.
    /// Called when a peer rejects our replication push with a stale-epoch ACK.
    pub fn step_down_to_follower(&self, observed_epoch: u64) {
        let mut lock = self.inner.lock();
        if observed_epoch > lock.current_term {
            tracing::warn!(
                "Hermes Consensus: Node {} stepping down to Follower. Observed epoch {} > current term {}.",
                lock.node_id,
                observed_epoch,
                lock.current_term
            );
            lock.current_term = observed_epoch;
            lock.state = ConsensusState::Follower;
            lock.voted_for = None;
            lock.last_heartbeat = Instant::now(); // reset timer to avoid immediate re-election
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_record_vote_grants_first_vote_in_term() {
        let c = HermesConsensus::new(1, 3);
        assert!(c.try_record_vote(2, 5));
    }

    #[test]
    fn try_record_vote_denies_second_candidate_same_term() {
        let c = HermesConsensus::new(1, 3);
        assert!(c.try_record_vote(2, 5));
        // A different candidate asking for the same term must be denied — this is the
        // core Raft "at most one vote per term" safety property.
        assert!(!c.try_record_vote(3, 5));
    }

    #[test]
    fn try_record_vote_is_idempotent_for_the_same_candidate_and_term() {
        let c = HermesConsensus::new(1, 3);
        assert!(c.try_record_vote(2, 5));
        // A retried VoteRequest from the same candidate in the same term (e.g. after a
        // dropped response) must still be granted, not denied as "already voted".
        assert!(c.try_record_vote(2, 5));
    }

    #[test]
    fn try_record_vote_denies_stale_term() {
        let c = HermesConsensus::new(1, 3);
        assert!(c.try_record_vote(2, 5));
        // A candidate campaigning for an old term (this node has since moved on) must
        // never win a vote — the term monotonicity invariant Raft safety depends on.
        assert!(!c.try_record_vote(3, 4));
    }

    #[test]
    fn try_record_vote_higher_term_resets_prior_vote() {
        let c = HermesConsensus::new(1, 3);
        assert!(c.try_record_vote(2, 5));
        // A candidate campaigning for a strictly higher term is a fresh election this
        // node hasn't voted in yet, even though it already voted for someone in term 5.
        assert!(c.try_record_vote(3, 6));
        // And now a second candidate for that same new term 6 must be denied.
        assert!(!c.try_record_vote(4, 6));
    }
}
