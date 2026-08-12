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
    voted_for: Option<u32>,
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
                    Some(node_id)
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
        lock.voted_for = Some(self_id);
    }

    /// Handles incoming heartbeat ping from active Leader node
    pub fn handle_leader_heartbeat(&self, leader_id: u32, term: u64) -> bool {
        let mut lock = self.inner.lock();
        if term >= lock.current_term {
            lock.current_term = term;
            lock.state = ConsensusState::Follower;
            lock.voted_for = Some(leader_id);
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
            lock.voted_for = Some(self_id);
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
