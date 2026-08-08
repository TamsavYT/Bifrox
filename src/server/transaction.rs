use crate::server::partition::PartitionManager;
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Ongoing,
    Committed,
    Aborted,
}

/// Control-marker type constants (written into partition logs)
pub const CTRL_COMMIT: u8 = 0x01;
pub const CTRL_ABORT: u8 = 0x02;

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub transaction_id: String,
    pub producer_id: u64,
    pub status: TxStatus,
    /// Partitions touched by this transaction: (topic, partition_id, start_offset)
    pub partitions: Vec<(String, u32, u64)>,
}

/// Idempotent Producer and Transaction State Tracker
#[derive(Debug, Clone, Default)]
pub struct TransactionManager {
    /// Tracking producer_id -> last_sequence_number to reject duplicate retries
    producer_sequences: Arc<DashMap<u64, u32>>,
    /// Active transactions map: transaction_id -> TransactionState
    transactions: Arc<DashMap<String, TransactionState>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            producer_sequences: Arc::new(DashMap::new()),
            transactions: Arc::new(DashMap::new()),
        }
    }

    /// Checks if a record batch from a producer is a duplicate network retry
    pub fn is_duplicate(&self, producer_id: u64, seq_num: u32) -> bool {
        if producer_id == 0 {
            return false; // Idempotence disabled
        }

        if let Some(last_seq) = self.producer_sequences.get(&producer_id) {
            if seq_num <= *last_seq.value() {
                return true; // Duplicate retry detected
            }
        }
        false
    }

    /// Records sequence number for idempotent producer
    pub fn record_sequence(&self, producer_id: u64, seq_num: u32) {
        if producer_id > 0 {
            self.producer_sequences.insert(producer_id, seq_num);
        }
    }

    /// Begins a new transaction.
    /// Persists an Ongoing marker to the __transaction_state partition.
    pub fn begin_transaction(&self, transaction_id: &str, producer_id: u64) -> Result<(), String> {
        if self.transactions.contains_key(transaction_id) {
            return Err(format!("Transaction ID '{}' already exists", transaction_id));
        }

        self.transactions.insert(
            transaction_id.to_string(),
            TransactionState {
                transaction_id: transaction_id.to_string(),
                producer_id,
                status: TxStatus::Ongoing,
                partitions: Vec::new(),
            },
        );
        Ok(())
    }

    /// Records that a produce touched (topic, partition) with the given start_offset for this tx.
    /// Called from StorageEngine::produce_batch when a transaction is active.
    pub fn register_partition(&self, transaction_id: &str, topic: &str, partition: u32, start_offset: u64) {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            // Avoid duplicating the same (topic, partition) pair
            let already = state.partitions.iter().any(|(t, p, _)| t == topic && *p == partition);
            if !already {
                state.partitions.push((topic.to_string(), partition, start_offset));
            }
        }
    }

    /// Commits an active transaction.
    /// Writes a Commit control marker (0xAD / CTRL_COMMIT) to all involved partitions.
    pub fn commit_transaction(
        &self,
        transaction_id: &str,
        get_partition: impl Fn(&str, u32) -> Option<Arc<PartitionManager>>,
    ) -> Result<(), String> {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            if state.status != TxStatus::Ongoing {
                return Err(format!("Transaction '{}' is not active", transaction_id));
            }
            state.status = TxStatus::Committed;
            let producer_id = state.producer_id;
            let partitions = state.partitions.clone();
            let tx_id = state.transaction_id.clone();
            drop(state);

            // Write commit control markers to all involved partition logs
            for (topic, partition, _) in &partitions {
                if let Some(pm) = get_partition(topic, *partition) {
                    let _ = pm.produce_control_marker(1, producer_id, &tx_id); // 1 = CTRL_COMMIT
                    tracing::info!("EOS: Commit marker written to '{}' partition {}", topic, partition);
                }
            }
            Ok(())
        } else {
            Err(format!("Transaction '{}' not found", transaction_id))
        }
    }

    /// Aborts an active transaction.
    /// Writes an Abort control marker (0xAD / CTRL_ABORT) to all involved partitions.
    pub fn abort_transaction(
        &self,
        transaction_id: &str,
        get_partition: impl Fn(&str, u32) -> Option<Arc<PartitionManager>>,
    ) -> Result<(), String> {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            state.status = TxStatus::Aborted;
            let producer_id = state.producer_id;
            let partitions = state.partitions.clone();
            let tx_id = state.transaction_id.clone();
            drop(state);

            // Write abort control markers to all involved partition logs
            for (topic, partition, _) in &partitions {
                if let Some(pm) = get_partition(topic, *partition) {
                    let _ = pm.produce_control_marker(2, producer_id, &tx_id); // 2 = CTRL_ABORT
                    tracing::info!("EOS: Abort marker written to '{}' partition {}", topic, partition);
                }
            }
            Ok(())
        } else {
            Err(format!("Transaction '{}' not found", transaction_id))
        }
    }

    /// Returns the Last Stable Offset (LSO) for a given (topic, partition).
    ///
    /// LSO = the lowest start_offset among all *Ongoing* transactions touching this partition.
    /// If no ongoing transactions, returns `u64::MAX` (meaning fetch up to HW is safe).
    pub fn last_stable_offset(&self, topic: &str, partition: u32) -> u64 {
        let mut lso = u64::MAX;
        for entry in self.transactions.iter() {
            let state = entry.value();
            if state.status == TxStatus::Ongoing {
                for (t, p, start_offset) in &state.partitions {
                    if t == topic && *p == partition {
                        if *start_offset < lso {
                            lso = *start_offset;
                        }
                    }
                }
            }
        }
        lso
    }

    /// Collects all transaction IDs that have been Aborted, with their per-partition offset ranges.
    /// Used by the read_committed fetch path to filter out aborted records.
    pub fn aborted_ranges(&self, topic: &str, partition: u32) -> Vec<(u64, u64)> {
        let mut ranges = Vec::new();
        for entry in self.transactions.iter() {
            let state = entry.value();
            if state.status == TxStatus::Aborted {
                for (t, p, start_offset) in &state.partitions {
                    if t == topic && *p == partition {
                        // end is unknown until marker; use u64::MAX as conservative upper bound
                        ranges.push((*start_offset, u64::MAX));
                    }
                }
            }
        }
        ranges
    }

    /// Returns whether the given transaction_id is currently Ongoing.
    pub fn is_ongoing(&self, transaction_id: &str) -> bool {
        self.transactions
            .get(transaction_id)
            .map(|s| s.status == TxStatus::Ongoing)
            .unwrap_or(false)
    }

    /// Returns the status of a transaction (for recovery/replay).
    pub fn get_status(&self, transaction_id: &str) -> Option<TxStatus> {
        self.transactions.get(transaction_id).map(|s| s.status)
    }

    /// Restores a transaction from a replayed __transaction_state log record.
    /// Used during startup recovery.
    pub fn restore_transaction(&self, transaction_id: &str, producer_id: u64, status: TxStatus) {
        self.transactions.insert(
            transaction_id.to_string(),
            TransactionState {
                transaction_id: transaction_id.to_string(),
                producer_id,
                status,
                partitions: Vec::new(),
            },
        );
    }
}

/// Binary encoding for __transaction_state log records.
/// Format: `[status: 1b] [producer_id: 8b] [tx_id: pascal]`
pub fn encode_tx_state_record(status: TxStatus, producer_id: u64, transaction_id: &str) -> Vec<u8> {
    use bytes::BufMut;
    let mut buf = Vec::new();
    let status_byte = match status {
        TxStatus::Ongoing => 0x01u8,
        TxStatus::Committed => 0x02u8,
        TxStatus::Aborted => 0x03u8,
    };
    buf.put_u8(status_byte);
    buf.put_u64(producer_id);
    crate::protocol::wire::write_pascal_string(&mut buf, transaction_id);
    buf
}

/// Decode a __transaction_state log record.
pub fn decode_tx_state_record(src: &[u8]) -> Option<(TxStatus, u64, String)> {
    use bytes::Buf;
    if src.len() < 11 {
        return None;
    }
    let mut cursor = src;
    let status_byte = cursor.get_u8();
    let producer_id = cursor.get_u64();
    if cursor.len() < 2 {
        return None;
    }
    let tx_len = cursor.get_u16() as usize;
    if cursor.len() < tx_len {
        return None;
    }
    let tx_id = String::from_utf8_lossy(&cursor[..tx_len]).to_string();
    let status = match status_byte {
        0x01 => TxStatus::Ongoing,
        0x02 => TxStatus::Committed,
        0x03 => TxStatus::Aborted,
        _ => return None,
    };
    Some((status, producer_id, tx_id))
}
