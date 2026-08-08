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
    /// Partitions touched by this transaction: (topic, partition_id, start_offset, end_offset)
    pub partitions: Vec<(String, u32, u64, u64)>,
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

    /// Begins a new transaction (RACE-04 atomic entry).
    pub fn begin_transaction(&self, transaction_id: &str, producer_id: u64) -> Result<(), String> {
        use dashmap::mapref::entry::Entry;
        match self.transactions.entry(transaction_id.to_string()) {
            Entry::Occupied(_) => Err(format!("Transaction ID '{}' already exists", transaction_id)),
            Entry::Vacant(e) => {
                e.insert(TransactionState {
                    transaction_id: transaction_id.to_string(),
                    producer_id,
                    status: TxStatus::Ongoing,
                    partitions: Vec::new(),
                });
                Ok(())
            }
        }
    }

    /// Records that a produce touched (topic, partition) with the given start_offset for this tx.
    pub fn register_partition(&self, transaction_id: &str, topic: &str, partition: u32, start_offset: u64) {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            let already = state.partitions.iter().any(|(t, p, _, _)| t == topic && *p == partition);
            if !already {
                state.partitions.push((topic.to_string(), partition, start_offset, u64::MAX));
            }
        }
    }

    /// Commits an active transaction.
    /// Writes a Commit control marker (0xAD / CTRL_COMMIT) to all involved partitions (ERR-02 check).
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
            for (topic, partition, _, _) in &partitions {
                if let Some(pm) = get_partition(topic, *partition) {
                    pm.produce_control_marker(CTRL_COMMIT, producer_id, &tx_id)
                        .map_err(|e| format!("Failed to write commit marker to {}-{}: {}", topic, partition, e))?;
                    tracing::info!("EOS: Commit marker written to '{}' partition {}", topic, partition);
                }
            }
            Ok(())
        } else {
            Err(format!("Transaction '{}' not found", transaction_id))
        }
    }

    /// Aborts an active transaction.
    /// Writes an Abort control marker (0xAD / CTRL_ABORT) and updates end_offset (BUG-03, ERR-02).
    pub fn abort_transaction(
        &self,
        transaction_id: &str,
        get_partition: impl Fn(&str, u32) -> Option<Arc<PartitionManager>>,
    ) -> Result<(), String> {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            state.status = TxStatus::Aborted;
            let producer_id = state.producer_id;
            let mut partitions = state.partitions.clone();
            let tx_id = state.transaction_id.clone();

            // Write abort control markers to all involved partition logs and record end_offset
            for (topic, partition, _, end_offset) in &mut partitions {
                if let Some(pm) = get_partition(topic, *partition) {
                    let marker_frame = pm.produce_control_marker(CTRL_ABORT, producer_id, &tx_id)
                        .map_err(|e| format!("Failed to write abort marker to {}-{}: {}", topic, partition, e))?;
                    *end_offset = marker_frame.offset;
                    tracing::info!("EOS: Abort marker written to '{}' partition {}", topic, partition);
                }
            }
            state.partitions = partitions;
            Ok(())
        } else {
            Err(format!("Transaction '{}' not found", transaction_id))
        }
    }

    /// Returns the Last Stable Offset (LSO) for a given (topic, partition).
    pub fn last_stable_offset(&self, topic: &str, partition: u32) -> u64 {
        let mut lso = u64::MAX;
        for entry in self.transactions.iter() {
            let state = entry.value();
            if state.status == TxStatus::Ongoing {
                for (t, p, start_offset, _) in &state.partitions {
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

    /// Collects all transaction IDs that have been Aborted, with their exact per-partition offset ranges (BUG-03).
    pub fn aborted_ranges(&self, topic: &str, partition: u32) -> Vec<(u64, u64)> {
        let mut ranges = Vec::new();
        for entry in self.transactions.iter() {
            let state = entry.value();
            if state.status == TxStatus::Aborted {
                for (t, p, start_offset, end_offset) in &state.partitions {
                    if t == topic && *p == partition {
                        ranges.push((*start_offset, *end_offset));
                    }
                }
            }
        }
        ranges
    }

    /// Returns the producer_id for a transaction (CORR-03).
    pub fn get_producer_id(&self, transaction_id: &str) -> u64 {
        self.transactions.get(transaction_id).map(|s| s.producer_id).unwrap_or(0)
    }

    /// Returns the partition list for a transaction (BUG-12).
    pub fn get_partitions(&self, transaction_id: &str) -> Vec<(String, u32, u64, u64)> {
        self.transactions.get(transaction_id).map(|s| s.partitions.clone()).unwrap_or_default()
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

    /// Restores a transaction with its full partition list during startup recovery (BUG-12).
    pub fn restore_transaction(&self, transaction_id: &str, producer_id: u64, status: TxStatus, partitions: Vec<(String, u32, u64, u64)>) {
        self.transactions.insert(
            transaction_id.to_string(),
            TransactionState {
                transaction_id: transaction_id.to_string(),
                producer_id,
                status,
                partitions,
            },
        );
    }
}

/// Binary encoding for __transaction_state log records with partition list (BUG-12).
/// Format: `[status: 1b] [producer_id: 8b] [tx_id: pascal] [part_count: 4b] { [topic: pascal] [partition: 4b] [start_offset: 8b] [end_offset: 8b] }...`
pub fn encode_tx_state_record(status: TxStatus, producer_id: u64, transaction_id: &str, partitions: &[(String, u32, u64, u64)]) -> Vec<u8> {
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
    buf.put_u32(partitions.len() as u32);
    for (t, p, start, end) in partitions {
        crate::protocol::wire::write_pascal_string(&mut buf, t);
        buf.put_u32(*p);
        buf.put_u64(*start);
        buf.put_u64(*end);
    }
    buf
}

/// Decode a __transaction_state log record with partition list (BUG-12).
pub fn decode_tx_state_record(src: &[u8]) -> Option<(TxStatus, u64, String, Vec<(String, u32, u64, u64)>)> {
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
    cursor = &cursor[tx_len..];

    let mut partitions = Vec::new();
    if cursor.len() >= 4 {
        let count = cursor.get_u32() as usize;
        for _ in 0..count {
            if cursor.len() < 2 {
                break;
            }
            let t_len = cursor.get_u16() as usize;
            if cursor.len() < t_len + 4 + 8 + 8 {
                break;
            }
            let topic = String::from_utf8_lossy(&cursor[..t_len]).to_string();
            cursor = &cursor[t_len..];
            let partition = cursor.get_u32();
            let start = cursor.get_u64();
            let end = cursor.get_u64();
            partitions.push((topic, partition, start, end));
        }
    }

    let status = match status_byte {
        0x01 => TxStatus::Ongoing,
        0x02 => TxStatus::Committed,
        0x03 => TxStatus::Aborted,
        _ => return None,
    };
    Some((status, producer_id, tx_id, partitions))
}

