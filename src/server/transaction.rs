use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    Committed,
    Aborted,
}

/// Control-marker type constants (written into partition logs)
pub const CTRL_COMMIT: u8 = 0x01;
pub const CTRL_ABORT: u8 = 0x02;

pub type PartitionRangeList = Vec<(String, u32, u64, u64)>;

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub transaction_id: String,
    pub producer_id: u64,
    pub status: TxStatus,
    /// Partitions touched by this transaction: (topic, partition_id, start_offset, end_offset)
    pub partitions: PartitionRangeList,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionalProducerState {
    pub producer_id: u64,
    pub producer_epoch: i16,
}

/// Idempotent Producer and Transaction State Tracker
#[derive(Debug, Clone, Default)]
pub struct TransactionManager {
    /// Tracking producer_id -> last_sequence_number to reject duplicate retries
    producer_sequences: Arc<DashMap<u64, u32>>,
    /// Active transactions map: transaction_id -> TransactionState
    transactions: Arc<DashMap<String, TransactionState>>,
    /// Transactional-ID coordinator state used to fence old producers and make InitProducerId durable.
    transactional_producers: Arc<DashMap<String, TransactionalProducerState>>,
    next_producer_id: Arc<AtomicU64>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            producer_sequences: Arc::new(DashMap::new()),
            transactions: Arc::new(DashMap::new()),
            transactional_producers: Arc::new(DashMap::new()),
            next_producer_id: Arc::new(AtomicU64::new(1)),
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
            if self.producer_sequences.len() >= 100_000
                && !self.producer_sequences.contains_key(&producer_id)
            {
                self.producer_sequences.clear();
            }
            self.producer_sequences.insert(producer_id, seq_num);
        }
    }

    pub fn generate_producer_id(&self) -> u64 {
        self.next_producer_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn init_transactional_producer(
        &self,
        transactional_id: &str,
    ) -> Result<(u64, i16), String> {
        use dashmap::mapref::entry::Entry;
        match self
            .transactional_producers
            .entry(transactional_id.to_string())
        {
            Entry::Occupied(mut entry) => {
                let next_epoch = entry.get().producer_epoch.checked_add(1).ok_or_else(|| {
                    format!(
                        "Producer epoch exhausted for transactional.id '{}'",
                        transactional_id
                    )
                })?;
                entry.get_mut().producer_epoch = next_epoch;
                Ok((entry.get().producer_id, next_epoch))
            }
            Entry::Vacant(entry) => {
                let producer_id = self.generate_producer_id();
                entry.insert(TransactionalProducerState {
                    producer_id,
                    producer_epoch: 0,
                });
                Ok((producer_id, 0))
            }
        }
    }

    /// Returns every known (transactional_id, producer_id, producer_epoch) triple —
    /// used to build a `__cluster_metadata` snapshot that fully captures fencing state,
    /// so trimming the log after a snapshot can never lose an old producer's epoch.
    pub fn all_transactional_producers(&self) -> Vec<(String, u64, i16)> {
        self.transactional_producers
            .iter()
            .map(|entry| {
                let state = entry.value();
                (entry.key().clone(), state.producer_id, state.producer_epoch)
            })
            .collect()
    }

    pub fn restore_transactional_producer(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: i16,
    ) {
        self.transactional_producers.insert(
            transactional_id.to_string(),
            TransactionalProducerState {
                producer_id,
                producer_epoch,
            },
        );
        self.observe_producer_id(producer_id);
    }

    pub fn validate_transactional_producer(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: i16,
    ) -> Result<(), String> {
        let current = self
            .transactional_producers
            .get(transactional_id)
            .ok_or_else(|| format!("TransactionalId '{}' is not initialized", transactional_id))?;
        if current.producer_id != producer_id {
            return Err(format!(
                "Producer ID mismatch for transactional.id '{}'",
                transactional_id
            ));
        }
        if current.producer_epoch != producer_epoch {
            return Err(format!(
                "Producer fenced for transactional.id '{}' (expected epoch {}, got {})",
                transactional_id, current.producer_epoch, producer_epoch
            ));
        }
        Ok(())
    }

    pub fn has_transactional_producer(&self, transactional_id: &str) -> bool {
        self.transactional_producers.contains_key(transactional_id)
    }

    pub fn observe_producer_id(&self, producer_id: u64) {
        let next_candidate = producer_id.saturating_add(1);
        let _ = self
            .next_producer_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current < next_candidate {
                    Some(next_candidate)
                } else {
                    None
                }
            });
    }

    pub fn add_partitions_to_txn(
        &self,
        transaction_id: &str,
        producer_id: u64,
        _producer_epoch: i16, // Ignore epoch for now
        topics: &[(String, Vec<u32>)],
    ) -> Result<(), String> {
        // Implicitly begin transaction if it doesn't exist
        if !self.transactions.contains_key(transaction_id) {
            self.begin_transaction(transaction_id, producer_id)?;
        }

        let mut state = self
            .transactions
            .get_mut(transaction_id)
            .ok_or_else(|| format!("Transaction '{}' not found", transaction_id))?;

        if state.producer_id != producer_id {
            return Err("Producer ID mismatch".to_string());
        }

        for (topic, parts) in topics {
            for part in parts {
                let already = state
                    .partitions
                    .iter()
                    .any(|(t, p, _, _)| t == topic && p == part);
                if !already {
                    state
                        .partitions
                        .push((topic.clone(), *part, u64::MAX, u64::MAX));
                }
            }
        }
        Ok(())
    }

    /// Begins a new transaction (RACE-04 atomic entry).
    pub fn begin_transaction(&self, transaction_id: &str, producer_id: u64) -> Result<(), String> {
        use dashmap::mapref::entry::Entry;
        match self.transactions.entry(transaction_id.to_string()) {
            Entry::Occupied(_) => Err(format!(
                "Transaction ID '{}' already exists",
                transaction_id
            )),
            Entry::Vacant(e) => {
                let created_at_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                e.insert(TransactionState {
                    transaction_id: transaction_id.to_string(),
                    producer_id,
                    status: TxStatus::Ongoing,
                    partitions: Vec::new(),
                    created_at_ms,
                });
                Ok(())
            }
        }
    }

    /// Records that a produce touched (topic, partition) with the given start_offset for this tx.
    pub fn register_partition(
        &self,
        transaction_id: &str,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> Option<(u64, PartitionRangeList)> {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            let mut found = false;
            for (t, p, ref mut so, _) in &mut state.partitions {
                if t == topic && *p == partition {
                    if *so == u64::MAX {
                        *so = start_offset;
                    }
                    found = true;
                    break;
                }
            }
            if !found {
                state
                    .partitions
                    .push((topic.to_string(), partition, start_offset, u64::MAX));
            }
            Some((state.producer_id, state.partitions.clone()))
        } else {
            None
        }
    }

    /// Phase 1 2PC: Transitions transaction to PrepareCommit
    pub fn prepare_commit(
        &self,
        transaction_id: &str,
    ) -> Result<(u64, PartitionRangeList), String> {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            if state.status != TxStatus::Ongoing {
                return Err(format!("Transaction '{}' is not active", transaction_id));
            }
            state.status = TxStatus::PrepareCommit;
            Ok((state.producer_id, state.partitions.clone()))
        } else {
            Err(format!("Transaction '{}' not found", transaction_id))
        }
    }

    /// Phase 1 2PC: Transitions transaction to PrepareAbort
    pub fn prepare_abort(&self, transaction_id: &str) -> Result<(u64, PartitionRangeList), String> {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            if state.status != TxStatus::Ongoing && state.status != TxStatus::PrepareAbort {
                return Err(format!(
                    "Transaction '{}' cannot be aborted",
                    transaction_id
                ));
            }
            state.status = TxStatus::PrepareAbort;
            Ok((state.producer_id, state.partitions.clone()))
        } else {
            Err(format!("Transaction '{}' not found", transaction_id))
        }
    }

    /// Phase 2 2PC: Transitions transaction to Committed
    pub fn complete_commit(&self, transaction_id: &str) -> Result<(), String> {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            state.status = TxStatus::Committed;
            Ok(())
        } else {
            Err(format!("Transaction '{}' not found", transaction_id))
        }
    }

    /// Phase 2 2PC: Transitions transaction to Aborted
    pub fn complete_abort(
        &self,
        transaction_id: &str,
        end_offsets: &[(String, u32, u64)],
    ) -> Result<(), String> {
        if let Some(mut state) = self.transactions.get_mut(transaction_id) {
            state.status = TxStatus::Aborted;
            for (topic, part, end_off) in end_offsets {
                for (t, p, _, ref mut end) in &mut state.partitions {
                    if t == topic && p == part {
                        *end = *end_off;
                    }
                }
            }
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
            if state.status == TxStatus::Ongoing
                || state.status == TxStatus::PrepareCommit
                || state.status == TxStatus::PrepareAbort
            {
                for (t, p, start_offset, _) in &state.partitions {
                    if t == topic && *p == partition && *start_offset < lso {
                        lso = *start_offset;
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
        self.transactions
            .get(transaction_id)
            .map(|s| s.producer_id)
            .unwrap_or(0)
    }

    /// Returns the partition list for a transaction (BUG-12).
    pub fn get_partitions(&self, transaction_id: &str) -> PartitionRangeList {
        self.transactions
            .get(transaction_id)
            .map(|s| s.partitions.clone())
            .unwrap_or_default()
    }

    /// Returns whether the given transaction_id is currently Ongoing.
    pub fn is_ongoing(&self, transaction_id: &str) -> bool {
        self.transactions
            .get(transaction_id)
            .map(|s| {
                s.status == TxStatus::Ongoing
                    || s.status == TxStatus::PrepareCommit
                    || s.status == TxStatus::PrepareAbort
            })
            .unwrap_or(false)
    }

    /// Returns the status of a transaction (for recovery/replay).
    pub fn get_status(&self, transaction_id: &str) -> Option<TxStatus> {
        self.transactions.get(transaction_id).map(|s| s.status)
    }

    /// Removes completed or aborted transactions from memory (MEM-02 & PARTIAL-04)
    pub fn cleanup_completed_transaction(&self, transaction_id: &str) {
        if let Some(state) = self.transactions.get(transaction_id) {
            if state.status == TxStatus::Committed || state.status == TxStatus::Aborted {
                drop(state);
                self.transactions.remove(transaction_id);
            }
        }
    }

    /// Restores a transaction with its full partition list during startup recovery (BUG-12).
    pub fn restore_transaction(
        &self,
        transaction_id: &str,
        producer_id: u64,
        status: TxStatus,
        partitions: PartitionRangeList,
    ) {
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.transactions.insert(
            transaction_id.to_string(),
            TransactionState {
                transaction_id: transaction_id.to_string(),
                producer_id,
                status,
                partitions,
                created_at_ms,
            },
        );
    }

    /// Returns transaction IDs still in a non-terminal state (`Ongoing`, `PrepareCommit`,
    /// or `PrepareAbort`) whose age exceeds `max_age_ms` — both a producer that has simply
    /// gone silent mid-transaction (Kafka `transaction.timeout.ms`) and, as a special case
    /// of the same mechanism, a transaction restored from `__transaction_state` on startup
    /// that no reconnecting producer ever resumed (`created_at_ms` is reset to "now" by
    /// `restore_transaction`, so a restored transaction's clock starts fresh at restart —
    /// giving a producer that reconnects a fair window to continue it before it's presumed
    /// abandoned). The caller is expected to actually abort each returned ID (see
    /// `StorageEngine::sweep_expired_transactions`); this only identifies them, since
    /// aborting requires writing control markers to every partition the transaction
    /// touched, which this manager has no access to do itself.
    pub fn expired_ongoing_transaction_ids(&self, max_age_ms: u64) -> Vec<String> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.transactions
            .iter()
            .filter(|entry| {
                let state = entry.value();
                let is_ongoing = matches!(
                    state.status,
                    TxStatus::Ongoing | TxStatus::PrepareCommit | TxStatus::PrepareAbort
                );
                is_ongoing && now_ms.saturating_sub(state.created_at_ms) > max_age_ms
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Time-bounded retention queue for transaction states based on max_age_ms (MEM-02).
    pub fn prune_stale_transactions(&self, max_age_ms: u64) -> usize {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut to_remove = Vec::new();
        for entry in self.transactions.iter() {
            let state = entry.value();
            let age_ms = now_ms.saturating_sub(state.created_at_ms);

            let is_completed =
                state.status == TxStatus::Committed || state.status == TxStatus::Aborted;
            if age_ms > max_age_ms || (max_age_ms == 0 && is_completed) {
                to_remove.push(entry.key().clone());
            }
        }

        let count = to_remove.len();
        for tx_id in to_remove {
            self.transactions.remove(&tx_id);
        }
        count
    }
}

pub type DecodedTxStateRecord = (TxStatus, u64, String, PartitionRangeList);

/// Binary encoding for __transaction_state log records with partition list (BUG-12).
/// Format: `[status: 1b] [producer_id: 8b] [tx_id: pascal] [part_count: 4b] { [topic: pascal] [partition: 4b] [start_offset: 8b] [end_offset: 8b] }...`
pub fn encode_tx_state_record(
    status: TxStatus,
    producer_id: u64,
    transaction_id: &str,
    partitions: &[(String, u32, u64, u64)],
) -> Vec<u8> {
    use bytes::BufMut;
    let mut buf = Vec::new();
    let status_byte = match status {
        TxStatus::Ongoing => 0x01u8,
        TxStatus::PrepareCommit => 0x02u8,
        TxStatus::PrepareAbort => 0x03u8,
        TxStatus::Committed => 0x04u8,
        TxStatus::Aborted => 0x05u8,
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
pub fn decode_tx_state_record(src: &[u8]) -> Option<DecodedTxStateRecord> {
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
        0x02 => TxStatus::PrepareCommit,
        0x03 => TxStatus::PrepareAbort,
        0x04 => TxStatus::Committed,
        0x05 => TxStatus::Aborted,
        _ => return None,
    };
    Some((status, producer_id, tx_id, partitions))
}
