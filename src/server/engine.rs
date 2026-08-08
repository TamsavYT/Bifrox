use crate::config::EngineConfig;
use crate::consumer_group::ConsumerGroupManager;
use crate::protocol::frame::CONTROL_MAGIC_BYTE;
use crate::protocol::RecordFrame;
use crate::replication::{ClusterConfig, NodeRole, ReplicationManager};
use crate::server::partition::PartitionManager;
use crate::server::transaction::{decode_tx_state_record, encode_tx_state_record, TransactionManager, TxStatus};
use bytes::Bytes;
use dashmap::DashMap;
use std::io::Result as IoResult;
use std::sync::Arc;

/// Hash router using CRC32 to determine target partition ID for a given record key
pub fn hash_key(key: &[u8], num_partitions: usize) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    let hash = crc32fast::hash(key);
    (hash as usize % num_partitions) as u32
}

/// StorageEngine maintaining multi-topic concurrent partition routing, consumer group offsets, and transactions
#[derive(Debug, Clone)]
pub struct StorageEngine {
    config: EngineConfig,
    partitions: Arc<DashMap<(String, u32), Arc<PartitionManager>>>,
    consumer_groups: ConsumerGroupManager,
    transactions: TransactionManager,
    replication: ReplicationManager,
}

impl StorageEngine {
    pub fn new(config: EngineConfig) -> IoResult<Self> {
        let consumer_groups = ConsumerGroupManager::open(&config.data_dir)?;
        let transactions = TransactionManager::new();
        let cluster_config = ClusterConfig {
            cluster_id: config.cluster_id.clone(),
            node_id: config.node_id,
            role: config.role,
            peer_addrs: config.peer_addrs.clone(),
            min_insync_replicas: config.min_insync_replicas,
        };
        let replication = ReplicationManager::new(cluster_config, config.bind_addr.clone());

        let engine = Self {
            config,
            partitions: Arc::new(DashMap::new()),
            consumer_groups,
            transactions,
            replication,
        };

        // 1. Scan data_dir and load existing partitions
        if engine.config.data_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&engine.config.data_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                            if let Some(pos) = dir_name.rfind('-') {
                                let (topic, part_str) = dir_name.split_at(pos);
                                let part_str = &part_str[1..];
                                if let Ok(partition) = part_str.parse::<u32>() {
                                    let _ = engine.get_or_create_partition(topic, partition);
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2. Replay metadata log to recover dynamic partitions
        let _ = engine.replay_metadata_log();

        // 3. P2: Replay __transaction_state log to restore in-flight transactions
        let _ = engine.replay_transaction_state();

        // 4. If Leader, register broker in the metadata log
        if engine.is_leader() {
            let reg_rec = crate::replication::MetadataRecord::BrokerRegister {
                node_id: engine.config.node_id,
                bind_addr: engine.config.bind_addr.clone(),
            };
            if let Ok(meta_pm) = engine.get_or_create_partition("__cluster_metadata", 0) {
                let _ = meta_pm.produce(&reg_rec.encode());
            }
        }

        Ok(engine)
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn consumer_groups(&self) -> &ConsumerGroupManager {
        &self.consumer_groups
    }

    pub fn transactions(&self) -> &TransactionManager {
        &self.transactions
    }

    pub fn replication(&self) -> &ReplicationManager {
        &self.replication
    }

    /// Returns true if this node is the cluster Leader (handles produces + replicates)
    pub fn is_leader(&self) -> bool {
        self.replication.is_leader()
    }

    /// Returns the current leader's bind address for produce forwarding.
    /// Leader returns its own address; Follower returns the address learned from heartbeats.
    pub fn leader_addr(&self) -> Option<String> {
        self.replication.get_leader_addr()
    }

    /// Called by heartbeat handler to store the leader's bind address on follower nodes.
    pub fn set_leader_addr(&self, addr: String) {
        self.replication.set_leader_addr(addr);
    }

    /// Retrieve existing partition or dynamically initialize directory `data/{topic}-{partition}` on demand
    pub fn get_or_create_partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> IoResult<Arc<PartitionManager>> {
        let key = (topic.to_string(), partition);
        if let Some(pm) = self.partitions.get(&key) {
            return Ok(pm.value().clone());
        }

        let partition_dir = self
            .config
            .data_dir
            .join(format!("{}-{}", topic, partition));

        let pm = Arc::new(PartitionManager::open(
            partition_dir,
            topic,
            partition,
            self.config.clone(),
        )?);

        self.partitions.insert(key, pm.clone());

        // If leader and not a system topic, write TopicPartition metadata record to __cluster_metadata-0
        if self.is_leader() && topic != "__cluster_metadata" && !topic.starts_with("__") {
            let meta_record = crate::replication::MetadataRecord::TopicPartition {
                topic: topic.to_string(),
                partition,
                leader_id: self.config.node_id,
                replicas: vec![self.config.node_id],
            };
            let payload = meta_record.encode();
            if let Ok(meta_pm) = self.get_or_create_partition("__cluster_metadata", 0) {
                let _ = meta_pm.produce(&payload);
            }
        }

        Ok(pm)
    }

    /// Replays the local cluster metadata log to initialize partitions registered on this node
    pub fn replay_metadata_log(&self) -> IoResult<()> {
        let meta_dir = self.config.data_dir.join("__cluster_metadata-0");
        if !meta_dir.exists() {
            return Ok(());
        }

        if let Ok(pm) = self.get_or_create_partition("__cluster_metadata", 0) {
            let mut offset = 0u64;
            loop {
                let frames = pm.fetch(offset, 1024 * 1024)?;
                if frames.is_empty() {
                    break;
                }
                for frame in &frames {
                    if let Ok(meta_rec) = crate::replication::MetadataRecord::decode(&frame.payload) {
                        match meta_rec {
                            crate::replication::MetadataRecord::TopicPartition { topic, partition, .. } => {
                                let _ = self.get_or_create_partition(&topic, partition);
                            }
                            _ => {}
                        }
                    }
                    offset = frame.offset + 1;
                }
            }
        }
        Ok(())
    }

    /// P2: Replays the __transaction_state log to restore in-flight transactions after restart.
    pub fn replay_transaction_state(&self) -> IoResult<()> {
        let tx_dir = self.config.data_dir.join("__transaction_state-0");
        if !tx_dir.exists() {
            return Ok(());
        }

        if let Ok(pm) = self.get_or_create_partition("__transaction_state", 0) {
            let mut offset = 0u64;
            loop {
                let frames = pm.fetch(offset, 1024 * 1024)?;
                if frames.is_empty() {
                    break;
                }
                for frame in &frames {
                    if let Some((status, producer_id, tx_id)) = decode_tx_state_record(&frame.payload) {
                        self.transactions.restore_transaction(&tx_id, producer_id, status);
                        tracing::info!(
                            "TxReplay: Restored transaction '{}' producer={} status={:?}",
                            tx_id, producer_id, status
                        );
                    }
                    offset = frame.offset + 1;
                }
            }
        }
        Ok(())
    }

    /// Produce a batch of records to a routed partition.
    /// Only the Leader writes locally and replicates to followers.
    /// Followers should forward produce requests to the leader via handler.rs.
    /// P5: If a transaction is active, registers this partition for control-marker writes on commit/abort.
    pub fn produce_batch(
        &self,
        topic: &str,
        key: &str,
        transaction_id: Option<&str>,
        num_partitions: u32,
        records: &[Bytes],
    ) -> IoResult<(u32, u64, u64)> {
        let partition_id = if !key.is_empty() && num_partitions > 0 {
            hash_key(key.as_bytes(), num_partitions as usize)
        } else {
            0
        };

        let pm = self.get_or_create_partition(topic, partition_id)?;
        let mut first_offset = 0u64;
        let mut last_offset = 0u64;
        let mut frames = Vec::with_capacity(records.len());

        for (idx, record) in records.iter().enumerate() {
            let frame = pm.produce_frame(record)?;
            if idx == 0 {
                first_offset = frame.offset;
            }
            last_offset = frame.offset;
            frames.push(frame);
        }

        // P5: If a transaction_id was provided, register this (topic, partition) with it.
        if let Some(tx_id) = transaction_id {
            self.register_tx_partition(tx_id, topic, partition_id, first_offset);
        }

        // Leader replicates writes to all follower peer nodes
        if self.config.role == NodeRole::Leader && !self.config.peer_addrs.is_empty() {
            let repl = self.replication.clone();
            let topic_str = topic.to_string();
            let frames_clone = frames.clone();
            tracing::info!(
                "HA Replication: Leader scheduling replication of {} frame(s) on Topic '{}' Partition {} to {} peer(s)",
                frames_clone.len(),
                topic_str,
                partition_id,
                self.config.peer_addrs.len()
            );
            tokio::spawn(async move {
                if let Err(e) = repl.replicate_batch(&topic_str, partition_id, &frames_clone).await {
                    tracing::error!("HA Replication: replicate_batch failed: {}", e);
                }
            });
        }

        Ok((partition_id, first_offset, last_offset))
    }

    /// Standard fetch (read_uncommitted): returns all frames up to high watermark.
    pub fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let pm = self.get_or_create_partition(topic, partition)?;
        pm.fetch(offset, max_bytes)
    }

    /// P1: Read-committed fetch: filters out aborted and uncommitted records using LSO.
    ///
    /// - Records beyond the Last Stable Offset (LSO) are hidden (ongoing transactions).
    /// - Records belonging to aborted transactions are skipped.
    /// - Control markers (0xAD) are always hidden from consumers.
    pub fn fetch_committed(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let pm = self.get_or_create_partition(topic, partition)?;
        let all_frames = pm.fetch(offset, max_bytes)?;

        // Compute LSO: hide records at or beyond LSO
        let lso = self.transactions.last_stable_offset(topic, partition);

        // Compute aborted offset ranges
        let aborted = self.transactions.aborted_ranges(topic, partition);

        let committed_frames: Vec<RecordFrame> = all_frames
            .into_iter()
            .filter(|frame| {
                // Hide control markers (0xAD) from consumers
                if frame.magic == CONTROL_MAGIC_BYTE {
                    return false;
                }
                // Hide records beyond LSO (part of an uncommitted transaction)
                if frame.offset >= lso {
                    return false;
                }
                // Hide records in aborted transaction offset ranges
                for (start, end) in &aborted {
                    if frame.offset >= *start && frame.offset <= *end {
                        return false;
                    }
                }
                true
            })
            .collect();

        Ok(committed_frames)
    }

    pub fn seek(&self, topic: &str, partition: u32, offset: u64) -> IoResult<Option<(u64, u64)>> {
        let pm = self.get_or_create_partition(topic, partition)?;
        Ok(pm.seek(offset))
    }

    pub fn latest_offset(&self, topic: &str, partition: u32) -> IoResult<u64> {
        let pm = self.get_or_create_partition(topic, partition)?;
        Ok(pm.latest_offset())
    }

    pub fn commit_offset(&self, group_id: &str, topic: &str, partition: u32, offset: u64) -> IoResult<()> {
        self.consumer_groups.commit_offset(group_id, topic, partition, offset)
    }

    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: u32) -> Option<u64> {
        self.consumer_groups.fetch_offset(group_id, topic, partition)
    }

    pub fn begin_transaction(&self, transaction_id: &str, producer_id: u64) -> Result<(), String> {
        let result = self.transactions.begin_transaction(transaction_id, producer_id);
        if result.is_ok() {
            // P2: Persist Ongoing state to __transaction_state log
            let record = encode_tx_state_record(TxStatus::Ongoing, producer_id, transaction_id);
            if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
                let _ = tx_pm.produce(&record);
            }
        }
        result
    }

    pub fn commit_transaction(&self, transaction_id: &str) -> Result<(), String> {
        let partitions_ref = self.partitions.clone();
        let result = self.transactions.commit_transaction(transaction_id, |topic, partition| {
            partitions_ref.get(&(topic.to_string(), partition)).map(|e| e.value().clone())
        });
        if result.is_ok() {
            // P2: Persist Committed state to __transaction_state log
            let producer_id = 0u64; // producer_id already stored in partition markers
            let record = encode_tx_state_record(TxStatus::Committed, producer_id, transaction_id);
            if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
                let _ = tx_pm.produce(&record);
            }
        }
        result
    }

    pub fn abort_transaction(&self, transaction_id: &str) -> Result<(), String> {
        let partitions_ref = self.partitions.clone();
        let result = self.transactions.abort_transaction(transaction_id, |topic, partition| {
            partitions_ref.get(&(topic.to_string(), partition)).map(|e| e.value().clone())
        });
        if result.is_ok() {
            // P2: Persist Aborted state to __transaction_state log
            let producer_id = 0u64;
            let record = encode_tx_state_record(TxStatus::Aborted, producer_id, transaction_id);
            if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
                let _ = tx_pm.produce(&record);
            }
        }
        result
    }

    /// P5: Register a (topic, partition, start_offset) against an active transaction.
    /// Call this after produce_batch when producing inside a transaction.
    pub fn register_tx_partition(&self, transaction_id: &str, topic: &str, partition: u32, start_offset: u64) {
        self.transactions.register_partition(transaction_id, topic, partition, start_offset);
    }

    pub fn apply_retention_all(&self) -> IoResult<usize> {
        let mut total_removed = 0;
        for entry in self.partitions.iter() {
            total_removed += entry.value().apply_retention()?;
        }
        Ok(total_removed)
    }

    pub fn flush_all(&self) -> IoResult<()> {
        for entry in self.partitions.iter() {
            entry.value().flush()?;
        }
        Ok(())
    }
}
