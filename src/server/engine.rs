use crate::config::EngineConfig;
use crate::consumer_group::ConsumerGroupManager;
use crate::protocol::frame::CONTROL_MAGIC_BYTE;
use crate::protocol::RecordFrame;
use crate::replication::{ClusterConfig, NodeRole, ReplicationManager};
use crate::server::coordinator::GroupCoordinator;
use crate::server::partition::PartitionManager;
use crate::server::quota::QuotaManager;
use crate::server::transaction::{
    decode_tx_state_record, encode_tx_state_record, TransactionManager, TxStatus,
};
use bytes::Bytes;
use dashmap::{DashMap, DashSet};
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

/// Validates topic names to prevent directory traversal and invalid paths (SEC-03)
pub fn validate_topic_name(topic: &str) -> IoResult<()> {
    if topic.is_empty()
        || topic.len() > 249
        || topic == "."
        || topic == ".."
        || topic.contains('/')
        || topic.contains('\\')
        || topic.contains("..")
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid topic name: '{}'", topic),
        ));
    }
    if !topic
        .chars()
        .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Topic name contains illegal characters: '{}'", topic),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ProduceBatchParams<'a> {
    pub topic: &'a str,
    pub key: &'a str,
    pub transaction_id: Option<&'a str>,
    pub num_partitions: u32,
    pub producer_id: u64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: &'a [Bytes],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    pub partition: u32,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub replicas: Vec<u32>,
    pub isr: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    pub topic: String,
    pub num_partitions: u32,
    pub replication_factor: u16,
    pub partitions: std::collections::HashMap<u32, PartitionAssignment>,
}

pub type TopicRegistry = DashMap<String, TopicConfig>;

/// StorageEngine maintaining multi-topic concurrent partition routing, consumer group offsets, and transactions
#[derive(Debug, Clone)]
pub struct StorageEngine {
    config: EngineConfig,
    partitions: Arc<DashMap<(String, u32), Arc<PartitionManager>>>,
    deleting_topics: Arc<DashSet<String>>,
    topic_registry: Arc<TopicRegistry>,
    consumer_groups: ConsumerGroupManager,
    transactions: TransactionManager,
    replication: ReplicationManager,
    group_coordinator: Arc<GroupCoordinator>,
    broker_addrs: Arc<DashMap<u32, String>>,
    quota: Arc<QuotaManager>,
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
        // Shared between StorageEngine and ReplicationManager so that broker addresses
        // learned via the heartbeat round-trip (see send_leader_heartbeat) are visible
        // to both partition-leader routing (StorageEngine) and heartbeat broadcasting
        // (ReplicationManager) without a second, out-of-sync copy of the registry.
        let broker_addrs: Arc<DashMap<u32, String>> = Arc::new(DashMap::new());
        let replication = ReplicationManager::new(
            cluster_config,
            config.bind_addr.clone(),
            broker_addrs.clone(),
        );
        let quota = Arc::new(QuotaManager::new(
            config.produce_quota_bytes_per_sec,
            config.fetch_quota_bytes_per_sec,
        ));

        let engine = Self {
            config,
            partitions: Arc::new(DashMap::new()),
            deleting_topics: Arc::new(DashSet::new()),
            topic_registry: Arc::new(DashMap::new()),
            consumer_groups,
            transactions,
            replication,
            group_coordinator: Arc::new(GroupCoordinator::new()),
            broker_addrs,
            quota,
        };

        engine
            .broker_addrs
            .insert(engine.config.node_id, engine.config.bind_addr.clone());

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

        // 2. Replay metadata log to recover dynamic partitions (ERR-01)
        engine.replay_metadata_log()?;

        // 3. P2: Replay __transaction_state log to restore in-flight transactions (ERR-01)
        engine.replay_transaction_state()?;

        // Unconditionally initialize system partition __consumer_offsets-0
        let _ = engine.get_or_create_partition("__consumer_offsets", 0);

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

        engine
            .replication
            .start_per_partition_fetcher_manager(engine.clone());

        Ok(engine)
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn consumer_groups(&self) -> &ConsumerGroupManager {
        &self.consumer_groups
    }

    pub fn group_coordinator(&self) -> Arc<GroupCoordinator> {
        self.group_coordinator.clone()
    }

    pub fn transactions(&self) -> &TransactionManager {
        &self.transactions
    }

    pub fn replication(&self) -> &ReplicationManager {
        &self.replication
    }

    /// Accounts `bytes` of produced data for `client_key` (typically the connecting
    /// client's source IP) and delays as needed to enforce `produce_quota_bytes_per_sec`.
    /// No-op when no produce quota is configured.
    pub async fn throttle_produce(&self, client_key: &str, bytes: u64) {
        self.quota.throttle_produce(client_key, bytes).await;
    }

    /// Accounts `bytes` of fetched data for `client_key` and delays as needed to
    /// enforce `fetch_quota_bytes_per_sec`. No-op when no fetch quota is configured.
    pub async fn throttle_fetch(&self, client_key: &str, bytes: u64) {
        self.quota.throttle_fetch(client_key, bytes).await;
    }

    /// Returns true if this node is the cluster Leader (handles produces + replicates)
    pub fn is_leader(&self) -> bool {
        self.replication.is_leader()
    }

    /// Returns the current leader's bind address for produce forwarding.
    pub fn leader_addr(&self) -> Option<String> {
        self.replication.get_leader_addr()
    }

    /// Called by heartbeat handler to store the leader's bind address on follower nodes.
    pub fn set_leader_addr(&self, addr: String) {
        self.replication.set_leader_addr(addr);
    }

    /// Retrieve existing partition or dynamically initialize directory `data/{topic}-{partition}` on demand (RACE-02, SEC-03)
    pub fn get_or_create_partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> IoResult<Arc<PartitionManager>> {
        validate_topic_name(topic)?;

        if self.deleting_topics.contains(topic) {
            return Err(std::io::Error::other(format!(
                "Topic {} is currently being deleted",
                topic
            )));
        }

        let key = (topic.to_string(), partition);
        use dashmap::mapref::entry::Entry;

        match self.partitions.entry(key) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
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

                if let Some(cfg) = self.topic_registry.get(topic) {
                    if let Some(assign) = cfg.partitions.get(&partition) {
                        pm.update_leadership(
                            assign.leader_id,
                            assign.leader_epoch,
                            assign.replicas.clone(),
                            assign.isr.clone(),
                        );
                    }
                }

                e.insert(pm.clone());
                Ok(pm)
            }
        }
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
                    if let Ok(rec) = crate::replication::MetadataRecord::decode(&frame.payload) {
                        match rec {
                            crate::replication::MetadataRecord::TopicPartition {
                                topic,
                                partition,
                                ..
                            } => {
                                let _ = self.get_or_create_partition(&topic, partition);
                            }
                            crate::replication::MetadataRecord::TopicCreated {
                                topic,
                                num_partitions,
                                replication_factor,
                            } => {
                                self.topic_registry.insert(
                                    topic.clone(),
                                    TopicConfig {
                                        topic,
                                        num_partitions,
                                        replication_factor,
                                        partitions: std::collections::HashMap::new(),
                                    },
                                );
                            }
                            crate::replication::MetadataRecord::PartitionLeadershipChange {
                                topic,
                                partition,
                                leader_id,
                                leader_epoch,
                                isr,
                            } => {
                                let replicas = isr.clone();
                                let assignment = PartitionAssignment {
                                    partition,
                                    leader_id,
                                    leader_epoch,
                                    replicas: replicas.clone(),
                                    isr: isr.clone(),
                                };
                                if let Some(mut cfg) = self.topic_registry.get_mut(&topic) {
                                    cfg.partitions.insert(partition, assignment);
                                }
                                if let Ok(pm) = self.get_or_create_partition(&topic, partition) {
                                    pm.update_leadership(leader_id, leader_epoch, replicas, isr);
                                }
                            }
                            crate::replication::MetadataRecord::BrokerRegister {
                                node_id,
                                bind_addr,
                            } => {
                                self.broker_addrs.insert(node_id, bind_addr);
                            }
                            crate::replication::MetadataRecord::TopicDeleted { topic } => {
                                self.topic_registry.remove(&topic);
                            }
                        }
                    }
                    offset = frame.offset + 1;
                }
            }
        }
        Ok(())
    }

    /// P2: Replays the __transaction_state log to restore in-flight transactions after restart (BUG-12).
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
                    if let Some((status, producer_id, tx_id, partitions)) =
                        decode_tx_state_record(&frame.payload)
                    {
                        // N7: Only restore Ongoing transactions.  Committed/Aborted entries
                        // have no runtime effect — their data is already baked into the
                        // partition logs.  Restoring them would permanently block reuse of
                        // the same transaction ID (begin_transaction returns Err on Occupied)
                        // and cause aborted_ranges to return stale ranges for unrelated producers.
                        if status == crate::server::transaction::TxStatus::Ongoing {
                            self.transactions.restore_transaction(
                                &tx_id,
                                producer_id,
                                status,
                                partitions,
                            );
                            tracing::info!(
                                "TxReplay: Restored in-flight transaction '{}' producer={}",
                                tx_id,
                                producer_id
                            );
                        }
                    }
                    offset = frame.offset + 1;
                }
            }
        }
        Ok(())
    }

    /// Produce a batch of records to a routed partition (PARTIAL-03 async).
    pub async fn produce_batch(&self, params: ProduceBatchParams<'_>) -> IoResult<(u32, u64, u64)> {
        let topic = params.topic;
        let key = params.key;
        let transaction_id = params.transaction_id;
        let num_partitions = params.num_partitions;
        let producer_id = params.producer_id;
        let producer_epoch = params.producer_epoch;
        let base_sequence = params.base_sequence;
        let records = params.records;
        let partition_id = if !key.is_empty() && num_partitions > 0 {
            hash_key(key.as_bytes(), num_partitions as usize)
        } else {
            0
        };

        let pm = self.get_or_create_partition(topic, partition_id)?;
        let mut first_offset = 0u64;
        let mut last_offset = 0u64;
        let mut frames = Vec::with_capacity(records.len());

        let mut current_seq = base_sequence;
        for (idx, record) in records.iter().enumerate() {
            match pm.produce_frame_eos(record, producer_id, producer_epoch, current_seq)? {
                Ok(frame) => {
                    if idx == 0 {
                        first_offset = frame.offset;
                    }
                    last_offset = frame.offset;
                    frames.push(frame);
                }
                Err(dup_last_offset) => {
                    if idx == 0 {
                        last_offset = dup_last_offset;
                        first_offset = last_offset.saturating_sub(records.len() as u64 - 1);
                    }
                }
            }
            if producer_id != 0 {
                current_seq += 1;
            }
        }

        if let Some(tx_id) = transaction_id {
            self.register_tx_partition(tx_id, topic, partition_id, first_offset);
        }

        if self.config.role == NodeRole::Leader && !self.config.peer_addrs.is_empty() {
            let repl = self.replication.clone();
            let topic_str = topic.to_string();
            let topic_for_spawn = topic_str.clone();
            let frames_clone = frames.clone();
            tokio::spawn(async move {
                if let Err(e) = repl
                    .replicate_batch(&topic_for_spawn, partition_id, &frames_clone)
                    .await
                {
                    tracing::error!("HA Replication: replicate_batch failed: {}", e);
                }
            });

            // Enforce min_insync_replicas requirement before returning success (REP-05 & PARTIAL-03)
            if self.config.min_insync_replicas > 1 {
                let quorum_ok = self
                    .replication
                    .await_isr_quorum(
                        &topic_str,
                        partition_id,
                        last_offset,
                        std::time::Duration::from_secs(5),
                    )
                    .await;
                if !quorum_ok {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "ISR quorum not reached for min_insync_replicas requirement",
                    ));
                }
            }
        }

        Ok((partition_id, first_offset, last_offset))
    }

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

    /// BUG-02: Fetch records starting from nearest offset for target_timestamp
    pub fn fetch_by_timestamp(
        &self,
        topic: &str,
        partition: u32,
        target_timestamp: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let pm = self.get_or_create_partition(topic, partition)?;
        pm.fetch_by_timestamp(target_timestamp, max_bytes)
    }

    pub fn fetch_committed(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let pm = self.get_or_create_partition(topic, partition)?;
        let lso = self.transactions.last_stable_offset(topic, partition);
        let aborted = self.transactions.aborted_ranges(topic, partition);
        let all_frames = self.fetch(topic, partition, offset, max_bytes)?;

        let committed_frames: Vec<RecordFrame> = all_frames
            .into_iter()
            .filter(|frame| {
                if frame.magic == CONTROL_MAGIC_BYTE {
                    return false;
                }
                if frame.offset >= lso {
                    return false;
                }
                if pm.is_offset_aborted(frame.offset) {
                    return false;
                }
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

    pub fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> IoResult<()> {
        self.consumer_groups
            .commit_offset(group_id, topic, partition, offset)
    }

    pub fn commit_offset_with_metadata(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        metadata: &str,
    ) -> IoResult<()> {
        self.consumer_groups
            .commit_offset_with_metadata(group_id, topic, partition, offset, metadata)
    }

    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: u32) -> Option<u64> {
        self.consumer_groups
            .fetch_offset(group_id, topic, partition)
    }

    pub fn fetch_offset_with_metadata(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Option<crate::consumer_group::OffsetEntry> {
        self.consumer_groups
            .fetch_offset_with_metadata(group_id, topic, partition)
    }

    pub fn begin_transaction(&self, transaction_id: &str, producer_id: u64) -> Result<(), String> {
        let result = self
            .transactions
            .begin_transaction(transaction_id, producer_id);
        if result.is_ok() {
            let parts = self.transactions.get_partitions(transaction_id);
            let record =
                encode_tx_state_record(TxStatus::Ongoing, producer_id, transaction_id, &parts);
            if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
                let _ = tx_pm.produce(&record);
            }
        }
        result
    }

    pub fn add_partitions_to_txn(
        &self,
        transaction_id: &str,
        producer_id: u64,
        producer_epoch: i16,
        topics: &[(String, Vec<u32>)],
    ) -> Result<(), String> {
        let result = self.transactions.add_partitions_to_txn(
            transaction_id,
            producer_id,
            producer_epoch,
            topics,
        );
        if result.is_ok() {
            let parts = self.transactions.get_partitions(transaction_id);
            let record =
                encode_tx_state_record(TxStatus::Ongoing, producer_id, transaction_id, &parts);
            if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
                let _ = tx_pm.produce(&record);
            }
        }
        result
    }

    pub fn commit_transaction(&self, transaction_id: &str) -> Result<(), String> {
        // Step 1: Transition memory state to PrepareCommit
        let (producer_id, partitions) = self.transactions.prepare_commit(transaction_id)?;

        // Step 2: Write PrepareCommit record to __transaction_state
        let prep_record = encode_tx_state_record(
            TxStatus::PrepareCommit,
            producer_id,
            transaction_id,
            &partitions,
        );
        if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
            let _ = tx_pm.produce(&prep_record);
        }

        // Step 3: Write CTRL_COMMIT control markers to all involved data partitions
        for (topic, partition, _, _) in &partitions {
            let pm = self
                .get_or_create_partition(topic, *partition)
                .map_err(|e| {
                    format!(
                        "Failed to get/create partition {}-{}: {}",
                        topic, partition, e
                    )
                })?;
            pm.produce_control_marker(
                crate::server::transaction::CTRL_COMMIT,
                producer_id,
                transaction_id,
            )
            .map_err(|e| {
                format!(
                    "Failed to write commit marker to {}-{}: {}",
                    topic, partition, e
                )
            })?;
            tracing::info!(
                "EOS 2PC: Commit marker written to '{}' partition {}",
                topic,
                partition
            );
        }

        // Step 4: Transition memory state to Committed & write CompleteCommit to __transaction_state
        self.transactions.complete_commit(transaction_id)?;
        let commit_record = encode_tx_state_record(
            TxStatus::Committed,
            producer_id,
            transaction_id,
            &partitions,
        );
        if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
            let _ = tx_pm.produce(&commit_record);
        }

        // Step 5: Clean up memory
        self.transactions
            .cleanup_completed_transaction(transaction_id);
        Ok(())
    }

    pub fn abort_transaction(&self, transaction_id: &str) -> Result<(), String> {
        // Step 1: Transition memory state to PrepareAbort
        let (producer_id, partitions) = self.transactions.prepare_abort(transaction_id)?;

        // Step 2: Write PrepareAbort record to __transaction_state
        let prep_record = encode_tx_state_record(
            TxStatus::PrepareAbort,
            producer_id,
            transaction_id,
            &partitions,
        );
        if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
            let _ = tx_pm.produce(&prep_record);
        }

        // Step 3: Write CTRL_ABORT control markers to all involved data partitions
        let mut end_offsets = Vec::new();
        for (topic, partition, first_offset, _) in &partitions {
            let pm = self
                .get_or_create_partition(topic, *partition)
                .map_err(|e| {
                    format!(
                        "Failed to get/create partition {}-{}: {}",
                        topic, partition, e
                    )
                })?;
            let frame = pm
                .produce_control_marker(
                    crate::server::transaction::CTRL_ABORT,
                    producer_id,
                    transaction_id,
                )
                .map_err(|e| {
                    format!(
                        "Failed to write abort marker to {}-{}: {}",
                        topic, partition, e
                    )
                })?;
            let _ = pm.append_aborted_txn(producer_id, *first_offset, frame.offset);
            end_offsets.push((topic.clone(), *partition, frame.offset));
            tracing::info!(
                "EOS 2PC: Abort marker written to '{}' partition {}",
                topic,
                partition
            );
        }

        // Step 4: Transition memory state to Aborted & write CompleteAbort to __transaction_state
        self.transactions
            .complete_abort(transaction_id, &end_offsets)?;
        let updated_partitions = self.transactions.get_partitions(transaction_id);
        let abort_record = encode_tx_state_record(
            TxStatus::Aborted,
            producer_id,
            transaction_id,
            &updated_partitions,
        );
        if let Ok(tx_pm) = self.get_or_create_partition("__transaction_state", 0) {
            let _ = tx_pm.produce(&abort_record);
        }

        // Step 5: Clean up memory
        self.transactions
            .cleanup_completed_transaction(transaction_id);
        Ok(())
    }

    pub fn register_tx_partition(
        &self,
        transaction_id: &str,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) {
        self.transactions
            .register_partition(transaction_id, topic, partition, start_offset);
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

    /// Creates a topic by writing a TopicCreated record to __cluster_metadata and populating registry
    pub fn create_topic(&self, topic: &str, num_partitions: u32) -> IoResult<()> {
        validate_topic_name(topic)?;
        if self.deleting_topics.contains(topic) {
            return Err(std::io::Error::other(format!(
                "Topic {} is currently being deleted",
                topic
            )));
        }

        let broker_ids = self.available_broker_ids();
        if broker_ids.is_empty() {
            return Err(std::io::Error::other(
                "No brokers available for topic assignment",
            ));
        }
        let replication_factor = std::cmp::min(
            self.config.default_replication_factor.max(1),
            broker_ids.len() as u16,
        );

        let record = crate::replication::MetadataRecord::TopicCreated {
            topic: topic.to_string(),
            num_partitions,
            replication_factor,
        };

        if let Ok(meta_pm) = self.get_or_create_partition("__cluster_metadata", 0) {
            let _ = meta_pm.produce(&record.encode());
        }

        let mut partitions = std::collections::HashMap::new();
        let total_nodes = broker_ids.len();
        let rf_usize = replication_factor as usize;

        for p in 0..num_partitions {
            let leader_idx = (p as usize) % total_nodes;
            let leader_id = broker_ids[leader_idx];

            let mut replicas = Vec::with_capacity(rf_usize);
            for i in 0..rf_usize {
                let idx = (leader_idx + i) % total_nodes;
                replicas.push(broker_ids[idx]);
            }
            let isr = replicas.clone();

            let plc_record = crate::replication::MetadataRecord::PartitionLeadershipChange {
                topic: topic.to_string(),
                partition: p,
                leader_id,
                leader_epoch: 0,
                isr: isr.clone(),
            };

            if let Ok(meta_pm) = self.get_or_create_partition("__cluster_metadata", 0) {
                let _ = meta_pm.produce(&plc_record.encode());
            }

            let assignment = PartitionAssignment {
                partition: p,
                leader_id,
                leader_epoch: 0,
                replicas,
                isr,
            };

            partitions.insert(p, assignment);
        }

        self.topic_registry.insert(
            topic.to_string(),
            TopicConfig {
                topic: topic.to_string(),
                num_partitions,
                replication_factor,
                partitions: partitions.clone(),
            },
        );

        for (p, assignment) in partitions {
            let pm = self.get_or_create_partition(topic, p)?;
            pm.update_leadership(
                assignment.leader_id,
                assignment.leader_epoch,
                assignment.replicas,
                assignment.isr,
            );
        }

        Ok(())
    }

    /// Returns list of all active non-system topics (Sprint 5)
    pub fn list_topics(&self) -> Vec<String> {
        let mut topics = std::collections::HashSet::new();

        for entry in self.topic_registry.iter() {
            let topic = entry.key();
            if !topic.starts_with("__") && !self.deleting_topics.contains(topic) {
                topics.insert(topic.clone());
            }
        }

        for entry in self.partitions.iter() {
            let (topic, _) = entry.key();
            if !topic.starts_with("__") && !self.deleting_topics.contains(topic) {
                topics.insert(topic.clone());
            }
        }

        let mut vec: Vec<_> = topics.into_iter().collect();
        vec.sort();
        vec
    }

    /// Returns true if this broker node is the active leader for the specified partition
    pub fn is_partition_leader(&self, topic: &str, partition: u32) -> bool {
        if let Ok(pm) = self.get_or_create_partition(topic, partition) {
            pm.is_leader(self.config.node_id)
        } else {
            false
        }
    }

    /// Returns the node_id currently registered as leader for the specified partition,
    /// if the partition has been initialized locally.
    pub fn partition_leader_id(&self, topic: &str, partition: u32) -> Option<u32> {
        self.get_or_create_partition(topic, partition)
            .ok()
            .map(|pm| pm.leader_id())
    }

    /// Registers a broker socket address mapping (node_id -> bind_addr)
    pub fn register_broker_address(&self, node_id: u32, bind_addr: String) {
        self.broker_addrs.insert(node_id, bind_addr);
    }

    /// Returns the active bind address for a broker node ID
    pub fn get_broker_address(&self, node_id: u32) -> Option<String> {
        self.broker_addrs.get(&node_id).map(|v| v.clone())
    }

    /// Returns all known broker endpoints sorted by node_id.
    pub fn broker_endpoints(&self) -> Vec<(u32, String)> {
        let mut brokers: Vec<(u32, String)> = self
            .broker_addrs
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        brokers.sort_by_key(|(id, _)| *id);
        brokers
    }

    fn available_broker_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.broker_addrs.iter().map(|entry| *entry.key()).collect();
        if !ids.contains(&self.config.node_id) {
            ids.push(self.config.node_id);
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Returns true if this broker node is a leader or registered replica hosting the specified partition (KIP-392 Follower Fetch)
    pub fn is_partition_replica(&self, topic: &str, partition: u32) -> bool {
        if let Ok(pm) = self.get_or_create_partition(topic, partition) {
            pm.is_leader(self.config.node_id) || pm.replicas().contains(&self.config.node_id)
        } else {
            false
        }
    }

    /// Returns metadata and initialized partition high watermarks for a topic
    pub fn describe_topic(
        &self,
        topic: &str,
    ) -> Option<Vec<crate::protocol::wire::DescribedPartition>> {
        if self.deleting_topics.contains(topic) {
            return None;
        }

        let reg_config = self.topic_registry.get(topic).map(|r| r.value().clone());
        let mut partitions_map = std::collections::HashMap::new();

        for entry in self.partitions.iter() {
            let (t, p) = entry.key();
            if t == topic {
                let hw = entry.value().latest_offset();
                let leader_id = entry.value().leader_id();
                let replicas = entry.value().replicas();
                partitions_map.insert(*p, (hw, leader_id, replicas));
            }
        }

        if reg_config.is_none() && partitions_map.is_empty() {
            return None;
        }

        let num_partitions = if let Some(ref cfg) = reg_config {
            cfg.num_partitions.max(partitions_map.len() as u32)
        } else {
            partitions_map.len() as u32
        };

        let mut partitions_info = Vec::with_capacity(num_partitions as usize);
        for p in 0..num_partitions {
            let (hw, leader_id, replicas) =
                if let Some((hw, leader_id, replicas)) = partitions_map.get(&p) {
                    (*hw, *leader_id, replicas.clone())
                } else if let Some(ref cfg) = reg_config {
                    if let Some(assign) = cfg.partitions.get(&p) {
                        (0, assign.leader_id, assign.replicas.clone())
                    } else {
                        (0, self.config.node_id, vec![self.config.node_id])
                    }
                } else {
                    (0, self.config.node_id, vec![self.config.node_id])
                };
            partitions_info.push(crate::protocol::wire::DescribedPartition {
                partition_id: p,
                high_watermark: hw,
                leader_id,
                replicas,
            });
        }

        Some(partitions_info)
    }

    /// Deletes topic partitions and removes disk directory (NEW-03)
    pub fn delete_topic(&self, topic: &str) -> IoResult<()> {
        self.deleting_topics.insert(topic.to_string());

        let record = crate::replication::MetadataRecord::TopicDeleted {
            topic: topic.to_string(),
        };

        if let Ok(meta_pm) = self.get_or_create_partition("__cluster_metadata", 0) {
            let _ = meta_pm.produce(&record.encode());
        }

        self.topic_registry.remove(topic);

        let mut to_remove = Vec::new();
        for entry in self.partitions.iter() {
            let (t, p) = entry.key();
            if t == topic {
                let _ = entry.value().flush();
                to_remove.push((t.clone(), *p));
            }
        }
        for key in to_remove {
            // Remove from map to drop the Arc<PartitionManager>
            self.partitions.remove(&key);
        }

        let mut err = None;
        if let Ok(entries) = std::fs::read_dir(&self.config.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with(&format!("{}-", topic)) {
                            let suffix = &name[topic.len() + 1..];
                            if suffix.parse::<u32>().is_ok() {
                                if let Err(e) = std::fs::remove_dir_all(&path) {
                                    err = Some(e);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        self.deleting_topics.remove(topic);

        if let Some(e) = err {
            return Err(e);
        }
        Ok(())
    }
}
