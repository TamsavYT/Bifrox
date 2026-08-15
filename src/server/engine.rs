use crate::config::EngineConfig;
use crate::consumer_group::ConsumerGroupManager;
use crate::protocol::frame::CONTROL_MAGIC_BYTE;
use crate::protocol::RecordFrame;
use crate::replication::{ClusterConfig, ReplicationManager};
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
    pub cleanup_policy: crate::config::CleanupPolicy,
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
    share_groups: crate::server::share::ShareGroupManager,
    replication: ReplicationManager,
    group_coordinator: Arc<GroupCoordinator>,
    broker_addrs: Arc<DashMap<u32, String>>,
    quota: Arc<QuotaManager>,
    acl: Arc<crate::server::acl::AclManager>,
    scram_credentials: Arc<DashMap<String, crate::scram::ScramCredential>>,
    metrics: Arc<crate::server::metrics::MetricsCollector>,
}

impl StorageEngine {
    pub fn new(config: EngineConfig) -> IoResult<Self> {
        let consumer_groups = ConsumerGroupManager::open(&config.data_dir)?;
        let share_groups = crate::server::share::ShareGroupManager::open(&config.data_dir)?;
        let transactions = TransactionManager::new();
        let cluster_config = ClusterConfig {
            cluster_id: config.cluster_id.clone(),
            node_id: config.node_id,
            role: config.role,
            peer_addrs: config.peer_addrs.clone(),
            min_insync_replicas: config.min_insync_replicas,
        };

        let broker_addrs = Arc::new(DashMap::new());
        let replication = ReplicationManager::new(
            cluster_config,
            config.bind_addr.clone(),
            broker_addrs.clone(),
        );
        let group_coordinator = Arc::new(GroupCoordinator::new());
        let quota = Arc::new(QuotaManager::new(
            config.produce_quota_bytes_per_sec,
            config.fetch_quota_bytes_per_sec,
        ));
        let acl = Arc::new(crate::server::acl::AclManager::new());
        let scram_credentials = Arc::new(DashMap::new());
        let metrics = Arc::new(crate::server::metrics::MetricsCollector::new());

        let engine = Self {
            config,
            partitions: Arc::new(DashMap::new()),
            deleting_topics: Arc::new(DashSet::new()),
            topic_registry: Arc::new(DashMap::new()),
            consumer_groups,
            transactions,
            share_groups,
            replication,
            group_coordinator,
            broker_addrs,
            quota,
            acl,
            scram_credentials,
            metrics,
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
        engine.bootstrap_legacy_sasl_users()?;

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

        // NOTE: the per-partition pull-fetcher loop (`start_per_partition_fetcher_manager`,
        // gRPC magic 0xBB) is intentionally not started. `handle_connection_stream`'s
        // dispatch never had a case for 0xBB, so that loop could never succeed against a
        // real peer — every fetch attempt failed silently and was retried forever. Steady-
        // state replication is push-only (0xAA, now byte-exact — see
        // `PartitionManager::append_replica_frame_verbatim`); running a second, redundant
        // fetch path here would also risk racing the push path's verbatim-offset checks.

        engine.start_isr_and_failover_sweep();

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
    pub async fn throttle_produce(&self, client_key: &str, bytes: u64, records: u64) {
        self.metrics.record_produce(bytes, records);
        let start = std::time::Instant::now();
        self.quota.throttle_produce(client_key, bytes).await;
        if start.elapsed() > std::time::Duration::from_millis(5) {
            self.metrics.record_quota_throttle();
        }
    }

    /// Accounts `bytes` of fetched data for `client_key` and delays as needed to
    /// enforce `fetch_quota_bytes_per_sec`. No-op when no fetch quota is configured.
    pub async fn throttle_fetch(&self, client_key: &str, bytes: u64) {
        self.metrics.record_fetch(bytes);
        let start = std::time::Instant::now();
        self.quota.throttle_fetch(client_key, bytes).await;
        if start.elapsed() > std::time::Duration::from_millis(5) {
            self.metrics.record_quota_throttle();
        }
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
                    pm.set_cleanup_policy(cfg.cleanup_policy);
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
                        self.apply_metadata_record(frame.offset, rec);
                    }
                    offset = frame.offset + 1;
                }
            }
        }
        Ok(())
    }

    /// Applies the in-memory/on-disk state effects of a single cluster-metadata record.
    ///
    /// This is the single source of truth for "what does observing this record do to
    /// broker state" — it never re-encodes or re-appends a `MetadataRecord` itself, so
    /// it is safe to call both from startup replay (`replay_metadata_log`) and from the
    /// live inter-node replication apply path. Proposing/writing new metadata records is
    /// a separate concern (see `create_topic`/`register_broker`/`delete_topic`/etc.),
    /// which is what actually produces the records this function later applies.
    ///
    /// `offset` is this record's position in the `__cluster_metadata` log; it is tracked
    /// as this node's "last log index" so that if it ever contests a leader election, its
    /// VoteRequest reflects how up to date its metadata log actually is (Raft §5.4.1).
    pub(crate) fn apply_metadata_record(
        &self,
        offset: u64,
        rec: crate::replication::MetadataRecord,
    ) {
        self.replication.set_local_metadata_log_index(offset + 1);
        match rec {
            crate::replication::MetadataRecord::TopicPartition {
                topic, partition, ..
            } => {
                if validate_topic_name(&topic).is_ok() {
                    let _ = self.get_or_create_partition(&topic, partition);
                } else {
                    tracing::warn!(
                        "apply_metadata_record: skipping invalid topic name '{}' in TopicPartition record",
                        topic
                    );
                }
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
                        cleanup_policy: self.config.cleanup_policy,
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
            crate::replication::MetadataRecord::BrokerRegister { node_id, bind_addr } => {
                self.register_broker_address(node_id, bind_addr);
            }
            crate::replication::MetadataRecord::BrokerUnregister { node_id } => {
                self.unregister_broker_address(node_id);
            }
            crate::replication::MetadataRecord::TopicDeleted { topic } => {
                self.apply_topic_deletion(&topic);
            }
            crate::replication::MetadataRecord::AclCreated { binding } => {
                self.acl.add_acl(binding);
            }
            crate::replication::MetadataRecord::AclDeleted { binding } => {
                self.acl.remove_acl(&binding);
            }
            crate::replication::MetadataRecord::ScramCredentialUpsert {
                username,
                iterations,
                salt,
                stored_key,
                server_key,
            } => {
                self.apply_scram_credential_state(
                    username, iterations, salt, stored_key, server_key,
                );
            }
            crate::replication::MetadataRecord::ScramCredentialDelete { username } => {
                self.remove_scram_credential_state(&username);
            }
            crate::replication::MetadataRecord::TransactionalProducerRegistration {
                transactional_id,
                producer_id,
                producer_epoch,
            } => {
                self.transactions.restore_transactional_producer(
                    &transactional_id,
                    producer_id,
                    producer_epoch,
                );
            }
        }
    }

    /// Proposes a cluster-metadata mutation: appends it to the local `__cluster_metadata`
    /// log, applies its state effects locally (via `apply_metadata_record`, so this is the
    /// only place metadata records get both written AND applied — no method ever produces
    /// a record without also being the one that later reads it back), replicates it to
    /// every peer, and — when `min_insync_replicas > 1` — waits for ISR quorum before
    /// returning, exactly like `produce_batch` does for regular topic data.
    ///
    /// Only the cluster leader may propose. Previously, `create_topic`/`register_broker`/
    /// ACL and SCRAM mutations/etc. wrote directly to the local metadata partition with no
    /// leader check and no replication at all — in a multi-node cluster this meant any node
    /// could silently write metadata nobody else in the cluster would ever see, forking
    /// cluster state. Callers reach this exclusively through `create_topic`/`register_broker`/
    /// `delete_topic`/etc. so every metadata mutation goes through the same gate.
    async fn propose_metadata(&self, record: crate::replication::MetadataRecord) -> IoResult<u64> {
        if !self.is_leader() {
            return Err(std::io::Error::other(
                "NOT_CONTROLLER: this node is not the cluster leader",
            ));
        }
        self.propose_metadata_unchecked(record).await
    }

    /// Proposes an ISR-membership-only update to a partition's `PartitionLeadershipChange`
    /// record — same `leader_id`/`leader_epoch`, only `isr` differs. Authorized by
    /// *partition* leadership rather than cluster leadership: the current partition leader
    /// is the node with real observability of follower replication lag for that partition
    /// (it's the one receiving the ACKs), so it — not necessarily the cluster's Raft
    /// leader — is the right authority for this specific decision. Real failovers (leader
    /// changing, epoch bumping) still require cluster leadership; see
    /// `propose_partition_failover`.
    async fn propose_isr_update(
        &self,
        topic: &str,
        partition: u32,
        leader_id: u32,
        leader_epoch: u32,
        isr: Vec<u32>,
    ) -> IoResult<u64> {
        if !self.is_partition_leader(topic, partition) {
            return Err(std::io::Error::other(
                "NOT_PARTITION_LEADER: only the current partition leader may update its ISR",
            ));
        }
        let record = crate::replication::MetadataRecord::PartitionLeadershipChange {
            topic: topic.to_string(),
            partition,
            leader_id,
            leader_epoch,
            isr,
        };
        self.propose_metadata_unchecked(record).await
    }

    /// Proposes a real partition failover: a new `leader_id` with a bumped `leader_epoch`.
    /// Authorized by *cluster* leadership — unlike an ISR-only update, choosing a new
    /// partition leader is a decision that must have a single, cluster-wide authority to
    /// avoid two nodes independently promoting themselves (split brain), so only the
    /// controller (Raft-elected cluster leader) may propose it.
    async fn propose_partition_failover(
        &self,
        topic: &str,
        partition: u32,
        new_leader_id: u32,
        new_leader_epoch: u32,
        isr: Vec<u32>,
    ) -> IoResult<u64> {
        if !self.is_leader() {
            return Err(std::io::Error::other(
                "NOT_CONTROLLER: only the cluster leader may fail a partition over",
            ));
        }
        let record = crate::replication::MetadataRecord::PartitionLeadershipChange {
            topic: topic.to_string(),
            partition,
            leader_id: new_leader_id,
            leader_epoch: new_leader_epoch,
            isr,
        };
        self.propose_metadata_unchecked(record).await
    }

    /// Core append+apply+replicate+quorum mechanics shared by every proposal path above.
    /// Callers are responsible for authorization — this function performs none.
    async fn propose_metadata_unchecked(
        &self,
        record: crate::replication::MetadataRecord,
    ) -> IoResult<u64> {
        let meta_pm = self.get_or_create_partition("__cluster_metadata", 0)?;
        let frame = meta_pm.produce_frame(&record.encode())?;
        self.apply_metadata_record(frame.offset, record);

        if !self.config.peer_addrs.is_empty() {
            let repl = self.replication.clone();
            let frame_for_replication = frame.clone();
            tokio::spawn(async move {
                if let Err(e) = repl
                    .replicate_batch(
                        "__cluster_metadata",
                        0,
                        std::slice::from_ref(&frame_for_replication),
                    )
                    .await
                {
                    tracing::error!("propose_metadata: replicate_batch failed: {}", e);
                }
            });

            if self.config.min_insync_replicas > 1 {
                let quorum_ok = self
                    .replication
                    .await_isr_quorum(
                        "__cluster_metadata",
                        0,
                        frame.offset,
                        std::time::Duration::from_secs(5),
                    )
                    .await;
                if !quorum_ok {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "ISR quorum not reached for metadata proposal",
                    ));
                }
            }
        }

        Ok(frame.offset)
    }

    pub fn acl(&self) -> &crate::server::acl::AclManager {
        &self.acl
    }

    pub fn metrics(&self) -> &Arc<crate::server::metrics::MetricsCollector> {
        &self.metrics
    }

    pub(crate) fn lookup_scram_credential(
        &self,
        username: &str,
    ) -> Option<crate::scram::ScramCredential> {
        self.scram_credentials
            .get(username)
            .map(|entry| entry.value().clone())
    }

    pub fn has_scram_user(&self, username: &str) -> bool {
        self.scram_credentials.contains_key(username)
    }

    pub(crate) fn apply_scram_credential_state(
        &self,
        username: String,
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    ) {
        self.scram_credentials.insert(
            username.clone(),
            crate::scram::ScramCredential::new(username, iterations, salt, stored_key, server_key),
        );
    }

    pub(crate) fn remove_scram_credential_state(&self, username: &str) {
        self.scram_credentials.remove(username);
    }

    pub async fn upsert_scram_credential(
        &self,
        username: &str,
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    ) -> IoResult<()> {
        let record = crate::replication::MetadataRecord::ScramCredentialUpsert {
            username: username.to_string(),
            iterations,
            salt,
            stored_key,
            server_key,
        };
        self.propose_metadata(record).await?;
        Ok(())
    }

    pub async fn upsert_scram_user(&self, username: &str, password: &str) -> IoResult<()> {
        let credential = crate::scram::ScramCredential::generate(
            username,
            password,
            crate::scram::DEFAULT_SCRAM_SHA256_ITERATIONS,
        )
        .map_err(|_| std::io::Error::other("Failed to generate SCRAM credential"))?;
        self.upsert_scram_credential(
            &credential.username,
            credential.iterations,
            credential.salt,
            credential.stored_key,
            credential.server_key,
        )
        .await
    }

    pub async fn delete_scram_user(&self, username: &str) -> IoResult<bool> {
        let existed = self.scram_credentials.contains_key(username);
        let record = crate::replication::MetadataRecord::ScramCredentialDelete {
            username: username.to_string(),
        };
        self.propose_metadata(record).await?;
        Ok(existed)
    }

    pub fn authorize(
        &self,
        principal: &str,
        host: &str,
        operation: u8,
        resource_type: u8,
        resource_name: &str,
    ) -> bool {
        let allowed = self.acl.authorize(
            principal,
            host,
            operation,
            resource_type,
            resource_name,
            &self.config.super_users,
            self.config.acls_enabled,
        );
        if !allowed {
            self.metrics.record_acl_deny();
        }
        allowed
    }

    pub async fn create_acl(&self, binding: crate::server::acl::AclBinding) -> IoResult<bool> {
        let is_new = !self.acl.contains(&binding);
        let record = crate::replication::MetadataRecord::AclCreated {
            binding: binding.clone(),
        };
        self.propose_metadata(record).await?;
        Ok(is_new)
    }

    pub async fn delete_acl(&self, binding: crate::server::acl::AclBinding) -> IoResult<bool> {
        let existed = self.acl.contains(&binding);
        let record = crate::replication::MetadataRecord::AclDeleted {
            binding: binding.clone(),
        };
        self.propose_metadata(record).await?;
        Ok(existed)
    }

    pub fn list_acls(
        &self,
        filter: crate::server::acl::AclBinding,
    ) -> Vec<crate::server::acl::AclBinding> {
        self.acl.list_acls(&filter)
    }

    /// P2: Replays the __transaction_state log to restore in-flight transactions after restart (BUG-12).
    pub fn replay_transaction_state(&self) -> IoResult<()> {
        let tx_dir = self.config.data_dir.join("__transaction_state-0");
        if !tx_dir.exists() {
            return Ok(());
        }

        if let Ok(pm) = self.get_or_create_partition("__transaction_state", 0) {
            let mut offset = 0u64;
            let mut latest_states: std::collections::HashMap<
                String,
                (
                    crate::server::transaction::TxStatus,
                    u64,
                    crate::server::transaction::PartitionRangeList,
                ),
            > = std::collections::HashMap::new();
            loop {
                let frames = pm.fetch(offset, 1024 * 1024)?;
                if frames.is_empty() {
                    break;
                }
                for frame in &frames {
                    if let Some((status, producer_id, tx_id, partitions)) =
                        decode_tx_state_record(&frame.payload)
                    {
                        latest_states.insert(tx_id, (status, producer_id, partitions));
                    }
                    offset = frame.offset + 1;
                }
            }
            for (tx_id, (status, producer_id, partitions)) in latest_states {
                if matches!(
                    status,
                    crate::server::transaction::TxStatus::Ongoing
                        | crate::server::transaction::TxStatus::PrepareCommit
                        | crate::server::transaction::TxStatus::PrepareAbort
                ) {
                    self.transactions
                        .restore_transaction(&tx_id, producer_id, status, partitions);
                    tracing::info!(
                        "TxReplay: Restored in-flight transaction '{}' producer={} status={:?}",
                        tx_id,
                        producer_id,
                        status
                    );
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
        if let Some(tx_id) = transaction_id {
            if self.transactions.has_transactional_producer(tx_id) {
                if producer_id == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Transactional produce requires a non-zero producer_id",
                    ));
                }
                self.transactions
                    .validate_transactional_producer(tx_id, producer_id, producer_epoch)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::PermissionDenied, e))?;
            }
            if !self.transactions.is_ongoing(tx_id) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Transaction '{}' is not active", tx_id),
                ));
            }
        }
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

        // Replication is gated on *partition* leadership, not cluster (Raft) leadership —
        // Hermes assigns an independent leader per partition (KIP-392-style; see
        // `is_partition_leader`/produce-forwarding in handler.rs), and the cluster Raft
        // leader is really the controller for `__cluster_metadata`, not necessarily the
        // leader of every data partition. Gating on cluster leadership here would silently
        // stop replication for any partition led by a node that isn't currently the
        // cluster leader.
        if self.is_partition_leader(topic, partition_id) && !self.config.peer_addrs.is_empty() {
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

            // Enforce min_insync_replicas requirement before returning success and before
            // advancing the committed high watermark (REP-05 & PARTIAL-03). Until quorum
            // is reached, `pm.latest_offset()` (LEO) has moved but `pm.high_watermark()`
            // has not — consumers can't fetch past what's actually ISR-committed, and if
            // the quorum wait times out here, the record stays durably on this leader but
            // is never exposed as committed (no false "it's safe to read" signal).
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

        // Reached only once quorum (if required) has been confirmed above — or
        // immediately for single-node/no-peer deployments and non-partition-leader
        // system-partition writes, where there's nothing else to wait on.
        pm.advance_committed_hw(last_offset + 1);

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
        // Clamp to the committed high watermark, not LEO: consumers must never be shown
        // data that isn't yet guaranteed replicated to the ISR (previously `fetch` exposed
        // everything up to LEO unconditionally, so a leader crash right after an
        // un-replicated append could mean a consumer read something no other replica ever
        // received).
        let hw = pm.high_watermark();
        let frames = pm.fetch(offset, max_bytes)?;
        Ok(frames.into_iter().filter(|f| f.offset < hw).collect())
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
        if self.transactions.has_transactional_producer(transaction_id) {
            self.transactions.validate_transactional_producer(
                transaction_id,
                producer_id,
                producer_epoch,
            )?;
        }
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

    pub fn init_producer_id(&self, transactional_id: &str) -> Result<(u64, i16), String> {
        if transactional_id.is_empty() {
            return Ok((self.transactions.generate_producer_id(), 0));
        }
        if self.transactions.is_ongoing(transactional_id) {
            self.abort_transaction(transactional_id)?;
        }
        let (producer_id, producer_epoch) = self
            .transactions
            .init_transactional_producer(transactional_id)?;
        let record = crate::replication::MetadataRecord::TransactionalProducerRegistration {
            transactional_id: transactional_id.to_string(),
            producer_id,
            producer_epoch,
        };
        let meta_pm = self
            .get_or_create_partition("__cluster_metadata", 0)
            .map_err(|e| format!("Failed to open metadata partition: {}", e))?;
        meta_pm
            .produce(&record.encode())
            .map_err(|e| format!("Failed to persist transactional producer state: {}", e))?;
        Ok((producer_id, producer_epoch))
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
        if let Some((producer_id, parts)) =
            self.transactions
                .register_partition(transaction_id, topic, partition, start_offset)
        {
            let record =
                encode_tx_state_record(TxStatus::Ongoing, producer_id, transaction_id, &parts);
            match self.get_or_create_partition("__transaction_state", 0) {
                Ok(tx_pm) => {
                    if let Err(err) = tx_pm.produce(&record) {
                        tracing::error!(
                            "Failed to persist transaction partition registration for '{}': {}",
                            transaction_id,
                            err
                        );
                    }
                }
                Err(err) => {
                    tracing::error!(
                        "Failed to open __transaction_state for '{}': {}",
                        transaction_id,
                        err
                    );
                }
            }
        }
    }

    pub fn end_transaction(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<(), String> {
        self.transactions.validate_transactional_producer(
            transactional_id,
            producer_id,
            producer_epoch,
        )?;
        if committed {
            self.commit_transaction(transactional_id)
        } else {
            self.abort_transaction(transactional_id)
        }
    }

    pub fn apply_retention_all(&self) -> IoResult<usize> {
        let mut total_removed = 0;
        for entry in self.partitions.iter() {
            total_removed += entry.value().apply_retention()?;
        }
        total_removed += self.transactions.prune_stale_transactions(604_800_000); // 7 days (604800000ms) retention
        Ok(total_removed)
    }

    pub fn flush_all(&self) -> IoResult<()> {
        for entry in self.partitions.iter() {
            entry.value().flush()?;
        }
        Ok(())
    }

    fn bootstrap_legacy_sasl_users(&self) -> IoResult<()> {
        for (username, password) in &self.config.sasl_users {
            if self.scram_credentials.contains_key(username) {
                continue;
            }
            let credential = crate::scram::ScramCredential::generate(
                username,
                password,
                crate::scram::DEFAULT_SCRAM_SHA256_ITERATIONS,
            )
            .map_err(|_| std::io::Error::other("Failed to bootstrap SCRAM credential"))?;
            self.scram_credentials
                .insert(username.clone(), credential.clone());
            if self.is_leader() {
                let record = crate::replication::MetadataRecord::ScramCredentialUpsert {
                    username: credential.username.clone(),
                    iterations: credential.iterations,
                    salt: credential.salt.clone(),
                    stored_key: credential.stored_key.clone(),
                    server_key: credential.server_key.clone(),
                };
                let meta_pm = self.get_or_create_partition("__cluster_metadata", 0)?;
                meta_pm.produce(&record.encode())?;
            }
        }
        Ok(())
    }

    /// Spawns the background task that keeps each partition's ISR membership honest and
    /// fails partitions over off dead leaders. Two independent, differently-authorized
    /// decisions happen per sweep (see `propose_isr_update`/`propose_partition_failover`):
    ///   - ISR shrink/expand: decided by whichever node currently leads a given partition,
    ///     since it's the one with real observability of follower replication lag.
    ///   - Leader failover: decided only by the cluster (Raft) leader, so at most one
    ///     authority ever promotes a replacement leader — avoiding split-brain promotion.
    pub fn start_isr_and_failover_sweep(&self) {
        let engine = self.clone();
        let interval = std::time::Duration::from_millis(self.config.isr_check_interval_ms.max(1));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                engine.run_isr_and_failover_sweep_once().await;
            }
        });
    }

    async fn run_isr_and_failover_sweep_once(&self) {
        // Snapshot keys first — DashMap iterators must not be held across `.await` points.
        let partition_keys: Vec<(String, u32)> =
            self.partitions.iter().map(|e| e.key().clone()).collect();

        for (topic, partition) in partition_keys {
            let Ok(pm) = self.get_or_create_partition(&topic, partition) else {
                continue;
            };
            let leader_id = pm.leader_id();
            let leader_epoch = pm.leader_epoch();
            let replicas = pm.replicas();
            let mut current_isr = pm.isr();
            current_isr.sort_unstable();

            // --- ISR shrink/expand (this node's call only if it leads the partition) ---
            if leader_id == self.config.node_id && replicas.len() > 1 {
                let mut new_isr: Vec<u32> = Vec::with_capacity(replicas.len());
                for &r in &replicas {
                    if r == self.config.node_id {
                        new_isr.push(r);
                        continue;
                    }
                    let in_sync = self
                        .get_broker_address(r)
                        .and_then(|addr| self.replication.replica_ack_age(&topic, partition, &addr))
                        .map(|age| age.as_millis() as u64 <= self.config.replica_lag_max_ms)
                        .unwrap_or(false);
                    if in_sync {
                        new_isr.push(r);
                    }
                }
                new_isr.sort_unstable();

                if !new_isr.is_empty() && new_isr != current_isr {
                    match self
                        .propose_isr_update(
                            &topic,
                            partition,
                            leader_id,
                            leader_epoch,
                            new_isr.clone(),
                        )
                        .await
                    {
                        Ok(_) => tracing::info!(
                            "ISR sweep: {}-{} ISR changed {:?} -> {:?}",
                            topic,
                            partition,
                            current_isr,
                            new_isr
                        ),
                        Err(e) => tracing::warn!(
                            "ISR sweep: failed to update ISR for {}-{}: {}",
                            topic,
                            partition,
                            e
                        ),
                    }
                }
            }

            // --- Failover (cluster leader's call only, for partitions led elsewhere) ---
            if self.is_leader() && leader_id != self.config.node_id {
                // Conservative: only act once we've positively observed this leader go
                // silent past the threshold. A broker we've simply never heard from yet
                // (e.g. right after a fresh topic assignment, before its first heartbeat)
                // is NOT treated as dead — that would cause spurious failovers on startup.
                let dead = self
                    .replication
                    .broker_last_seen_age(leader_id)
                    .map(|age| age.as_millis() as u64 >= self.config.broker_down_threshold_ms)
                    .unwrap_or(false);

                if dead {
                    let mut isr_candidates: Vec<u32> = current_isr
                        .iter()
                        .copied()
                        .filter(|&r| r != leader_id)
                        .collect();
                    isr_candidates.sort_unstable();

                    let (new_leader_id, new_isr) = match isr_candidates.first().copied() {
                        Some(id) => (Some(id), isr_candidates.clone()),
                        None if self.config.allow_unclean_leader_election => {
                            let fallback =
                                replicas.iter().copied().filter(|&r| r != leader_id).min();
                            (fallback, fallback.into_iter().collect())
                        }
                        None => (None, Vec::new()),
                    };

                    match new_leader_id {
                        Some(new_leader_id) => {
                            match self
                                .propose_partition_failover(
                                    &topic,
                                    partition,
                                    new_leader_id,
                                    leader_epoch.wrapping_add(1),
                                    new_isr,
                                )
                                .await
                            {
                                Ok(_) => tracing::warn!(
                                    "Failover: {}-{} leader {} appears dead — promoted {} (epoch {})",
                                    topic,
                                    partition,
                                    leader_id,
                                    new_leader_id,
                                    leader_epoch.wrapping_add(1)
                                ),
                                Err(e) => tracing::error!(
                                    "Failover: failed to fail {}-{} over from dead leader {}: {}",
                                    topic,
                                    partition,
                                    leader_id,
                                    e
                                ),
                            }
                        }
                        None => {
                            tracing::error!(
                                "Failover: {}-{} leader {} appears dead and no in-sync replica survives — \
                                 leaving partition leaderless (set allow_unclean_leader_election to override)",
                                topic,
                                partition,
                                leader_id
                            );
                        }
                    }
                }
            }
        }
    }

    /// Creates a topic by writing a TopicCreated record to __cluster_metadata and populating registry
    pub async fn create_topic(&self, topic: &str, num_partitions: u32) -> IoResult<()> {
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
        // propose_metadata both writes this record AND applies it (registers the topic
        // in topic_registry with an empty partition map) via apply_metadata_record, so
        // there's no separate direct topic_registry.insert here anymore.
        self.propose_metadata(record).await?;

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
                isr,
            };
            // Likewise applies topic_registry.partitions + pm.update_leadership itself.
            self.propose_metadata(plc_record).await?;
        }

        Ok(())
    }

    /// Dynamically update a topic's cleanup policy (Kafka topic config alteration)
    pub fn set_topic_cleanup_policy(&self, topic: &str, policy: crate::config::CleanupPolicy) {
        if let Some(mut cfg) = self.topic_registry.get_mut(topic) {
            cfg.cleanup_policy = policy;
        }
        for entry in self.partitions.iter() {
            if entry.key().0 == topic {
                entry.value().set_cleanup_policy(policy);
            }
        }
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

    /// Unregisters a broker socket address mapping
    pub fn unregister_broker_address(&self, node_id: u32) {
        self.broker_addrs.remove(&node_id);
    }

    /// Dynamically registers a broker in the cluster metadata catalog and replicates to peers
    pub async fn register_broker(&self, node_id: u32, bind_addr: String) -> IoResult<()> {
        let record = crate::replication::MetadataRecord::BrokerRegister { node_id, bind_addr };
        self.propose_metadata(record).await?;
        Ok(())
    }

    /// Dynamically unregisters a broker from the cluster metadata catalog and replicates to peers
    pub async fn unregister_broker(&self, node_id: u32) -> IoResult<()> {
        let record = crate::replication::MetadataRecord::BrokerUnregister { node_id };
        self.propose_metadata(record).await?;
        Ok(())
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
                let hw = entry.value().high_watermark();
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
    pub async fn delete_topic(&self, topic: &str) -> IoResult<()> {
        let record = crate::replication::MetadataRecord::TopicDeleted {
            topic: topic.to_string(),
        };
        // propose_metadata's apply_metadata_record call runs the deletion's state/fs
        // effects via apply_topic_deletion (which logs rather than propagates fs errors,
        // matching how a follower applying the same record would handle it).
        self.propose_metadata(record).await?;
        Ok(())
    }

    /// State-only counterpart to `delete_topic`, used by `apply_metadata_record` so that
    /// replaying/receiving a `TopicDeleted` record never re-produces another one.
    fn apply_topic_deletion(&self, topic: &str) {
        if let Err(e) = self.apply_topic_deletion_inner(topic) {
            tracing::error!(
                "apply_metadata_record: TopicDeleted cleanup for '{}' failed: {}",
                topic,
                e
            );
        }
    }

    fn apply_topic_deletion_inner(&self, topic: &str) -> IoResult<()> {
        self.deleting_topics.insert(topic.to_string());

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

    pub fn share_groups(&self) -> &crate::server::share::ShareGroupManager {
        &self.share_groups
    }

    /// Fetches acquired records for ShareFetch
    pub fn share_fetch(
        &self,
        group_id: &str,
        member_id: &str,
        topic: &str,
        partition: u32,
        max_records: u32,
        lock_timeout_ms: u32,
    ) -> Result<Vec<crate::protocol::wire::AcquiredRecordBatch>, String> {
        let pm = self
            .get_or_create_partition(topic, partition)
            .map_err(|e| e.to_string())?;
        let lock_timeout = if lock_timeout_ms > 0 {
            Some(std::time::Duration::from_millis(lock_timeout_ms as u64))
        } else {
            None
        };
        self.share_groups.share_fetch(
            group_id,
            member_id,
            topic,
            partition,
            max_records as usize,
            lock_timeout,
            &pm,
        )
    }

    /// Acknowledges records for ShareAcknowledge (or piggybacked on ShareFetch)
    pub fn share_acknowledge(
        &self,
        group_id: &str,
        member_id: &str,
        topic: &str,
        partition: u32,
        batches: &[crate::protocol::wire::AckBatch],
    ) -> Result<(), String> {
        let dlq_writer = |dlq_offsets: &[u64]| {
            let dlq_topic = format!("{}-dlq", topic);
            if let Ok(dlq_pm) = self.get_or_create_partition(&dlq_topic, 0) {
                if let Ok(src_pm) = self.get_or_create_partition(topic, partition) {
                    for &off in dlq_offsets {
                        if let Ok(frames) = src_pm.fetch(off, 1024 * 1024) {
                            if let Some(f) = frames.into_iter().find(|fr| fr.offset == off) {
                                let _ = dlq_pm.produce_frame(&f.payload);
                            }
                        }
                    }
                }
            }
        };

        self.share_groups.share_acknowledge(
            group_id,
            member_id,
            topic,
            partition,
            batches,
            Some(&dlq_writer),
        )
    }

    /// Records share group member heartbeat
    pub fn share_group_heartbeat(&self, group_id: &str, member_id: &str) {
        self.share_groups.record_heartbeat(group_id, member_id);
    }

    /// Describes share group state, active members, and tracked metrics
    pub fn share_group_describe(&self, group_id: &str) -> (String, Vec<String>, usize, u64) {
        let members = self.share_groups.list_active_members(group_id);
        let state = if members.is_empty() {
            "Empty".to_string()
        } else {
            "Stable".to_string()
        };
        (state, members, 0, 0)
    }

    /// Sweeps expired acquisition locks and routes poison pills to DLQ
    pub fn sweep_share_lock_timeouts(&self) {
        let dlq_writer = |topic: &str, partition: u32, dlq_offsets: &[u64]| {
            let dlq_topic = format!("{}-dlq", topic);
            if let Ok(dlq_pm) = self.get_or_create_partition(&dlq_topic, 0) {
                if let Ok(src_pm) = self.get_or_create_partition(topic, partition) {
                    for &off in dlq_offsets {
                        if let Ok(frames) = src_pm.fetch(off, 1024 * 1024) {
                            if let Some(f) = frames.into_iter().find(|fr| fr.offset == off) {
                                let _ = dlq_pm.produce_frame(&f.payload);
                            }
                        }
                    }
                }
            }
        };
        self.share_groups.sweep_lock_timeouts(Some(&dlq_writer));
    }
}
