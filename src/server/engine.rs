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

/// `std::fs::remove_dir_all` with bounded retries.
///
/// On Windows, deleting a file still held open by any handle fails with a sharing
/// violation (`ERROR_SHARING_VIOLATION` / `AccessDenied`) rather than unlinking it the way
/// POSIX does. Dropping the `Arc<PartitionManager>` closes Hermes's own handles, but that
/// only takes effect once the *last* clone is gone — an in-flight fetch holding a clone,
/// or a `plan_zero_copy_fetch` file handle still being transmitted, can keep the directory
/// briefly undeletable. Retrying over a short window covers that, instead of failing a
/// topic delete that would have succeeded a few milliseconds later.
fn remove_dir_all_with_retry(path: &std::path::Path) -> IoResult<()> {
    const MAX_ATTEMPTS: u32 = 5;
    const BACKOFF_MS: u64 = 50;

    let mut last_err = None;
    for attempt in 0..MAX_ATTEMPTS {
        match std::fs::remove_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < MAX_ATTEMPTS {
                    std::thread::sleep(std::time::Duration::from_millis(
                        BACKOFF_MS * (attempt as u64 + 1),
                    ));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| std::io::Error::other("remove_dir_all failed")))
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
    /// Dynamic per-topic config overrides (Kafka `AlterConfigs`/`IncrementalAlterConfigs`).
    /// Recognized keys (`cleanup.policy`, `compression.type`, `retention.ms`,
    /// `retention.bytes`, `min.insync.replicas`) are applied to every open partition of
    /// this topic; unrecognized keys are stored and returned by `DescribeConfigs` but
    /// have no runtime effect.
    pub configs: std::collections::HashMap<String, String>,
}

pub type TopicRegistry = DashMap<String, TopicConfig>;

/// StorageEngine maintaining multi-topic concurrent partition routing, consumer group offsets, and transactions
#[derive(Debug, Clone)]
pub struct StorageEngine {
    config: EngineConfig,
    partitions: Arc<DashMap<(String, u32), Arc<PartitionManager>>>,
    /// Index of topic -> that topic's currently-open partition IDs, maintained alongside
    /// `partitions` (inserted in `get_or_create_partition`, cleared in
    /// `apply_topic_deletion_inner`). Lets `list_topics`/`describe_topic` work in time
    /// proportional to the number of topics (or, for `describe_topic`, that one topic's
    /// own partition count) instead of scanning every partition of every topic on the
    /// broker for each call.
    topic_partitions: Arc<DashMap<String, DashSet<u32>>>,
    deleting_topics: Arc<DashSet<String>>,
    topic_registry: Arc<TopicRegistry>,
    consumer_groups: ConsumerGroupManager,
    transactions: TransactionManager,
    share_groups: crate::server::share::ShareGroupManager,
    replication: ReplicationManager,
    group_coordinator: Arc<GroupCoordinator>,
    broker_addrs: Arc<DashMap<u32, String>>,
    broker_roles: Arc<DashMap<u32, Vec<crate::config::ProcessRole>>>,
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
            roles: config.roles.clone(),
            controller_peer_addrs: config.controller_peer_addrs.clone(),
        };

        let broker_addrs = Arc::new(DashMap::new());
        let broker_roles = Arc::new(DashMap::new());
        let replication = ReplicationManager::new(
            cluster_config,
            config.bind_addr.clone(),
            broker_addrs.clone(),
            broker_roles.clone(),
        );
        let group_coordinator = Arc::new(GroupCoordinator::with_rebalance_delay(
            std::time::Duration::from_millis(config.group_initial_rebalance_delay_ms),
        ));
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
            topic_partitions: Arc::new(DashMap::new()),
            deleting_topics: Arc::new(DashSet::new()),
            topic_registry: Arc::new(DashMap::new()),
            consumer_groups,
            transactions,
            share_groups,
            replication,
            group_coordinator,
            broker_addrs,
            broker_roles,
            quota,
            acl,
            scram_credentials,
            metrics,
        };

        engine
            .broker_addrs
            .insert(engine.config.node_id, engine.config.bind_addr.clone());
        engine
            .broker_roles
            .insert(engine.config.node_id, engine.config.roles.clone());

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
                roles: crate::config::roles_to_bytes(&engine.config.roles),
            };
            if let Ok(meta_pm) = engine.get_or_create_partition("__cluster_metadata", 0) {
                let _ = meta_pm.produce(&reg_rec.encode());
            }
        }

        // Per-partition pull-fetcher loop (`start_per_partition_fetcher_manager`, gRPC
        // magic 0xBB) — Kafka-style follower-driven replication: for every partition this
        // node replicates but doesn't lead, a background loop asks the leader for
        // everything past its own log end offset and applies it verbatim. This is now the
        // sole mechanism for *data-topic* replication (see `StorageEngine::produce_batch`,
        // which no longer pushes data-topic batches to followers); `__cluster_metadata`
        // still replicates via leader-push (`ReplicationManager::replicate_batch`) since
        // it's low-volume control-plane traffic where the added latency of a pull round
        // trip isn't worth it. `handler.rs`'s connection dispatch now has a real 0xBB case
        // (`decode_grpc_replication_fetch_packet`), so — unlike when this loop was
        // previously disabled — fetch requests actually succeed against a real peer.
        engine.replication.start_per_partition_fetcher_manager(engine.clone());

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

    /// Charges `bytes` against `client_key`'s produce quota and delays for however long
    /// the token bucket says, **before** the caller performs the write.
    ///
    /// The byte count is fully known up front (the records are already decoded and in
    /// memory), so there's no reason to wait until after the append to apply the delay —
    /// and good reason not to. Throttling only afterwards meant an over-quota client's
    /// burst was already fully committed to disk by the time the broker got around to
    /// slowing it down, so the quota bounded the *response rate* but never actually
    /// protected the disk from the burst it was configured to prevent. Delaying first
    /// paces the writes themselves.
    pub async fn apply_produce_quota(&self, client_key: &str, bytes: u64) {
        let start = std::time::Instant::now();
        self.quota.throttle_produce(client_key, bytes).await;
        if start.elapsed() > std::time::Duration::from_millis(5) {
            self.metrics.record_quota_throttle();
        }
    }

    /// Records produce metrics (global + per-topic) for a write that actually succeeded.
    /// Separate from `apply_produce_quota` so quota pacing can happen before the write
    /// while the counters only move for writes that really landed.
    pub fn record_produce_metrics(&self, topic: &str, bytes: u64, records: u64) {
        self.metrics.record_produce_topic(topic, bytes, records);
    }

    /// Accounts `bytes` of fetched data for `client_key`/`topic` and delays as needed to
    /// enforce `fetch_quota_bytes_per_sec`. No-op (quota-wise) when no fetch quota is
    /// configured — metrics are still recorded either way.
    pub async fn throttle_fetch(&self, topic: &str, client_key: &str, bytes: u64) {
        self.metrics.record_fetch_topic(topic, bytes);
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
    /// Waits until **every replica currently in the ISR** has acknowledged `target_offset`.
    ///
    /// This is what `acks=all` has to mean. The previous behavior counted acknowledgements
    /// until it reached `min_insync_replicas` and then returned success, which inverts the
    /// purpose of that setting: `min.insync.replicas` is a *floor* that makes a write fail
    /// when the ISR has shrunk too far, not a target that lets the write succeed early.
    /// With an ISR of 5 and a floor of 2, the old code acknowledged a write to the producer
    /// once any 2 replicas had it — so losing those 2 lost data the producer had been told
    /// was fully replicated.
    ///
    /// The ISR is re-read on every poll rather than snapshotted, so a write blocked on a
    /// replica that the ISR sweep subsequently evicts unblocks as soon as that replica
    /// leaves the ISR, instead of waiting out the full timeout.
    ///
    /// Returns `Ok(())` once committed, or an error describing which requirement failed.
    pub async fn await_full_isr_ack(
        &self,
        pm: &Arc<PartitionManager>,
        topic: &str,
        partition: u32,
        target_offset: u64,
        timeout: std::time::Duration,
    ) -> IoResult<()> {
        let min_isr = self.effective_min_insync_replicas(topic);
        let start = std::time::Instant::now();

        loop {
            let isr = pm.isr();

            // No ISR metadata has ever been applied to this partition (no leadership
            // record yet). There is no authoritative in-sync set to wait on, so fall back
            // to the configured peers and the `min_insync_replicas` floor — the pre-
            // existing behavior — rather than inventing a stricter requirement that would
            // reject writes on a cluster that simply hasn't published assignments yet.
            if isr.is_empty() {
                let mut acked = 1usize; // this leader holds the record
                for peer in &self.config.peer_addrs {
                    if self
                        .replication
                        .replica_watermark(topic, partition, peer)
                        .is_some_and(|w| w >= target_offset)
                    {
                        acked += 1;
                    }
                }
                if acked >= min_isr {
                    return Ok(());
                }
                if start.elapsed() >= timeout {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "Only {} of the required {} replicas acknowledged {}-{} at offset {}",
                            acked, min_isr, topic, partition, target_offset
                        ),
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                continue;
            }

            // The floor: refuse the write outright when the ISR has shrunk below
            // `min.insync.replicas`. Failing fast is the point — waiting cannot help,
            // because there are not enough in-sync replicas for the write to ever be
            // as durable as `acks=all` promises.
            if isr.len() < min_isr {
                return Err(std::io::Error::other(format!(
                    "NOT_ENOUGH_REPLICAS: {}-{} ISR has {} replica(s), min.insync.replicas is {}",
                    topic,
                    partition,
                    isr.len(),
                    min_isr
                )));
            }

            let mut pending: Vec<u32> = Vec::new();
            for &node_id in &isr {
                if node_id == self.config.node_id {
                    continue; // the leader necessarily holds it
                }
                let acked = self
                    .get_broker_address(node_id)
                    .and_then(|addr| self.replication.replica_watermark(topic, partition, &addr))
                    .is_some_and(|w| w >= target_offset);
                if !acked {
                    pending.push(node_id);
                }
            }

            if pending.is_empty() {
                return Ok(());
            }

            if start.elapsed() >= timeout {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "ISR replicas {:?} did not acknowledge {}-{} at offset {} within {:?}",
                        pending, topic, partition, target_offset, timeout
                    ),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Looks up an already-open partition without ever creating one.
    ///
    /// Read/probe paths must use this rather than `get_or_create_partition`: those are
    /// reachable by any client naming any topic, and creating on a read means a request
    /// that should have been a lookup instead materializes on-disk state.
    pub fn get_partition(&self, topic: &str, partition: u32) -> Option<Arc<PartitionManager>> {
        self.partitions
            .get(&(topic.to_string(), partition))
            .map(|e| e.clone())
    }

    /// Whether `topic` is an internal system topic (`__cluster_metadata`,
    /// `__consumer_offsets`, `__transaction_state`, …). These are created by the broker
    /// itself, never by client request, and are exempt from the auto-create policy and
    /// the partition caps — refusing to create them would break the broker's own bookkeeping.
    fn is_system_topic(topic: &str) -> bool {
        topic.starts_with("__")
    }

    /// Partition creation on behalf of a **client request**, subject to
    /// `auto.create.topics.enable`.
    ///
    /// Implicit creation is allowed only when the topic is already known (explicitly
    /// created, or assigned to this node through the replicated metadata log) or when the
    /// operator has opted into auto-creation. Without this gate, any client that can reach
    /// the broker can materialize unbounded topic/partition directories just by naming
    /// them in a request, exhausting inodes and leaving state behind that outlives the
    /// connection.
    pub fn get_or_create_partition_for_client(
        &self,
        topic: &str,
        partition: u32,
    ) -> IoResult<Arc<PartitionManager>> {
        if let Some(pm) = self.get_partition(topic, partition) {
            return Ok(pm);
        }
        let known = Self::is_system_topic(topic) || self.topic_registry.contains_key(topic);
        if !known && !self.config.auto_create_topics_enable {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Unknown topic '{}' (auto.create.topics.enable is false)",
                    topic
                ),
            ));
        }
        self.get_or_create_partition(topic, partition)
    }

    /// Resolves a partition for a **read** path.
    ///
    /// Returns the open partition if there is one; opens it if the topic is genuinely
    /// known (a system topic, or one assigned to this broker through the replicated
    /// metadata log — in which case this node is supposed to host it and opening is just
    /// deferred initialization); and returns `None` for a topic this broker has never
    /// heard of, rather than bringing a directory tree into existence to serve a read that
    /// can only ever come back empty.
    fn partition_for_read(
        &self,
        topic: &str,
        partition: u32,
    ) -> IoResult<Option<Arc<PartitionManager>>> {
        if let Some(pm) = self.get_partition(topic, partition) {
            return Ok(Some(pm));
        }
        if Self::is_system_topic(topic) || self.topic_registry.contains_key(topic) {
            return self.get_or_create_partition(topic, partition).map(Some);
        }
        Ok(None)
    }

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

        if partition >= self.config.max_partitions_per_topic && !Self::is_system_topic(topic) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Partition {} for topic '{}' exceeds max.partitions.per.topic ({})",
                    partition, topic, self.config.max_partitions_per_topic
                ),
            ));
        }

        let key = (topic.to_string(), partition);

        // Broker-wide backstop, enforced regardless of how creation was reached (client
        // request, metadata replay, admin call). System topics are exempt so the broker
        // can always maintain its own bookkeeping.
        //
        // This MUST be evaluated before taking the `entry()` below, never inside the
        // `Vacant` arm: `entry()` holds a write guard on one DashMap shard, while `len()`
        // reads every shard — including the one already held — which self-deadlocks the
        // moment a partition actually needs creating.
        if !Self::is_system_topic(topic)
            && !self.partitions.contains_key(&key)
            && self.partitions.len() >= self.config.max_partitions_per_broker
        {
            return Err(std::io::Error::other(format!(
                "Cannot create {}-{}: broker is at max.partitions.per.broker ({})",
                topic, partition, self.config.max_partitions_per_broker
            )));
        }

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
                self.topic_partitions
                    .entry(topic.to_string())
                    .or_default()
                    .insert(partition);
                Ok(pm)
            }
        }
    }

    /// Replays the local cluster metadata log to initialize partitions registered on this node.
    ///
    /// If a snapshot exists (see `snapshot_metadata_if_needed`), it's loaded first and
    /// replay resumes from the snapshot's offset instead of 0 — the KRaft-style mechanism
    /// that keeps startup fast and keeps the on-disk log itself trimmable, since replay
    /// no longer needs every record back to the beginning of time to reconstruct state.
    pub fn replay_metadata_log(&self) -> IoResult<()> {
        let meta_dir = self.config.data_dir.join("__cluster_metadata-0");
        if !meta_dir.exists() {
            return Ok(());
        }

        let mut start_offset = 0u64;
        if let Some((snapshot_offset, records)) = self.read_metadata_snapshot()? {
            for rec in records {
                self.apply_metadata_record(snapshot_offset, rec);
            }
            start_offset = snapshot_offset;
            tracing::info!(
                "Metadata Snapshot: Loaded snapshot at offset {}, resuming replay from there",
                snapshot_offset
            );
        }

        if let Ok(pm) = self.get_or_create_partition("__cluster_metadata", 0) {
            let mut offset = start_offset;
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

    fn metadata_snapshot_path(&self) -> std::path::PathBuf {
        self.config
            .data_dir
            .join("__cluster_metadata-0")
            .join("metadata.snapshot")
    }

    /// Builds the minimal set of records that reconstruct the engine's current
    /// cluster-metadata state — topics/partitions, brokers, ACLs, SCRAM credentials, and
    /// transactional-producer fencing state. This is also, implicitly, the definition of
    /// "everything a snapshot must cover" before any log data can be safely trimmed.
    fn build_metadata_snapshot_records(&self) -> Vec<crate::replication::MetadataRecord> {
        use crate::replication::MetadataRecord;
        let mut records = Vec::new();

        for entry in self.broker_addrs.iter() {
            let node_id = *entry.key();
            let roles = self
                .broker_roles
                .get(&node_id)
                .map(|r| crate::config::roles_to_bytes(&r))
                .unwrap_or_default();
            records.push(MetadataRecord::BrokerRegister {
                node_id,
                bind_addr: entry.value().clone(),
                roles,
            });
        }

        for entry in self.topic_registry.iter() {
            let cfg = entry.value();
            records.push(MetadataRecord::TopicCreated {
                topic: cfg.topic.clone(),
                num_partitions: cfg.num_partitions,
                replication_factor: cfg.replication_factor,
            });
            for assign in cfg.partitions.values() {
                records.push(MetadataRecord::PartitionLeadershipChange {
                    topic: cfg.topic.clone(),
                    partition: assign.partition,
                    leader_id: assign.leader_id,
                    leader_epoch: assign.leader_epoch,
                    isr: assign.isr.clone(),
                });
            }
            if !cfg.configs.is_empty() {
                records.push(MetadataRecord::TopicConfigChanged {
                    topic: cfg.topic.clone(),
                    configs: cfg.configs.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                });
            }
        }

        // A default/all-wildcard binding matches every stored ACL (see AclManager::list_acls).
        let match_all = crate::server::acl::AclBinding {
            resource_type: 0,
            resource_name: String::new(),
            pattern_type: 0,
            principal: String::new(),
            host: String::new(),
            operation: 0,
            permission_type: 0,
        };
        for binding in self.acl.list_acls(&match_all) {
            records.push(MetadataRecord::AclCreated { binding });
        }

        for entry in self.scram_credentials.iter() {
            let cred = entry.value();
            records.push(MetadataRecord::ScramCredentialUpsert {
                username: cred.username.clone(),
                iterations: cred.iterations,
                salt: cred.salt.clone(),
                stored_key: cred.stored_key.clone(),
                server_key: cred.server_key.clone(),
            });
        }

        for (transactional_id, producer_id, producer_epoch) in
            self.transactions.all_transactional_producers()
        {
            records.push(MetadataRecord::TransactionalProducerRegistration {
                transactional_id,
                producer_id,
                producer_epoch,
            });
        }

        records
    }

    /// Snapshot file format: `[snapshot_offset: u64][record_count: u32] { [len: u32] [encoded MetadataRecord] }...`
    fn write_metadata_snapshot(&self, snapshot_offset: u64) -> IoResult<()> {
        let records = self.build_metadata_snapshot_records();
        let path = self.metadata_snapshot_path();
        let tmp_path = path.with_extension("snapshot.tmp");

        let mut buf = Vec::new();
        buf.extend_from_slice(&snapshot_offset.to_be_bytes());
        buf.extend_from_slice(&(records.len() as u32).to_be_bytes());
        for record in &records {
            let encoded = record.encode();
            buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            buf.extend_from_slice(&encoded);
        }

        std::fs::write(&tmp_path, &buf)?;
        // Rename is atomic on the same filesystem, so a crash mid-write never leaves a
        // half-written snapshot in place of a good one.
        std::fs::rename(&tmp_path, &path)?;
        Ok(())
    }

    fn read_metadata_snapshot(
        &self,
    ) -> IoResult<Option<(u64, Vec<crate::replication::MetadataRecord>)>> {
        let path = self.metadata_snapshot_path();
        if !path.exists() {
            return Ok(None);
        }
        let buf = std::fs::read(&path)?;
        if buf.len() < 12 {
            return Ok(None);
        }
        let snapshot_offset = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        let record_count = u32::from_be_bytes(buf[8..12].try_into().unwrap()) as usize;

        let mut cursor = &buf[12..];
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            if cursor.len() < 4 {
                break;
            }
            let len = u32::from_be_bytes(cursor[0..4].try_into().unwrap()) as usize;
            cursor = &cursor[4..];
            if cursor.len() < len {
                break;
            }
            if let Ok(rec) = crate::replication::MetadataRecord::decode(&cursor[..len]) {
                records.push(rec);
            }
            cursor = &cursor[len..];
        }
        Ok(Some((snapshot_offset, records)))
    }

    /// Takes a new `__cluster_metadata` snapshot and trims the now-fully-covered prefix
    /// of the log, if enough new records have accumulated since the last snapshot to be
    /// worth it. Safe to call on every node independently (no leader/quorum coordination
    /// needed) — each node's in-memory state is exactly the replay of its own local log up
    /// to its own LEO, so a node's snapshot always matches what its own log already says.
    ///
    /// This is the fix for the metadata log otherwise having no bound on growth — and,
    /// just as importantly, for `__cluster_metadata` (a system partition never explicitly
    /// created via `create_topic`) inheriting the engine's *default* cleanup policy, which
    /// for most deployments is time/size delete-retention. Without a snapshot boundary,
    /// that default retention deleting old segments could silently drop a topic's only
    /// `TopicCreated` record (or similar) if it happened to be old enough — corrupting
    /// what a freshly-replaying node would ever learn about that topic.
    pub(crate) fn snapshot_metadata_if_needed(&self) {
        const MIN_NEW_RECORDS_FOR_SNAPSHOT: u64 = 500;

        let Ok(pm) = self.get_or_create_partition("__cluster_metadata", 0) else {
            return;
        };
        let leo = pm.latest_offset();

        let last_snapshot_offset = match self.read_metadata_snapshot() {
            Ok(Some((offset, _))) => offset,
            Ok(None) => 0,
            Err(e) => {
                tracing::warn!("Metadata Snapshot: Failed to read existing snapshot: {}", e);
                return;
            }
        };

        if leo <= last_snapshot_offset
            || leo - last_snapshot_offset < MIN_NEW_RECORDS_FOR_SNAPSHOT
        {
            return;
        }

        if let Err(e) = self.write_metadata_snapshot(leo) {
            tracing::error!("Metadata Snapshot: Failed to write snapshot: {}", e);
            return;
        }

        match pm.trim_before(leo) {
            Ok(n) if n > 0 => {
                tracing::info!(
                    "Metadata Snapshot: Took snapshot at offset {} and trimmed {} fully-covered segment(s)",
                    leo,
                    n
                );
            }
            Err(e) => {
                tracing::error!("Metadata Snapshot: Failed to trim log after snapshot: {}", e);
            }
            _ => {}
        }
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
                        configs: std::collections::HashMap::new(),
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
                roles,
            } => {
                self.register_broker_address(node_id, bind_addr);
                self.register_broker_roles(node_id, &roles);
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
            crate::replication::MetadataRecord::TopicConfigChanged { topic, configs } => {
                self.apply_topic_config_change(&topic, configs);
            }
        }
    }

    /// Returns every currently-open `PartitionManager` for `topic`, via the
    /// `topic_partitions` index — O(this topic's partition count) rather than scanning
    /// every partition of every topic on the broker.
    fn partitions_for_topic(&self, topic: &str) -> Vec<Arc<PartitionManager>> {
        let Some(partition_ids) = self.topic_partitions.get(topic) else {
            return Vec::new();
        };
        partition_ids
            .iter()
            .filter_map(|p| self.partitions.get(&(topic.to_string(), *p)).map(|e| e.clone()))
            .collect()
    }

    /// Applies a full-replace topic config change: updates the stored config map and
    /// pushes recognized keys down to every currently-open partition of this topic.
    fn apply_topic_config_change(&self, topic: &str, configs: Vec<(String, String)>) {
        let configs_map: std::collections::HashMap<String, String> = configs.into_iter().collect();

        if let Some(mut cfg) = self.topic_registry.get_mut(topic) {
            cfg.configs = configs_map.clone();
        }

        for pm in self.partitions_for_topic(topic) {
            if let Some(v) = configs_map.get("cleanup.policy") {
                if let Ok(policy) = v.parse::<crate::config::CleanupPolicy>() {
                    pm.set_cleanup_policy(policy);
                }
            }
            if let Some(v) = configs_map.get("compression.type") {
                if let Ok(codec) = v.parse::<crate::config::CompressionCodec>() {
                    pm.set_compression_codec(codec);
                }
            }
            if let Some(v) = configs_map.get("retention.ms") {
                pm.set_retention_millis(v.parse::<u64>().ok());
            }
            if let Some(v) = configs_map.get("retention.bytes") {
                pm.set_retention_bytes(v.parse::<u64>().ok());
            }
            if let Some(v) = configs_map.get("delete.retention.ms") {
                pm.set_delete_retention_millis(v.parse::<u64>().ok());
            }
            if let Some(v) = configs_map.get("min.cleanable.dirty.ratio") {
                if let Ok(ratio) = v.parse::<f64>() {
                    pm.set_min_cleanable_dirty_ratio(ratio);
                }
            }
            // min.insync.replicas is read per-topic from `topic_registry` directly at
            // produce time (see `produce_batch`) rather than pushed into PartitionManager.
        }
    }

    /// Resolves the effective `min.insync.replicas` for `topic`: its per-topic
    /// `AlterConfigs` override if set and parseable, else the broker's global default.
    fn effective_min_insync_replicas(&self, topic: &str) -> usize {
        self.topic_registry
            .get(topic)
            .and_then(|cfg| cfg.configs.get("min.insync.replicas").cloned())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(self.config.min_insync_replicas)
    }

    /// Returns the effective config map for `topic` (currently-stored overrides only —
    /// keys not present here fall back to the broker's global defaults).
    pub fn describe_configs(&self, topic: &str) -> Vec<(String, String)> {
        self.topic_registry
            .get(topic)
            .map(|cfg| cfg.configs.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// Full-replace config update (Kafka `AlterConfigs`). Only the cluster leader may
    /// propose — routes through the same `propose_metadata` gate as every other
    /// controller-plane mutation.
    pub async fn alter_configs(
        &self,
        topic: &str,
        configs: Vec<(String, String)>,
    ) -> IoResult<()> {
        if self.topic_registry.get(topic).is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Unknown topic '{}'", topic),
            ));
        }
        let record = crate::replication::MetadataRecord::TopicConfigChanged {
            topic: topic.to_string(),
            configs,
        };
        self.propose_metadata(record).await?;
        Ok(())
    }

    /// Merge-then-replace config update (Kafka `IncrementalAlterConfigs`): computes the
    /// new full config map from the topic's current one plus `upserts`/`deletes`, then
    /// proposes it the same way `alter_configs` does.
    pub async fn incremental_alter_configs(
        &self,
        topic: &str,
        upserts: Vec<(String, String)>,
        deletes: Vec<String>,
    ) -> IoResult<()> {
        let mut merged: std::collections::HashMap<String, String> = self
            .topic_registry
            .get(topic)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Unknown topic '{}'", topic),
                )
            })?
            .configs
            .clone();

        for key in &deletes {
            merged.remove(key);
        }
        for (key, value) in upserts {
            merged.insert(key, value);
        }

        let record = crate::replication::MetadataRecord::TopicConfigChanged {
            topic: topic.to_string(),
            configs: merged.into_iter().collect(),
        };
        self.propose_metadata(record).await?;
        Ok(())
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
            let leader_hw = meta_pm.high_watermark();
            // The metadata log is fenced by the controller's Raft term: leadership of this
            // log *is* controller leadership.
            let fencing_epoch = self.replication.get_epoch();
            tokio::spawn(async move {
                if let Err(e) = repl
                    .replicate_batch(
                        "__cluster_metadata",
                        0,
                        fencing_epoch,
                        leader_hw,
                        std::slice::from_ref(&frame_for_replication),
                    )
                    .await
                {
                    tracing::error!("propose_metadata: replicate_batch failed: {}", e);
                }
            });

            if self.config.min_insync_replicas > 1 {
                self.await_full_isr_ack(
                    &meta_pm,
                    "__cluster_metadata",
                    0,
                    frame.offset,
                    std::time::Duration::from_secs(5),
                )
                .await?;
            }

            // Same reasoning as the produce path: the push carried the pre-commit
            // watermark, so followers need the committed point delivered after the fact.
            // `__cluster_metadata` has no pull fetcher at all (it is deliberately excluded,
            // to avoid applying records through two paths at once), so this broadcast is
            // the only thing that ever advances a follower's metadata watermark.
            meta_pm.advance_committed_hw(frame.offset + 1);
            let repl = self.replication.clone();
            let committed_hw = meta_pm.high_watermark();
            let hw_fencing_epoch = self.replication.get_epoch();
            tokio::spawn(async move {
                repl.broadcast_high_watermark(
                    "__cluster_metadata",
                    0,
                    hw_fencing_epoch,
                    committed_hw,
                )
                .await;
            });
        }

        Ok(frame.offset)
    }

    /// `JoinGroup` with the rebalance barrier applied: registers the member, then holds
    /// the response until the group's join window closes.
    ///
    /// Returning immediately — as this used to — meant a member's assignment only ever
    /// covered whoever happened to have joined at that instant. Because each arrival
    /// formed its own generation and bumped `generation_id`, it invalidated the assignment
    /// just handed to the previous joiner and forced it to rejoin, so a group starting up
    /// produced roughly one rebalance per member and under churn could fail to converge at
    /// all, leaving partitions unassigned and consumption stalled.
    ///
    /// The group lock is never held across an await: the coordinator is polled for the
    /// remaining window, this sleeps outside the lock, then re-checks.
    pub async fn join_group_awaited(
        &self,
        group_id: &str,
        member_id: &str,
        protocols: Vec<String>,
    ) -> Result<(String, u32, bool, String), String> {
        let coordinator = self.group_coordinator();
        let (m_id, generation_id, is_leader, protocol_name) =
            coordinator.join_group(group_id, member_id, protocols)?;

        // Bound the total wait so a pathological extension chain can't pin a connection.
        let max_wait = coordinator.initial_rebalance_delay() * 3;
        let started = std::time::Instant::now();
        while let Some(remaining) = coordinator.join_window_remaining(group_id) {
            if started.elapsed() >= max_wait {
                break;
            }
            // Wake at least every 25ms so a window extended by a late joiner is observed
            // promptly rather than slept through.
            tokio::time::sleep(remaining.min(std::time::Duration::from_millis(25))).await;
        }
        coordinator.close_join_window(group_id);

        // Re-read after the barrier: the generation this member ends up in is the window's
        // generation, which may have been formed by an earlier joiner, and leadership may
        // have settled on a different member.
        match coordinator.join_result(group_id, &m_id) {
            Some((final_generation, final_is_leader, final_protocol)) => Ok((
                m_id,
                final_generation,
                final_is_leader,
                final_protocol,
            )),
            // Group vanished while we waited (every member's session expired).
            None => Ok((m_id, generation_id, is_leader, protocol_name)),
        }
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

    /// Aborts any transaction that has sat in a non-terminal state
    /// (`Ongoing`/`PrepareCommit`/`PrepareAbort`) longer than `config.transaction_timeout_ms`
    /// (Kafka `transaction.timeout.ms`). Meant to be called periodically (see
    /// `Server::run_with_listener`'s background task loop).
    ///
    /// This is also what makes a hanging transaction restored from `__transaction_state` on
    /// startup (`replay_transaction_state`) eventually get unstuck: before this existed, a
    /// transaction that was `Ongoing` at the moment of a crash/restart stayed that way
    /// forever, since nothing ever aborted it — permanently pinning the Last Stable Offset
    /// for every partition it touched and blocking `ReadCommitted` consumers from reading
    /// anything past that point, even data produced long after the restart. Restored
    /// transactions get `created_at_ms` reset to the restart time by `restore_transaction`,
    /// so they get the same grace window as any other in-flight transaction before this
    /// sweep gives up on them — enough time for a reconnecting producer (via `InitProducerId`
    /// with a bumped epoch) to resume and properly commit/abort it first, if one shows up.
    pub fn sweep_expired_transactions(&self) {
        let expired = self
            .transactions
            .expired_ongoing_transaction_ids(self.config.transaction_timeout_ms);
        for tx_id in expired {
            tracing::warn!(
                "TxTimeout: transaction '{}' exceeded transaction.timeout.ms ({} ms) — aborting.",
                tx_id,
                self.config.transaction_timeout_ms
            );
            if let Err(e) = self.abort_transaction(&tx_id) {
                tracing::error!("TxTimeout: failed to abort expired transaction '{}': {}", tx_id, e);
            }
        }
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
        // Enforce `message.max.bytes` before doing any disk work: an oversized record
        // should be rejected outright rather than partially written and then discovered
        // to be too large, and rejecting the whole batch keeps the offsets contiguous
        // (no half-applied batch).
        if let Some(max_bytes) = self.config.message_max_bytes {
            if let Some(oversized) = records.iter().find(|r| r.len() as u64 > max_bytes) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Record of {} bytes exceeds message.max.bytes ({})",
                        oversized.len(),
                        max_bytes
                    ),
                ));
            }
        }

        if num_partitions > self.config.max_partitions_per_topic {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Requested {} partitions for topic '{}' exceeds max.partitions.per.topic ({})",
                    num_partitions, topic, self.config.max_partitions_per_topic
                ),
            ));
        }

        let partition_id = if !key.is_empty() && num_partitions > 0 {
            hash_key(key.as_bytes(), num_partitions as usize)
        } else {
            0
        };

        // Client-driven creation: honors `auto.create.topics.enable` (see
        // `get_or_create_partition_for_client`).
        let pm = self.get_or_create_partition_for_client(topic, partition_id)?;

        // The actual disk work (segment append per record, plus the batch's group-commit
        // fsync) is synchronous, lock-and-syscall-heavy I/O. Running it inline in this
        // `async fn` would block whichever Tokio worker thread picked up this task for the
        // whole duration — under disk contention, that worker can't service any other
        // task in the meantime. `spawn_blocking` moves it to Tokio's dedicated blocking
        // thread pool instead, so a slow disk only stalls this one request.
        let pm_blocking = pm.clone();
        let records_owned: Vec<Bytes> = records.to_vec();
        let num_records = records_owned.len();
        let (first_offset, last_offset, frames) =
            tokio::task::spawn_blocking(move || -> IoResult<(u64, u64, Vec<RecordFrame>)> {
                let mut first_offset = 0u64;
                let mut last_offset = 0u64;
                let mut frames = Vec::with_capacity(num_records);
                let mut current_seq = base_sequence;

                for (idx, record) in records_owned.iter().enumerate() {
                    match pm_blocking.produce_frame_eos(
                        record.clone(),
                        producer_id,
                        producer_epoch,
                        current_seq,
                    )? {
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
                                first_offset = last_offset.saturating_sub(num_records as u64 - 1);
                            }
                        }
                    }
                    if producer_id != 0 {
                        current_seq += 1;
                    }
                }

                // Group commit: one fsync for the whole batch instead of one per record
                // (see `PartitionManager::flush_if_sync_policy`). `produce_frame_eos`
                // itself never syncs for exactly this reason.
                pm_blocking.flush_if_sync_policy()?;

                Ok((first_offset, last_offset, frames))
            })
            .await
            .map_err(|e| std::io::Error::other(format!("produce_batch join error: {}", e)))??;

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
        //
        // Data-topic replication is now Kafka-style follower-pull *in addition to*
        // leader-push, not a full replacement of it. Every follower's background fetch
        // loop (`ReplicationManager::start_per_partition_fetcher_manager`) independently
        // pulls from this partition's log, and the leader's `handler.rs` 0xBB handler
        // (`decode_grpc_replication_fetch_packet`) records each follower's confirmed
        // progress into the exact same `replica_watermarks`/`replica_ack_time` maps this
        // push ack updates below — `await_isr_quorum` doesn't care which mechanism
        // populated them.
        //
        // Push stays as a backstop rather than being fully retired because pull discovery
        // depends on `describe_topic`/`list_topics()`, which requires a real replica
        // assignment recorded in `__cluster_metadata` (via `create_topic`). A topic that's
        // only ever been implicitly auto-created by a bare produce (no explicit
        // `CreateTopic`) never gets that assignment propagated, so a follower can never
        // discover — and therefore never pull — a partition it doesn't already know it
        // replicates. `append_replica_frame_verbatim` is idempotent/gap-safe by design
        // (`AlreadyApplied`/`Gap` are both no-ops, never corruption), so having both
        // mechanisms active for a partition pull *can* reach is redundant, not unsafe.
        if self.is_partition_leader(topic, partition_id) && !self.config.peer_addrs.is_empty() {
            let repl = self.replication.clone();
            let topic_str = topic.to_string();
            let topic_for_spawn = topic_str.clone();
            let frames_clone = frames.clone();
            let leader_hw = pm.high_watermark();
            // A data partition is fenced by its own leader epoch, never the controller term.
            let fencing_epoch = pm.leader_epoch() as u64;
            tokio::spawn(async move {
                if let Err(e) = repl
                    .replicate_batch(
                        &topic_for_spawn,
                        partition_id,
                        fencing_epoch,
                        leader_hw,
                        &frames_clone,
                    )
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
            if self.effective_min_insync_replicas(topic) > 1 {
                self.await_full_isr_ack(
                    &pm,
                    &topic_str,
                    partition_id,
                    last_offset,
                    std::time::Duration::from_secs(5),
                )
                .await?;
            }
        }

        // Reached only once quorum (if required) has been confirmed above — or
        // immediately for single-node/no-peer deployments and non-partition-leader
        // system-partition writes, where there's nothing else to wait on.
        pm.advance_committed_hw(last_offset + 1);

        // Tell the followers about the newly committed point. The push that delivered this
        // batch necessarily carried the *previous* watermark (a batch isn't committed until
        // the ISR has it), so without this the records would sit replicated-but-unreadable
        // on every follower until some later write happened to push the watermark along.
        if self.is_partition_leader(topic, partition_id) && !self.config.peer_addrs.is_empty() {
            let repl = self.replication.clone();
            let topic_for_hw = topic.to_string();
            let committed_hw = pm.high_watermark();
            let fencing_epoch = pm.leader_epoch() as u64;
            tokio::spawn(async move {
                repl.broadcast_high_watermark(
                    &topic_for_hw,
                    partition_id,
                    fencing_epoch,
                    committed_hw,
                )
                .await;
            });
        }

        Ok((partition_id, first_offset, last_offset))
    }

    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let Some(pm) = self.partition_for_read(topic, partition)? else {
            return Ok(Vec::new()); // unknown topic — empty, without creating it
        };
        // Clamp to the committed high watermark, not LEO: consumers must never be shown
        // data that isn't yet guaranteed replicated to the ISR (previously `fetch` exposed
        // everything up to LEO unconditionally, so a leader crash right after an
        // un-replicated append could mean a consumer read something no other replica ever
        // received).
        //
        // The actual segment read is blocking file I/O (mutex + syscalls) — moved to
        // Tokio's blocking thread pool so a slow disk doesn't stall this worker thread
        // out from under every other in-flight request.
        //
        // This intentionally still returns control-marker frames (magic ==
        // CONTROL_MAGIC_BYTE) alongside real records — matching real Kafka, where the
        // server exposes raw control batches at the wire level and it's the client
        // library's job to recognize and skip them (see `RecordFrame::is_control_marker`
        // and the client-authoring guidance in docs/HERMES_CLIENT_CREATOR_REFERENCE.md).
        // `fetch_committed` is the path that hides them for callers that want that done
        // for them. Filtering here too would break raw log introspection (this is also
        // exactly what the transactional-recovery test suite relies on to verify control
        // markers were actually durably written).
        tokio::task::spawn_blocking(move || {
            let hw = pm.high_watermark();
            let frames = pm.fetch(offset, max_bytes)?;
            Ok(frames.into_iter().filter(|f| f.offset < hw).collect())
        })
        .await
        .map_err(|e| std::io::Error::other(format!("fetch join error: {}", e)))?
    }

    /// Plans a zero-copy fetch for the plain `Fetch` command: same offset/high-watermark
    /// semantics as `fetch` (clamped to the committed HW, single segment only), but instead
    /// of reading frame bytes into a `Vec<RecordFrame>`, resolves the exact on-disk byte
    /// range so the caller can stream it straight to the socket via `TransmitFile`/
    /// `sendfile` without copying payload bytes through a Rust buffer.
    ///
    /// Returns `Ok(None)` whenever there's nothing eligible for a zero-copy transmit (no
    /// frames at/after `offset` within the committed HW, or the range doesn't start within
    /// the located segment) — callers should fall back to the buffered `fetch` path in that
    /// case, not treat it as an error.
    pub async fn plan_zero_copy_fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Option<crate::segment::ZeroCopyFetchPlan>> {
        let Some(pm) = self.partition_for_read(topic, partition)? else {
            return Ok(None); // unknown topic — nothing to stream, and nothing to create
        };
        tokio::task::spawn_blocking(move || pm.plan_zero_copy_fetch(offset, max_bytes))
            .await
            .map_err(|e| std::io::Error::other(format!("plan_zero_copy_fetch join error: {}", e)))?
    }

    /// BUG-02: Fetch records starting from nearest offset for target_timestamp
    pub async fn fetch_by_timestamp(
        &self,
        topic: &str,
        partition: u32,
        target_timestamp: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let Some(pm) = self.partition_for_read(topic, partition)? else {
            return Ok(Vec::new());
        };
        tokio::task::spawn_blocking(move || pm.fetch_by_timestamp(target_timestamp, max_bytes))
            .await
            .map_err(|e| std::io::Error::other(format!("fetch_by_timestamp join error: {}", e)))?
    }

    pub async fn fetch_committed(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let Some(pm) = self.partition_for_read(topic, partition)? else {
            return Ok(Vec::new());
        };
        let lso = self.transactions.last_stable_offset(topic, partition);
        let aborted = self.transactions.aborted_ranges(topic, partition);
        let all_frames = self.fetch(topic, partition, offset, max_bytes).await?;

        let pm_blocking = pm.clone();
        let committed_frames: Vec<RecordFrame> = tokio::task::spawn_blocking(move || {
            all_frames
                .into_iter()
                .filter(|frame| {
                    if frame.magic == CONTROL_MAGIC_BYTE {
                        return false;
                    }
                    if frame.offset >= lso {
                        return false;
                    }
                    if pm_blocking.is_offset_aborted(frame.offset) {
                        return false;
                    }
                    for (start, end) in &aborted {
                        if frame.offset >= *start && frame.offset <= *end {
                            return false;
                        }
                    }
                    true
                })
                .collect()
        })
        .await
        .map_err(|e| std::io::Error::other(format!("fetch_committed join error: {}", e)))?;

        Ok(committed_frames)
    }

    pub fn seek(&self, topic: &str, partition: u32, offset: u64) -> IoResult<Option<(u64, u64)>> {
        match self.partition_for_read(topic, partition)? {
            Some(pm) => Ok(pm.seek(offset)),
            None => Ok(None),
        }
    }

    pub fn latest_offset(&self, topic: &str, partition: u32) -> IoResult<u64> {
        match self.partition_for_read(topic, partition)? {
            Some(pm) => Ok(pm.latest_offset()),
            None => Ok(0),
        }
    }

    /// Commits a consumer group offset. `ConsumerGroupManager::commit_offset` does a
    /// synchronous disk write plus `fsync` on every single call (offset commits are
    /// frequent — once per consumer poll cycle, for every consumer group on the broker),
    /// so this runs on Tokio's blocking thread pool rather than inline on the async
    /// runtime's worker threads, same reasoning as the produce/fetch paths.
    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> IoResult<()> {
        let consumer_groups = self.consumer_groups.clone();
        let group_id = group_id.to_string();
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || {
            consumer_groups.commit_offset(&group_id, &topic, partition, offset)
        })
        .await
        .map_err(|e| std::io::Error::other(format!("commit_offset join error: {}", e)))?
    }

    /// Same blocking-pool reasoning as `commit_offset`.
    pub async fn commit_offset_with_metadata(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        metadata: &str,
    ) -> IoResult<()> {
        let consumer_groups = self.consumer_groups.clone();
        let group_id = group_id.to_string();
        let topic = topic.to_string();
        let metadata = metadata.to_string();
        tokio::task::spawn_blocking(move || {
            consumer_groups.commit_offset_with_metadata(&group_id, &topic, partition, offset, &metadata)
        })
        .await
        .map_err(|e| std::io::Error::other(format!("commit_offset_with_metadata join error: {}", e)))?
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

    /// Runs retention/compaction across every partition, fanning out up to
    /// `config.compaction_worker_threads` partitions' `apply_retention()` calls
    /// concurrently (Kafka-style `log.cleaner.threads`) instead of one sequential loop.
    /// A slow or large compaction pass on one partition no longer delays every other
    /// partition's GC within the same tick, and one partition's error no longer aborts
    /// the rest (each is caught, logged, and skipped independently).
    pub async fn apply_retention_all(&self) -> IoResult<usize> {
        let concurrency = self.config.compaction_worker_threads.max(1);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

        let partition_managers: Vec<Arc<PartitionManager>> =
            self.partitions.iter().map(|entry| entry.value().clone()).collect();

        let mut join_set = tokio::task::JoinSet::new();
        for pm in partition_managers {
            let permit_sem = semaphore.clone();
            join_set.spawn(async move {
                let _permit = permit_sem
                    .acquire_owned()
                    .await
                    .expect("compaction worker semaphore is never closed");
                tokio::task::spawn_blocking(move || pm.apply_retention())
                    .await
                    .map_err(|e| std::io::Error::other(format!("apply_retention join error: {}", e)))?
            });
        }

        let mut total_removed = 0usize;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(n)) => total_removed += n,
                Ok(Err(e)) => tracing::error!("Retention GC: partition compaction failed: {}", e),
                Err(e) => tracing::error!("Retention GC: partition compaction task panicked: {}", e),
            }
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

                // Piggybacked on the same tick rather than a dedicated task: cheap to
                // check (a no-op unless enough new records have accumulated) and doesn't
                // need its own timer. Snapshotting/trimming is blocking file I/O, so it
                // runs on Tokio's blocking pool rather than inline on this worker thread.
                let engine_for_blocking = engine.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    engine_for_blocking.snapshot_metadata_if_needed();
                })
                .await;
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
        for pm in self.partitions_for_topic(topic) {
            pm.set_cleanup_policy(policy);
        }
    }

    /// Returns list of all active non-system topics (Sprint 5).
    ///
    /// Runs in time proportional to the number of *topics*, not the number of partitions —
    /// `topic_partitions` carries one entry per topic no matter how many partitions it
    /// has, unlike iterating `partitions` directly (one entry per topic-partition, so a
    /// broker with many partitions per topic previously paid for all of them on every call).
    pub fn list_topics(&self) -> Vec<String> {
        let mut topics = std::collections::HashSet::new();

        for entry in self.topic_registry.iter() {
            let topic = entry.key();
            if !topic.starts_with("__") && !self.deleting_topics.contains(topic) {
                topics.insert(topic.clone());
            }
        }

        for entry in self.topic_partitions.iter() {
            let topic = entry.key();
            if !topic.starts_with("__") && !self.deleting_topics.contains(topic) {
                topics.insert(topic.clone());
            }
        }

        let mut vec: Vec<_> = topics.into_iter().collect();
        vec.sort();
        vec
    }

    /// Resolves the leader for a partition **without creating anything**: from the open
    /// partition if there is one, else from the replicated topic assignment, else `None`
    /// for a topic this broker has never heard of.
    ///
    /// These predicates are reachable by any client naming any topic (every `Fetch` calls
    /// one), so they must not have the side effect of materializing partition directories
    /// on disk — that turned a read of a nonexistent topic into a write.
    fn resolve_partition_leader(&self, topic: &str, partition: u32) -> Option<u32> {
        if let Some(pm) = self.get_partition(topic, partition) {
            return Some(pm.leader_id());
        }
        self.topic_registry
            .get(topic)
            .and_then(|cfg| cfg.partitions.get(&partition).map(|a| a.leader_id))
    }

    /// Returns true if this broker node is the active leader for the specified partition.
    ///
    /// A topic that is entirely unknown (not open, not in the registry) resolves to this
    /// node, preserving the single-broker default where the local node leads anything it
    /// is asked about — the produce path is what then decides, via
    /// `get_or_create_partition_for_client`, whether the topic may actually be created.
    pub fn is_partition_leader(&self, topic: &str, partition: u32) -> bool {
        match self.resolve_partition_leader(topic, partition) {
            Some(leader_id) => leader_id == self.config.node_id,
            None => true,
        }
    }

    /// Returns the node_id currently registered as leader for the specified partition,
    /// if the partition has been initialized locally or is assigned in cluster metadata.
    pub fn partition_leader_id(&self, topic: &str, partition: u32) -> Option<u32> {
        self.resolve_partition_leader(topic, partition)
    }

    /// Registers a broker socket address mapping (node_id -> bind_addr)
    pub fn register_broker_address(&self, node_id: u32, bind_addr: String) {
        self.broker_addrs.insert(node_id, bind_addr);
    }

    /// Unregisters a broker socket address mapping
    pub fn unregister_broker_address(&self, node_id: u32) {
        self.broker_addrs.remove(&node_id);
        self.broker_roles.remove(&node_id);
    }

    /// Records a broker's process role(s). Empty/unrecognized bytes mean "unknown role" —
    /// treated as combined mode (both roles) by `crate::config::parse_process_role_bytes`.
    pub fn register_broker_roles(&self, node_id: u32, role_bytes: &[u8]) {
        self.broker_roles
            .insert(node_id, crate::config::parse_process_role_bytes(role_bytes));
    }

    /// Returns whether `node_id` is known to have the `Broker` role. Nodes never seen
    /// (e.g. this one, before its own bootstrap insert, or a node we simply haven't
    /// learned about yet) are conservatively treated as broker-eligible — combined mode,
    /// the safe default, rather than silently excluding an unrecognized node from ever
    /// receiving partition assignments.
    pub fn is_broker_eligible(&self, node_id: u32) -> bool {
        self.broker_roles
            .get(&node_id)
            .map(|roles| roles.contains(&crate::config::ProcessRole::Broker))
            .unwrap_or(true)
    }

    /// Dynamically registers a broker in the cluster metadata catalog and replicates to
    /// peers. This is the manual/admin registration path (`RegisterBroker` wire command);
    /// it doesn't know the target node's actual process roles, so it leaves `roles` empty
    /// — which `apply_metadata_record`/`parse_process_role_bytes` treats as "unknown,
    /// assume combined mode". A node with dedicated controller-only/broker-only roles
    /// self-declares them authoritatively via its own heartbeat ACKs instead (see
    /// `send_leader_heartbeat`), which takes precedence once received.
    pub async fn register_broker(&self, node_id: u32, bind_addr: String) -> IoResult<()> {
        let record = crate::replication::MetadataRecord::BrokerRegister {
            node_id,
            bind_addr,
            roles: Vec::new(),
        };
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

    /// Returns node_ids eligible to be assigned data-partition leadership/replicas —
    /// i.e. known brokers with the `Broker` role. A controller-only node must never be
    /// handed a data partition, so it's excluded here even if it's this node itself.
    fn available_broker_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self
            .broker_addrs
            .iter()
            .map(|entry| *entry.key())
            .filter(|&id| self.is_broker_eligible(id))
            .collect();
        if self.config.is_broker_role() && !ids.contains(&self.config.node_id) {
            ids.push(self.config.node_id);
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Returns true if this broker node is a leader or registered replica hosting the
    /// specified partition (KIP-392 Follower Fetch). Non-creating, for the same reason as
    /// `is_partition_leader` — this is on the `Fetch` path.
    pub fn is_partition_replica(&self, topic: &str, partition: u32) -> bool {
        if let Some(pm) = self.get_partition(topic, partition) {
            return pm.is_leader(self.config.node_id)
                || pm.replicas().contains(&self.config.node_id);
        }
        if let Some(cfg) = self.topic_registry.get(topic) {
            if let Some(assign) = cfg.partitions.get(&partition) {
                return assign.leader_id == self.config.node_id
                    || assign.replicas.contains(&self.config.node_id);
            }
        }
        // Entirely unknown topic: treat as local, same rationale as `is_partition_leader`.
        true
    }

    /// Returns metadata and initialized partition high watermarks for a topic.
    ///
    /// Looks up only this topic's own partitions via `topic_partitions` instead of
    /// scanning every partition of every topic on the broker.
    pub fn describe_topic(
        &self,
        topic: &str,
    ) -> Option<Vec<crate::protocol::wire::DescribedPartition>> {
        if self.deleting_topics.contains(topic) {
            return None;
        }

        let reg_config = self.topic_registry.get(topic).map(|r| r.value().clone());
        let mut partitions_map = std::collections::HashMap::new();

        if let Some(partition_ids) = self.topic_partitions.get(topic) {
            for p in partition_ids.iter() {
                let p = *p;
                if let Some(pm) = self.partitions.get(&(topic.to_string(), p)) {
                    let hw = pm.high_watermark();
                    let leader_id = pm.leader_id();
                    let replicas = pm.replicas();
                    partitions_map.insert(p, (hw, leader_id, replicas));
                }
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

    /// Deletes topic partitions and removes disk directory (NEW-03).
    ///
    /// `apply_metadata_record` has no error channel (it's shared with the follower replay
    /// path, where there's no client to report to), so the actual filesystem removal in
    /// `apply_topic_deletion` can only log its failures. That previously meant a delete
    /// that left directories behind — the common case on Windows, where `remove_dir_all`
    /// fails with a sharing violation while any handle into the directory is still open —
    /// reported success to the client anyway. So this verifies the outcome directly after
    /// the record has been applied, and reports a failure the caller can actually see.
    pub async fn delete_topic(&self, topic: &str) -> IoResult<()> {
        let record = crate::replication::MetadataRecord::TopicDeleted {
            topic: topic.to_string(),
        };
        self.propose_metadata(record).await?;

        let leftover = self.topic_partition_dirs(topic);
        if !leftover.is_empty() {
            return Err(std::io::Error::other(format!(
                "Topic '{}' metadata was deleted, but {} partition director{} could not be \
                 removed from disk (still in use?): {}",
                topic,
                leftover.len(),
                if leftover.len() == 1 { "y" } else { "ies" },
                leftover
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
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

        let partition_ids: Vec<u32> = self
            .topic_partitions
            .get(topic)
            .map(|ids| ids.iter().map(|p| *p).collect())
            .unwrap_or_default();
        for p in partition_ids {
            let key = (topic.to_string(), p);
            if let Some((_, pm)) = self.partitions.remove(&key) {
                let _ = pm.flush();
            }
        }
        self.topic_partitions.remove(topic);

        let mut err = None;
        for path in self.topic_partition_dirs(topic) {
            if let Err(e) = remove_dir_all_with_retry(&path) {
                tracing::error!(
                    "Topic deletion: failed to remove partition directory {}: {}",
                    path.display(),
                    e
                );
                err = Some(e);
                // Keep going rather than bailing on the first failure — one stuck
                // partition directory shouldn't leave every *other* partition of the same
                // topic behind on disk too. The first error is still returned below.
            }
        }

        self.deleting_topics.remove(topic);

        if let Some(e) = err {
            return Err(e);
        }
        Ok(())
    }

    /// Every on-disk `"{topic}-{partition}"` directory belonging to `topic`.
    fn topic_partition_dirs(&self, topic: &str) -> Vec<std::path::PathBuf> {
        let mut dirs = Vec::new();
        let prefix = format!("{}-", topic);
        if let Ok(entries) = std::fs::read_dir(&self.config.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                // Guard against a prefix collision: "orders-2" belongs to topic "orders",
                // but "orders-eu-0" belongs to the *different* topic "orders-eu" and must
                // not be swept up by a delete of "orders".
                if let Some(suffix) = name.strip_prefix(&prefix) {
                    if suffix.parse::<u32>().is_ok() {
                        dirs.push(path);
                    }
                }
            }
        }
        dirs
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
