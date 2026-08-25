use crate::config::EngineConfig;
use crate::consumer_group::ConsumerGroupManager;
use crate::protocol::RecordBatch;
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
/// Parses a numeric topic-config value into the `Option<u64>` its setter expects.
///
/// `Some(None)` is an explicit clear (empty value), `Some(Some(n))` a real value, and
/// `None` means "unparseable — leave the current setting alone", which is what keeps a bad
/// value from silently disabling a setting.
fn parse_optional_u64_config(value: &str) -> Option<Option<u64>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Some(None);
    }
    trimmed.parse::<u64>().ok().map(Some)
}

/// Kafka-style striped replica placement (`AdminUtils.assignReplicasToBrokers` /
/// `StripedReplicaPlacer`).
///
/// Plain round-robin — `replicas[i] = brokers[(partition + i) % n]`, which is what this
/// used to do — makes broker *i*'s follower always broker *i+1*. Losing two adjacent
/// brokers then loses whole partitions outright, and every partition led by a given broker
/// piles its followers onto the same peers. Striping shifts the non-leader replicas by a
/// stride that advances every `n` partitions, so consecutive partitions place their
/// followers on different brokers and the loss of any two brokers costs far fewer
/// partitions.
///
/// `seed` replaces Kafka's random start index. Kafka randomizes so that every topic does
/// not begin at broker 0; deriving the offset from the topic name instead gives the same
/// spread across topics while keeping a single topic's layout reproducible — which matters
/// because assignment here can be recomputed by a sweep rather than only at creation.
///
/// `pinned_leader` forces `replicas[0]`, used when assigning a partition that already holds
/// data: its current holder must remain leader so assignment never moves bytes. Followers
/// are still striped around it.
fn striped_replicas(
    brokers: &[u32],
    partition: u32,
    replication_factor: usize,
    seed: u64,
    pinned_leader: Option<u32>,
) -> Vec<u32> {
    let n = brokers.len();
    if n == 0 || replication_factor == 0 {
        return Vec::new();
    }
    let rf = replication_factor.min(n);

    let start_index = (seed % n as u64) as usize;
    let first_index = match pinned_leader.and_then(|l| brokers.iter().position(|&b| b == l)) {
        Some(idx) => idx,
        None => (partition as usize + start_index) % n,
    };

    let mut replicas = Vec::with_capacity(rf);
    replicas.push(brokers[first_index]);
    if n == 1 {
        return replicas; // no other broker to place a follower on
    }

    // Advances every `n` partitions so that the follower stride differs between
    // consecutive rounds rather than repeating the same leader/follower pairing.
    let shift_round = (partition as usize / n) + (seed / n as u64) as usize;
    for j in 0..rf.saturating_sub(1) {
        let shift = 1 + (shift_round + j) % (n - 1);
        let replica = brokers[(first_index + shift) % n];
        if !replicas.contains(&replica) {
            replicas.push(replica);
        }
    }

    // A stride collision can leave us short of the requested factor; fill deterministically
    // rather than silently returning fewer replicas than the caller asked for.
    if replicas.len() < rf {
        for &b in brokers {
            if replicas.len() >= rf {
                break;
            }
            if !replicas.contains(&b) {
                replicas.push(b);
            }
        }
    }
    replicas
}

/// Stable per-topic seed for `striped_replicas`, so different topics start at different
/// brokers while one topic's layout stays reproducible.
fn topic_placement_seed(topic: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a
    for byte in topic.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// Reads this broker's own certificate and derives its `tls-server-end-point` channel
/// binding. `None` when TLS is not configured or the certificate cannot be read — either
/// way there is nothing to bind to, which is what keeps the `-PLUS` mechanisms unadvertised
/// rather than advertised and unusable.
fn compute_tls_channel_binding(config: &EngineConfig) -> Option<Vec<u8>> {
    let path = config.ssl_cert_path.as_ref()?;
    let pem = std::fs::read(path).ok()?;
    // Take the first certificate in the file: the leaf, which is the one the peer binds to.
    let der = rustls_pemfile::certs(&mut pem.as_slice()).next()?.ok()?;
    Some(crate::scram::tls_server_end_point(&der))
}

/// Rejects an address that cannot function as this node's advertised identity: anything
/// that doesn't parse as `host:port`, an unspecified IP (`0.0.0.0` / `::`, matched by
/// `SocketAddr::ip().is_unspecified()`), or port `0`. None of these can be dialed back by
/// a peer, so advertising one is strictly worse than refusing — see
/// `StorageEngine::finalize_advertised_addr` and issue #62.
fn validate_advertised_addr(addr: &str) -> Result<(), &'static str> {
    let parsed: std::net::SocketAddr = addr.parse().map_err(|_| "not a valid host:port address")?;
    if parsed.ip().is_unspecified() {
        return Err("its IP is unspecified (a wildcard bind address, not a dialable identity)");
    }
    if parsed.port() == 0 {
        return Err("its port is 0 (an unbound ephemeral placeholder, not a real port)");
    }
    Ok(())
}

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
    /// The batch exactly as the producer built it, compressed in the producer's own
    /// codec. The broker stores these bytes; it does not decode the records inside.
    /// `producer_id`, `producer_epoch` and `base_sequence` are read from the batch's
    /// plaintext header rather than passed alongside it.
    pub batch: RecordBatch,
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
    /// SCRAM credentials keyed by `(username, mechanism)`. A credential is derived under one
    /// specific hash and cannot verify an exchange under another, so a user may hold a
    /// SHA-256 and a SHA-512 credential simultaneously and each is stored separately.
    scram_credentials:
        Arc<DashMap<(String, crate::scram::ScramMechanism), crate::scram::ScramCredential>>,
    metrics: Arc<crate::server::metrics::MetricsCollector>,
    /// This broker's `tls-server-end-point` channel-binding value, computed once from its
    /// own certificate. `None` when TLS is not configured, which is also what disables the
    /// `-PLUS` mechanisms: a binding cannot be offered without a certificate to bind to.
    tls_channel_binding: Option<Vec<u8>>,
    /// Wall-clock time the current bout of "peers configured but undiscovered" began —
    /// shared between `ensure_topic_created` and `reconcile_unassigned_partitions` since
    /// they defer for the identical reason (issue #62) and should escalate together
    /// rather than tracking independent timers. `None` while discovery is complete (or no
    /// peers are configured); reset back to `None` the moment discovery completes, so a
    /// *later* recurrence (e.g. a peer flaps) gets a fresh grace period rather than
    /// warning again immediately from stale state.
    undiscovered_peers_since: Arc<parking_lot::Mutex<Option<std::time::Instant>>>,
    /// Whether the `warn!` escalation has already fired for the current bout tracked by
    /// `undiscovered_peers_since`. Set the moment it fires, cleared alongside
    /// `undiscovered_peers_since` once discovery completes — so the warning fires once per
    /// stuck bout, not on every sweep.
    undiscovered_peers_warned: Arc<std::sync::atomic::AtomicBool>,
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
        let group_coordinator = Arc::new(GroupCoordinator::with_config(
            std::time::Duration::from_millis(config.group_initial_rebalance_delay_ms),
            std::time::Duration::from_millis(config.max_poll_interval_ms),
        ));
        let quota = Arc::new(QuotaManager::new(
            config.produce_quota_bytes_per_sec,
            config.fetch_quota_bytes_per_sec,
        ));
        let acl = Arc::new(crate::server::acl::AclManager::new());
        let scram_credentials = Arc::new(DashMap::new());
        let metrics = Arc::new(crate::server::metrics::MetricsCollector::new());

        let tls_channel_binding = compute_tls_channel_binding(&config);
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
            tls_channel_binding,
            undiscovered_peers_since: Arc::new(parking_lot::Mutex::new(None)),
            undiscovered_peers_warned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        engine
            .replication
            .start_per_partition_fetcher_manager(engine.clone());

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

    /// Corrects this node's advertised identity once the real bound TCP address is
    /// known. Called by `Server::bind` immediately after the listener binds.
    ///
    /// `ReplicationManager::new` (called from `StorageEngine::new`, well before the
    /// listener binds) has no way to know the real address yet — for the real server
    /// entry point, `bind_addr` is read from config before any socket exists, so it may
    /// be a wildcard host (`0.0.0.0`) or carry an ephemeral `:0` port. Every identity this
    /// node publishes up to this point (heartbeats, heartbeat ACKs, its own
    /// `BrokerRegister`) would otherwise carry that placeholder — unusable by a peer
    /// trying to dial back — for as long as it takes the periodic heartbeat loop to
    /// self-correct (issue #62). This closes that gap immediately instead.
    ///
    /// An explicit `advertised_addr` config override (Kafka's `advertised.listeners`)
    /// takes precedence over `bound_addr` when set — e.g. behind NAT or a load balancer,
    /// where the locally bound address isn't what a peer should dial.
    ///
    /// Returns `Err` (naming the config key to set) without changing anything, rather
    /// than advertising it, if the resolved address is unusable as an identity — an
    /// unspecified IP (`0.0.0.0`/`::`), port `0`, or simply not parseable as a
    /// `host:port`. Matches Kafka, which refuses to start in the equivalent case; here
    /// the caller (`Server::bind`) turns this into a startup-time `Err`, which is a hard
    /// failure in the real server entry point (`main.rs` propagates it and exits) without
    /// this function itself needing to reach for `std::process::exit`.
    pub fn finalize_advertised_addr(&self, bound_addr: String) -> Result<(), String> {
        let resolved = self.config.advertised_addr.clone().unwrap_or(bound_addr);
        if let Err(reason) = validate_advertised_addr(&resolved) {
            return Err(format!(
                "refusing to advertise '{}' as this node's identity: {}. Set \
                 `advertised.listeners` (EngineConfig::advertised_addr) to this node's \
                 real, externally-reachable address, or bind `listeners` (bind_addr) to a \
                 concrete host so the real bound address can be used instead.",
                resolved, reason
            ));
        }

        self.replication.set_advertised_addr(resolved.clone());
        self.broker_addrs
            .insert(self.config.node_id, resolved.clone());

        // (Re-)publish this node's own BrokerRegister with the corrected address. The
        // constructor already wrote one (if leader) using the placeholder address — this
        // supersedes it in every node's metadata log the same way any later record
        // supersedes an earlier one for the same key, and `build_metadata_snapshot_records`
        // always reads the live `broker_addrs` entry just updated above, so a
        // snapshot taken after this point never re-introduces the placeholder.
        if self.is_leader() {
            let reg_rec = crate::replication::MetadataRecord::BrokerRegister {
                node_id: self.config.node_id,
                bind_addr: resolved,
                roles: crate::config::roles_to_bytes(&self.config.roles),
            };
            if let Ok(meta_pm) = self.get_or_create_partition("__cluster_metadata", 0) {
                let _ = meta_pm.produce(&reg_rec.encode());
            }
        }

        // Now that this node's identity is real, it's safe to start announcing it — see
        // the deferral comment in `ReplicationManager::new`.
        self.replication.start_heartbeat_broadcasting_if_leader();

        Ok(())
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

    /// Whether this partition carries any transactional data whose visibility depends on
    /// transaction state — an in-flight transaction pinning its LSO, or a recorded aborted
    /// range.
    ///
    /// Used to keep the zero-copy fetch path (which cannot filter) away from partitions
    /// where filtering is what makes the answer correct. A partition that has never seen a
    /// transaction answers `false` and keeps the fast path.
    pub fn partition_has_transactional_data(&self, topic: &str, partition: u32) -> bool {
        if self.transactions.last_stable_offset(topic, partition) != u64::MAX {
            return true;
        }
        if !self
            .transactions
            .aborted_ranges(topic, partition)
            .is_empty()
        {
            return true;
        }
        self.get_partition(topic, partition)
            .map(|pm| pm.has_aborted_transactions())
            .unwrap_or(false)
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

    /// This broker's `tls-server-end-point` channel-binding value, or `None` when TLS is
    /// not configured. Also decides whether the `-PLUS` mechanisms are offered at all.
    pub fn tls_channel_binding(&self) -> Option<&[u8]> {
        self.tls_channel_binding.as_deref()
    }

    /// SASL mechanisms this broker offers, including the `-PLUS` variants when a channel
    /// binding is available. Advertising `-PLUS` without a certificate to bind to would
    /// promise a protection the server cannot actually verify.
    pub fn advertised_sasl_mechanisms(&self) -> Vec<String> {
        let mut mechs = self.config.sasl_mechanisms.clone();
        if self.tls_channel_binding.is_some() {
            for base in ["SCRAM-SHA-256", "SCRAM-SHA-512"] {
                if mechs.iter().any(|m| m.eq_ignore_ascii_case(base)) {
                    let plus = format!("{}-PLUS", base);
                    if !mechs.iter().any(|m| m.eq_ignore_ascii_case(&plus)) {
                        mechs.push(plus);
                    }
                }
            }
        }
        mechs
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
                    let payload = frame.value.clone().unwrap_or_default();
                    if let Ok(rec) = crate::replication::MetadataRecord::decode(&payload) {
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
                    replicas: Some(assign.replicas.clone()),
                });
            }
            if !cfg.configs.is_empty() {
                records.push(MetadataRecord::TopicConfigChanged {
                    topic: cfg.topic.clone(),
                    configs: cfg
                        .configs
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
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
                mechanism: cred.mechanism.to_byte(),
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

        if leo <= last_snapshot_offset || leo - last_snapshot_offset < MIN_NEW_RECORDS_FOR_SNAPSHOT
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
                tracing::error!(
                    "Metadata Snapshot: Failed to trim log after snapshot: {}",
                    e
                );
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
                replicas,
            } => {
                // Preserve the configured replica set; only the ISR moves here.
                //
                // `replicas` is the set of brokers that are *supposed* to hold this
                // partition; `isr` is the subset currently caught up. This used to assign
                // `replicas = isr.clone()`, collapsing the former into the latter — so a
                // replica that fell behind even briefly was not merely dropped from the
                // ISR, it was erased from the partition's roster entirely. It would then
                // never be fetched from, never re-admitted, and never counted toward
                // replication factor again, so every transient lag spike permanently
                // reduced durability and repeated shrink/expand cycles drove partitions
                // toward a replica set of one.
                //
                // A `PartitionLeadershipChange` record carries no replica set precisely
                // because it isn't meant to change one — reassignment is a separate
                // decision. So the existing roster is kept, from the registry if we have
                // it, else from the live partition, and only falls back to the ISR when
                // this broker has genuinely never seen the partition before (first
                // observation, where the ISR is the only roster information available).
                // An explicit roster on the record wins: that is the record *establishing*
                // a replica set (assignment or reassignment). Otherwise this is an
                // ordinary leadership/ISR update, which must leave the roster untouched.
                let known_replicas = replicas.filter(|r| !r.is_empty()).or_else(|| {
                    self.topic_registry
                        .get(&topic)
                        .and_then(|cfg| cfg.partitions.get(&partition).map(|a| a.replicas.clone()))
                        .filter(|r| !r.is_empty())
                        .or_else(|| {
                            self.get_partition(&topic, partition)
                                .map(|pm| pm.replicas())
                                .filter(|r| !r.is_empty())
                        })
                });
                let replicas = known_replicas.unwrap_or_else(|| isr.clone());

                // The ISR must always be a subset of the roster. If leadership hands us an
                // ISR member that isn't currently listed as a replica (e.g. a replica
                // added by a reassignment this node hasn't applied yet), widen the roster
                // rather than silently dropping that member.
                let mut replicas = replicas;
                for &node in &isr {
                    if !replicas.contains(&node) {
                        replicas.push(node);
                    }
                }

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
                mechanism,
            } => {
                self.apply_scram_credential_state(
                    username,
                    crate::scram::ScramMechanism::from_byte(mechanism),
                    iterations,
                    salt,
                    stored_key,
                    server_key,
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
            .filter_map(|p| {
                self.partitions
                    .get(&(topic.to_string(), *p))
                    .map(|e| e.clone())
            })
            .collect()
    }

    /// Validates a topic config map before it is proposed, so a bad value is reported to
    /// the client instead of being applied.
    ///
    /// The whole request is checked before any of it is proposed, so a partially-valid
    /// request can't leave a half-updated config behind.
    pub fn validate_topic_configs(configs: &[(String, String)]) -> Result<(), String> {
        for (key, value) in configs {
            let trimmed = value.trim();
            // An explicitly empty value means "clear this setting" and is always allowed.
            if trimmed.is_empty() {
                continue;
            }
            let invalid = |expected: &str| {
                Err(format!(
                    "Invalid value '{}' for '{}': expected {}",
                    value, key, expected
                ))
            };
            match key.as_str() {
                "retention.ms"
                | "retention.bytes"
                | "delete.retention.ms"
                | "segment.ms"
                | "segment.bytes"
                | "max.message.bytes" => {
                    if trimmed.parse::<u64>().is_err() {
                        return invalid("a non-negative integer");
                    }
                }
                "min.insync.replicas" => match trimmed.parse::<usize>() {
                    Ok(0) => return invalid("an integer >= 1"),
                    Ok(_) => {}
                    Err(_) => return invalid("an integer >= 1"),
                },
                "min.cleanable.dirty.ratio" => match trimmed.parse::<f64>() {
                    Ok(r) if (0.0..=1.0).contains(&r) => {}
                    _ => return invalid("a number between 0.0 and 1.0"),
                },
                "cleanup.policy" if trimmed.parse::<crate::config::CleanupPolicy>().is_err() => {
                    return invalid("'delete', 'compact', or 'compact,delete'");
                }
                "compression.type"
                    if trimmed.parse::<crate::config::CompressionCodec>().is_err() =>
                {
                    return invalid("'none', 'lz4', or 'zstd'");
                }
                // Unrecognized keys are stored as-is (they may be consumed elsewhere or by
                // a later version) rather than rejected.
                _ => {}
            }
        }
        Ok(())
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
            // An unparseable numeric value must never reach these setters as `None`.
            // `parse().ok()` used to do exactly that, so a typo in `retention.ms` didn't
            // fail — it silently turned retention *off*, and the topic then grew without
            // bound until the disk filled, with the client having received a success
            // response and the resulting state indistinguishable from a deliberate
            // "unlimited". `validate_topic_configs` rejects such values up front; an
            // explicitly empty value remains the way to clear a setting.
            if let Some(v) = configs_map.get("retention.ms") {
                if let Some(parsed) = parse_optional_u64_config(v) {
                    pm.set_retention_millis(parsed);
                }
            }
            if let Some(v) = configs_map.get("retention.bytes") {
                if let Some(parsed) = parse_optional_u64_config(v) {
                    pm.set_retention_bytes(parsed);
                }
            }
            if let Some(v) = configs_map.get("delete.retention.ms") {
                if let Some(parsed) = parse_optional_u64_config(v) {
                    pm.set_delete_retention_millis(parsed);
                }
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
            .map(|cfg| {
                cfg.configs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Full-replace config update (Kafka `AlterConfigs`). Only the cluster leader may
    /// propose — routes through the same `propose_metadata` gate as every other
    /// controller-plane mutation.
    pub async fn alter_configs(&self, topic: &str, configs: Vec<(String, String)>) -> IoResult<()> {
        if self.topic_registry.get(topic).is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Unknown topic '{}'", topic),
            ));
        }
        // Validate the whole request before proposing any of it, so a bad value is
        // reported rather than silently disabling the setting it names.
        Self::validate_topic_configs(&configs)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
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

        // Validate only what the caller is actually setting. The already-stored values are
        // left unchecked so a config written by an older build can't make every subsequent
        // incremental update fail.
        Self::validate_topic_configs(&upserts)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

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
            // ISR-only update: the roster is deliberately left alone.
            replicas: None,
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
            // Failover moves leadership, never the roster — offline replicas must stay
            // on it so they can rejoin when they return.
            replicas: None,
        };
        self.propose_metadata_unchecked(record).await
    }

    /// Core append+apply+replicate+quorum mechanics shared by every proposal path above.
    /// Callers are responsible for authorization — this function performs none.
    /// Waits until a majority of the controller quorum has durably acknowledged
    /// `__cluster_metadata` up to `offset`.
    ///
    /// Only controller-eligible peers count. A broker-only peer replicates the metadata log
    /// so it can learn topics and ACLs, but it never votes, so counting it would make the
    /// majority threshold wrong — too low, which is the dangerous direction.
    ///
    /// The leader counts itself: `propose_metadata_unchecked` forces its own append durable
    /// (via `PartitionManager::flush_durable`, unconditionally, not gated by the configured
    /// flush policy) before calling this, so the record is already durable in its own log
    /// by the time this is called.
    async fn await_metadata_commit(&self, offset: u64, timeout: std::time::Duration) -> bool {
        let controller_peers = self.config.effective_controller_peer_addrs();
        let quorum_size = controller_peers.len() + 1;
        let majority = quorum_size / 2 + 1;
        if majority <= 1 {
            // Sole controller: its own durable append already is a majority.
            return true;
        }

        let start = std::time::Instant::now();
        loop {
            let mut acked = 1usize; // this leader
            for peer in &controller_peers {
                if self
                    .replication
                    .replica_watermark("__cluster_metadata", 0, peer)
                    .is_some_and(|w| w >= offset)
                {
                    acked += 1;
                }
            }
            if acked >= majority {
                return true;
            }
            if start.elapsed() >= timeout {
                tracing::warn!(
                    "Metadata commit: only {} of the required {} controller(s) acknowledged \
                     offset {} within {:?} — not applying",
                    acked,
                    majority,
                    offset,
                    timeout
                );
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    async fn propose_metadata_unchecked(
        &self,
        record: crate::replication::MetadataRecord,
    ) -> IoResult<u64> {
        let meta_pm = self.get_or_create_partition("__cluster_metadata", 0)?;
        let frame = meta_pm.produce_frame(&record.encode())?;
        // Force this leader's own append durable regardless of the configured flush
        // policy — `produce_frame`'s internal `flush_if_sync_policy()` is a no-op under
        // the default `AsyncPeriodic` policy. `await_metadata_commit` below counts the
        // leader as an automatic ACK on the assumption that its own copy is already
        // durable; without this, that assumption was false by default (issue #24).
        meta_pm.flush_durable()?;

        if !self.config.peer_addrs.is_empty() {
            let repl = self.replication.clone();
            let entry_for_replication = crate::replication::EncodedEntry::from_batch(&frame);
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
                        &[],
                        std::slice::from_ref(&entry_for_replication),
                    )
                    .await
                {
                    tracing::error!("propose_metadata: replicate_batch failed: {}", e);
                }
            });

            // Majority commit gate.
            //
            // A metadata record used to be applied the instant it was appended locally,
            // before any peer had seen it. A record that only ever reached a minority was
            // still acted upon, so if leadership then moved to a node that never received
            // it the cluster ended up with two divergent views — different topic configs,
            // different partition assignments, different ACLs — with nothing to detect or
            // reconcile the split. Because metadata drives authorization and placement, the
            // divergence was not confined to the metadata layer: it changed who was allowed
            // to write and where data landed.
            if !self
                .await_metadata_commit(frame.base_offset, std::time::Duration::from_secs(5))
                .await
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "NOT_ENOUGH_CONTROLLERS: metadata record at offset {} was not \
                         acknowledged by a majority of the controller quorum",
                        frame.base_offset
                    ),
                ));
            }

            if self.config.min_insync_replicas > 1 {
                self.await_full_isr_ack(
                    &meta_pm,
                    "__cluster_metadata",
                    0,
                    frame.base_offset,
                    std::time::Duration::from_secs(5),
                )
                .await?;
            }

            // Same reasoning as the produce path: the push carried the pre-commit
            // watermark, so followers need the committed point delivered after the fact.
            // `__cluster_metadata` has no pull fetcher at all (it is deliberately excluded,
            // to avoid applying records through two paths at once), so this broadcast is
            // the only thing that ever advances a follower's metadata watermark.
            meta_pm.advance_committed_hw(frame.base_offset + 1);
            let repl = self.replication.clone();
            let committed_hw = meta_pm.high_watermark();
            let hw_fencing_epoch = self.replication.get_epoch();
            tokio::spawn(async move {
                repl.broadcast_high_watermark(
                    "__cluster_metadata",
                    0,
                    hw_fencing_epoch,
                    committed_hw,
                    &[],
                )
                .await;
            });
        }

        // Applied only now — once the record is durable on this leader AND acknowledged by
        // a majority of the controller quorum (or immediately, for a sole controller, whose
        // own durable append already is a majority).
        //
        // On a failed commit the record stays in the local log but is deliberately NOT
        // applied and the caller gets an error, so the leader never serves state the
        // cluster has not agreed on.
        self.apply_metadata_record(frame.base_offset, record);

        Ok(frame.base_offset)
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
        group_instance_id: Option<&str>,
        protocols: Vec<String>,
    ) -> Result<(String, u32, bool, String), String> {
        self.join_group_awaited_with_session_timeout(
            group_id,
            member_id,
            group_instance_id,
            protocols,
            None,
        )
        .await
    }

    /// Same as [`Self::join_group_awaited`], but also carries the client's requested
    /// `session.timeout.ms` — the `SESSION_TIMEOUT_MS` tagged field off the request
    /// envelope, or `None` if the request carried no such tag — through to
    /// `GroupCoordinator::join_group`, which resolves (and clamps) it into the member's
    /// actual eviction threshold. Split out from `join_group_awaited` rather than adding a
    /// parameter there so the many existing callers that don't care about this are
    /// unaffected.
    pub async fn join_group_awaited_with_session_timeout(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocols: Vec<String>,
        session_timeout_ms: Option<u32>,
    ) -> Result<(String, u32, bool, String), String> {
        let coordinator = self.group_coordinator();
        let (m_id, generation_id, is_leader, protocol_name) = coordinator.join_group(
            group_id,
            member_id,
            group_instance_id,
            protocols,
            session_timeout_ms,
        )?;

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
            Some((final_generation, final_is_leader, final_protocol)) => {
                Ok((m_id, final_generation, final_is_leader, final_protocol))
            }
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

    /// Fetches the credential for `username` under `mechanism`.
    ///
    /// `None` for the mechanism means the caller has no negotiated preference (e.g. a
    /// legacy path that predates mechanism selection); in that case the strongest
    /// credential the user actually has is returned, so adding a SHA-512 credential
    /// upgrades those callers rather than breaking them.
    pub(crate) fn lookup_scram_credential(
        &self,
        username: &str,
        mechanism: Option<crate::scram::ScramMechanism>,
    ) -> Option<crate::scram::ScramCredential> {
        if let Some(mechanism) = mechanism {
            return self
                .scram_credentials
                .get(&(username.to_string(), mechanism))
                .map(|entry| entry.value().clone());
        }
        for candidate in [
            crate::scram::ScramMechanism::Sha512,
            crate::scram::ScramMechanism::Sha256,
        ] {
            if let Some(entry) = self
                .scram_credentials
                .get(&(username.to_string(), candidate))
            {
                return Some(entry.value().clone());
            }
        }
        None
    }

    /// True if `username` has a credential under any mechanism.
    pub fn has_scram_user(&self, username: &str) -> bool {
        self.lookup_scram_credential(username, None).is_some()
    }

    /// Mechanisms `username` currently holds a credential for, strongest first. Empty for
    /// an unknown user. Lets an operator see which SCRAM mechanisms a user can actually
    /// authenticate with, rather than only whether the user exists.
    pub fn scram_user_mechanisms(&self, username: &str) -> Vec<crate::scram::ScramMechanism> {
        [
            crate::scram::ScramMechanism::Sha512,
            crate::scram::ScramMechanism::Sha256,
        ]
        .into_iter()
        .filter(|m| {
            self.scram_credentials
                .contains_key(&(username.to_string(), *m))
        })
        .collect()
    }

    pub(crate) fn apply_scram_credential_state(
        &self,
        username: String,
        mechanism: crate::scram::ScramMechanism,
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    ) {
        self.scram_credentials.insert(
            (username.clone(), mechanism),
            crate::scram::ScramCredential::new(
                username, mechanism, iterations, salt, stored_key, server_key,
            ),
        );
    }

    /// Removes every credential belonging to `username`, across all mechanisms — deleting
    /// a user must not leave them able to authenticate under a different hash.
    pub(crate) fn remove_scram_credential_state(&self, username: &str) {
        self.scram_credentials
            .retain(|(stored_user, _), _| stored_user != username);
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_scram_credential(
        &self,
        username: &str,
        mechanism: crate::scram::ScramMechanism,
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
            mechanism: mechanism.to_byte(),
        };
        self.propose_metadata(record).await?;
        Ok(())
    }

    /// Creates or replaces a user's SCRAM credential under the default mechanism.
    pub async fn upsert_scram_user(&self, username: &str, password: &str) -> IoResult<()> {
        self.upsert_scram_user_with_mechanism(
            username,
            password,
            crate::scram::ScramMechanism::default(),
        )
        .await
    }

    /// Creates or replaces a user's SCRAM credential under a specific mechanism.
    ///
    /// A credential is bound to one hash family — the salted password, stored key and
    /// server key all depend on it — so re-running this with a different mechanism
    /// *replaces* the user's credential rather than adding a second one alongside it.
    pub async fn upsert_scram_user_with_mechanism(
        &self,
        username: &str,
        password: &str,
        mechanism: crate::scram::ScramMechanism,
    ) -> IoResult<()> {
        let credential = crate::scram::ScramCredential::generate(
            username,
            password,
            mechanism,
            crate::scram::DEFAULT_SCRAM_SHA256_ITERATIONS,
        )
        .map_err(|_| std::io::Error::other("Failed to generate SCRAM credential"))?;
        self.upsert_scram_credential(
            &credential.username,
            credential.mechanism,
            credential.iterations,
            credential.salt,
            credential.stored_key,
            credential.server_key,
        )
        .await
    }

    pub async fn delete_scram_user(&self, username: &str) -> IoResult<bool> {
        let existed = self.has_scram_user(username);
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
                        decode_tx_state_record(&frame.value.clone().unwrap_or_default())
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
                tracing::error!(
                    "TxTimeout: failed to abort expired transaction '{}': {}",
                    tx_id,
                    e
                );
            }
        }
    }

    /// Produce a batch of records to a routed partition (PARTIAL-03 async).
    pub async fn produce_batch(&self, params: ProduceBatchParams<'_>) -> IoResult<(u32, u64, u64)> {
        let topic = params.topic;
        let key = params.key;
        let transaction_id = params.transaction_id;
        let num_partitions = params.num_partitions;
        let batch = params.batch;
        // Idempotence identity travels inside the batch header, where Kafka keeps it —
        // readable without touching the (possibly compressed) records.
        let producer_id = batch.producer_id;
        let producer_epoch = batch.producer_epoch;
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
        // Enforce `message.max.bytes` before doing any disk work. This is measured on the
        // batch as stored — its encoded, compressed size — which is both what Kafka limits
        // and the only size the broker can know without decompressing.
        let batch_encoded_size = batch.encoded_size() as u64;
        if let Some(max_bytes) = self.config.message_max_bytes {
            if batch_encoded_size > max_bytes {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Batch of {} bytes exceeds message.max.bytes ({})",
                        batch_encoded_size, max_bytes
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

        // A compacted topic dedupes by key, so a record without one has no meaning there:
        // it can never be superseded and can never be compacted away, so it would sit in
        // the log forever and surface much later as unexplained growth. Kafka rejects such
        // a produce outright (`InvalidRecordException`, "Compacted topic cannot accept
        // message without key"); this does the same.
        //
        // Checked against the batch's decoded records, which means decompressing it — the
        // one produce-path decompression, and only on compacted topics.
        if pm.cleanup_policy().is_compact() {
            let records = batch.records().map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Malformed record batch: {}", e),
                )
            })?;
            if let Some(bad) = records.iter().position(|r| r.key.is_none()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Compacted topic '{}' cannot accept a record without a key \
                         (record {} of {} in this batch)",
                        topic,
                        bad,
                        records.len()
                    ),
                ));
            }
        }

        // The actual disk work (segment append per record, plus the batch's group-commit
        // fsync) is synchronous, lock-and-syscall-heavy I/O. Running it inline in this
        // `async fn` would block whichever Tokio worker thread picked up this task for the
        // whole duration — under disk contention, that worker can't service any other
        // task in the meantime. `spawn_blocking` moves it to Tokio's dedicated blocking
        // thread pool instead, so a slow disk only stalls this one request.
        let pm_blocking = pm.clone();
        // `record_count` is read off the header before the batch moves into the blocking
        // closure; it is only needed to reconstruct `first_offset` on the duplicate path.
        let num_records = batch.record_count as usize;
        let (first_offset, last_offset) =
            tokio::task::spawn_blocking(move || -> IoResult<(u64, u64)> {
                // Every produced request is written as exactly one `RecordBatch`. There is
                // no per-record-frame alternative: a single record format keeps the produce,
                // replication, compaction and fetch paths from having to agree on two.
                let (first_offset, last_offset) = match pm_blocking.produce_batch_eos(batch)? {
                    Ok(batch) => (
                        batch.base_offset,
                        batch.base_offset + batch.last_offset_delta as u64,
                    ),
                    Err(dup_last_offset) => {
                        let last_offset = dup_last_offset;
                        let first_offset = last_offset.saturating_sub(num_records as u64 - 1);
                        (first_offset, last_offset)
                    }
                };

                // Group commit: one fsync for the whole `RecordBatch` rather than one per
                // record (see `PartitionManager::flush_if_sync_policy`) —
                // `produce_batch_eos` never syncs itself for exactly this reason.
                pm_blocking.flush_if_sync_policy()?;

                Ok((first_offset, last_offset))
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
        // Data-topic replication is Kafka-style follower-pull only. Every follower's
        // background fetch loop (`ReplicationManager::start_per_partition_fetcher_manager`)
        // independently pulls from this partition's log, and the leader's `handler.rs`
        // 0xBB handler (`decode_grpc_replication_fetch_packet`) records each follower's
        // confirmed progress into the `replica_watermarks`/`replica_ack_time` maps that
        // `await_full_isr_ack` below reads — there is no leader-push counterpart on this
        // path to race it. (`__cluster_metadata` is the exception: it has no pull fetcher
        // by design and still replicates via leader-push — see `propose_metadata_unchecked`.)
        if self.is_partition_leader(topic, partition_id) && !self.config.peer_addrs.is_empty() {
            // Enforce min_insync_replicas requirement before returning success and before
            // advancing the committed high watermark (REP-05 & PARTIAL-03). Until quorum
            // is reached, `pm.latest_offset()` (LEO) has moved but `pm.high_watermark()`
            // has not — consumers can't fetch past what's actually ISR-committed, and if
            // the quorum wait times out here, the record stays durably on this leader but
            // is never exposed as committed (no false "it's safe to read" signal).
            if self.effective_min_insync_replicas(topic) > 1 {
                self.await_full_isr_ack(
                    &pm,
                    topic,
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
        //
        // Nothing needs to be told about this commit point separately: every pull fetch
        // response already carries `leader_watermark`, so followers pick up the newly
        // committed high watermark on their own next fetch.
        pm.advance_committed_hw(last_offset + 1);

        Ok((partition_id, first_offset, last_offset))
    }

    /// Serves a fetch as stored bytes: whole log entries, exactly as written, never
    /// decoded into records and never decompressed. What a consumer receives is what the
    /// producer wrote, and decompressing it is the consumer's job.
    ///
    /// Entries are clamped to the committed high watermark, and an entry containing
    /// `offset` is returned whole — a batch is atomic on disk, so a consumer asking from
    /// the middle of one gets the whole batch and filters it itself, as in Kafka.
    /// A fetch that waits for data rather than answering an idle partition with an empty
    /// response.
    ///
    /// Returns as soon as at least `min_bytes` are available, or when `max_wait_ms`
    /// elapses, whichever comes first — Kafka's `fetch.max.wait.ms` / `fetch.min.bytes`.
    /// With `max_wait_ms` of 0 (an untagged request) this is exactly
    /// [`Self::fetch_entries`], so nothing changes for a client that does not ask to wait.
    ///
    /// The wait is driven by the partition's high-watermark notification, not by polling:
    /// the request parks until data actually commits. That cuts request volume on an idle
    /// partition and *lowers* delivery latency at the same time, since a parked consumer is
    /// woken the moment a record commits instead of on its next poll tick.
    pub async fn fetch_entries_waiting(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
        min_bytes: u32,
    ) -> IoResult<Bytes> {
        let entries = self
            .fetch_entries(topic, partition, offset, max_bytes)
            .await?;
        if max_wait_ms == 0 || entries.len() as u32 >= min_bytes {
            return Ok(entries);
        }
        let Some(pm) = self.partition_for_read(topic, partition)? else {
            return Ok(entries);
        };

        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(max_wait_ms as u64);
        let mut best = entries;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            // The watermark this fetch has already accounted for. Waiting for it to move
            // past this point is what distinguishes "no new data yet" from "data arrived
            // while we were reading", so a commit landing mid-read cannot be slept through.
            let seen_hw = pm.high_watermark();
            let woke = pm.await_hw_beyond(seen_hw, deadline - now).await;

            // Re-read unconditionally, including on timeout. Returning the snapshot taken
            // before the wait would hand back an empty response for data that arrived
            // during it: the notification says *when* to look, it is never the authority
            // on whether there is anything to find.
            best = self
                .fetch_entries(topic, partition, offset, max_bytes)
                .await?;
            if best.len() as u32 >= min_bytes || !woke {
                break;
            }
        }
        Ok(best)
    }

    /// The transactional metadata a read-committed consumer needs to filter for itself:
    /// the last stable offset and the aborted offset ranges.
    ///
    /// The broker cannot drop aborted records out of a compressed batch without decoding
    /// it, and it does not decode — so it reports what was aborted and lets the consumer
    /// drop them. Read-uncommitted gets `u64::MAX` and an empty list, i.e. filter nothing.
    pub fn read_committed_filter(
        &self,
        topic: &str,
        partition: u32,
        isolation: crate::protocol::wire::IsolationLevel,
    ) -> (u64, Vec<(u64, u64)>) {
        if isolation != crate::protocol::wire::IsolationLevel::ReadCommitted {
            return (u64::MAX, Vec::new());
        }
        let lso = self.transactions.last_stable_offset(topic, partition);
        let mut aborted = self.transactions.aborted_ranges(topic, partition);
        // The in-memory transaction manager only knows about transactions this broker has
        // seen since start-up; the partition's own txn index is what survives a restart.
        if let Ok(Some(pm)) = self.partition_for_read(topic, partition) {
            aborted.extend(pm.aborted_ranges());
        }
        aborted.sort_unstable();
        aborted.dedup();
        (lso, aborted)
    }

    pub async fn fetch_entries(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Bytes> {
        let Some(pm) = self.partition_for_read(topic, partition)? else {
            return Ok(Bytes::new());
        };
        tokio::task::spawn_blocking(move || pm.fetch_entries(offset, max_bytes))
            .await
            .map_err(|e| std::io::Error::other(format!("fetch_entries join error: {}", e)))?
    }

    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<crate::segment::Record>> {
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
        // library's job to recognize and skip them (see `Record::is_control`
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

    /// BUG-02: Fetch records starting from nearest offset for target_timestamp
    /// Stored bytes from the first offset reaching `target_timestamp`. The timestamp
    /// filter is the reader's to apply — see [`PartitionManager::fetch_entries_by_timestamp`].
    pub async fn fetch_entries_by_timestamp(
        &self,
        topic: &str,
        partition: u32,
        target_timestamp: u64,
        max_bytes: u32,
    ) -> IoResult<Bytes> {
        let Some(pm) = self.partition_for_read(topic, partition)? else {
            return Ok(Bytes::new());
        };
        tokio::task::spawn_blocking(move || {
            pm.fetch_entries_by_timestamp(target_timestamp, max_bytes)
        })
        .await
        .map_err(|e| {
            std::io::Error::other(format!("fetch_entries_by_timestamp join error: {}", e))
        })?
    }

    pub async fn fetch_by_timestamp(
        &self,
        topic: &str,
        partition: u32,
        target_timestamp: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<crate::segment::Record>> {
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
    ) -> IoResult<Vec<crate::segment::Record>> {
        let Some(pm) = self.partition_for_read(topic, partition)? else {
            return Ok(Vec::new());
        };
        let lso = self.transactions.last_stable_offset(topic, partition);
        let aborted = self.transactions.aborted_ranges(topic, partition);
        let all_frames = self.fetch(topic, partition, offset, max_bytes).await?;

        let pm_blocking = pm.clone();
        let committed_frames: Vec<crate::segment::Record> =
            tokio::task::spawn_blocking(move || {
                all_frames
                    .into_iter()
                    .filter(|frame| {
                        if frame.is_control {
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
            consumer_groups
                .commit_offset_with_metadata(&group_id, &topic, partition, offset, &metadata)
        })
        .await
        .map_err(|e| {
            std::io::Error::other(format!("commit_offset_with_metadata join error: {}", e))
        })?
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

        // Step 3: Write CTRL_COMMIT control markers to all involved data partitions,
        // recording where each partition's transactional range ends — the same bookkeeping
        // the abort path does, so both terminal states leave a fully-populated record.
        let mut end_offsets: Vec<(String, u32, u64)> = Vec::with_capacity(partitions.len());
        for (topic, partition, _, _) in &partitions {
            let pm = self
                .get_or_create_partition(topic, *partition)
                .map_err(|e| {
                    format!(
                        "Failed to get/create partition {}-{}: {}",
                        topic, partition, e
                    )
                })?;
            let marker_frame = pm
                .produce_control_marker(
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
            end_offsets.push((topic.clone(), *partition, marker_frame.base_offset));
            tracing::info!(
                "EOS 2PC: Commit marker written to '{}' partition {}",
                topic,
                partition
            );
        }

        // Step 4: Make the commit durable FIRST, then transition memory.
        //
        // This order matters and used to be the other way round: memory was set to
        // `Committed` before the record was written, the write's result was discarded, and
        // the function returned `Ok(())` regardless. So the broker could tell a producer
        // its transaction had committed while nothing durable said so — and on restart,
        // replay would reconstruct the transaction as still in flight and eventually abort
        // it by timeout. The producer had no way to detect that, which is precisely the
        // outcome transactional writes exist to prevent.
        let committed_partitions: crate::server::transaction::PartitionRangeList = partitions
            .iter()
            .map(|(topic, partition, start, end)| {
                let resolved_end = end_offsets
                    .iter()
                    .find(|(t, p, _)| t == topic && p == partition)
                    .map(|(_, _, off)| *off)
                    .unwrap_or(*end);
                (topic.clone(), *partition, *start, resolved_end)
            })
            .collect();

        let commit_record = encode_tx_state_record(
            TxStatus::Committed,
            producer_id,
            transaction_id,
            &committed_partitions,
        );
        let tx_pm = self
            .get_or_create_partition("__transaction_state", 0)
            .map_err(|e| format!("Failed to open __transaction_state: {}", e))?;
        tx_pm.produce(&commit_record).map_err(|e| {
            format!(
                "Failed to durably record commit of '{}': {}",
                transaction_id, e
            )
        })?;
        tx_pm.flush().map_err(|e| {
            format!(
                "Failed to flush commit of '{}' to __transaction_state: {}",
                transaction_id, e
            )
        })?;

        // Only now is it safe to say the transaction committed.
        self.transactions
            .complete_commit(transaction_id, &end_offsets)?;

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
            let _ = pm.append_aborted_txn(producer_id, *first_offset, frame.base_offset);
            end_offsets.push((topic.clone(), *partition, frame.base_offset));
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

        let partition_managers: Vec<Arc<PartitionManager>> = self
            .partitions
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

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
                    .map_err(|e| {
                        std::io::Error::other(format!("apply_retention join error: {}", e))
                    })?
            });
        }

        let mut total_removed = 0usize;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(n)) => total_removed += n,
                Ok(Err(e)) => tracing::error!("Retention GC: partition compaction failed: {}", e),
                Err(e) => {
                    tracing::error!("Retention GC: partition compaction task panicked: {}", e)
                }
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
            if self.has_scram_user(username) {
                continue;
            }
            let credential = crate::scram::ScramCredential::generate(
                username,
                password,
                crate::scram::ScramMechanism::default(),
                crate::scram::DEFAULT_SCRAM_SHA256_ITERATIONS,
            )
            .map_err(|_| std::io::Error::other("Failed to bootstrap SCRAM credential"))?;
            self.scram_credentials
                .insert((username.clone(), credential.mechanism), credential.clone());
            if self.is_leader() {
                let record = crate::replication::MetadataRecord::ScramCredentialUpsert {
                    username: credential.username.clone(),
                    iterations: credential.iterations,
                    salt: credential.salt.clone(),
                    stored_key: credential.stored_key.clone(),
                    server_key: credential.server_key.clone(),
                    mechanism: credential.mechanism.to_byte(),
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
                // Metadata first: a follower cannot act on an assignment it never received.
                engine.catch_up_follower_metadata().await;
                engine.reconcile_unassigned_partitions().await;
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

    /// Whether cluster metadata knows this topic at all (i.e. a `TopicCreated` record has
    /// been applied). A topic can exist as local directories without being registered —
    /// that is exactly the state controller-mediated creation exists to prevent.
    pub fn topic_is_registered(&self, topic: &str) -> bool {
        self.topic_registry.contains_key(topic)
    }

    /// Creates `topic` through the controller if cluster metadata does not know it yet,
    /// assigning replicas **before** the partition holds any data.
    ///
    /// This is what makes the standard full-ISR start correct: every replica of a brand-new
    /// partition is at LEO = HW = 0, so all of them are genuinely in sync and there is no
    /// catch-up phase to model. Creating the partition first and assigning later inverts
    /// that ordering, which is why `reconcile_unassigned_partitions` — the repair path for
    /// partitions created before this existed — must start the ISR with the leader alone.
    ///
    /// Idempotent and safe to call on every produce: a registered topic returns
    /// immediately without proposing anything.
    pub async fn ensure_topic_created(&self, topic: &str, num_partitions: u32) -> IoResult<()> {
        if Self::is_system_topic(topic) || self.topic_is_registered(topic) {
            return Ok(());
        }
        if !self.config.auto_create_topics_enable {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Unknown topic '{}' (auto.create.topics.enable is false)",
                    topic
                ),
            ));
        }
        if !self.is_leader() {
            // Only the controller may propose metadata. Callers route produces for unknown
            // topics to it (see `is_partition_leader`), so reaching here off-controller
            // means the request arrived by another path and should not create anything.
            return Err(std::io::Error::other(
                "NOT_CONTROLLER: only the cluster leader may create a topic",
            ));
        }
        // Don't bake in a roster before the cluster is known.
        //
        // Replication factor is fixed at creation and never raised automatically, so a
        // topic auto-created while peers are configured but not yet discovered would be
        // stuck at a single replica for its entire life — and worse, publishing that roster
        // tells a peer already receiving the partition's data that it is *not* a replica,
        // which stops it serving reads it can correctly serve.
        //
        // Deferring costs nothing: the produce still succeeds through the local path below,
        // and `reconcile_unassigned_partitions` assigns the partition properly on a later
        // sweep once the peers have been discovered.
        let discovered = self.available_broker_ids().len();
        if discovered <= 1 && !self.config.peer_addrs.is_empty() {
            tracing::debug!(
                "Auto-create: deferring creation of '{}' — {} peer(s) configured but none \
                 discovered yet, so any roster written now would be single-replica",
                topic,
                self.config.peer_addrs.len()
            );
            self.note_peers_undiscovered_and_maybe_warn("Auto-create");
            return Ok(());
        }
        self.note_peers_discovered();

        self.create_topic(topic, num_partitions.max(1)).await
    }

    /// Whether cluster metadata carries a replica assignment for this partition. A
    /// partition can exist locally (and hold data) without one — see
    /// `reconcile_unassigned_partitions`.
    pub fn has_partition_assignment(&self, topic: &str, partition: u32) -> bool {
        self.topic_registry
            .get(topic)
            .map(|cfg| cfg.partitions.contains_key(&partition))
            .unwrap_or(false)
    }

    /// Runs one metadata catch-up pass. Exposed so tests can drive it deterministically
    /// rather than waiting on the background timer.
    pub async fn catch_up_follower_metadata_for_test(&self) {
        self.catch_up_follower_metadata().await;
    }

    /// Runs one assignment sweep. Exposed so tests can drive it deterministically rather
    /// than waiting on the background timer.
    pub async fn reconcile_unassigned_partitions_for_test(&self) {
        self.reconcile_unassigned_partitions().await;
    }

    /// Re-sends any `__cluster_metadata` a follower is missing.
    ///
    /// Without this, a follower that joins a leader which has already written metadata
    /// receives **none of it, ever**. Two things combine to cause that:
    ///
    /// 1. The leader's bootstrap records (broker registration, legacy SASL users) are
    ///    written with a direct `meta_pm.produce()` in `StorageEngine::new`, which is
    ///    synchronous and so cannot call the async `propose_metadata` — they are appended
    ///    locally and never pushed.
    /// 2. The follower's log is therefore still at offset 0 when the leader pushes its next
    ///    record at a higher offset. `append_replica_frame_verbatim` correctly reports a
    ///    `Gap` and the batch is rejected — and since `__cluster_metadata` is deliberately
    ///    excluded from the pull fetcher (applying its records through two paths at once
    ///    would race), nothing could ever re-send the missing prefix.
    ///
    /// The gap was permanent, and because partition assignments travel through this log it
    /// meant followers never learned they were replicas — so data-topic replication
    /// silently did not happen, and ISR and failover had nothing to work with.
    ///
    /// This closes it without a wire change: the leader already records each peer's acked
    /// metadata offset, and `append_verbatim` treats an already-applied offset as a no-op,
    /// so re-sending from the peer's last known position is both sufficient and safe. A
    /// peer that has never acked is sent the log from offset 0.
    async fn catch_up_follower_metadata(&self) {
        if !self.is_leader() || self.config.peer_addrs.is_empty() {
            return;
        }
        let Ok(meta_pm) = self.get_or_create_partition("__cluster_metadata", 0) else {
            return;
        };
        let leo = meta_pm.latest_offset();
        if leo == 0 {
            return; // nothing written yet
        }

        // Bound the per-sweep work: the first catch-up after a follower joins could
        // otherwise ship an entire metadata history in one batch.
        const MAX_CATCHUP_BYTES: u32 = 1 << 20;

        let fencing_epoch = self.replication.get_epoch();
        let leader_hw = meta_pm.high_watermark();

        for peer in &self.config.peer_addrs {
            // `Some(w)` is the highest offset this peer has confirmed; `None` means it has
            // never acked anything, so it needs the log from the very beginning.
            let from = match self
                .replication
                .replica_watermark("__cluster_metadata", 0, peer)
            {
                Some(acked) => acked + 1,
                None => 0,
            };
            if from >= leo {
                continue; // already current
            }

            // Read as stored bytes and decode the frames back out, rather than going
            // through `fetch`, which yields decoded records. The follower appends what
            // arrives verbatim, so the frames pushed here must be the leader's own bytes —
            // rebuilding them from decoded payloads would produce different bytes (an
            // uncompressed frame where the leader stored a compressed one) and break the
            // byte-identity the replica relies on.
            let entry_bytes = match meta_pm.fetch_entries_for_replication(from, MAX_CATCHUP_BYTES) {
                Ok(bytes) if !bytes.is_empty() => bytes,
                _ => continue,
            };
            let mut frames = Vec::new();
            let mut cursor = 0usize;
            while cursor < entry_bytes.len() {
                let Ok((entry, consumed)) = crate::segment::decode_entry(&entry_bytes[cursor..])
                else {
                    break;
                };
                let bytes = entry_bytes.slice(cursor..cursor + consumed);
                cursor += consumed;
                let crate::segment::LogEntry::Batch(b) = &entry;
                let last_offset = b.base_offset + b.last_offset_delta as u64;
                frames.push(crate::replication::EncodedEntry { bytes, last_offset });
            }
            if frames.is_empty() {
                continue;
            }

            match self
                .replication
                .push_frames_to_peer(
                    peer,
                    "__cluster_metadata",
                    0,
                    fencing_epoch,
                    leader_hw,
                    &frames,
                )
                .await
            {
                Ok(()) => tracing::info!(
                    "Metadata catch-up: sent {} record(s) from offset {} to {} (leader LEO {})",
                    frames.len(),
                    from,
                    peer,
                    leo
                ),
                Err(e) => tracing::debug!(
                    "Metadata catch-up: failed to back-fill {} from offset {}: {}",
                    peer,
                    from,
                    e
                ),
            }
        }
    }

    /// Publishes a replica assignment for partitions that don't have one.
    ///
    /// `ensure_topic_created` now assigns replicas to an implicitly-created topic through
    /// the controller *before* the first byte is produced, which is the sound way to do
    /// it and is what every produce to a genuinely new topic goes through. This sweep is
    /// the repair path for the cases that don't: partitions created before this existed,
    /// and the one deliberate gap `ensure_topic_created` leaves — a topic auto-created
    /// while peers are configured but not yet discovered, where publishing an assignment
    /// immediately would mean writing a single-replica (or otherwise premature) roster.
    /// Either way, a partition can end up with data on disk that cluster metadata has no
    /// record of, which disables three things at once — no follower knows to fetch it (so
    /// it is never really replicated), the ISR sweep has no membership to manage, and
    /// failover has nothing to promote if the holder dies. A follower can end up
    /// physically holding a complete copy while metadata does not consider it a replica
    /// at all.
    ///
    /// This retrofits an assignment onto such partitions **without moving any data**: the
    /// broker currently holding the partition is named leader, and the ISR starts as just
    /// that broker — unlike `ensure_topic_created`'s full-ISR start, because here the
    /// partition may already hold data other replicas have not caught up on. The other
    /// replicas join through the normal catch-up path once their fetchers start, exactly
    /// as they would after falling behind.
    ///
    /// Detection is by absence from `topic_registry[topic].partitions`, not by an empty
    /// replica list — a freshly opened `PartitionManager` defaults to `leader_id = self`
    /// and `replicas = [self]`, so an unassigned partition looks locally "owned" and its
    /// replica list is never empty.
    async fn reconcile_unassigned_partitions(&self) {
        if !self.config.auto_assign_partitions_enable || !self.is_leader() {
            return;
        }
        let brokers = self.available_broker_ids();
        if brokers.is_empty() {
            return;
        }

        // Don't publish an assignment while broker discovery is still incomplete.
        //
        // If peers are configured but none has been discovered yet, the only broker this
        // controller can see is itself, so the computed roster would be `[self]` — and
        // publishing that is actively destructive rather than merely premature: a peer
        // that is already receiving this partition's data would be told it is *not* a
        // replica, which stops it serving reads it can correctly serve and drops it from
        // ISR accounting. Waiting costs nothing; the next sweep reassesses once the peer
        // has been discovered through a heartbeat ACK or a BrokerRegister record.
        if brokers.len() == 1 && !self.config.peer_addrs.is_empty() {
            tracing::debug!(
                "Assignment sweep: {} peer(s) configured but none discovered yet — deferring \
                 assignment rather than publishing a single-replica roster",
                self.config.peer_addrs.len()
            );
            self.note_peers_undiscovered_and_maybe_warn("Assignment sweep");
            return;
        }
        self.note_peers_discovered();

        // Bound the work per sweep. The first run after enabling this on an existing
        // cluster could otherwise propose thousands of metadata records back to back,
        // and every proposal is a replicated write.
        const MAX_ASSIGNMENTS_PER_SWEEP: usize = 50;

        let unassigned: Vec<(String, u32)> = self
            .partitions
            .iter()
            .map(|e| e.key().clone())
            .filter(|(topic, partition)| {
                if Self::is_system_topic(topic) {
                    return false;
                }
                !self
                    .topic_registry
                    .get(topic)
                    .map(|cfg| cfg.partitions.contains_key(partition))
                    .unwrap_or(false)
            })
            .take(MAX_ASSIGNMENTS_PER_SWEEP)
            .collect();

        for (topic, partition) in unassigned {
            let Some(pm) = self.get_partition(&topic, partition) else {
                continue;
            };

            // Keep the current holder as leader where it is a broker we can still see;
            // otherwise fall back to this node, which is the one doing the assigning.
            let current_leader = pm.leader_id();
            let leader_id = if brokers.contains(&current_leader) {
                current_leader
            } else {
                self.config.node_id
            };

            let rf = std::cmp::min(
                self.config.default_replication_factor.max(1) as usize,
                brokers.len(),
            );
            // Leader is pinned to whoever already holds the data, so this assignment moves
            // no bytes; the followers are still striped around it. Appending brokers in id
            // order instead — which is what this did first — gave every partition led by a
            // given broker the identical follower set, so losing one pair of brokers took
            // out far more partitions than necessary.
            let replicas = striped_replicas(
                &brokers,
                partition,
                rf,
                topic_placement_seed(&topic),
                Some(leader_id),
            );

            // Register the topic first if this broker only ever knew it implicitly —
            // `PartitionLeadershipChange` is applied into `topic_registry[topic]`, so
            // without a `TopicCreated` record there is nothing to apply it into.
            if !self.topic_registry.contains_key(&topic) {
                let num_partitions = self
                    .topic_partitions
                    .get(&topic)
                    .map(|set| set.len() as u32)
                    .unwrap_or(1)
                    .max(partition + 1);
                let record = crate::replication::MetadataRecord::TopicCreated {
                    topic: topic.clone(),
                    num_partitions,
                    replication_factor: rf as u16,
                };
                if let Err(e) = self.propose_metadata(record).await {
                    tracing::warn!(
                        "Assignment sweep: failed to register implicitly-created topic '{}': {}",
                        topic,
                        e
                    );
                    continue;
                }
            }

            let record = crate::replication::MetadataRecord::PartitionLeadershipChange {
                topic: topic.clone(),
                partition,
                leader_id,
                leader_epoch: pm.leader_epoch(),
                // Only the leader is in-sync to begin with. The new replicas have none of
                // the data yet; claiming otherwise would let `acks=all` succeed against
                // brokers that hold nothing.
                isr: vec![leader_id],
                replicas: Some(replicas.clone()),
            };
            match self.propose_metadata(record).await {
                Ok(_) => tracing::info!(
                    "Assignment sweep: {}-{} had no replica assignment — assigned leader {} with replicas {:?}",
                    topic,
                    partition,
                    leader_id,
                    replicas
                ),
                Err(e) => tracing::warn!(
                    "Assignment sweep: failed to assign {}-{}: {}",
                    topic,
                    partition,
                    e
                ),
            }
        }
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

                    // Only the ISR is narrowed here. The partition's configured replica set
                    // is deliberately NOT touched: `apply_metadata_record` preserves it
                    // across a `PartitionLeadershipChange`, so replicas that were merely
                    // offline at this moment stay on the roster and rejoin through the
                    // normal catch-up path when they return. Previously the roster was
                    // overwritten with whatever survived the election, which left the
                    // partition permanently running with a single replica — so the next
                    // failure lost it outright, with no path back to full replication
                    // short of a manual reassignment.
                    let (new_leader_id, new_isr, unclean) = match isr_candidates.first().copied() {
                        Some(id) => (Some(id), isr_candidates.clone(), false),
                        None if self.config.allow_unclean_leader_election => {
                            let fallback =
                                replicas.iter().copied().filter(|&r| r != leader_id).min();
                            (fallback, fallback.into_iter().collect(), true)
                        }
                        None => (None, Vec::new(), false),
                    };

                    if unclean {
                        if let Some(promoted) = new_leader_id {
                            // Worth surfacing loudly and separately from the ordinary
                            // failover log line below: an unclean election knowingly
                            // accepts data loss, promoting a replica that was NOT in the
                            // ISR and may therefore be missing committed records.
                            tracing::error!(
                                "UNCLEAN Failover: {}-{} had no surviving in-sync replica — promoting \
                                 out-of-sync replica {} (allow_unclean_leader_election is enabled). \
                                 Records committed on the old leader but absent from this replica are \
                                 lost. Roster {:?} is retained so the remaining replicas can rejoin.",
                                topic,
                                partition,
                                promoted,
                                replicas
                            );
                        }
                    }

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

    /// Creates a topic with an **explicitly requested** replication factor.
    ///
    /// Unlike `create_topic`, which treats the configured default as a preference and
    /// clamps it to the cluster size, an explicit factor is a durability contract: if the
    /// cluster cannot satisfy it the topic is not created at all, matching Kafka's
    /// `INVALID_REPLICATION_FACTOR`. Silently returning fewer replicas than the caller
    /// asked for would leave them believing data is replicated when it isn't — and because
    /// replication factor is fixed at creation and never raised automatically, that belief
    /// would persist for the life of the topic.
    pub async fn create_topic_with_replication_factor(
        &self,
        topic: &str,
        num_partitions: u32,
        replication_factor: u16,
    ) -> IoResult<()> {
        if replication_factor == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "replication factor must be at least 1",
            ));
        }
        let available = self.available_broker_ids().len();
        if replication_factor as usize > available {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "INVALID_REPLICATION_FACTOR: topic '{}' requested replication factor {} \
                     but only {} broker(s) are available",
                    topic, replication_factor, available
                ),
            ));
        }
        self.create_topic_inner(topic, num_partitions, Some(replication_factor))
            .await
    }

    /// Creates a topic by writing a TopicCreated record to __cluster_metadata and populating registry
    pub async fn create_topic(&self, topic: &str, num_partitions: u32) -> IoResult<()> {
        self.create_topic_inner(topic, num_partitions, None).await
    }

    /// Shared creation path. `explicit_rf` is `Some` when the caller stated a replication
    /// factor (already validated as satisfiable) and `None` when the configured default
    /// should be used and clamped.
    async fn create_topic_inner(
        &self,
        topic: &str,
        num_partitions: u32,
        explicit_rf: Option<u16>,
    ) -> IoResult<()> {
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
        // Implicit (default) replication factor: clamp to what the cluster can satisfy,
        // but say so. Kafka instead rejects the whole creation with
        // INVALID_REPLICATION_FACTOR and never degrades silently — the right instinct, but
        // adopting it wholesale would leave a single-broker deployment unable to create any
        // topic at all, since our default is 3 rather than Kafka's 1.
        //
        // So the contract is split by intent: a *default* RF is a preference and gets
        // clamped loudly, while an explicitly requested one is a durability contract and
        // fails outright (see `create_topic_with_replication_factor`).
        let requested_rf = explicit_rf.unwrap_or(self.config.default_replication_factor.max(1));
        let replication_factor = std::cmp::min(requested_rf, broker_ids.len() as u16);
        if explicit_rf.is_none() && replication_factor < requested_rf {
            tracing::warn!(
                "Topic '{}': default.replication.factor is {} but only {} broker(s) are \
                 available — creating with replication factor {}. This topic will NOT gain \
                 replicas automatically when more brokers join.",
                topic,
                requested_rf,
                broker_ids.len(),
                replication_factor
            );
        }

        let record = crate::replication::MetadataRecord::TopicCreated {
            topic: topic.to_string(),
            num_partitions,
            replication_factor,
        };
        // propose_metadata both writes this record AND applies it (registers the topic
        // in topic_registry with an empty partition map) via apply_metadata_record, so
        // there's no separate direct topic_registry.insert here anymore.
        self.propose_metadata(record).await?;

        let rf_usize = replication_factor as usize;
        let seed = topic_placement_seed(topic);

        for p in 0..num_partitions {
            let replicas = striped_replicas(&broker_ids, p, rf_usize, seed, None);
            // The "preferred replica": leadership defaults to the head of the list, and
            // striping is what keeps that from concentrating on one broker.
            let leader_id = replicas[0];
            // Every replica of a brand-new partition is at LEO = HW = 0, so they are all
            // genuinely in sync — there is nothing to catch up on. That is only sound
            // because assignment happens before the partition holds a single byte; the
            // sweep that retrofits an assignment onto a partition which *already* has data
            // must start with the leader alone (see `reconcile_unassigned_partitions`).
            let isr = replicas.clone();

            let plc_record = crate::replication::MetadataRecord::PartitionLeadershipChange {
                topic: topic.to_string(),
                partition: p,
                leader_id,
                leader_epoch: 0,
                isr,
                replicas: Some(replicas.clone()),
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
    /// A topic cluster metadata has never heard of resolves to the **controller**, so that
    /// creation and replica assignment happen once, in one place, before any data exists.
    ///
    /// Every node used to answer "yes" here, which meant a produce for a new topic was
    /// served wherever it happened to land: two clients producing the same new topic
    /// through different brokers each created their own local copy of the "same" partition,
    /// with nothing reconciling them, and neither copy had a replica assignment.
    ///
    /// This routing is only safe because forwarded requests are marked (see
    /// `wire::tags::FORWARDED`). Without that mark it deadlocks: the controller creates the
    /// topic, forwards to the newly assigned leader, and that leader — which has not yet
    /// received the assignment through the metadata log — sees an unknown topic, concludes
    /// it is not the leader, and forwards straight back. The marker stops the second hop,
    /// so the assigned leader serves the request even while its metadata is still catching
    /// up.
    ///
    /// Answering "yes" on the controller is what stops it forwarding to itself, and a
    /// single-node deployment is its own controller, so it still serves new topics directly.
    pub fn is_partition_leader(&self, topic: &str, partition: u32) -> bool {
        match self.resolve_partition_leader(topic, partition) {
            Some(leader_id) => leader_id == self.config.node_id,
            None => Self::is_system_topic(topic) || self.is_leader(),
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

    /// How long a "peers configured but undiscovered" deferral (see
    /// `note_peers_undiscovered_and_maybe_warn`) must persist before it escalates from
    /// routine `debug!` to `warn!`. Long enough that an ordinary startup race — a peer
    /// typically ACKs a heartbeat within one round trip — never fires it; short enough
    /// that a genuinely stuck cluster is flagged well before an operator would otherwise
    /// notice partitions stuck without a replica assignment.
    const UNDISCOVERED_PEERS_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(30);

    /// The subset of `config.peer_addrs` that no currently-known broker's address
    /// matches — i.e. the peers `ensure_topic_created`/`reconcile_unassigned_partitions`
    /// are still waiting to discover via a heartbeat ACK or `BrokerRegister` replay.
    fn undiscovered_peer_addrs(&self) -> Vec<String> {
        self.config
            .peer_addrs
            .iter()
            .filter(|addr| !self.broker_addrs.iter().any(|entry| entry.value() == *addr))
            .cloned()
            .collect()
    }

    /// Called by `ensure_topic_created`/`reconcile_unassigned_partitions` every time they
    /// defer replica assignment because peers are configured but not all discovered yet —
    /// each caller keeps its own `debug!` at the call site for that routine sweep.  This
    /// additionally escalates to `warn!` — naming the undiscovered peers — the first time
    /// the condition is observed to have persisted past `UNDISCOVERED_PEERS_WARN_AFTER`,
    /// and never again for the same stuck bout (see `note_peers_discovered`). `context` is
    /// a short label (e.g. "Auto-create", "Assignment sweep") identifying which caller
    /// triggered the escalation, since both share this one timer/flag pair.
    fn note_peers_undiscovered_and_maybe_warn(&self, context: &str) {
        let now = std::time::Instant::now();
        let first_seen = {
            let mut guard = self.undiscovered_peers_since.lock();
            *guard.get_or_insert(now)
        };
        let elapsed = now.saturating_duration_since(first_seen);

        if elapsed < Self::UNDISCOVERED_PEERS_WARN_AFTER {
            return;
        }
        if self
            .undiscovered_peers_warned
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return; // already warned for this bout
        }

        let missing = self.undiscovered_peer_addrs();
        tracing::warn!(
            "{}: broker discovery has been stuck for {:?} — {} of {} configured peer(s) \
             still undiscovered: {:?}. Partitions cannot get a replica assignment until \
             these peers are discovered via a heartbeat ACK or BrokerRegister replay; check \
             connectivity, cluster_id, and (if the peer's own peer_addrs is non-empty) its \
             whitelist.",
            context,
            elapsed,
            missing.len(),
            self.config.peer_addrs.len(),
            missing
        );
    }

    /// Called once `ensure_topic_created`/`reconcile_unassigned_partitions` observe that
    /// discovery is no longer stuck (or was never stuck), resetting the timer/flag pair
    /// above so a *future* bout of undiscovered peers (e.g. a peer flaps) gets its own
    /// fresh grace period rather than warning again immediately from stale state.
    fn note_peers_discovered(&self) {
        let mut guard = self.undiscovered_peers_since.lock();
        if guard.take().is_some() {
            self.undiscovered_peers_warned
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
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
                                let _ = dlq_pm.produce_frame(&f.value.unwrap_or_default());
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
                                let _ = dlq_pm.produce_frame(&f.value.unwrap_or_default());
                            }
                        }
                    }
                }
            }
        };
        self.share_groups.sweep_lock_timeouts(Some(&dlq_writer));
    }
}

#[cfg(test)]
mod placement_tests {
    use super::*;

    /// Plain round-robin makes broker i's follower always broker i+1, so consecutive
    /// partitions share a follower pairing and losing two adjacent brokers takes out whole
    /// partitions. Striping must produce more than one distinct follower per leader.
    #[test]
    fn striping_spreads_followers_across_brokers() {
        let brokers = vec![1, 2, 3, 4, 5];
        let seed = topic_placement_seed("orders");

        let mut followers_by_leader: std::collections::HashMap<
            u32,
            std::collections::HashSet<u32>,
        > = std::collections::HashMap::new();
        for p in 0..25u32 {
            let replicas = striped_replicas(&brokers, p, 3, seed, None);
            assert_eq!(
                replicas.len(),
                3,
                "partition {} short of RF: {:?}",
                p,
                replicas
            );
            let mut seen = std::collections::HashSet::new();
            for r in &replicas {
                assert!(seen.insert(*r), "duplicate replica in {:?}", replicas);
            }
            followers_by_leader
                .entry(replicas[0])
                .or_default()
                .extend(replicas[1..].iter().copied());
        }

        for (leader, followers) in &followers_by_leader {
            assert!(
                followers.len() > 1,
                "leader {} always uses the same followers {:?} — that is round-robin, not striping",
                leader,
                followers
            );
        }
    }

    /// Leadership must spread too, or one broker takes every partition's write traffic.
    #[test]
    fn striping_spreads_leadership() {
        let brokers = vec![1, 2, 3, 4];
        let seed = topic_placement_seed("events");
        let mut leaders = std::collections::HashSet::new();
        for p in 0..12u32 {
            leaders.insert(striped_replicas(&brokers, p, 2, seed, None)[0]);
        }
        assert_eq!(
            leaders.len(),
            brokers.len(),
            "every broker should lead some partition"
        );
    }

    /// Retrofitting an assignment must never move data: the broker already holding the
    /// partition stays leader, and only the followers are chosen.
    #[test]
    fn pinned_leader_is_preserved_and_followers_still_stripe() {
        let brokers = vec![1, 2, 3, 4];
        let seed = topic_placement_seed("retrofit");
        let mut follower_sets = std::collections::HashSet::new();
        for p in 0..8u32 {
            let replicas = striped_replicas(&brokers, p, 3, seed, Some(2));
            assert_eq!(replicas[0], 2, "the pinned leader must lead");
            assert_eq!(replicas.len(), 3);
            follower_sets.insert(replicas[1..].to_vec());
        }
        assert!(
            follower_sets.len() > 1,
            "a pinned leader should still get varied followers, got {:?}",
            follower_sets
        );
    }

    /// Degenerate inputs must not panic — a single broker has no peer to place a follower
    /// on, and the modulo arithmetic divides by `n - 1`.
    #[test]
    fn single_broker_and_oversized_factor_are_handled() {
        assert_eq!(striped_replicas(&[7], 0, 3, 42, None), vec![7]);
        assert_eq!(striped_replicas(&[7], 5, 1, 0, Some(7)), vec![7]);
        assert!(striped_replicas(&[], 0, 3, 1, None).is_empty());
        // Requesting more replicas than brokers yields every broker exactly once.
        let all = striped_replicas(&[1, 2, 3], 0, 9, 5, None);
        assert_eq!(all.len(), 3);
    }

    /// Different topics must not all start on the same broker, but one topic's layout must
    /// be reproducible — the sweep can recompute it.
    #[test]
    fn placement_seed_varies_by_topic_but_is_stable() {
        assert_eq!(topic_placement_seed("a"), topic_placement_seed("a"));
        assert_ne!(topic_placement_seed("a"), topic_placement_seed("b"));

        let brokers = vec![1, 2, 3, 4, 5];
        let first_leaders: std::collections::HashSet<u32> = ["alpha", "beta", "gamma", "delta"]
            .iter()
            .map(|t| striped_replicas(&brokers, 0, 2, topic_placement_seed(t), None)[0])
            .collect();
        assert!(
            first_leaders.len() > 1,
            "every topic started its partition 0 on the same broker"
        );
    }
}

/// Issue #62 (commit 3): stuck broker discovery escalates from `debug!` to `warn!` once
/// persisted, but only once per bout, and names which configured peers are still missing.
/// Exercised at the unit level — directly manipulating the private timer — rather than via
/// a real 30-second sleep in an integration test.
#[cfg(test)]
mod discovery_warn_tests {
    use super::*;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "hermes_engine_discovery_warn_test_{}_{}_{}",
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

    fn open_engine(dir: &TempDir, peer_addrs: Vec<String>) -> StorageEngine {
        StorageEngine::new(EngineConfig {
            data_dir: dir.0.clone(),
            bind_addr: "127.0.0.1:0".to_string(),
            peer_addrs,
            ..EngineConfig::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn undiscovered_peer_addrs_lists_only_peers_with_no_matching_broker_entry() {
        let dir = TempDir::new("missing_list");
        let engine = open_engine(
            &dir,
            vec!["127.0.0.1:11111".to_string(), "127.0.0.1:22222".to_string()],
        );
        // Only one of the two configured peers has been discovered so far.
        engine.register_broker_address(2, "127.0.0.1:11111".to_string());

        assert_eq!(
            engine.undiscovered_peer_addrs(),
            vec!["127.0.0.1:22222".to_string()]
        );
    }

    #[tokio::test]
    async fn undiscovered_peer_addrs_is_empty_once_every_peer_is_known() {
        let dir = TempDir::new("all_known");
        let engine = open_engine(&dir, vec!["127.0.0.1:11111".to_string()]);
        engine.register_broker_address(2, "127.0.0.1:11111".to_string());

        assert!(engine.undiscovered_peer_addrs().is_empty());
    }

    #[tokio::test]
    async fn warn_escalation_is_gated_by_persistence_and_fires_once_per_bout() {
        let dir = TempDir::new("escalation");
        let engine = open_engine(&dir, vec!["127.0.0.1:33333".to_string()]);

        // First observation starts the timer but must not warn yet — an ordinary brief
        // startup race must never escalate.
        engine.note_peers_undiscovered_and_maybe_warn("test");
        assert!(!engine
            .undiscovered_peers_warned
            .load(std::sync::atomic::Ordering::SeqCst));

        // Backdate the timer past the escalation threshold, simulating a genuinely stuck
        // bout without an actual 30s sleep.
        {
            let mut guard = engine.undiscovered_peers_since.lock();
            *guard = Some(
                std::time::Instant::now()
                    - StorageEngine::UNDISCOVERED_PEERS_WARN_AFTER
                    - std::time::Duration::from_secs(1),
            );
        }
        engine.note_peers_undiscovered_and_maybe_warn("test");
        assert!(
            engine
                .undiscovered_peers_warned
                .load(std::sync::atomic::Ordering::SeqCst),
            "must escalate once the condition has persisted past the threshold"
        );

        // Calling again while still stuck must not be observably different — this is the
        // "fires once per bout, not on every sweep" requirement. (There's no distinct
        // "already warned twice" state to assert against; the point is it doesn't panic
        // or otherwise misbehave on a repeat call.)
        engine.note_peers_undiscovered_and_maybe_warn("test");
        assert!(engine
            .undiscovered_peers_warned
            .load(std::sync::atomic::Ordering::SeqCst));

        // Once discovery completes, the state resets so a future bout (e.g. a peer flaps)
        // gets its own fresh grace period rather than warning again immediately.
        engine.note_peers_discovered();
        assert!(!engine
            .undiscovered_peers_warned
            .load(std::sync::atomic::Ordering::SeqCst));
        assert!(engine.undiscovered_peers_since.lock().is_none());
    }
}
