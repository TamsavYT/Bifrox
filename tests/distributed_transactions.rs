static NEXT_TEST_DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
use bifrox::protocol::{wire::IsolationLevel, BatchCompression};
use bifrox::server::engine::ProduceBatchParams;
use bifrox::{EngineConfig, NodeRole, RecordBatch, Server, StorageEngine, TxStatus};
use bytes::Bytes;
use std::net::SocketAddr;
use std::time::Duration;
struct Cluster {
    root: std::path::PathBuf,
    engines: Vec<StorageEngine>,
    tasks: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
    configs: Vec<EngineConfig>,
    addrs: Vec<SocketAddr>,
}
impl Cluster {
    async fn new() -> Self {
        Self::with_tls(false).await
    }
    async fn with_tls(tls: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "distributed_tx_{}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_TEST_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let sockets: Vec<_> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<_> = sockets.iter().map(|s| s.local_addr().unwrap()).collect();
        drop(sockets);
        let mut configs: Vec<_> = (0..3)
            .map(|i| EngineConfig {
                node_id: i + 1,
                role: if i == 0 {
                    NodeRole::Leader
                } else {
                    NodeRole::Follower
                },
                data_dir: root.join(format!("node{}", i + 1)),
                bind_addr: addrs[i as usize].to_string(),
                peer_addrs: addrs
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i as usize)
                    .map(|(_, a)| a.to_string())
                    .collect(),
                allow_insecure_replication: true,
                preallocate_segments: false,
                ..Default::default()
            })
            .collect();
        if tls {
            use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
            std::fs::create_dir_all(&root).unwrap();
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec![]).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let ca = params.self_signed(&key).unwrap();
            std::fs::write(root.join("ca.pem"), ca.pem()).unwrap();
            for id in 1..=3 {
                let leaf_key = KeyPair::generate().unwrap();
                let cert = CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])
                    .unwrap()
                    .signed_by(&leaf_key, &ca, &key)
                    .unwrap();
                std::fs::write(root.join(format!("node{id}.pem")), cert.pem()).unwrap();
                std::fs::write(root.join(format!("node{id}.key")), leaf_key.serialize_pem())
                    .unwrap();
            }
            for config in &mut configs {
                config.allow_insecure_replication = false;
                config.security_protocol = bifrox::SecurityProtocol::Ssl;
                config.ssl_ca_path = Some(root.join("ca.pem"));
                config.ssl_cert_path = Some(root.join(format!("node{}.pem", config.node_id)));
                config.ssl_key_path = Some(root.join(format!("node{}.key", config.node_id)));
                config.replication_peer_certificates = (1..=3)
                    .filter(|id| *id != config.node_id)
                    .map(|id| (id, root.join(format!("node{id}.pem"))))
                    .collect();
            }
        }
        let engines: Vec<_> = configs
            .iter()
            .map(|c| StorageEngine::new(c.clone()).unwrap())
            .collect();
        let tasks = engines
            .iter()
            .map(|e| {
                let server = Server::new(e.clone());
                let (l, _) = server.bind().unwrap();
                tokio::spawn(async move { server.run_with_listener(l).await })
            })
            .collect();
        for engine in &engines {
            for (i, a) in addrs.iter().enumerate() {
                engine.register_broker_address(i as u32 + 1, a.to_string());
            }
            engine.set_leader_addr(addrs[0].to_string());
        }
        Self {
            root,
            engines,
            tasks,
            configs,
            addrs,
        }
    }
    async fn topic(&self) {
        self.engines[0]
            .create_topic_with_replication_factor("orders", 2, 3)
            .await
            .unwrap();
        wait(|| {
            self.engines
                .iter()
                .all(|e| e.topic_is_registered("orders") && e.get_partition("orders", 1).is_some())
        })
        .await;
    }
    async fn produce(
        &self,
        id: &str,
        pid: u64,
        epoch: i16,
        partition: u32,
    ) -> std::io::Result<(u32, u64, u64)> {
        let key = (0..100)
            .map(|i| i.to_string())
            .find(|s| bifrox::hash_key(s.as_bytes(), 2) == partition)
            .unwrap();
        self.engines[0]
            .produce_batch(ProduceBatchParams {
                topic: "orders",
                key: &key,
                transaction_id: Some(id),
                num_partitions: 2,
                batch: RecordBatch::create(
                    0,
                    0,
                    0,
                    pid,
                    epoch,
                    partition as i32,
                    true,
                    BatchCompression::None,
                    &[(0, None, Some(Bytes::from_static(b"transactional")))],
                ),
            })
            .await
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn wait(mut predicate: impl FnMut() -> bool) {
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(
            tokio::time::Instant::now() < until,
            "condition did not converge"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_and_abort_span_remote_partition_leaders_and_replicas() {
    exercise_commit_abort(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_tls_transaction_commands_reach_remote_leaders() {
    exercise_commit_abort(true).await;
}
async fn exercise_commit_abort(tls: bool) {
    let c = Cluster::with_tls(tls).await;
    c.topic().await;
    assert_ne!(
        c.engines[0].partition_leader_id("orders", 0),
        c.engines[0].partition_leader_id("orders", 1)
    );
    for (id, commit) in [("committed", true), ("aborted", false)] {
        let (pid, epoch) = c.engines[0].init_producer_id_coordinated(id).await.unwrap();
        c.engines[0]
            .begin_transaction_coordinated(id, pid)
            .await
            .unwrap();
        c.engines[0]
            .add_partitions_coordinated(id, pid, epoch, &[("orders".into(), vec![0, 1])])
            .await
            .unwrap();
        for part in 0..2 {
            c.produce(id, pid, epoch, part).await.unwrap();
        }
        wait(|| {
            c.engines.iter().all(|e| {
                e.transactions().get_partitions(id).len() == 2
                    && e.transactions()
                        .get_partitions(id)
                        .iter()
                        .all(|p| p.2 != u64::MAX)
            })
        })
        .await;
        for e in &c.engines {
            for part in 0..2 {
                assert_ne!(
                    e.read_committed_filter("orders", part, IsolationLevel::ReadCommitted)
                        .0,
                    u64::MAX
                );
            }
        }
        c.engines[0]
            .end_transaction_coordinated(id, pid, epoch, commit)
            .await
            .unwrap();
        let status = if commit {
            TxStatus::Committed
        } else {
            TxStatus::Aborted
        };
        wait(|| {
            c.engines
                .iter()
                .all(|e| e.transactions().get_status(id) == Some(status))
        })
        .await;
        c.engines[0]
            .end_transaction_coordinated(id, pid, epoch, commit)
            .await
            .unwrap();
        for e in &c.engines {
            for part in 0..2 {
                let (lso, ranges) =
                    e.read_committed_filter("orders", part, IsolationLevel::ReadCommitted);
                assert_eq!(lso, u64::MAX);
                if !commit {
                    assert!(!ranges.is_empty());
                }
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_marker_stays_prepared_and_another_controller_recovers_it() {
    let c = Cluster::new().await;
    c.topic().await;
    let (pid, epoch) = c.engines[0]
        .init_producer_id_coordinated("recovery")
        .await
        .unwrap();
    c.engines[0]
        .begin_transaction_coordinated("recovery", pid)
        .await
        .unwrap();
    c.produce("recovery", pid, epoch, 1).await.unwrap();
    // Force only the data leader's commit checkpoint to fail; metadata still reaches quorum.
    let leader = c.engines[0].partition_leader_id("orders", 1).unwrap();
    let blocker = c.configs[leader as usize - 1]
        .data_dir
        .join("orders-1/committed.tmp");
    std::fs::create_dir(&blocker).unwrap();
    assert!(c.engines[0]
        .finish_transaction_coordinated("recovery", true)
        .await
        .is_err());
    wait(|| c.engines[1].transactions().get_status("recovery") == Some(TxStatus::PrepareCommit))
        .await;
    assert!(c.engines[0]
        .finish_transaction_coordinated("recovery", false)
        .await
        .is_err());
    std::fs::remove_dir(&blocker).unwrap();
    // Explicitly grant the already replicated node an election majority to isolate coordinator recovery from election timing.
    c.engines[0]
        .replication()
        .consensus()
        .step_down_to_follower(1);
    c.engines[0].replication().set_epoch(1);
    c.engines[1]
        .replication()
        .consensus()
        .force_candidate_state();
    let elected_term = c.engines[1].replication().consensus().current_term();
    assert!(c.engines[0]
        .replication()
        .consensus()
        .try_record_vote(2, elected_term));
    assert!(c.engines[1]
        .replication()
        .consensus()
        .tally_election_votes(2));
    c.engines[1].replication().set_epoch(1);
    for e in &c.engines {
        e.set_leader_addr(c.addrs[1].to_string());
        e.replication().set_epoch(1);
        for (i, a) in c.addrs.iter().enumerate() {
            e.register_broker_address(i as u32 + 1, a.to_string());
        }
    }
    c.engines[1]
        .replication()
        .start_heartbeat_broadcasting_if_leader();
    c.engines[1].recover_transactions_coordinated().await;
    wait(|| {
        c.engines
            .iter()
            .all(|e| e.transactions().get_status("recovery") == Some(TxStatus::Committed))
    })
    .await;
    // Reopen a different replica on copied on-disk state: recovery must not depend on the old coordinator's memory.
    c.tasks[2].abort();
    tokio::task::yield_now().await;
    let copy = c.root.join("reopened");
    copy_dir(&c.configs[2].data_dir, &copy);
    let reopened = StorageEngine::new(EngineConfig {
        data_dir: copy,
        peer_addrs: vec![],
        ..c.configs[2].clone()
    })
    .unwrap();
    assert_eq!(
        reopened.transactions().get_status("recovery"),
        Some(TxStatus::Committed)
    );
}
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for item in std::fs::read_dir(from).unwrap() {
        let item = item.unwrap();
        if item.path().extension().is_some_and(|ext| ext == "tmp") {
            continue;
        }
        if item.path().is_dir() {
            copy_dir(&item.path(), &to.join(item.file_name()));
        } else {
            std::fs::copy(item.path(), to.join(item.file_name())).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn marker_without_data_quorum_cannot_complete_or_release_lso() {
    let c = Cluster::new().await;
    c.topic().await;
    let (pid, epoch) = c.engines[0]
        .init_producer_id_coordinated("quorum")
        .await
        .unwrap();
    c.engines[0]
        .begin_transaction_coordinated("quorum", pid)
        .await
        .unwrap();
    c.produce("quorum", pid, epoch, 0).await.unwrap();
    let leader = c.engines[0].partition_leader_id("orders", 0).unwrap();
    let pm = c.engines[leader as usize - 1]
        .get_partition("orders", 0)
        .unwrap();
    let original = pm.replicas();
    let isr = pm.isr();
    let epoch = pm.leader_epoch();
    pm.update_leadership(leader, epoch, vec![leader, 99, 100], vec![leader, 99, 100]);
    assert!(c.engines[0]
        .finish_transaction_coordinated("quorum", true)
        .await
        .is_err());
    assert_eq!(
        c.engines[0].transactions().get_status("quorum"),
        Some(TxStatus::PrepareCommit)
    );
    assert!(pm.latest_offset() > pm.high_watermark());
    assert_ne!(
        c.engines[0]
            .read_committed_filter("orders", 0, IsolationLevel::ReadCommitted)
            .0,
        u64::MAX
    );
    pm.update_leadership(leader, epoch, original, isr);
    c.engines[0]
        .finish_transaction_coordinated("quorum", true)
        .await
        .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_on_a_follower_routes_transaction_operations_through_the_controller() {
    let c = Cluster::new().await;
    c.topic().await;
    let mut client = bifrox::TestClient::connect(c.addrs[2]).await.unwrap();
    let (pid, epoch) = client.init_producer_id("network").await.unwrap();
    client.begin_transaction("network", pid).await.unwrap();
    client
        .add_partitions_to_txn("network", pid, epoch, &[("orders", &[0, 1])])
        .await
        .unwrap();
    let key = (0..100)
        .map(|i| i.to_string())
        .find(|s| bifrox::hash_key(s.as_bytes(), 2) == 1)
        .unwrap();
    client
        .produce_batch_eos(
            "orders",
            &key,
            Some("network"),
            2,
            pid,
            epoch,
            0,
            &[b"over the client wire"],
        )
        .await
        .unwrap();
    client.end_txn("network", pid, epoch, true).await.unwrap();
    wait(|| {
        c.engines
            .iter()
            .all(|e| e.transactions().get_status("network") == Some(TxStatus::Committed))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_replica_pin_prevents_any_transaction_data_append() {
    let c = Cluster::new().await;
    c.topic().await;
    c.tasks[2].abort();
    tokio::task::yield_now().await;
    let (pid, epoch) = c.engines[0]
        .init_producer_id_coordinated("pin-barrier")
        .await
        .unwrap();
    c.engines[0]
        .begin_transaction_coordinated("pin-barrier", pid)
        .await
        .unwrap();
    let leader = c.engines[0].partition_leader_id("orders", 0).unwrap();
    let pm = c.engines[leader as usize - 1]
        .get_partition("orders", 0)
        .unwrap();
    let before = pm.latest_offset();
    assert!(c.produce("pin-barrier", pid, epoch, 0).await.is_err());
    assert_eq!(pm.latest_offset(), before);
}
