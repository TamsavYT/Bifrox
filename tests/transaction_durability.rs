use bifrox::server::transaction::{TransactionManager, TxStatus};
use bifrox::{EngineConfig, StorageEngine};
struct Dir(std::path::PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "tx_durability_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn config(&self) -> EngineConfig {
        EngineConfig {
            data_dir: self.0.clone(),
            preallocate_segments: false,
            ..Default::default()
        }
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[test]
fn failed_persistence_never_publishes_a_transition_or_releases_lso() {
    let tx = TransactionManager::new();
    assert!(tx
        .begin_transaction_durable("t", 1, |_| Err("disk failure".into()))
        .is_err());
    assert!(tx.state("t").is_none());
    tx.begin_transaction("t", 1).unwrap();
    assert!(tx
        .add_partitions_durable("t", 1, 0, &[("topic".into(), vec![0])], |_| Err(
            "disk failure".into()
        ))
        .is_err());
    assert!(tx.get_partitions("t").is_empty());
    assert!(tx
        .register_partition_durable("t", "topic", 0, 12, |_| Err("disk failure".into()))
        .is_err());
    assert!(tx.get_partitions("t").is_empty());
    tx.register_partition("t", "topic", 0, 12).unwrap();
    for decision in [TxStatus::PrepareCommit, TxStatus::PrepareAbort] {
        assert!(tx
            .prepare_durable("t", decision, |_| Err("disk failure".into()))
            .is_err());
        assert_eq!(tx.get_status("t"), Some(TxStatus::Ongoing));
        assert_eq!(tx.get_partitions("t")[0].2, 12);
    }
}
#[tokio::test]
async fn failed_state_partition_open_is_reported_without_beginning_transaction() {
    let dir = Dir::new();
    let engine = StorageEngine::new(dir.config()).unwrap();
    std::fs::write(dir.0.join("__transaction_state-0"), b"blocked").unwrap();
    assert!(engine.begin_transaction("t", 1).is_err());
    assert!(!engine.transactions().is_ongoing("t"));
    engine.transactions().begin_transaction("t", 1).unwrap();
    assert!(engine
        .add_partitions_to_txn("t", 1, 0, &[("topic".into(), vec![0])])
        .is_err());
    assert!(engine.transactions().get_partitions("t").is_empty());
    assert!(engine.commit_transaction("t").is_err());
    assert_eq!(
        engine.transactions().get_status("t"),
        Some(TxStatus::Ongoing)
    );
    assert!(engine.abort_transaction("t").is_err());
    assert_eq!(
        engine.transactions().get_status("t"),
        Some(TxStatus::Ongoing)
    );
}
#[tokio::test]
async fn prepared_commit_survives_restart_and_retries_without_switching_to_abort() {
    let dir = Dir::new();
    let engine = StorageEngine::new(dir.config()).unwrap();
    engine.begin_transaction("t", 1).unwrap();
    engine
        .add_partitions_to_txn("t", 1, 0, &[("blocked".into(), vec![0])])
        .unwrap();
    std::fs::write(dir.0.join("blocked-0"), b"blocked").unwrap();
    assert!(engine.commit_transaction("t").is_err());
    assert_eq!(
        engine.transactions().get_status("t"),
        Some(TxStatus::PrepareCommit)
    );
    assert!(engine.abort_transaction("t").is_err());
    assert!(engine
        .add_partitions_to_txn("t", 1, 0, &[("other".into(), vec![0])])
        .is_err());
    drop(engine);
    std::fs::remove_file(dir.0.join("blocked-0")).unwrap();
    let restarted = StorageEngine::new(dir.config()).unwrap();
    assert_eq!(
        restarted.transactions().get_status("t"),
        Some(TxStatus::PrepareCommit)
    );
    restarted.commit_transaction("t").unwrap();
    assert!(restarted.transactions().get_status("t").is_none());
    drop(restarted);
    let reopened = StorageEngine::new(dir.config()).unwrap();
    assert!(reopened.transactions().get_status("t").is_none());
}
#[tokio::test]
async fn registration_write_failure_does_not_publish_new_epoch() {
    let dir = Dir::new();
    let engine = StorageEngine::new(dir.config()).unwrap();
    std::fs::create_dir(dir.0.join("__cluster_metadata-0/committed.tmp")).unwrap();
    engine
        .transactions()
        .restore_transactional_producer("t", 7, 2);
    assert!(engine.init_producer_id_coordinated("t").await.is_err());
    engine
        .transactions()
        .validate_transactional_producer("t", 7, 2)
        .unwrap();
    assert!(engine.init_producer_id("t").is_err());
    engine
        .transactions()
        .validate_transactional_producer("t", 7, 2)
        .unwrap();
}
#[tokio::test]
async fn producer_registration_requires_controller_and_quorum() {
    let dir = Dir::new();
    let mut cfg = dir.config();
    cfg.peer_addrs = vec!["127.0.0.1:1".into()];
    let engine = StorageEngine::new(cfg).unwrap();
    assert!(engine.init_producer_id("t").is_err());
    assert!(engine.init_producer_id_coordinated("t").await.is_err());
    assert!(!engine.transactions().has_transactional_producer("t"));
    let dir2 = Dir::new();
    let mut cfg = dir2.config();
    cfg.role = bifrox::replication::NodeRole::Follower;
    let follower = StorageEngine::new(cfg).unwrap();
    assert!(follower.init_producer_id_coordinated("t").await.is_err());
    assert!(!follower.transactions().has_transactional_producer("t"));
}
