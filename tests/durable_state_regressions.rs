use bifrox::replication::BifroxConsensus;
use bifrox::{EngineConfig, PartitionManager};
struct Temp(std::path::PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "bifrox_durable_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[test]
fn uncommitted_records_remain_uncommitted_after_reopen() {
    let t = Temp::new();
    let cfg = EngineConfig {
        preallocate_segments: false,
        ..Default::default()
    };
    let p = PartitionManager::open(&t.0, "t", 0, cfg.clone()).unwrap();
    p.produce_frame_eos(bytes::Bytes::from_static(b"uncommitted"), 0, 0, 0)
        .unwrap()
        .unwrap();
    drop(p);
    let p = PartitionManager::open(&t.0, "t", 0, cfg.clone()).unwrap();
    assert_eq!(p.latest_offset(), 1);
    assert_eq!(p.high_watermark(), 0);
    assert!(p.fetch_entries(0, 4096).unwrap().is_empty());
    p.advance_committed_hw(1).unwrap();
    drop(p);
    assert_eq!(
        PartitionManager::open(&t.0, "t", 0, cfg)
            .unwrap()
            .high_watermark(),
        1
    );
}
#[test]
fn failed_checkpoint_cannot_publish_commit() {
    let t = Temp::new();
    let p = PartitionManager::open(
        &t.0,
        "t",
        0,
        EngineConfig {
            preallocate_segments: false,
            ..Default::default()
        },
    )
    .unwrap();
    p.produce_frame_eos(bytes::Bytes::from_static(b"pending"), 0, 0, 0)
        .unwrap()
        .unwrap();
    std::fs::remove_file(t.0.join("committed.offset")).unwrap();
    std::fs::create_dir(t.0.join("committed.offset")).unwrap();
    assert!(p.advance_committed_hw(1).is_err());
    assert_eq!(p.high_watermark(), 0);
}
#[test]
fn legacy_records_without_a_checkpoint_are_not_assumed_committed() {
    let t = Temp::new();
    let p = PartitionManager::open(
        &t.0,
        "t",
        0,
        EngineConfig {
            preallocate_segments: false,
            ..Default::default()
        },
    )
    .unwrap();
    p.produce(b"legacy").unwrap();
    drop(p);
    std::fs::remove_file(t.0.join("committed.offset")).unwrap();
    assert!(PartitionManager::open(&t.0, "t", 0, EngineConfig::default()).is_err());
}
#[test]
fn raft_votes_survive_restart_and_heartbeat() {
    let t = Temp::new();
    let path = t.0.join("raft");
    {
        let c = BifroxConsensus::new(1, 3);
        c.enable_persistence(path.clone()).unwrap();
        assert!(c.try_record_vote(2, 5));
    }
    let c = BifroxConsensus::new(1, 3);
    c.enable_persistence(path).unwrap();
    assert_eq!(c.current_term(), 5);
    assert!(!c.try_record_vote(3, 5));
    assert!(c.handle_leader_heartbeat(3, 5));
    assert!(!c.try_record_vote(3, 5));
}
#[test]
fn failed_raft_persistence_fences_voting_until_restart() {
    let t = Temp::new();
    let path = t.0.join("raft");
    let c = BifroxConsensus::new(1, 3);
    c.enable_persistence(path.clone()).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(!c.try_record_vote(2, 5));
    std::fs::remove_dir(&path).unwrap();
    assert!(!c.try_record_vote(3, 6));
}
