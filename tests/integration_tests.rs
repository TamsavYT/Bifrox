use hermes::{
    hash_key, EngineConfig, FlushPolicy, GroupConsumer, GroupConsumerConfig, RecordFrame, Server,
    StorageEngine, TestClient,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

/// RAII Guard ensuring automatic cleanup of unique test data directories upon test completion (Rule C)
pub struct TestEnv {
    pub addr: SocketAddr,
    pub data_dir: std::path::PathBuf,
    pub engine: StorageEngine,
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

pub struct TestDataDirGuard {
    pub path: std::path::PathBuf,
}

impl TestDataDirGuard {
    pub fn new(prefix: &str) -> Self {
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hermes_test_{}_{}_{}_{}",
            prefix,
            std::process::id(),
            nanos,
            count
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TestDataDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Helper function implementing Rules A, B, and C:
/// - Rule A: Unique Data Directory per test using nanosecond timestamping
/// - Rule B: Bind TCP Server to Port 0 (Dynamic Ephemeral Port: 127.0.0.1:0)
/// - Rule C: Clean up test artifacts upon completion via Drop guard
use std::sync::atomic::{AtomicU64, Ordering};
static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

async fn start_test_server() -> TestEnv {
    start_test_server_with_quota(None, None).await
}

/// Same as `start_test_server` but allows configuring per-client produce/fetch byte-rate
/// quotas, used to verify Kafka-style throttling behavior end-to-end over the wire.
async fn start_test_server_with_quota(
    produce_quota_bytes_per_sec: Option<u64>,
    fetch_quota_bytes_per_sec: Option<u64>,
) -> TestEnv {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!(
        "storage_test_{}_{}_{}",
        std::process::id(),
        nanos,
        count
    ));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();

    let config = EngineConfig {
        data_dir: data_dir.clone(),
        max_segment_bytes: 64 * 1024,
        index_interval_bytes: 256,
        flush_policy: FlushPolicy::AsyncPeriodic {
            interval: Duration::from_millis(5),
            max_bytes: 4 * 1024,
        },
        preallocate_segments: false,
        bind_addr: "127.0.0.1:0".to_string(), // Rule B: Dynamic Ephemeral Port
        retention_bytes: None,
        retention_millis: None,
        retention_check_interval: Duration::from_secs(60),
        produce_quota_bytes_per_sec,
        fetch_quota_bytes_per_sec,
        // The production default (3s, matching Kafka) would add that much latency to
        // every JoinGroup in the suite. The barrier's behavior is what's under test, not
        // its duration, so keep the window short here.
        group_initial_rebalance_delay_ms: 60,
        ..EngineConfig::default()
    };

    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine.clone());
    let (listener, addr) = server.bind().unwrap();

    tokio::spawn(async move {
        let _ = server.run_with_listener(listener).await;
    });

    TestEnv {
        addr,
        data_dir,
        engine,
    }
}

/// Same as `start_test_server` but with a configurable `max.poll.interval.ms` (issue #54)
/// — needed to exercise a stalled-consumption eviction in bounded real time instead of the
/// production default's five minutes.
async fn start_test_server_with_max_poll_interval(max_poll_interval_ms: u64) -> TestEnv {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!(
        "storage_test_{}_{}_{}",
        std::process::id(),
        nanos,
        count
    ));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();

    let config = EngineConfig {
        data_dir: data_dir.clone(),
        max_segment_bytes: 64 * 1024,
        index_interval_bytes: 256,
        flush_policy: FlushPolicy::AsyncPeriodic {
            interval: Duration::from_millis(5),
            max_bytes: 4 * 1024,
        },
        preallocate_segments: false,
        bind_addr: "127.0.0.1:0".to_string(), // Rule B: Dynamic Ephemeral Port
        retention_bytes: None,
        retention_millis: None,
        retention_check_interval: Duration::from_secs(60),
        // Same reasoning as `start_test_server_with_quota`: keep the join barrier short
        // so it doesn't add latency to every JoinGroup in tests using this helper.
        group_initial_rebalance_delay_ms: 60,
        max_poll_interval_ms,
        ..EngineConfig::default()
    };

    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine.clone());
    let (listener, addr) = server.bind().unwrap();

    tokio::spawn(async move {
        let _ = server.run_with_listener(listener).await;
    });

    TestEnv {
        addr,
        data_dir,
        engine,
    }
}

#[tokio::test]
async fn test_scenario_1_connection_and_handshake() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr)
        .await
        .expect("Failed to connect");
    assert!(client.is_connected());

    // Verify basic protocol ping-pong / handshake
    let healthy = client.ping_handshake().await.expect("Handshake failed");
    assert!(healthy, "Ping handshake should return true");

    // Perform multiple sequential handshakes over same persistent connection
    for _ in 0..5 {
        let ok = client.ping_handshake().await.unwrap();
        assert!(ok);
    }

    client.disconnect();
    assert!(!client.is_connected());
}

#[tokio::test]
async fn test_scenario_2_produce_flow() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "test_produce_topic";
    let num_partitions = 4u32;

    // 1. Single message produce
    let key1 = "order_101";
    let expected_partition1 = hash_key(key1.as_bytes(), num_partitions as usize);
    let prod_res_1 = client
        .produce_single(topic, key1, None, num_partitions, "payload_alpha")
        .await
        .unwrap();

    assert_eq!(prod_res_1.assigned_partition, expected_partition1);
    assert_eq!(prod_res_1.first_offset, 0);
    assert_eq!(prod_res_1.last_offset, 0);

    // 2. Batched message produce with same key
    let batch = vec!["payload_beta_1", "payload_beta_2", "payload_beta_3"];
    let prod_res_batch = client
        .produce_batch(topic, key1, None, num_partitions, &batch)
        .await
        .unwrap();

    assert_eq!(prod_res_batch.assigned_partition, expected_partition1);
    assert_eq!(prod_res_batch.first_offset, 1);
    assert_eq!(prod_res_batch.last_offset, 3);

    // 3. Batched message produce with different key
    let key2 = "user_9999";
    let expected_partition2 = hash_key(key2.as_bytes(), num_partitions as usize);
    let prod_res_key2 = client
        .produce_batch(topic, key2, None, num_partitions, &["msg_x", "msg_y"])
        .await
        .unwrap();

    assert_eq!(prod_res_key2.assigned_partition, expected_partition2);
    if expected_partition1 == expected_partition2 {
        assert_eq!(prod_res_key2.first_offset, 4);
        assert_eq!(prod_res_key2.last_offset, 5);
    } else {
        assert_eq!(prod_res_key2.first_offset, 0);
        assert_eq!(prod_res_key2.last_offset, 1);
    }
}

#[tokio::test]
async fn test_scenario_3_consume_fetch_flow() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "test_fetch_topic";
    let partition = 0u32;
    let records = vec!["record_0", "record_1", "record_2", "record_3", "record_4"];

    // Produce 5 records
    let prod_resp = client
        .produce_batch(topic, "", None, 1, &records)
        .await
        .unwrap();
    assert_eq!(prod_resp.first_offset, 0);
    assert_eq!(prod_resp.last_offset, 4);

    // Fetch starting from offset 0
    let frames = client.fetch(topic, partition, 0, 64 * 1024).await.unwrap();
    assert_eq!(frames.len(), 5);

    for (i, frame) in frames.iter().enumerate() {
        assert_eq!(frame.offset, i as u64, "Logical offset sequence mismatch");
        assert_eq!(
            frame.payload,
            records[i].as_bytes(),
            "Payload content mismatch"
        );
        // Verify CRC32 checksum calculation matches expected
        let calculated_crc =
            RecordFrame::calculate_crc(frame.offset, frame.timestamp, &frame.payload);
        assert_eq!(frame.crc, calculated_crc, "Frame CRC32 checksum mismatch");
    }

    // Fetch starting from offset 2
    let sliced_frames = client.fetch(topic, partition, 2, 64 * 1024).await.unwrap();
    assert_eq!(sliced_frames.len(), 3);
    assert_eq!(sliced_frames[0].offset, 2);
    assert_eq!(sliced_frames[0].payload, "record_2".as_bytes());

    // Fetch from out-of-bounds offset
    let oob_frames = client
        .fetch(topic, partition, 100, 64 * 1024)
        .await
        .unwrap();
    assert!(
        oob_frames.is_empty(),
        "Out of bounds fetch should return empty list"
    );
}

#[tokio::test]
async fn test_scenario_4_metadata_and_management() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "test_meta_topic";
    let partition = 0u32;
    let group_id = "consumer_group_alpha";

    // 1. Initial latest offset should be 0
    let initial_watermark = client.latest_offset(topic, partition).await.unwrap();
    assert_eq!(initial_watermark, 0);

    // 2. Uncommitted offset should return u64::MAX
    let uncommitted = client
        .fetch_offset(group_id, topic, partition)
        .await
        .unwrap();
    assert_eq!(uncommitted, u64::MAX);

    // 3. Produce 3 records
    let _p_res = client
        .produce_batch(topic, "", None, 1, &["m1", "m2", "m3"])
        .await
        .unwrap();

    // 4. Latest offset watermark updated to 3
    let new_watermark = client.latest_offset(topic, partition).await.unwrap();
    assert_eq!(new_watermark, 3);

    // 5. Commit offset 2 for group
    client
        .commit_offset(group_id, topic, partition, 2)
        .await
        .unwrap();

    // 6. Fetch committed offset
    let committed = client
        .fetch_offset(group_id, topic, partition)
        .await
        .unwrap();
    assert_eq!(committed, 2);

    // 7. Seek offset position
    let seek_res = client.seek(topic, partition, 0).await.unwrap();
    assert_eq!(seek_res.base_offset, 0);
}

#[tokio::test]
async fn test_scenario_5_concurrency_testing() {
    let env = start_test_server().await;
    let topic = "concurrent_topic";
    let num_partitions = 4u32;
    let num_producers = 5;
    let records_per_producer = 20;

    let mut tasks = Vec::new();

    // Spawn concurrent producer tasks
    for p_id in 0..num_producers {
        let server_addr = env.addr;
        let t_name = topic.to_string();
        tasks.push(tokio::spawn(async move {
            let mut p_client = TestClient::connect(server_addr).await.unwrap();
            let key = format!("user_key_{}", p_id);
            for i in 0..records_per_producer {
                let payload = format!("prod_{}_msg_{}", p_id, i);
                p_client
                    .produce_single(&t_name, &key, None, num_partitions, payload)
                    .await
                    .expect("Concurrent produce failed");
            }
        }));
    }

    // Spawn concurrent consumer tasks
    for part in 0..num_partitions {
        let server_addr = env.addr;
        let t_name = topic.to_string();
        tasks.push(tokio::spawn(async move {
            let mut c_client = TestClient::connect(server_addr).await.unwrap();
            let mut total_read = 0;
            // Poll until all produced records are consumed or timeout
            for _ in 0..30 {
                let frames = c_client
                    .fetch(&t_name, part, total_read as u64, 64 * 1024)
                    .await
                    .unwrap();
                total_read += frames.len();
                if total_read >= (num_producers * records_per_producer / num_partitions as usize) {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    // Wait for all producer & consumer tasks to complete
    for task in tasks {
        task.await.unwrap();
    }

    // Verify overall total count across all partitions
    let mut total_produced = 0u64;
    let mut client = TestClient::connect(env.addr).await.unwrap();
    for part in 0..num_partitions {
        let watermark = client.latest_offset(topic, part).await.unwrap();
        total_produced += watermark;
    }

    assert_eq!(
        total_produced,
        (num_producers * records_per_producer) as u64
    );
}

#[tokio::test]
async fn test_scenario_6_fault_tolerance_and_edge_cases() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    // 1. Fetching from non-existent topic returns empty list cleanly
    let non_existent_frames = client
        .fetch("non_existent_topic_xyz", 0, 0, 1024)
        .await
        .unwrap();
    assert!(non_existent_frames.is_empty());

    // 2. Malformed Command Code (e.g. 0xFF)
    let malformed_cmd_raw = vec![0xFF, 0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04];
    let resp1 = client.send_raw_bytes(&malformed_cmd_raw).await.unwrap();
    assert_eq!(
        resp1.status, 1,
        "Malformed command code must return status 1 (Error)"
    );

    // Reconnect after protocol error (since server drops socket on invalid framing)
    client.reconnect().await.unwrap();

    // 3. Truncated Payload (Payload Len = 100, but only 4 bytes provided)
    let truncated_frame = vec![0x01, 0x00, 0x00, 0x00, 0x64, 0x01, 0x02, 0x03, 0x04];
    let _ = client.send_raw_bytes_no_wait(&truncated_frame).await;

    // Server should close connection or timeout cleanly on incomplete frame
    sleep(Duration::from_millis(50)).await;
    let _ = client.reconnect().await;

    // 4. Unexpected socket disconnect and reconnection recovery
    client.disconnect();
    assert!(!client.is_connected());

    client.reconnect().await.expect("Client reconnect failed");
    assert!(client.is_connected());

    // Confirm client can produce and fetch normally after reconnection
    let prod_res = client
        .produce_single("reconnect_topic", "k1", None, 1, "post_reconnect_msg")
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    let fetched = client.fetch("reconnect_topic", 0, 0, 1024).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].payload, "post_reconnect_msg".as_bytes());
}

#[tokio::test]
async fn test_scenario_7_milestone3_features() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_dir = std::env::temp_dir().join(format!("m3_test_{}", nanos));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();

    // 1. Time-based Indexing (.timeindex) Test
    let time_idx_path = test_dir.join("00000000000000000000.timeindex");
    let mut time_idx = hermes::TimeIndexSegment::open(&time_idx_path, 0).unwrap();
    time_idx.append(1000, 0).unwrap();
    time_idx.append(2000, 10).unwrap();
    time_idx.append(3000, 20).unwrap();

    assert_eq!(time_idx.find_offset_for_timestamp(1500), Some(0));
    assert_eq!(time_idx.find_offset_for_timestamp(2500), Some(10));
    assert_eq!(time_idx.find_offset_for_timestamp(3000), Some(20));

    // 2. Transaction Manager Test (Begin, Commit, Abort, Duplicate sequence check)
    let tx_mgr = hermes::TransactionManager::new();
    assert!(!tx_mgr.is_duplicate(101, 1));
    tx_mgr.record_sequence(101, 1);
    assert!(tx_mgr.is_duplicate(101, 1)); // Duplicate retry detected
    assert!(!tx_mgr.is_duplicate(101, 2));

    tx_mgr.begin_transaction("tx_orders_99", 101).unwrap();
    tx_mgr.prepare_commit("tx_orders_99").unwrap();
    // No partitions were registered on this transaction, so there are no end offsets to
    // record — commit now takes the same range bookkeeping abort always did.
    tx_mgr.complete_commit("tx_orders_99", &[]).unwrap();

    // 3. High Availability Replication Manager Test
    let cluster_config = hermes::ClusterConfig {
        cluster_id: "test-cluster".to_string(),
        node_id: 1,
        role: hermes::NodeRole::Leader,
        peer_addrs: Vec::new(),
        min_insync_replicas: 1,
        roles: vec![
            hermes::config::ProcessRole::Controller,
            hermes::config::ProcessRole::Broker,
        ],
        controller_peer_addrs: Vec::new(),
    };
    let repl_mgr = hermes::ReplicationManager::new(
        cluster_config,
        "127.0.0.1:0".to_string(),
        std::sync::Arc::new(dashmap::DashMap::new()),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );
    assert_eq!(repl_mgr.role(), hermes::NodeRole::Leader);

    let frame = hermes::RecordFrame::create(0, 1000, "replicated_payload");
    let res = repl_mgr
        .replicate_batch("events", 0, 0, 0, &[], &[frame])
        .await;
    assert!(res.is_ok());

    let _ = std::fs::remove_dir_all(&test_dir);
}

#[tokio::test]
async fn test_scenario_8_kraft_grpc_isr() {
    // 1. Priority 1 & 3: gRPC Pull Replication Codec Test
    let fetch_req = hermes::ReplicationFetchRequest {
        follower_node_id: 2,
        topic: "orders_stream".to_string(),
        partition: 1,
        fetch_offset: 50,
        max_bytes: 65536,
    };
    let encoded_req = fetch_req.encode();
    let (decoded_req, _) = hermes::ReplicationFetchRequest::decode(&encoded_req).unwrap();
    assert_eq!(decoded_req.follower_node_id, 2);
    assert_eq!(decoded_req.topic, "orders_stream");
    assert_eq!(decoded_req.fetch_offset, 50);

    // 2. Priority 2: In-Sync Replicas (ISR) Quorum Gating Test
    let cluster_config = hermes::ClusterConfig {
        cluster_id: "test-cluster".to_string(),
        node_id: 1,
        role: hermes::NodeRole::Leader,
        peer_addrs: vec!["127.0.0.1:9093".to_string()],
        min_insync_replicas: 2,
        roles: vec![
            hermes::config::ProcessRole::Controller,
            hermes::config::ProcessRole::Broker,
        ],
        controller_peer_addrs: Vec::new(),
    };
    let repl_mgr = hermes::ReplicationManager::new(
        cluster_config,
        "127.0.0.1:0".to_string(),
        std::sync::Arc::new(dashmap::DashMap::new()),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );

    // Watermark bookkeeping is still owned by the replication manager, but the decision
    // of *who must acknowledge* now lives with the engine, which is what knows the live
    // ISR (see `test_scenario_39_acks_all_waits_for_every_isr_member`).
    assert_eq!(
        repl_mgr.replica_watermark("orders_stream", 1, "127.0.0.1:9093"),
        None,
        "no ack observed yet"
    );
    repl_mgr.update_replica_watermark("orders_stream", 1, "127.0.0.1:9093", 100);
    assert_eq!(
        repl_mgr.replica_watermark("orders_stream", 1, "127.0.0.1:9093"),
        Some(100),
        "watermark must be recorded for the acking replica"
    );

    // 3. Hermes Consensus Leader Election Test
    let consensus = hermes::HermesConsensus::new(2, 3);
    assert_eq!(consensus.state(), hermes::ConsensusState::Follower);

    // Simulate leader heartbeat ping
    assert!(consensus.handle_leader_heartbeat(1, 1));
    assert_eq!(consensus.current_term(), 1);

    // Simulate candidate election state & vote tallying
    consensus.force_candidate_state();
    assert_eq!(consensus.state(), hermes::ConsensusState::Candidate);

    assert!(consensus.tally_election_votes(2)); // 2/3 votes = Quorum majority achieved
    assert_eq!(consensus.state(), hermes::ConsensusState::Leader);
}

#[tokio::test]
async fn test_scenario_9_network_transactions_and_timestamp_fetch() {
    let test_env = start_test_server().await;
    let mut client = TestClient::connect(test_env.addr).await.unwrap();

    // 1. Test Network Transactions over TCP Socket (BeginTx, CommitTx, AbortTx)
    assert!(client
        .begin_transaction("tx_checkout_1001", 88)
        .await
        .is_ok());
    assert!(client.commit_transaction("tx_checkout_1001").await.is_ok());

    assert!(client
        .begin_transaction("tx_checkout_1002", 88)
        .await
        .is_ok());
    assert!(client.abort_transaction("tx_checkout_1002").await.is_ok());

    // 2. Test FetchByTimestamp over TCP Socket
    let prod_res = client
        .produce_single("time_topic", "k1", None, 1, "Timestamp Payload")
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    let frames = client
        .fetch_by_timestamp("time_topic", 0, 1000, 65536)
        .await
        .unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].payload, "Timestamp Payload".as_bytes());
}

#[tokio::test]
async fn test_scenario_10_multi_node_cluster_replication() {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let base_dir = std::env::temp_dir().join(format!("cluster_test_{}_{}", pid, count));

    let node1_dir = base_dir.join("node1_data");
    let node2_dir = base_dir.join("node2_data");
    let _ = std::fs::remove_dir_all(&base_dir);

    // 1. Start Follower (Node 2) on ephemeral port
    let config_node2 = EngineConfig {
        node_id: 2,
        role: hermes::NodeRole::Follower,
        data_dir: node2_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        ..EngineConfig::default()
    };
    let engine_node2 = StorageEngine::new(config_node2).unwrap();
    let server_node2 = Server::new(engine_node2.clone());
    let (listener_node2, addr_node2) = server_node2.bind().unwrap();

    let server_node2_task = tokio::spawn(async move {
        let _ = server_node2.run_with_listener(listener_node2).await;
    });

    // 2. Start Leader (Node 1) with Node 2's address as peer
    let config_node1 = EngineConfig {
        node_id: 1,
        role: hermes::NodeRole::Leader,
        data_dir: node1_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        peer_addrs: vec![addr_node2.to_string()],
        ..EngineConfig::default()
    };
    let engine_node1 = StorageEngine::new(config_node1).unwrap();
    let server_node1 = Server::new(engine_node1.clone());
    let (listener_node1, addr_node1) = server_node1.bind().unwrap();

    let server_node1_task = tokio::spawn(async move {
        let _ = server_node1.run_with_listener(listener_node1).await;
    });

    sleep(Duration::from_millis(50)).await;

    // 3. Client produces to Leader (Node 1)
    let mut client_leader = TestClient::connect(addr_node1).await.unwrap();
    let prod_res = client_leader
        .produce_single(
            "cluster_topic",
            "key1",
            None,
            1,
            "Cluster Replication Event 101",
        )
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    // Sleep briefly for async inter-node replication packet transfer
    sleep(Duration::from_millis(100)).await;

    // 4. Client fetches from Follower (Node 2) - Asserts record was replicated to Node 2's disk!
    let mut client_follower = TestClient::connect(addr_node2).await.unwrap();
    let fetched = client_follower
        .fetch("cluster_topic", 0, 0, 65536)
        .await
        .unwrap();

    assert_eq!(fetched.len(), 1);
    assert_eq!(
        fetched[0].payload,
        "Cluster Replication Event 101".as_bytes()
    );

    // Issue #62: prove broker discovery genuinely completed rather than merely that data
    // arrived — Node 2 (the follower) booted with the default *empty* peer_addrs, which
    // deadlocked heartbeat acceptance before this fix and left the leader's view of the
    // cluster permanently at `[self]`. Delivery above could previously happen anyway
    // because the leader also pushes to `config.peer_addrs` regardless of discovery, so
    // it alone doesn't prove the deadlock is fixed — a real replica assignment does: it
    // can only exist once the leader has learned Node 2 exists (via Node 2's heartbeat
    // ACK) and named it a replica through `ensure_topic_created`/the reconcile sweep.
    let mut replicas = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if let Some(parts) = engine_node1.describe_topic("cluster_topic") {
            if let Some(p0) = parts.iter().find(|p| p.partition_id == 0) {
                if p0.replicas.len() >= 2 {
                    replicas = p0.replicas.clone();
                    break;
                }
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        replicas.len() >= 2,
        "cluster_topic-0 must have a multi-replica assignment once discovery completes; \
         got replicas={:?}",
        replicas
    );
    assert!(
        replicas.contains(&1) && replicas.contains(&2),
        "the assignment must name both nodes as replicas, got {:?}",
        replicas
    );

    server_node1_task.abort();
    server_node2_task.abort();
    let _ = std::fs::remove_dir_all(&base_dir);
}

#[tokio::test]
async fn test_scenario_11_metadata_replayed_topic_creation() {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let base_dir = std::env::temp_dir().join(format!("metadata_test_{}_{}", pid, count));

    let node1_dir = base_dir.join("node1_data");
    let node2_dir = base_dir.join("node2_data");
    let _ = std::fs::remove_dir_all(&base_dir);

    // 1. Start Follower (Node 2) on ephemeral port
    let config_node2 = EngineConfig {
        node_id: 2,
        role: hermes::NodeRole::Follower,
        data_dir: node2_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        ..EngineConfig::default()
    };
    let engine_node2 = StorageEngine::new(config_node2).unwrap();
    let server_node2 = Server::new(engine_node2);
    let (listener_node2, addr_node2) = server_node2.bind().unwrap();

    let server_node2_task = tokio::spawn(async move {
        let _ = server_node2.run_with_listener(listener_node2).await;
    });

    // 2. Start Leader (Node 1) with Node 2's address as peer
    let config_node1 = EngineConfig {
        node_id: 1,
        role: hermes::NodeRole::Leader,
        data_dir: node1_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        peer_addrs: vec![addr_node2.to_string()],
        ..EngineConfig::default()
    };
    let engine_node1 = StorageEngine::new(config_node1).unwrap();
    let server_node1 = Server::new(engine_node1.clone());
    let (listener_node1, addr_node1) = server_node1.bind().unwrap();

    let server_node1_task = tokio::spawn(async move {
        let _ = server_node1.run_with_listener(listener_node1).await;
    });

    sleep(Duration::from_millis(100)).await;

    // 3. Client produces to Leader (Node 1) on a brand new topic
    let mut client_leader = TestClient::connect(addr_node1).await.unwrap();
    let prod_res = client_leader
        .produce_single(
            "dynamic_topic",
            "key123",
            None,
            1,
            "Dynamic Metadata Replicated Event",
        )
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    // Sleep briefly to allow __cluster_metadata and dynamic_topic to replicate
    sleep(Duration::from_millis(200)).await;

    // 4. Fetch directly from Follower (Node 2) on the dynamically created partition
    let mut client_follower = TestClient::connect(addr_node2).await.unwrap();
    let fetched = client_follower
        .fetch("dynamic_topic", 0, 0, 65536)
        .await
        .unwrap();

    assert_eq!(fetched.len(), 1);
    assert_eq!(
        fetched[0].payload,
        "Dynamic Metadata Replicated Event".as_bytes()
    );

    // Issue #62: same proof as scenario 10 — the topic must carry a genuine multi-replica
    // assignment, not merely have delivered a record via the leader's unconditional push
    // to its static `peer_addrs`. Node 2 booted with an empty `peer_addrs`, the exact
    // topology that deadlocked heartbeat acceptance before this fix.
    let mut replicas = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if let Some(parts) = engine_node1.describe_topic("dynamic_topic") {
            if let Some(p0) = parts.iter().find(|p| p.partition_id == 0) {
                if p0.replicas.len() >= 2 {
                    replicas = p0.replicas.clone();
                    break;
                }
            }
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        replicas.len() >= 2,
        "dynamic_topic-0 must have a multi-replica assignment once discovery completes; \
         got replicas={:?}",
        replicas
    );
    assert!(
        replicas.contains(&1) && replicas.contains(&2),
        "the assignment must name both nodes as replicas, got {:?}",
        replicas
    );

    server_node1_task.abort();
    server_node2_task.abort();
    let _ = std::fs::remove_dir_all(&base_dir);
}

// ─────────────────────────────────────────────────────────
// P1: Read-Committed Consumer Isolation (LSO) Tests
// ─────────────────────────────────────────────────────────

/// Scenario 12: fetch_committed returns data frames without panicking and hides control markers.
/// After CommitTx the commit control-marker is written; fetch_committed still returns only data records.
#[tokio::test]
async fn test_scenario_12_read_committed_isolation() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "lso_test_topic";
    let partition = 0u32;
    let num_parts = 1u32;

    // 1. Begin a transaction
    client.begin_transaction("tx_lso_1", 42).await.unwrap();

    // 2. Produce 3 records
    let r1 = client
        .produce_single(topic, "", Some("tx_lso_1"), num_parts, "msg_hidden_1")
        .await
        .unwrap();
    let r2 = client
        .produce_single(topic, "", Some("tx_lso_1"), num_parts, "msg_hidden_2")
        .await
        .unwrap();
    let r3 = client
        .produce_single(topic, "", Some("tx_lso_1"), num_parts, "msg_hidden_3")
        .await
        .unwrap();
    assert_eq!(r1.first_offset, 0);
    assert_eq!(r2.first_offset, 1);
    assert_eq!(r3.first_offset, 2);

    // 3. read_uncommitted must see all 3 records
    let frames_uncommitted = client.fetch(topic, partition, 0, 65536).await.unwrap();
    assert_eq!(
        frames_uncommitted.len(),
        3,
        "read_uncommitted must see all 3 records"
    );

    // 4. fetch_committed returns only non-control-marker data records
    let frames_committed = client
        .fetch_committed(topic, partition, 0, 65536)
        .await
        .unwrap();
    for frame in &frames_committed {
        assert_ne!(
            frame.magic, 0xAD,
            "fetch_committed must never return control markers"
        );
    }

    // 5. Commit the transaction — this writes a Commit control marker (0xAD)
    client.commit_transaction("tx_lso_1").await.unwrap();

    // 6. After commit, read_uncommitted sees data records + control marker
    let frames_all = client.fetch(topic, partition, 0, 65536).await.unwrap();
    assert!(
        frames_all.len() >= 3,
        "After commit, at least 3+ frames exist (data + marker)"
    );

    // 7. fetch_committed filters out 0xAD control markers — returns only data records
    let frames_after_commit = client
        .fetch_committed(topic, partition, 0, 65536)
        .await
        .unwrap();
    for frame in &frames_after_commit {
        assert_ne!(
            frame.magic, 0xAD,
            "fetch_committed must not return control markers"
        );
    }
    assert_eq!(
        frames_after_commit.len(),
        3,
        "All 3 data records visible after commit"
    );
    assert_eq!(frames_after_commit[0].payload, "msg_hidden_1".as_bytes());
    assert_eq!(frames_after_commit[2].payload, "msg_hidden_3".as_bytes());
}

/// Scenario 13: After AbortTx, fetch_committed filters control markers.
/// Data records produced under the tx are still readable (aborted_ranges filtering
/// requires partition registration via register_tx_partition which is tested separately).
#[tokio::test]
async fn test_scenario_13_read_committed_abort_filtering() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "abort_filter_topic";
    let partition = 0u32;
    let num_parts = 1u32;

    // Produce one committed record before transaction (offset 0)
    client
        .produce_single(topic, "", None, num_parts, "committed_record")
        .await
        .unwrap();

    // Begin transaction and produce records that will be aborted
    client.begin_transaction("tx_abort_1", 99).await.unwrap();
    client
        .produce_single(topic, "", Some("tx_abort_1"), num_parts, "aborted_record_1")
        .await
        .unwrap();
    client
        .produce_single(topic, "", Some("tx_abort_1"), num_parts, "aborted_record_2")
        .await
        .unwrap();

    // Abort the transaction — writes 0xAD abort control marker
    client.abort_transaction("tx_abort_1").await.unwrap();

    // read_uncommitted returns all data records + abort marker
    let all = client.fetch(topic, partition, 0, 65536).await.unwrap();
    assert!(
        all.len() >= 3,
        "read_uncommitted sees 3+ frames, got {}",
        all.len()
    );

    // fetch_committed: no control markers (0xAD) must appear
    let committed = client
        .fetch_committed(topic, partition, 0, 65536)
        .await
        .unwrap();
    for frame in &committed {
        assert_ne!(frame.magic, 0xAD, "No control markers in fetch_committed");
    }
    // The pre-tx committed record must always be present
    assert!(
        committed
            .iter()
            .any(|f| f.payload.as_ref() == b"committed_record"),
        "Committed record must appear in read_committed"
    );
}

#[tokio::test]
async fn test_scenario_14_ping_list_describe_delete() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    // 1. Ping
    let pong = client.ping().await.unwrap();
    assert!(pong, "Ping should return true / PONG");

    // 2. Create topics by producing to them
    client
        .produce_single("topic_alpha", "k1", None, 1, "hello")
        .await
        .unwrap();
    client
        .produce_single("topic_beta", "k2", None, 1, "world")
        .await
        .unwrap();

    // 3. List topics
    let topics = client.list_topics().await.unwrap();
    assert!(topics.contains(&"topic_alpha".to_string()));
    assert!(topics.contains(&"topic_beta".to_string()));

    // 4. Delete topic_alpha
    client.delete_topic("topic_alpha").await.unwrap();
    let topics_after = client.list_topics().await.unwrap();
    assert!(!topics_after.contains(&"topic_alpha".to_string()));
    assert!(topics_after.contains(&"topic_beta".to_string()));
}

#[tokio::test]
async fn test_scenario_15_admin_apis_and_consumer_groups() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    // 1. Create Topic via Admin API
    client
        .create_topic("admin_topic", 4)
        .await
        .expect("Failed to create topic via Admin API");

    // Produce one message to force initialization check if needed
    let _ = client
        .produce_single("admin_topic", "key1", None, 4, "hello admin")
        .await
        .unwrap();

    let topics = client.list_topics().await.unwrap();
    assert!(topics.contains(&"admin_topic".to_string()));

    // 2. Consumer Group Join & Sync
    let group_id = "test_group";
    let member_id_1 = "member-1";
    let protocols = vec!["roundrobin"];

    // Member 1 joins
    client
        .join_group(group_id, member_id_1, &protocols)
        .await
        .expect("Failed to join group");

    // Member 1 syncs (leader assigning partitions)
    let assignments = vec![hermes::protocol::wire::MemberAssignment {
        member_id: member_id_1.to_string(),
        topic: "admin_topic".to_string(),
        partitions: vec![0, 1, 2, 3],
    }];
    client
        .sync_group(group_id, 1, member_id_1, &assignments)
        .await
        .expect("Failed to sync group");

    // 3. Heartbeat
    client
        .heartbeat(group_id, 1, member_id_1)
        .await
        .expect("Failed to send heartbeat");

    // 4. List Groups via Admin API
    let mut list_groups_req = Vec::new();
    list_groups_req.push(hermes::protocol::wire::CommandCode::ListGroups as u8);
    list_groups_req.extend_from_slice(&0u32.to_be_bytes());
    let resp = client.send_raw_bytes(&list_groups_req).await.unwrap();
    assert_eq!(resp.status, 0);
    // Parse groups
    let mut payload = &resp.payload[..];
    use bytes::Buf;
    if payload.len() >= 4 {
        let count = payload.get_u32() as usize;
        let mut groups = Vec::new();
        for _ in 0..count {
            let len = payload.get_u16() as usize;
            let g = String::from_utf8_lossy(&payload[..len]).to_string();
            payload = &payload[len..];
            groups.push(g);
        }
        assert!(groups.contains(&group_id.to_string()));
    }

    // 5. Describe Topic via Admin API
    let (desc_topic, desc_parts) = client
        .describe_topic("admin_topic")
        .await
        .expect("Failed to describe topic");
    assert_eq!(desc_topic, "admin_topic");
    assert_eq!(
        desc_parts.len(),
        4,
        "Should report 4 initialized partitions"
    );

    // 6. Describe Group via Admin API
    let (group_state, members) = client
        .describe_group(group_id)
        .await
        .expect("Failed to describe group");
    assert_eq!(group_state, "Stable");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].member_id, member_id_1);
    assert_eq!(members[0].assigned_partitions.len(), 4);

    // 7. Leave Group
    client
        .leave_group(group_id, member_id_1)
        .await
        .expect("Failed to leave group");
}

#[tokio::test]
async fn test_scenario_16_eos_idempotence() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "eos_topic";
    let transactional_id = "tx-eos-test-1";

    // 1. InitProducerId
    let (pid, epoch) = client
        .init_producer_id(transactional_id)
        .await
        .expect("InitProducerId failed");
    assert!(pid > 0, "Producer ID should be > 0");
    assert_eq!(epoch, 0, "Initial epoch should be 0");

    // 2. AddPartitionsToTxn
    let parts = vec![0];
    let topics = vec![(topic, parts.as_slice())];
    client
        .add_partitions_to_txn(transactional_id, pid, epoch, &topics)
        .await
        .expect("AddPartitionsToTxn failed");

    // 3. Produce with sequence 0
    let records = vec![b"record1".as_slice()];
    let _res1 = client
        .produce_batch_eos(
            topic,
            "k1",
            Some(transactional_id),
            1,
            pid,
            epoch,
            0,
            &records,
        )
        .await
        .expect("Produce seq 0 failed");

    // 4. Produce with sequence 0 again (Duplicate)
    let _res2 = client
        .produce_batch_eos(
            topic,
            "k1",
            Some(transactional_id),
            1,
            pid,
            epoch,
            0,
            &records,
        )
        .await
        .expect("Produce duplicate seq 0 failed");

    // Duplicate produce returns `Ok` with dummy offsets since they were not generated.
    // We expect the first one to be 0 or 1, the second one might just be whatever we returned. Let's just not assert on res2's offsets.

    // 5. Produce with sequence 1
    let records2 = vec![b"record2".as_slice()];
    let _res3 = client
        .produce_batch_eos(
            topic,
            "k1",
            Some(transactional_id),
            1,
            pid,
            epoch,
            1,
            &records2,
        )
        .await
        .expect("Produce seq 1 failed");

    // 6. EndTxn
    client
        .end_txn(transactional_id, pid, epoch, true)
        .await
        .expect("EndTxn failed");

    // Verify records
    let committed = client.fetch_committed(topic, 0, 0, 1024).await.unwrap();

    // Only two records should be present: "record1" and "record2". The duplicate "record1" should be dropped.
    assert_eq!(
        committed.len(),
        2,
        "Should only have 2 committed records (duplicate dropped)"
    );
    assert_eq!(committed[0].payload.as_ref(), b"record1");
    assert_eq!(committed[1].payload.as_ref(), b"record2");
}

#[tokio::test]
async fn test_scenario_17_durable_metadata_catalog() {
    let test_dir = std::env::temp_dir().join(format!(
        "hermes_test_meta_catalog_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();

    let config = hermes::EngineConfig {
        data_dir: test_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        ..hermes::EngineConfig::default()
    };

    let engine = std::sync::Arc::new(hermes::StorageEngine::new(config.clone()).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let engine_clone = engine.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let eng = engine_clone.clone();
                tokio::spawn(async move {
                    let _ =
                        hermes::server::handler::handle_connection(socket, (*eng).clone()).await;
                });
            }
        }
    });

    let mut client = TestClient::connect(addr).await.unwrap();

    // 1. Create topic via Admin API with 3 partitions
    client
        .create_topic("meta_topic", 3)
        .await
        .expect("CreateTopic failed");

    // 2. DescribeTopic should report 3 partitions with high watermark 0 even before messages are produced
    let (desc_topic, desc_parts) = client
        .describe_topic("meta_topic")
        .await
        .expect("DescribeTopic failed");
    assert_eq!(desc_topic, "meta_topic");
    assert_eq!(desc_parts[0].partition_id, 0);
    assert_eq!(desc_parts[0].high_watermark, 0);
    assert_eq!(desc_parts[1].partition_id, 1);
    assert_eq!(desc_parts[1].high_watermark, 0);
    assert_eq!(desc_parts[2].partition_id, 2);
    assert_eq!(desc_parts[2].high_watermark, 0);

    // 3. Verify ListTopics includes "meta_topic"
    let topics = client.list_topics().await.unwrap();
    assert!(topics.contains(&"meta_topic".to_string()));

    // 4. Simulate server restart and verify metadata log replay restores topic
    drop(engine);
    let restarted_engine = hermes::StorageEngine::new(config.clone()).unwrap();
    let restarted_parts = restarted_engine
        .describe_topic("meta_topic")
        .expect("DescribeTopic after restart failed");
    assert_eq!(
        restarted_parts.len(),
        3,
        "Restored topic should still report 3 partitions"
    );

    // 5. Delete topic and verify removal from registry and describe
    restarted_engine
        .delete_topic("meta_topic")
        .await
        .expect("DeleteTopic failed");
    assert!(restarted_engine.describe_topic("meta_topic").is_none());
    assert!(!restarted_engine
        .list_topics()
        .contains(&"meta_topic".to_string()));

    let _ = std::fs::remove_dir_all(&test_dir);
}

#[tokio::test]
async fn test_scenario_18_durable_consumer_offsets() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let group_id = "test-group-offsets";
    let topic = "test-offset-topic";
    let partition = 0u32;
    let commit_offset = 125u64;
    let metadata = "committed-by-integration-test";

    // 1. Commit offset with metadata via Opcode 0x1B
    client
        .offset_commit(group_id, topic, partition, commit_offset, metadata)
        .await
        .expect("OffsetCommit failed");

    // 2. Fetch offset via Opcode 0x1C
    let (fetched_offset, fetched_metadata) = client
        .offset_fetch(group_id, topic, partition)
        .await
        .expect("OffsetFetch failed");
    assert_eq!(fetched_offset, commit_offset);
    assert_eq!(fetched_metadata, metadata);

    // 3. Overwrite offset to test updated values
    let new_offset = 200u64;
    let new_metadata = "updated-metadata-v2";
    client
        .offset_commit(group_id, topic, partition, new_offset, new_metadata)
        .await
        .expect("OffsetCommit v2 failed");

    let (re_fetched_offset, re_fetched_metadata) = client
        .offset_fetch(group_id, topic, partition)
        .await
        .expect("OffsetFetch v2 failed");
    assert_eq!(re_fetched_offset, new_offset);
    assert_eq!(re_fetched_metadata, new_metadata);
}

#[tokio::test]
async fn test_scenario_19_relative_index_and_txnindex() {
    let test_dir = std::env::temp_dir().join(format!(
        "hermes_test_txn_index_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();

    let config = hermes::EngineConfig {
        data_dir: test_dir.clone(),
        index_interval_bytes: 10,
        ..hermes::EngineConfig::default()
    };

    let engine = hermes::StorageEngine::new(config).unwrap();
    let topic = "txn_idx_topic";

    // 1. Produce 5 records in a transaction
    let (tx_id, pid) = ("tx-idx-1", 9999u64);
    engine.begin_transaction(tx_id, pid).unwrap();
    engine
        .add_partitions_to_txn(tx_id, pid, 0, &[(topic.to_string(), vec![0])])
        .unwrap();

    let records = vec![
        bytes::Bytes::from("msg1"),
        bytes::Bytes::from("msg2"),
        bytes::Bytes::from("msg3"),
    ];
    let params = hermes::server::engine::ProduceBatchParams {
        topic,
        key: "",
        transaction_id: Some(tx_id),
        num_partitions: 1,
        producer_id: pid,
        producer_epoch: 0,
        base_sequence: 0,
        records: &records,
    };
    engine.produce_batch(params).await.unwrap();

    // 2. Abort transaction -> populates .txnindex
    engine.abort_transaction(tx_id).unwrap();

    // 3. Produce another non-transactional record
    let records_ok = vec![bytes::Bytes::from("good_msg")];
    let params_ok = hermes::server::engine::ProduceBatchParams {
        topic,
        key: "",
        transaction_id: None,
        num_partitions: 1,
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: 0,
        records: &records_ok,
    };
    engine.produce_batch(params_ok).await.unwrap();

    // 4. Verify fetch_committed drops the aborted messages via .txnindex
    let frames = engine
        .fetch_committed(topic, 0, 0, 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        frames.len(),
        1,
        "Should only contain good_msg (aborted batch filtered via .txnindex)"
    );
    assert_eq!(frames[0].payload.as_ref(), b"good_msg");

    // 5. Verify .index file size mathematically matches 8 bytes per index entry
    let index_file = test_dir.join(format!("{}-0/00000000000000000000.index", topic));
    if index_file.exists() {
        let meta = std::fs::metadata(&index_file).unwrap();
        assert_eq!(
            meta.len() % 8,
            0,
            "Index file size must be a multiple of 8 bytes"
        );
    }

    // 6. Verify .txnindex file exists and size matches 24 bytes per entry
    let txn_index_file = test_dir.join(format!("{}-0/00000000000000000000.txnindex", topic));
    assert!(
        txn_index_file.exists(),
        ".txnindex file must be created on transaction abort"
    );
    let txn_meta = std::fs::metadata(&txn_index_file).unwrap();
    assert_eq!(
        txn_meta.len(),
        24,
        ".txnindex file must contain exactly 1 entry of 24 bytes"
    );

    let _ = std::fs::remove_dir_all(&test_dir);
}

#[tokio::test]
async fn test_scenario_20_partition_level_leadership() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "leader_partition_topic";

    // 1. Create topic with 2 partitions
    client
        .create_topic(topic, 2)
        .await
        .expect("CreateTopic failed");

    // 2. DescribeTopic should report leader_id = 1 for both partitions on single-node setup
    let (desc_topic, desc_parts) = client
        .describe_topic(topic)
        .await
        .expect("DescribeTopic failed");
    assert_eq!(desc_topic, topic);
    assert_eq!(desc_parts.len(), 2);
    assert_eq!(desc_parts[0].leader_id, 1);
    assert_eq!(desc_parts[1].leader_id, 1);

    // 3. Produce to partition 0 succeeds on leader node 1
    client
        .produce_single(topic, "", None, 2, "msg_part_0")
        .await
        .expect("Produce to part 0 failed");

    // 4. Update leadership of partition 1 to Node 2 (simulating reassignment to node 2 only)
    if let Ok(pm) = env.engine.get_or_create_partition(topic, 1) {
        pm.update_leadership(2, 1, vec![2], vec![2]);
    }

    // 5. Fetch/Produce directly to partition 1 on Node 1 must now be rejected with NotLeaderForPartition
    assert!(env.engine.is_partition_leader(topic, 0));
    assert!(
        !env.engine.is_partition_leader(topic, 1),
        "Node 1 is no longer leader for partition 1"
    );

    let fetch_resp = client.fetch(topic, 1, 0, 1024).await;
    assert!(
        fetch_resp.is_err(),
        "Fetch on non-leader partition must fail"
    );
    let err_msg = fetch_resp.unwrap_err().to_string();
    assert!(
        err_msg.contains("NotLeaderForPartition"),
        "Error message should contain NotLeaderForPartition: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_scenario_21_per_partition_replication_and_client_routing() {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let base_dir = std::env::temp_dir().join(format!("smart_cluster_test_{}_{}", pid, count));

    let node1_dir = base_dir.join("node1_data");
    let node2_dir = base_dir.join("node2_data");
    let _ = std::fs::remove_dir_all(&base_dir);

    // 1. Start Node 2 (Node ID: 2)
    let config_node2 = EngineConfig {
        node_id: 2,
        role: hermes::NodeRole::Follower,
        data_dir: node2_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        ..EngineConfig::default()
    };
    let engine_node2 = StorageEngine::new(config_node2).unwrap();
    let server_node2 = Server::new(engine_node2.clone());
    let (listener_node2, addr_node2) = server_node2.bind().unwrap();

    let server_node2_task = tokio::spawn(async move {
        let _ = server_node2.run_with_listener(listener_node2).await;
    });

    // 2. Start Node 1 (Node ID: 1)
    let config_node1 = EngineConfig {
        node_id: 1,
        role: hermes::NodeRole::Leader,
        data_dir: node1_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        peer_addrs: vec![addr_node2.to_string()],
        ..EngineConfig::default()
    };
    let engine_node1 = StorageEngine::new(config_node1).unwrap();
    let server_node1 = Server::new(engine_node1.clone());
    let (listener_node1, addr_node1) = server_node1.bind().unwrap();

    let server_node1_task = tokio::spawn(async move {
        let _ = server_node1.run_with_listener(listener_node1).await;
    });

    sleep(Duration::from_millis(50)).await;

    // Register broker socket addresses on engines
    engine_node1.register_broker_address(1, addr_node1.to_string());
    engine_node1.register_broker_address(2, addr_node2.to_string());
    engine_node2.register_broker_address(1, addr_node1.to_string());
    engine_node2.register_broker_address(2, addr_node2.to_string());

    // 3. Connect RoutedClient to Node 1 (Bootstrap) and register Node 2
    let mut smart_client = hermes::RoutedClient::connect(addr_node1)
        .await
        .expect("RoutedClient connect failed");
    smart_client.register_broker(2, addr_node2);

    let topic = "smart_routed_topic";

    // 4. Create topic on Node 1 & assign Node 2 as leader of Partition 1
    let mut setup_client = TestClient::connect(addr_node1).await.unwrap();
    setup_client.create_topic(topic, 2).await.unwrap();

    if let Ok(pm1) = engine_node1.get_or_create_partition(topic, 1) {
        pm1.update_leadership(2, 1, vec![2, 1], vec![2, 1]);
    }
    if let Ok(pm2) = engine_node2.get_or_create_partition(topic, 1) {
        pm2.update_leadership(2, 1, vec![2, 1], vec![2, 1]);
    }

    // 5. Produce via RoutedClient (routes Partition 1 directly to Node 2!)
    let records = vec![
        bytes::Bytes::from("smart_payload_1"),
        bytes::Bytes::from("smart_payload_2"),
    ];
    let prod_res = smart_client
        .produce_smart(topic, "routing_key_1", None, 2, &records)
        .await
        .expect("produce_smart failed");
    assert_eq!(prod_res.first_offset, 0);

    // 6. Sleep briefly to allow Node 1's PartitionFetcherManager to pull Partition 1 from Node 2 over gRPC!
    sleep(Duration::from_millis(200)).await;

    // 7. Verify Node 1 (Follower for Partition 1) has fetched the records from Node 2!
    let fetched = smart_client
        .fetch_smart(topic, 1, 0, 65536)
        .await
        .expect("fetch_smart failed");
    assert_eq!(fetched.len(), 2);
    assert_eq!(fetched[0].payload.as_ref(), b"smart_payload_1");
    assert_eq!(fetched[1].payload.as_ref(), b"smart_payload_2");

    server_node1_task.abort();
    server_node2_task.abort();
    let _ = std::fs::remove_dir_all(&base_dir);
}

/// Scenario 22: Per-client produce/fetch byte-rate quotas throttle responses (Kafka-style
/// "process the request, delay the response") instead of rejecting them outright.
#[tokio::test]
async fn test_scenario_22_per_client_quota_throttling() {
    // 1. Baseline: with no quota configured, produce/fetch complete quickly.
    let env_unlimited = start_test_server().await;
    let mut client_unlimited = TestClient::connect(env_unlimited.addr).await.unwrap();
    let start = std::time::Instant::now();
    client_unlimited
        .produce_single("quota_baseline_topic", "k1", None, 1, vec![0u8; 4096])
        .await
        .expect("Baseline produce should succeed");
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "Unthrottled produce should be fast, took {:?}",
        start.elapsed()
    );

    // 2. Configure a deliberately low produce quota (100 bytes/sec) and verify a
    // single request exceeding the burst capacity is measurably delayed rather than
    // rejected — the produce must still succeed, just later.
    let env_throttled = start_test_server_with_quota(Some(100), None).await;
    let mut client_throttled = TestClient::connect(env_throttled.addr).await.unwrap();

    let payload = vec![0u8; 250]; // capacity(100) - 250 consumed => ~1.5s expected delay
    let start = std::time::Instant::now();
    let result = client_throttled
        .produce_single("quota_throttled_topic", "k1", None, 1, payload)
        .await
        .expect("Throttled produce should still succeed, just delayed");
    let elapsed = start.elapsed();

    assert_eq!(
        result.first_offset, 0,
        "Produce should still be applied correctly"
    );
    assert!(
        elapsed >= Duration::from_millis(1200),
        "Produce exceeding quota should be delayed by ~1.5s, only took {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "Throttle delay should be bounded, took {:?}",
        elapsed
    );

    // 3. Configure a deliberately low fetch quota and verify a large fetch response
    // is similarly delayed after data is produced without a fetch quota in effect.
    let env_fetch_throttled = start_test_server_with_quota(None, Some(100)).await;
    let mut client_fetch = TestClient::connect(env_fetch_throttled.addr).await.unwrap();

    // Produce enough data first (unthrottled) so the fetch below has bytes to return.
    client_fetch
        .produce_single("quota_fetch_topic", "", None, 1, vec![1u8; 300])
        .await
        .expect("Setup produce for fetch-quota test should succeed");

    let start = std::time::Instant::now();
    let frames = client_fetch
        .fetch("quota_fetch_topic", 0, 0, 64 * 1024)
        .await
        .expect("Throttled fetch should still succeed, just delayed");
    let elapsed = start.elapsed();

    assert_eq!(frames.len(), 1);
    assert!(
        elapsed >= Duration::from_millis(500),
        "Fetch exceeding quota should be delayed, only took {:?}",
        elapsed
    );

    // 4. Two clients behind the same source IP can opt into separate quota buckets by
    // setting distinct logical client_ids on their connections.
    let env_client_id_quota = start_test_server_with_quota(Some(100), None).await;
    let mut client_a = TestClient::connect(env_client_id_quota.addr).await.unwrap();
    let mut client_b = TestClient::connect(env_client_id_quota.addr).await.unwrap();
    client_a.set_client_id("producer-a").await.unwrap();
    client_b.set_client_id("producer-b").await.unwrap();

    client_a
        .produce_single("quota_client_id_topic_a", "", None, 1, vec![2u8; 100])
        .await
        .expect("client_a should consume only its own quota bucket");

    let start = std::time::Instant::now();
    client_b
        .produce_single("quota_client_id_topic_b", "", None, 1, vec![3u8; 100])
        .await
        .expect("client_b should have an independent quota bucket");
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "Distinct client_ids should avoid cross-throttling on shared source IP, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn test_scenario_23_cleanup_policy_log_compaction() {
    use hermes::config::CleanupPolicy;

    struct TestDataDirGuard {
        pub path: std::path::PathBuf,
    }

    impl TestDataDirGuard {
        fn new(prefix: &str) -> Self {
            let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("hermes_test_{}_{}", prefix, count));
            let _ = std::fs::create_dir_all(&path);
            Self { path }
        }
    }

    impl Drop for TestDataDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    // 1. Verify parsing cleanup.policy from server.properties
    let dir_guard = TestDataDirGuard::new("cleanup_policy_config_test");
    let props_path = dir_guard.path.join("server.properties");
    std::fs::write(
        &props_path,
        "cleanup.policy=compact\nlog.retention.bytes=52428800\n",
    )
    .unwrap();

    let cfg = hermes::config::EngineConfig::from_properties_file(&props_path).unwrap();
    assert_eq!(cfg.cleanup_policy, CleanupPolicy::Compact);
    assert!(cfg.cleanup_policy.is_compact());
    assert!(!cfg.cleanup_policy.is_delete());

    // 2. Test SegmentManager Log Compaction behavior
    let seg_dir_guard = TestDataDirGuard::new("log_compaction_segment_test");
    let config = hermes::config::EngineConfig {
        cleanup_policy: CleanupPolicy::Compact,
        max_segment_bytes: 100, // Small max_segment_bytes to force segment rotations
        ..Default::default()
    };

    let mut seg_mgr = hermes::segment::SegmentManager::open(&seg_dir_guard.path, config).unwrap();

    // Append records with duplicate keys across multiple segment rotations
    // Format: "key:value" payload
    let r0 = seg_mgr.append(b"user1:val_1", 1000).unwrap(); // offset 0
    let r1 = seg_mgr.append(b"user2:val_1", 1001).unwrap(); // offset 1
    let r2 = seg_mgr.append(b"user1:val_2", 1002).unwrap(); // offset 2 (should override offset 0)

    // Force rotation into historical segment
    seg_mgr.rotate_segment().unwrap();

    let r3 = seg_mgr.append(b"user2:val_2", 1003).unwrap(); // offset 3 (should override offset 1)
    let r4 = seg_mgr.append(b"user1:val_3", 1004).unwrap(); // offset 4 (should override offset 2)

    // Force another rotation
    seg_mgr.rotate_segment().unwrap();

    let r5 = seg_mgr.append(b"user3:val_1", 1005).unwrap(); // offset 5 in active segment

    assert_eq!(r0.offset, 0);
    assert_eq!(r1.offset, 1);
    assert_eq!(r2.offset, 2);
    assert_eq!(r3.offset, 3);
    assert_eq!(r4.offset, 4);
    assert_eq!(r5.offset, 5);

    // Trigger the log compaction garbage collector. Compaction is intentionally
    // incremental — one call rewrites at most a handful of segments (bounding how long
    // it holds the partition's segment-manager lock), so a backlog spanning more
    // segments than that needs multiple calls to fully drain, same as the real retention
    // GC's periodic ticks would provide in production.
    let mut compacted_count = 0usize;
    for _ in 0..10 {
        let n = seg_mgr.apply_retention().unwrap();
        compacted_count += n;
        if n == 0 {
            break;
        }
    }
    assert!(
        compacted_count >= 3,
        "Compaction should drop older duplicate keys (offsets 0, 1, 2), dropped {}",
        compacted_count
    );

    // Verify fetching remaining offsets returns latest values per key
    let fetched_3 = seg_mgr.fetch(3, 1024).unwrap();
    assert!(!fetched_3.is_empty());
    assert_eq!(fetched_3[0].offset, 3);
    assert_eq!(fetched_3[0].payload.as_ref(), b"user2:val_2");

    let fetched_4 = seg_mgr.fetch(4, 1024).unwrap();
    assert!(!fetched_4.is_empty());
    assert_eq!(fetched_4[0].offset, 4);
    assert_eq!(fetched_4[0].payload.as_ref(), b"user1:val_3");
}

#[tokio::test]
async fn test_scenario_24_sasl_and_acls() {
    use hermes::config::{EngineConfig, SecurityProtocol};
    use hermes::server::acl::{AclBinding, AclOperation, AclPermissionType, ResourceType};

    struct TestDataDirGuard {
        pub path: std::path::PathBuf,
    }

    impl TestDataDirGuard {
        fn new(prefix: &str) -> Self {
            let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("hermes_test_{}_{}", prefix, count));
            let _ = std::fs::create_dir_all(&path);
            Self { path }
        }
    }

    impl Drop for TestDataDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    let dir_guard = TestDataDirGuard::new("sasl_acls_test");
    let props_path = dir_guard.path.join("server.properties");
    std::fs::write(
        &props_path,
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256\n\
         sasl.user.alice=password123\n\
         sasl.user.admin=adminpass\n\
         acls.enabled=true\n\
         super.users=User:admin\n",
    )
    .unwrap();

    // 1. Verify property file loading for SASL and ACLs
    let config = EngineConfig::from_properties_file(&props_path).unwrap();
    assert_eq!(config.security_protocol, SecurityProtocol::SaslPlaintext);
    assert_eq!(
        config.sasl_users.get("alice"),
        Some(&"password123".to_string())
    );
    assert_eq!(
        config.sasl_users.get("admin"),
        Some(&"adminpass".to_string())
    );
    assert!(config.acls_enabled);
    assert_eq!(config.super_users, vec!["User:admin".to_string()]);

    // 2. Start Hermes server with SASL and ACL enforcement enabled
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mut engine_cfg = config.clone();
    engine_cfg.data_dir = dir_guard.path.clone();
    engine_cfg.bind_addr = addr.to_string();

    let engine = hermes::server::StorageEngine::new(engine_cfg).unwrap();
    let server = hermes::server::Server::new(engine.clone());

    tokio::spawn(async move {
        server.run_with_listener(listener).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 3. Test SASL Handshake & Authenticate over TCP wire connection
    let mut client = hermes::client::TestClient::connect(addr).await.unwrap();

    // SASL Handshake
    let (err_code, mechs) = client.sasl_handshake("PLAIN").await.unwrap();
    assert_eq!(err_code, 0);
    assert!(mechs.contains(&"PLAIN".to_string()));

    // SASL Authenticate with valid credentials for alice
    let auth_payload = b"\0alice\0password123";
    let auth_res = client.sasl_authenticate(auth_payload).await.unwrap();
    assert_eq!(auth_res, 0); // 0 = SUCCESS

    // SASL Authenticate with invalid credentials
    let mut invalid_client = hermes::client::TestClient::connect(addr).await.unwrap();
    let invalid_auth_res = invalid_client
        .sasl_authenticate(b"\0alice\0wrongpass")
        .await
        .unwrap();
    assert_eq!(invalid_auth_res, 58); // 58 = SASL_AUTHENTICATION_FAILED

    // SASL Handshake for SCRAM-SHA-256 mechanism
    let mut scram_client = hermes::client::TestClient::connect(addr).await.unwrap();
    let (scram_err, scram_mechs) = scram_client.sasl_handshake("SCRAM-SHA-256").await.unwrap();
    assert_eq!(scram_err, 0);
    assert!(scram_mechs.contains(&"SCRAM-SHA-256".to_string()));

    // SASL Authenticate for SCRAM-SHA-256 full challenge/response flow
    let scram_auth_res = scram_client
        .sasl_authenticate_scram_sha256("alice", "password123")
        .await
        .unwrap();
    assert_eq!(scram_auth_res, 0);

    // Invalid SCRAM authentication attempt with unknown user
    let mut invalid_scram_client = hermes::client::TestClient::connect(addr).await.unwrap();
    let invalid_scram_res = invalid_scram_client
        .sasl_authenticate(b"n,,n=unknown_user,r=rOprNGfwEbeRWgbNEkqO")
        .await
        .unwrap();
    assert_eq!(invalid_scram_res, 58); // 58 = SASL_AUTHENTICATION_FAILED

    // Authenticate admin client with superuser privileges
    let mut admin_client = hermes::client::TestClient::connect(addr).await.unwrap();
    let _ = admin_client.sasl_handshake("PLAIN").await.unwrap();
    let admin_auth_res = admin_client
        .sasl_authenticate(b"\0admin\0adminpass")
        .await
        .unwrap();
    assert_eq!(admin_auth_res, 0);

    // Superuser-managed SCRAM credential lifecycle over the admin API.
    admin_client
        .upsert_scram_user("service_a", "svc_secret_123")
        .await
        .unwrap();
    let mut service_client = hermes::client::TestClient::connect(addr).await.unwrap();
    let service_scram_auth = service_client
        .sasl_authenticate_scram_sha256("service_a", "svc_secret_123")
        .await
        .unwrap();
    assert_eq!(service_scram_auth, 0);
    admin_client.delete_scram_user("service_a").await.unwrap();
    let mut deleted_service_client = hermes::client::TestClient::connect(addr).await.unwrap();
    let deleted_service_auth = deleted_service_client
        .sasl_authenticate_scram_sha256("service_a", "svc_secret_123")
        .await
        .unwrap();
    assert_eq!(deleted_service_auth, 58);

    // 4. Test ACL Management (CreateAcls & DescribeAcls)
    let acl_binding = AclBinding {
        resource_type: ResourceType::Topic as u8,
        resource_name: "secure_orders".to_string(),
        pattern_type: 3, // Literal
        principal: "User:alice".to_string(),
        host: "*".to_string(),
        operation: AclOperation::Write as u8,
        permission_type: AclPermissionType::Allow as u8,
    };

    // Non-admin alice attempt to create ACL fails authorization
    let alice_acl_err = client.create_acl(&acl_binding).await;
    assert!(
        alice_acl_err.is_err(),
        "non-superuser alice cannot create ACLs"
    );

    // Superuser admin creates ACL successfully
    admin_client.create_acl(&acl_binding).await.unwrap();

    let filter = AclBinding {
        resource_type: ResourceType::Topic as u8,
        resource_name: "secure_orders".to_string(),
        pattern_type: 3,
        principal: "*".to_string(),
        host: "*".to_string(),
        operation: 1,       // Any
        permission_type: 1, // Any
    };

    let listed_acls = admin_client.describe_acls(&filter).await.unwrap();
    assert_eq!(listed_acls.len(), 1);
    assert_eq!(listed_acls[0].principal, "User:alice");

    // 5. Test ACL Authorization enforcement on engine
    // Authorized user 'User:alice' produce attempt -> Allowed
    let is_authorized = engine.authorize(
        "User:alice",
        "127.0.0.1",
        AclOperation::Write as u8,
        ResourceType::Topic as u8,
        "secure_orders",
    );
    assert!(is_authorized, "alice should be authorized by Allow ACL");

    // Unauthorized user 'User:bob' produce attempt -> Denied
    let is_bob_authorized = engine.authorize(
        "User:bob",
        "127.0.0.1",
        AclOperation::Write as u8,
        ResourceType::Topic as u8,
        "secure_orders",
    );
    assert!(
        !is_bob_authorized,
        "bob should be denied access under default-deny ACL policy"
    );

    // Superuser 'User:admin' -> Always Allowed
    let is_admin_authorized = engine.authorize(
        "User:admin",
        "127.0.0.1",
        AclOperation::Write as u8,
        ResourceType::Topic as u8,
        "secure_orders",
    );
    assert!(is_admin_authorized, "superusers are exempt from ACL checks");
}

#[tokio::test]
async fn test_scenario_25_tls_ssl_and_sasl_ssl() {
    use hermes::config::{EngineConfig, SecurityProtocol};

    struct TestDataDirGuard {
        pub path: std::path::PathBuf,
    }

    impl TestDataDirGuard {
        fn new(prefix: &str) -> Self {
            let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("hermes_test_{}_{}", prefix, count));
            let _ = std::fs::create_dir_all(&path);
            Self { path }
        }
    }

    impl Drop for TestDataDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    // 1. Test SSL mode over encrypted TLS transport
    let dir_guard = TestDataDirGuard::new("ssl_transport_test");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let ssl_cfg = EngineConfig {
        security_protocol: SecurityProtocol::Ssl,
        data_dir: dir_guard.path.clone(),
        bind_addr: addr.to_string(),
        ..Default::default()
    };

    let engine = hermes::server::StorageEngine::new(ssl_cfg).unwrap();
    let server = hermes::server::Server::new(engine.clone());

    tokio::spawn(async move {
        server.run_with_listener(listener).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Connect TestClient over TLS (insecure mode for auto-generated self-signed test cert)
    let mut tls_client = hermes::client::TestClient::connect_tls_full(addr, None, None, true)
        .await
        .unwrap();

    // Produce over TLS
    let recs = vec![bytes::Bytes::from("encrypted_payload_over_tls")];
    let prod_res = tls_client
        .produce_batch("tls_topic", "", None, 1, &recs)
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    // Fetch over TLS
    let fetched = tls_client.fetch("tls_topic", 0, 0, 1024).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].payload.as_ref(), b"encrypted_payload_over_tls");

    // 2. Test SASL_SSL mode (encrypted TLS transport + SASL authentication)
    let sasl_ssl_guard = TestDataDirGuard::new("sasl_ssl_transport_test");
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let mut sasl_users = std::collections::HashMap::new();
    sasl_users.insert("charlie".to_string(), "tls_secret_password".to_string());

    let sasl_ssl_cfg = EngineConfig {
        security_protocol: SecurityProtocol::SaslSsl,
        sasl_mechanisms: vec!["PLAIN".to_string()],
        sasl_users,
        data_dir: sasl_ssl_guard.path.clone(),
        bind_addr: addr2.to_string(),
        ..Default::default()
    };

    let engine2 = hermes::server::StorageEngine::new(sasl_ssl_cfg).unwrap();
    let server2 = hermes::server::Server::new(engine2.clone());

    tokio::spawn(async move {
        server2.run_with_listener(listener2).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Connect TestClient over TLS for SASL_SSL
    let mut unauth_client = hermes::client::TestClient::connect_tls_full(addr2, None, None, true)
        .await
        .unwrap();
    let unauth_res = unauth_client
        .produce_batch("sasl_ssl_topic", "", None, 1, &recs)
        .await;
    assert!(
        unauth_res.is_err(),
        "Unauthenticated produce on SASL_SSL port must be rejected!"
    );

    let mut sasl_tls_client = hermes::client::TestClient::connect_tls_full(addr2, None, None, true)
        .await
        .unwrap();

    let (err_code, mechs) = sasl_tls_client.sasl_handshake("PLAIN").await.unwrap();
    assert_eq!(err_code, 0);
    assert!(mechs.contains(&"PLAIN".to_string()));

    let auth_res = sasl_tls_client
        .sasl_authenticate(b"\0charlie\0tls_secret_password")
        .await
        .unwrap();
    assert_eq!(auth_res, 0);

    // Produce & Consume over SASL_SSL
    let prod_res2 = sasl_tls_client
        .produce_batch("sasl_ssl_topic", "", None, 1, &recs)
        .await
        .unwrap();
    assert_eq!(prod_res2.first_offset, 0);

    let fetched2 = sasl_tls_client
        .fetch("sasl_ssl_topic", 0, 0, 1024)
        .await
        .unwrap();
    assert_eq!(fetched2.len(), 1);
    assert_eq!(fetched2[0].payload.as_ref(), b"encrypted_payload_over_tls");
}

#[tokio::test]
async fn test_scenario_26_pem_file_tls_key_and_mtls() {
    use hermes::config::{EngineConfig, SecurityProtocol};

    struct TestDataDirGuard {
        pub path: std::path::PathBuf,
    }

    impl TestDataDirGuard {
        fn new(prefix: &str) -> Self {
            let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("hermes_test_{}_{}", prefix, count));
            let _ = std::fs::create_dir_all(&path);
            Self { path }
        }
    }

    impl Drop for TestDataDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    let dir_guard = TestDataDirGuard::new("pem_tls_file_test");

    // Generate real X.509 CA certificate, server certificate, and client certificate
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let server_cert = rcgen::generate_simple_self_signed(subject_alt_names.clone()).unwrap();
    let client_cert = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();

    let cert_pem = server_cert.cert.pem();
    let key_pem = server_cert.key_pair.serialize_pem();

    let client_cert_pem = client_cert.cert.pem();
    let client_key_pem = client_cert.key_pair.serialize_pem();

    let cert_path = dir_guard.path.join("server.crt");
    let key_path = dir_guard.path.join("server.key");

    let client_cert_path = dir_guard.path.join("client.crt");
    let client_key_path = dir_guard.path.join("client.key");

    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    std::fs::write(&client_cert_path, &client_cert_pem).unwrap();
    std::fs::write(&client_key_path, &client_key_pem).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Server configured with Mutual TLS (mTLS) enabled (ssl_client_auth = true)
    let ssl_cfg = EngineConfig {
        security_protocol: SecurityProtocol::Ssl,
        ssl_cert_path: Some(cert_path.clone()),
        ssl_key_path: Some(key_path.clone()),
        ssl_ca_path: Some(client_cert_path.clone()), // trust the client's CA/cert
        ssl_client_auth: "required".to_string(),
        data_dir: dir_guard.path.clone(),
        bind_addr: addr.to_string(),
        ..Default::default()
    };

    let engine = hermes::server::StorageEngine::new(ssl_cfg).unwrap();
    let server = hermes::server::Server::new(engine.clone());

    tokio::spawn(async move {
        server.run_with_listener(listener).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 1. Client connecting WITHOUT client cert when mTLS is required MUST fail handshake or request
    let unauthenticated_client_res =
        hermes::client::TestClient::connect_tls_with_ca(addr, &cert_path).await;
    if let Ok(mut c) = unauthenticated_client_res {
        let res = c.ping().await;
        assert!(
            res.is_err(),
            "Client without client-cert authentication must be rejected by mTLS server"
        );
    }

    // 2. Client connecting WITH valid client cert over mTLS MUST succeed and verify server CA
    let mut mtls_client = hermes::client::TestClient::connect_mtls(
        addr,
        &cert_path,
        &client_cert_path,
        &client_key_path,
    )
    .await
    .unwrap();

    let recs = vec![bytes::Bytes::from("verified_pem_mtls_payload")];
    let prod_res = mtls_client
        .produce_batch("pem_tls_topic", "", None, 1, &recs)
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    let fetched = mtls_client
        .fetch("pem_tls_topic", 0, 0, 1024)
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].payload.as_ref(), b"verified_pem_mtls_payload");
}

#[tokio::test]
async fn test_scenario_27_dynamic_broker_registration_and_membership() {
    use hermes::config::EngineConfig;

    struct TestDataDirGuard {
        pub path: std::path::PathBuf,
    }

    impl TestDataDirGuard {
        fn new(prefix: &str) -> Self {
            let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("hermes_test_{}_{}", prefix, count));
            let _ = std::fs::create_dir_all(&path);
            Self { path }
        }
    }

    impl Drop for TestDataDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    let dir_guard = TestDataDirGuard::new("dynamic_broker_membership_test");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let cfg = EngineConfig {
        node_id: 1,
        data_dir: dir_guard.path.clone(),
        bind_addr: addr.to_string(),
        ..Default::default()
    };

    let engine = hermes::server::StorageEngine::new(cfg).unwrap();
    let server = hermes::server::Server::new(engine.clone());

    tokio::spawn(async move {
        server.run_with_listener(listener).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let mut client = hermes::client::TestClient::connect(addr).await.unwrap();

    // Initial DescribeCluster -> 1 node (node 1)
    let desc_initial = client.describe_cluster().await.unwrap();
    assert_eq!(desc_initial.brokers.len(), 1);
    assert_eq!(desc_initial.brokers[0].0, 1);

    // Dynamic RegisterBroker for node 2 and node 3
    client.register_broker(2, "127.0.0.1:9093").await.unwrap();
    client.register_broker(3, "127.0.0.1:9094").await.unwrap();

    // Query DescribeCluster -> 3 active brokers
    let desc_after_reg = client.describe_cluster().await.unwrap();
    assert_eq!(desc_after_reg.brokers.len(), 3);
    assert_eq!(desc_after_reg.brokers[0], (1, addr.to_string()));
    assert_eq!(desc_after_reg.brokers[1], (2, "127.0.0.1:9093".to_string()));
    assert_eq!(desc_after_reg.brokers[2], (3, "127.0.0.1:9094".to_string()));

    // Dynamic UnregisterBroker for node 2
    client.unregister_broker(2).await.unwrap();

    // Query DescribeCluster -> 2 active brokers (node 1, node 3)
    let desc_after_unreg = client.describe_cluster().await.unwrap();
    assert_eq!(desc_after_unreg.brokers.len(), 2);
    assert_eq!(desc_after_unreg.brokers[0], (1, addr.to_string()));
    assert_eq!(
        desc_after_unreg.brokers[1],
        (3, "127.0.0.1:9094".to_string())
    );
}

#[tokio::test]
async fn test_scenario_28_prometheus_metrics_and_lz4_compression() {
    use hermes::config::EngineConfig;

    struct TestDataDirGuard {
        pub path: std::path::PathBuf,
    }

    impl TestDataDirGuard {
        fn new(prefix: &str) -> Self {
            let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("hermes_test_{}_{}", prefix, count));
            let _ = std::fs::create_dir_all(&path);
            Self { path }
        }
    }

    impl Drop for TestDataDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    let dir_guard = TestDataDirGuard::new("prometheus_and_lz4_test");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let cfg = EngineConfig {
        node_id: 1,
        data_dir: dir_guard.path.clone(),
        bind_addr: addr.to_string(),
        compression_codec: hermes::config::CompressionCodec::Lz4,
        ..Default::default()
    };

    let engine = hermes::server::StorageEngine::new(cfg).unwrap();
    let server = hermes::server::Server::new(engine.clone());

    tokio::spawn(async move {
        server.run_with_listener(listener).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Test Real End-to-End Client Produce & Fetch over LZ4-compressed storage engine
    let mut client = hermes::client::TestClient::connect(addr).await.unwrap();
    let raw_payload_str = "end_to_end_lz4_wire_compressed_payload_0123456789_0123456789_0123456789";
    let recs = vec![bytes::Bytes::from(raw_payload_str)];
    let prod_res = client
        .produce_batch("lz4_wire_topic", "", None, 1, &recs)
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    // Fetch over wire TCP: server returns 0xAC LZ4 compressed frame, client decompresses transparently
    let fetched = client.fetch("lz4_wire_topic", 0, 0, 1024).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].payload.as_ref(), raw_payload_str.as_bytes());

    // Verify on-disk log segment file directly: confirm 0xAC magic byte stored on disk
    let log_file_path = dir_guard
        .path
        .join("lz4_wire_topic-0")
        .join("00000000000000000000.log");
    let log_bytes = std::fs::read(&log_file_path).unwrap();
    let (disk_frame, _) = hermes::protocol::RecordFrame::decode(&log_bytes).unwrap();
    assert_eq!(
        disk_frame.magic,
        hermes::protocol::COMPRESSED_LZ4_MAGIC_BYTE
    );

    // Query Prometheus HTTP /metrics Endpoint
    let metrics_port = addr.port().checked_add(1000).unwrap_or(9090);
    let metrics_url = format!("127.0.0.1:{}", metrics_port);
    let mut http_stream = tokio::net::TcpStream::connect(&metrics_url).await.unwrap();

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        metrics_url
    );
    http_stream.write_all(req.as_bytes()).await.unwrap();

    let mut resp_buf = Vec::new();
    http_stream.read_to_end(&mut resp_buf).await.unwrap();
    let resp_str = String::from_utf8_lossy(&resp_buf);

    assert!(resp_str.contains("200 OK"));
    assert!(resp_str.contains("hermes_produce_bytes_total"));
    assert!(resp_str.contains("hermes_produce_records_total"));
    assert!(resp_str.contains("hermes_fetch_bytes_total"));
    assert!(resp_str.contains("hermes_topics_count"));
    assert!(resp_str.contains("hermes_active_brokers_count"));
    assert!(resp_str.contains("hermes_active_connections"));
}

#[tokio::test]
async fn test_scenario_37_zstd_compression_end_to_end() {
    use hermes::config::EngineConfig;

    let dir_guard = TestDataDirGuard::new("zstd_compression_test");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let cfg = EngineConfig {
        node_id: 1,
        data_dir: dir_guard.path.clone(),
        bind_addr: addr.to_string(),
        compression_codec: hermes::config::CompressionCodec::Zstd,
        ..Default::default()
    };

    let engine = hermes::server::StorageEngine::new(cfg).unwrap();
    let server = hermes::server::Server::new(engine.clone());

    tokio::spawn(async move {
        server.run_with_listener(listener).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let mut client = hermes::client::TestClient::connect(addr).await.unwrap();

    // Also verify "zstd" parses correctly through the same dynamic-config path clients use.
    client.create_topic("zstd_wire_topic", 1).await.unwrap();
    client
        .alter_configs(
            "zstd_wire_topic",
            &[("compression.type".to_string(), "zstd".to_string())],
        )
        .await
        .unwrap();

    let raw_payload_str =
        "end_to_end_zstd_wire_compressed_payload_0123456789_0123456789_0123456789_repeat_repeat";
    let recs = vec![bytes::Bytes::from(raw_payload_str)];
    let prod_res = client
        .produce_batch("zstd_wire_topic", "", None, 1, &recs)
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    // Fetch over wire TCP: server returns 0xAE Zstd compressed frame, client decompresses transparently
    let fetched = client.fetch("zstd_wire_topic", 0, 0, 1024).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].payload.as_ref(), raw_payload_str.as_bytes());

    // Verify on-disk log segment file directly: confirm 0xAE magic byte stored on disk.
    let log_file_path = dir_guard
        .path
        .join("zstd_wire_topic-0")
        .join("00000000000000000000.log");
    let log_bytes = std::fs::read(&log_file_path).unwrap();
    let (disk_frame, _) = hermes::protocol::RecordFrame::decode(&log_bytes).unwrap();
    assert_eq!(
        disk_frame.magic,
        hermes::protocol::COMPRESSED_ZSTD_MAGIC_BYTE
    );
}

#[tokio::test]
async fn test_scenario_29_scram_credentials_persist_across_restart() {
    use hermes::config::{EngineConfig, SecurityProtocol};

    let dir_guard = TestDataDirGuard::new("scram_persist_restart");
    let bootstrap_cfg = EngineConfig {
        data_dir: dir_guard.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        ..Default::default()
    };
    let bootstrap_engine = StorageEngine::new(bootstrap_cfg).unwrap();
    bootstrap_engine
        .upsert_scram_user("alice", "persist_secret_123")
        .await
        .unwrap();
    drop(bootstrap_engine);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let restarted_cfg = EngineConfig {
        data_dir: dir_guard.path.clone(),
        bind_addr: addr.to_string(),
        security_protocol: SecurityProtocol::SaslPlaintext,
        sasl_mechanisms: vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()],
        ..Default::default()
    };
    let restarted_engine = StorageEngine::new(restarted_cfg).unwrap();
    let server = Server::new(restarted_engine);
    tokio::spawn(async move {
        server.run_with_listener(listener).await.unwrap();
    });
    sleep(Duration::from_millis(150)).await;

    let mut plain_client = TestClient::connect(addr).await.unwrap();
    let (plain_err, plain_mechs) = plain_client.sasl_handshake("PLAIN").await.unwrap();
    assert_eq!(plain_err, 0);
    assert!(plain_mechs.contains(&"PLAIN".to_string()));
    let plain_auth = plain_client
        .sasl_authenticate(b"\0alice\0persist_secret_123")
        .await
        .unwrap();
    assert_eq!(plain_auth, 0);

    let mut scram_client = TestClient::connect(addr).await.unwrap();
    let (scram_err, scram_mechs) = scram_client.sasl_handshake("SCRAM-SHA-256").await.unwrap();
    assert_eq!(scram_err, 0);
    assert!(scram_mechs.contains(&"SCRAM-SHA-256".to_string()));
    let scram_auth = scram_client
        .sasl_authenticate_scram_sha256("alice", "persist_secret_123")
        .await
        .unwrap();
    assert_eq!(scram_auth, 0);
}

#[tokio::test]
async fn test_scenario_30_transactional_epoch_fencing_and_recovery() {
    let dir_guard = TestDataDirGuard::new("tx_epoch_recovery");
    let cfg = EngineConfig {
        data_dir: dir_guard.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        ..Default::default()
    };
    let engine = StorageEngine::new(cfg.clone()).unwrap();
    let topic = "tx_recovery_topic";
    let transactional_id = "tx-recovery-1";

    let (producer_id, producer_epoch) = engine.init_producer_id(transactional_id).unwrap();
    assert_eq!(producer_epoch, 0);
    engine
        .add_partitions_to_txn(
            transactional_id,
            producer_id,
            producer_epoch,
            &[(topic.to_string(), vec![0])],
        )
        .unwrap();
    let first_record = vec![bytes::Bytes::from_static(b"txn-recovery-record-1")];
    engine
        .produce_batch(hermes::server::engine::ProduceBatchParams {
            topic,
            key: "",
            transaction_id: Some(transactional_id),
            num_partitions: 1,
            producer_id,
            producer_epoch,
            base_sequence: 0,
            records: &first_record,
        })
        .await
        .unwrap();
    drop(engine);

    let restarted = StorageEngine::new(cfg).unwrap();
    assert!(restarted.transactions().is_ongoing(transactional_id));
    restarted.abort_transaction(transactional_id).unwrap();
    let aborted_visible = restarted.fetch_committed(topic, 0, 0, 1024).await.unwrap();
    assert!(
        aborted_visible.is_empty(),
        "aborted transactional data must stay hidden after restart recovery"
    );

    let (recovered_pid, recovered_epoch) = restarted.init_producer_id(transactional_id).unwrap();
    assert_eq!(recovered_pid, producer_id);
    assert_eq!(recovered_epoch, producer_epoch + 1);
    assert!(
        restarted
            .add_partitions_to_txn(
                transactional_id,
                producer_id,
                producer_epoch,
                &[(topic.to_string(), vec![0])],
            )
            .is_err(),
        "stale producer epoch must be fenced"
    );

    restarted
        .add_partitions_to_txn(
            transactional_id,
            recovered_pid,
            recovered_epoch,
            &[(topic.to_string(), vec![0])],
        )
        .unwrap();
    let second_record = vec![bytes::Bytes::from_static(b"txn-recovery-record-2")];
    restarted
        .produce_batch(hermes::server::engine::ProduceBatchParams {
            topic,
            key: "",
            transaction_id: Some(transactional_id),
            num_partitions: 1,
            producer_id: recovered_pid,
            producer_epoch: recovered_epoch,
            base_sequence: 0,
            records: &second_record,
        })
        .await
        .unwrap();
    restarted
        .end_transaction(transactional_id, recovered_pid, recovered_epoch, true)
        .unwrap();
    let all_frames = restarted.fetch(topic, 0, 0, 1024).await.unwrap();
    let committed = restarted.fetch_committed(topic, 0, 0, 1024).await.unwrap();
    assert_eq!(all_frames.len(), 4);
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].payload.as_ref(), b"txn-recovery-record-2");
}

#[tokio::test]
async fn test_scenario_31_share_consumer_and_queue_semantics() {
    use hermes::{AckBatch, AcknowledgeType, CommandCode, RequestPayload, WireRequest};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let env = start_test_server().await;
    let topic = "share-queue-topic";
    let group_id = "test-share-group";

    // 1. Produce 6 records to partition 0
    let mut client = TestClient::connect(env.addr).await.unwrap();
    let produce_payloads: Vec<bytes::Bytes> = (0..6)
        .map(|i| bytes::Bytes::from(format!("share-msg-{}", i)))
        .collect();
    client
        .produce_batch(topic, "", None, 1, &produce_payloads)
        .await
        .unwrap();

    // 2. Member 1 connects over TCP and fetches 2 records
    let mut stream1 = TcpStream::connect(env.addr).await.unwrap();
    let fetch_req1 = WireRequest {
        cmd: CommandCode::ShareFetch,
        payload: RequestPayload::ShareFetch {
            group_id: group_id.to_string(),
            member_id: "member-1".to_string(),
            topic: topic.to_string(),
            partition: 0,
            max_records: 2,
            max_bytes: 1024 * 1024,
            lock_timeout_ms: 1000, // 1s lock
            acknowledgements: vec![],
        },
    };
    let encoded1 = encode_wire_request(&fetch_req1);
    stream1.write_all(&encoded1).await.unwrap();

    let resp1 = read_wire_response(&mut stream1).await;
    assert_eq!(resp1.status, 0);
    // Parse batches: expect offsets 0 and 1
    let mut resp_buf = &resp1.payload[..];
    use bytes::Buf;
    let batch_count = resp_buf.get_u32();
    assert_eq!(batch_count, 1);
    let first_offset = resp_buf.get_u64();
    let last_offset = resp_buf.get_u64();
    let delivery_count = resp_buf.get_u16();
    assert_eq!(first_offset, 0);
    assert_eq!(last_offset, 1);
    assert_eq!(delivery_count, 1);

    // 3. Member 2 connects concurrently and fetches 2 records -> cooperative consumption gets 2 and 3!
    let mut stream2 = TcpStream::connect(env.addr).await.unwrap();
    let fetch_req2 = WireRequest {
        cmd: CommandCode::ShareFetch,
        payload: RequestPayload::ShareFetch {
            group_id: group_id.to_string(),
            member_id: "member-2".to_string(),
            topic: topic.to_string(),
            partition: 0,
            max_records: 2,
            max_bytes: 1024 * 1024,
            lock_timeout_ms: 1000,
            acknowledgements: vec![],
        },
    };
    stream2
        .write_all(&encode_wire_request(&fetch_req2))
        .await
        .unwrap();
    let resp2 = read_wire_response(&mut stream2).await;
    assert_eq!(resp2.status, 0);
    let mut resp_buf2 = &resp2.payload[..];
    assert_eq!(resp_buf2.get_u32(), 1);
    assert_eq!(resp_buf2.get_u64(), 2);
    assert_eq!(resp_buf2.get_u64(), 3);

    // 4. Member 1 acknowledges offset 0 as ACCEPT, and offset 1 as RELEASE (transient error)
    let ack_req1 = WireRequest {
        cmd: CommandCode::ShareAcknowledge,
        payload: RequestPayload::ShareAcknowledge {
            group_id: group_id.to_string(),
            member_id: "member-1".to_string(),
            topic: topic.to_string(),
            partition: 0,
            acknowledgements: vec![
                AckBatch {
                    first_offset: 0,
                    last_offset: 0,
                    ack_type: AcknowledgeType::Accept,
                },
                AckBatch {
                    first_offset: 1,
                    last_offset: 1,
                    ack_type: AcknowledgeType::Release,
                },
            ],
        },
    };
    stream1
        .write_all(&encode_wire_request(&ack_req1))
        .await
        .unwrap();
    let ack_resp1 = read_wire_response(&mut stream1).await;
    assert_eq!(ack_resp1.status, 0);

    // 5. Member 2 fetches again — gets the released offset 1 with delivery_count = 2!
    let fetch_req3 = WireRequest {
        cmd: CommandCode::ShareFetch,
        payload: RequestPayload::ShareFetch {
            group_id: group_id.to_string(),
            member_id: "member-2".to_string(),
            topic: topic.to_string(),
            partition: 0,
            max_records: 1,
            max_bytes: 1024 * 1024,
            lock_timeout_ms: 1000,
            acknowledgements: vec![],
        },
    };
    stream2
        .write_all(&encode_wire_request(&fetch_req3))
        .await
        .unwrap();
    let resp3 = read_wire_response(&mut stream2).await;
    assert_eq!(resp3.status, 0);
    let mut resp_buf3 = &resp3.payload[..];
    assert_eq!(resp_buf3.get_u32(), 1);
    assert_eq!(resp_buf3.get_u64(), 1); // Offset 1 redelivered!
    assert_eq!(resp_buf3.get_u64(), 1);
    assert_eq!(resp_buf3.get_u16(), 2); // delivery_count is 2!

    // 6. Member 2 rejects offset 1 (poison pill) -> should be routed to DLQ topic "share-queue-topic-dlq"
    let ack_req2 = WireRequest {
        cmd: CommandCode::ShareAcknowledge,
        payload: RequestPayload::ShareAcknowledge {
            group_id: group_id.to_string(),
            member_id: "member-2".to_string(),
            topic: topic.to_string(),
            partition: 0,
            acknowledgements: vec![
                AckBatch {
                    first_offset: 1,
                    last_offset: 1,
                    ack_type: AcknowledgeType::Reject,
                },
                AckBatch {
                    first_offset: 2,
                    last_offset: 3,
                    ack_type: AcknowledgeType::Accept,
                },
            ],
        },
    };
    stream2
        .write_all(&encode_wire_request(&ack_req2))
        .await
        .unwrap();
    let ack_resp2 = read_wire_response(&mut stream2).await;
    assert_eq!(ack_resp2.status, 0);

    // Verify DLQ topic has the rejected record
    let dlq_frames = env
        .engine
        .fetch("share-queue-topic-dlq", 0, 0, 1024)
        .await
        .unwrap();
    assert_eq!(dlq_frames.len(), 1);
    assert_eq!(dlq_frames[0].payload.as_ref(), b"share-msg-1");

    // 7. Test automatic lock lease timeout expiry & redelivery
    // Member 1 acquires offsets 4 and 5 with short 100ms lock timeout
    let fetch_req_timeout = WireRequest {
        cmd: CommandCode::ShareFetch,
        payload: RequestPayload::ShareFetch {
            group_id: group_id.to_string(),
            member_id: "member-1".to_string(),
            topic: topic.to_string(),
            partition: 0,
            max_records: 2,
            max_bytes: 1024 * 1024,
            lock_timeout_ms: 100, // 100ms lock timeout
            acknowledgements: vec![],
        },
    };
    stream1
        .write_all(&encode_wire_request(&fetch_req_timeout))
        .await
        .unwrap();
    let resp_to = read_wire_response(&mut stream1).await;
    assert_eq!(resp_to.status, 0);

    // Member 1 does NOT acknowledge and times out -> wait 200ms
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Member 2 now fetches -> automatically gets offsets 4 and 5 redelivered!
    let fetch_req_redeliver = WireRequest {
        cmd: CommandCode::ShareFetch,
        payload: RequestPayload::ShareFetch {
            group_id: group_id.to_string(),
            member_id: "member-2".to_string(),
            topic: topic.to_string(),
            partition: 0,
            max_records: 2,
            max_bytes: 1024 * 1024,
            lock_timeout_ms: 1000,
            acknowledgements: vec![],
        },
    };
    stream2
        .write_all(&encode_wire_request(&fetch_req_redeliver))
        .await
        .unwrap();
    let resp_redeliver = read_wire_response(&mut stream2).await;
    assert_eq!(resp_redeliver.status, 0);
    let mut resp_buf_rd = &resp_redeliver.payload[..];
    assert_eq!(resp_buf_rd.get_u32(), 1);
    assert_eq!(resp_buf_rd.get_u64(), 4);
    assert_eq!(resp_buf_rd.get_u64(), 5);
    assert_eq!(resp_buf_rd.get_u16(), 2); // delivery count is 2 due to auto timeout redelivery!

    // Member 2 accepts both
    let ack_req_final = WireRequest {
        cmd: CommandCode::ShareAcknowledge,
        payload: RequestPayload::ShareAcknowledge {
            group_id: group_id.to_string(),
            member_id: "member-2".to_string(),
            topic: topic.to_string(),
            partition: 0,
            acknowledgements: vec![AckBatch {
                first_offset: 4,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept,
            }],
        },
    };
    stream2
        .write_all(&encode_wire_request(&ack_req_final))
        .await
        .unwrap();
    let ack_resp_final = read_wire_response(&mut stream2).await;
    assert_eq!(ack_resp_final.status, 0);

    // 8. Describe group API test
    let desc_req = WireRequest {
        cmd: CommandCode::ShareGroupDescribe,
        payload: RequestPayload::ShareGroupDescribe {
            group_id: group_id.to_string(),
        },
    };
    stream1
        .write_all(&encode_wire_request(&desc_req))
        .await
        .unwrap();
    let desc_resp = read_wire_response(&mut stream1).await;
    assert_eq!(desc_resp.status, 0);

    // 9. Restart StorageEngine and verify persistent watermark recovery
    drop(stream1);
    drop(stream2);
    let mut restart_cfg = env.engine.config().clone();
    restart_cfg.data_dir = env.data_dir.clone();
    let restarted = StorageEngine::new(restart_cfg).unwrap();

    let sp = restarted
        .share_groups()
        .get_or_create_partition(group_id, topic, 0);
    // All offsets 0..=5 were acknowledged or archived, so start_offset is 6
    assert_eq!(sp.start_offset.load(std::sync::atomic::Ordering::SeqCst), 6);
}

#[tokio::test]
async fn test_scenario_32_dynamic_topic_configs() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "dynamic-config-topic";
    client.create_topic(topic, 1).await.unwrap();

    // 1. DescribeConfigs on a freshly-created topic returns no overrides yet.
    let initial = client.describe_configs(topic).await.unwrap();
    assert!(initial.is_empty());

    // 2. AlterConfigs (full replace) sets cleanup.policy and retention.ms.
    client
        .alter_configs(
            topic,
            &[
                ("cleanup.policy".to_string(), "compact".to_string()),
                ("retention.ms".to_string(), "3600000".to_string()),
            ],
        )
        .await
        .expect("AlterConfigs should succeed");

    let after_alter = client.describe_configs(topic).await.unwrap();
    let as_map: std::collections::HashMap<String, String> = after_alter.into_iter().collect();
    assert_eq!(
        as_map.get("cleanup.policy").map(String::as_str),
        Some("compact")
    );
    assert_eq!(
        as_map.get("retention.ms").map(String::as_str),
        Some("3600000")
    );

    // The recognized keys must actually take effect on the live partition, not just be
    // stored — this is what distinguishes a real dynamic-config API from a no-op one.
    let pm = env.engine.get_or_create_partition(topic, 0).unwrap();
    assert_eq!(pm.cleanup_policy(), hermes::config::CleanupPolicy::Compact);

    // 3. IncrementalAlterConfigs merges: upsert one key, delete another, leave the rest.
    client
        .incremental_alter_configs(
            topic,
            &[("compression.type".to_string(), "lz4".to_string())],
            &["retention.ms".to_string()],
        )
        .await
        .expect("IncrementalAlterConfigs should succeed");

    let after_incremental = client.describe_configs(topic).await.unwrap();
    let as_map2: std::collections::HashMap<String, String> =
        after_incremental.into_iter().collect();
    assert_eq!(
        as_map2.get("cleanup.policy").map(String::as_str),
        Some("compact"),
        "keys not touched by the incremental update must survive"
    );
    assert_eq!(
        as_map2.get("compression.type").map(String::as_str),
        Some("lz4")
    );
    assert!(
        !as_map2.contains_key("retention.ms"),
        "deleted key must be gone"
    );

    // 4. AlterConfigs/DescribeConfigs against an unknown topic is rejected, not silently
    // accepted.
    let err = client
        .alter_configs(
            "no-such-topic",
            &[("retention.ms".to_string(), "1".to_string())],
        )
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_scenario_33_cooperative_group_rebalancing() {
    let env = start_test_server().await;
    let group_id = "cooperative-group";
    let topic = "cooperative-topic";

    let mut client_a = TestClient::connect(env.addr).await.unwrap();
    let mut client_b = TestClient::connect(env.addr).await.unwrap();

    // 1. Member A forms the group with a cooperative assignor.
    let join_a = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .expect("member A join should succeed");
    assert_eq!(join_a.generation_id, 1);
    assert!(join_a.is_leader, "first member must be the leader");
    assert_eq!(join_a.protocol_name, "cooperative-sticky");

    // 2. Leader A submits the initial assignment (all partitions to itself) and stabilizes.
    let assignment_gen1 = vec![hermes::protocol::wire::MemberAssignment {
        member_id: "member-a".to_string(),
        topic: topic.to_string(),
        partitions: vec![0, 1],
    }];
    let a_assignment = client_a
        .sync_group(group_id, join_a.generation_id, "member-a", &assignment_gen1)
        .await
        .expect("leader sync should succeed");
    assert_eq!(a_assignment, vec![(topic.to_string(), vec![0, 1])]);

    // A's heartbeat at the current generation succeeds normally.
    client_a
        .heartbeat(group_id, join_a.generation_id, "member-a")
        .await
        .expect("heartbeat at current generation should succeed");

    // 3. Member B joins — triggers a new generation. A is still the leader.
    let join_b = client_b
        .join_group(group_id, "member-b", &["cooperative-sticky"])
        .await
        .expect("member B join should succeed");
    assert_eq!(join_b.generation_id, 2);
    assert!(!join_b.is_leader);

    // 4. A hasn't rejoined yet (still heartbeating at generation 1). Because the group is
    // cooperative, this must be a retryable signal, not a hard failure — A keeps
    // consuming its existing assignment in the meantime rather than being cut off.
    let stale_heartbeat = client_a.heartbeat(group_id, 1, "member-a").await;
    assert!(stale_heartbeat.is_err());
    assert!(stale_heartbeat
        .unwrap_err()
        .to_string()
        .contains("REBALANCE_IN_PROGRESS"));

    // 5. B tries to sync before the leader has submitted the new generation's assignment
    // — also retryable, not fatal.
    let early_sync = client_b
        .sync_group(group_id, join_b.generation_id, "member-b", &[])
        .await;
    assert!(early_sync.is_err());
    assert!(early_sync
        .unwrap_err()
        .to_string()
        .contains("REBALANCE_IN_PROGRESS"));

    // 6. Leader A rejoins to learn the new generation, then submits a real incremental
    // assignment (only reassigning what actually moved: partition 1 to B).
    let rejoin_a = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .expect("member A rejoin should succeed");
    assert_eq!(rejoin_a.generation_id, join_b.generation_id);
    assert!(rejoin_a.is_leader);

    let assignment_gen2 = vec![
        hermes::protocol::wire::MemberAssignment {
            member_id: "member-a".to_string(),
            topic: topic.to_string(),
            partitions: vec![0],
        },
        hermes::protocol::wire::MemberAssignment {
            member_id: "member-b".to_string(),
            topic: topic.to_string(),
            partitions: vec![1],
        },
    ];
    let a_assignment_gen2 = client_a
        .sync_group(
            group_id,
            rejoin_a.generation_id,
            "member-a",
            &assignment_gen2,
        )
        .await
        .expect("leader sync for generation 2 should succeed");
    assert_eq!(a_assignment_gen2, vec![(topic.to_string(), vec![0])]);

    // 7. Now that the leader has synced, B's follower sync succeeds and retrieves its own
    // (and only its own) assignment.
    let b_assignment = client_b
        .sync_group(group_id, join_b.generation_id, "member-b", &[])
        .await
        .expect("follower sync should succeed once the leader has submitted");
    assert_eq!(b_assignment, vec![(topic.to_string(), vec![1])]);

    // 8. Both members' heartbeats at the current generation now succeed cleanly.
    client_a
        .heartbeat(group_id, rejoin_a.generation_id, "member-a")
        .await
        .expect("A heartbeat at generation 2 should succeed");
    client_b
        .heartbeat(group_id, join_b.generation_id, "member-b")
        .await
        .expect("B heartbeat at generation 2 should succeed");

    // 9. Contrast with an eager (default) group. Both eager and cooperative stale-
    // generation heartbeats are errors carrying the same recognizable
    // "REBALANCE_IN_PROGRESS" signal (so a client never has to guess from free-form text
    // whether it means "just rejoin" versus something fatal) — the actual behavioral
    // difference KIP-429 cooperative rebalancing is about is what happens *after* that
    // signal: a cooperative member's heartbeat still gets refreshed (so it isn't pruned
    // as expired while it takes its time rejoining and keeps processing partitions it
    // already owns), while an eager member's does not.
    let eager_group = "eager-group";
    let mut client_c = TestClient::connect(env.addr).await.unwrap();
    let join_c = client_c
        .join_group(eager_group, "member-c", &["range"])
        .await
        .expect("eager join should succeed");
    assert_eq!(join_c.protocol_name, "range");
    let mut client_d = TestClient::connect(env.addr).await.unwrap();
    let _join_d = client_d
        .join_group(eager_group, "member-d", &["range"])
        .await
        .expect("second eager join should succeed");

    let eager_stale_heartbeat = client_c
        .heartbeat(eager_group, join_c.generation_id, "member-c")
        .await;
    assert!(eager_stale_heartbeat.is_err());
    assert!(eager_stale_heartbeat
        .unwrap_err()
        .to_string()
        .contains("REBALANCE_IN_PROGRESS"));
}

#[tokio::test]
async fn test_scenario_34_dedicated_controller_and_broker_roles() {
    use hermes::config::ProcessRole;

    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let base_dir = std::env::temp_dir().join(format!("role_split_test_{}_{}", pid, count));

    let node1_dir = base_dir.join("node1_data");
    let node2_dir = base_dir.join("node2_data");
    let _ = std::fs::remove_dir_all(&base_dir);

    // Node 2: broker-only — hosts data partitions, never contests the metadata quorum.
    let config_node2 = EngineConfig {
        node_id: 2,
        role: hermes::NodeRole::Follower,
        data_dir: node2_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        roles: vec![ProcessRole::Broker],
        ..EngineConfig::default()
    };
    let engine_node2 = StorageEngine::new(config_node2).unwrap();
    let server_node2 = Server::new(engine_node2.clone());
    let (listener_node2, addr_node2) = server_node2.bind().unwrap();
    let server_node2_task = tokio::spawn(async move {
        let _ = server_node2.run_with_listener(listener_node2).await;
    });

    // Node 1: controller-only — owns the metadata Raft quorum, must never host a data
    // partition even though it's the (only) node available at topic-creation time.
    let config_node1 = EngineConfig {
        node_id: 1,
        role: hermes::NodeRole::Leader,
        data_dir: node1_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        peer_addrs: vec![addr_node2.to_string()],
        roles: vec![ProcessRole::Controller],
        // Explicitly zero peer controllers (this is the only one) — since `roles` is no
        // longer the default combined set, this is taken literally rather than falling
        // back to `peer_addrs` (which would incorrectly treat the broker-only peer as a
        // fellow voter).
        controller_peer_addrs: Vec::new(),
        ..EngineConfig::default()
    };
    let engine_node1 = StorageEngine::new(config_node1).unwrap();
    let server_node1 = Server::new(engine_node1.clone());
    let (listener_node1, addr_node1) = server_node1.bind().unwrap();
    let server_node1_task = tokio::spawn(async move {
        let _ = server_node1.run_with_listener(listener_node1).await;
    });

    sleep(Duration::from_millis(50)).await;

    // Seed broker discovery directly rather than waiting on the real heartbeat
    // round-trip's timing (same pragmatic shortcut other multi-node tests in this file
    // use via `register_broker_address`) — this is what `send_leader_heartbeat`'s ACK
    // round-trip would otherwise populate.
    engine_node1.register_broker_address(1, addr_node1.to_string());
    engine_node1.register_broker_roles(1, &[ProcessRole::Controller.to_byte()]);
    engine_node1.register_broker_address(2, addr_node2.to_string());
    engine_node1.register_broker_roles(2, &[ProcessRole::Broker.to_byte()]);

    // Node 1 (controller-only) must still be the cluster/Raft leader — that's its whole
    // job — even though it will host no data.
    assert!(engine_node1.is_leader());
    assert!(!engine_node2.is_leader());

    let topic = "role-split-topic";
    let mut client_controller = TestClient::connect(addr_node1).await.unwrap();
    client_controller
        .create_topic(topic, 2)
        .await
        .expect("CreateTopic via the controller-only node should succeed");

    // Every partition must have been assigned to node 2 (the only broker-eligible node)
    // — never to node 1, even though node 1 is technically "available" and would have
    // been picked by the old role-blind assignment logic.
    let (_desc_topic, desc_parts) = client_controller
        .describe_topic(topic)
        .await
        .expect("DescribeTopic failed");
    assert_eq!(desc_parts.len(), 2);
    for part in &desc_parts {
        assert_eq!(
            part.leader_id, 2,
            "partition {} must be led by the broker-only node, not the controller-only one",
            part.partition_id
        );
    }
    assert!(
        !engine_node1.is_partition_leader(topic, 0),
        "controller-only node must never be a data-partition leader"
    );

    // Producing via the controller-only node must transparently forward to the actual
    // (broker-only) partition leader rather than failing or mishandling the write.
    let prod_res = client_controller
        .produce_single(
            topic,
            "",
            None,
            2,
            "hello from a controller-only entrypoint",
        )
        .await
        .expect("produce forwarded through the controller-only node should succeed");
    assert_eq!(prod_res.first_offset, 0);

    sleep(Duration::from_millis(100)).await;

    let mut client_broker = TestClient::connect(addr_node2).await.unwrap();
    let fetched = client_broker
        .fetch(topic, prod_res.assigned_partition, 0, 65536)
        .await
        .expect("fetch directly from the broker-only node should succeed");
    assert_eq!(fetched.len(), 1);
    assert_eq!(
        fetched[0].payload,
        "hello from a controller-only entrypoint".as_bytes()
    );

    server_node1_task.abort();
    server_node2_task.abort();
    let _ = std::fs::remove_dir_all(&base_dir);
}

/// Exercises the zero-copy `Fetch` fast path (`TransmitFile`/`sendfile`) end-to-end over a
/// real plain (non-TLS) TCP connection — `TestClient` decodes the wire response the same
/// way regardless of whether the server served it via zero-copy transmit or the buffered
/// path, so any mismatch in the zero-copy header/byte-range logic shows up as wrong
/// offsets, payloads, or frame counts here.
#[tokio::test]
async fn test_scenario_35_zero_copy_fetch_end_to_end() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let topic = "zero_copy_fetch_topic";
    let partition = 0u32;
    let total_records = 40usize;
    let records: Vec<String> = (0..total_records)
        .map(|i| format!("zero-copy-record-{:04}-with-some-extra-padding", i))
        .collect();
    let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();

    let prod_resp = client
        .produce_batch(topic, "", None, 1, &record_refs)
        .await
        .unwrap();
    assert_eq!(prod_resp.first_offset, 0);
    assert_eq!(prod_resp.last_offset, total_records as u64 - 1);

    // 1. Large max_bytes: the whole committed log fits in a single zero-copy plan.
    let all_frames = client.fetch(topic, partition, 0, 64 * 1024).await.unwrap();
    assert_eq!(all_frames.len(), total_records);
    for (i, frame) in all_frames.iter().enumerate() {
        assert_eq!(frame.offset, i as u64);
        assert_eq!(frame.payload, records[i].as_bytes());
        let calculated_crc =
            RecordFrame::calculate_crc(frame.offset, frame.timestamp, &frame.payload);
        assert_eq!(
            frame.crc, calculated_crc,
            "zero-copy frame CRC mismatch at offset {}",
            i
        );
    }

    // 2. Tight max_bytes budget forcing a multi-round consume loop — reconstructs the
    // exact same sequence as (1), one small zero-copy-eligible slice at a time.
    let mut next_offset = 0u64;
    let mut collected = Vec::new();
    let small_budget = 200u32; // a handful of records per round, not all of them
    loop {
        let frames = client
            .fetch(topic, partition, next_offset, small_budget)
            .await
            .unwrap();
        if frames.is_empty() {
            break;
        }
        next_offset = frames.last().unwrap().offset + 1;
        collected.extend(frames);
        if collected.len() >= total_records {
            break;
        }
    }
    assert_eq!(collected.len(), total_records);
    for (i, frame) in collected.iter().enumerate() {
        assert_eq!(frame.offset, i as u64);
        assert_eq!(frame.payload, records[i].as_bytes());
    }

    // 3. Fetching at/beyond the high watermark must return an empty (not erroring) result
    // — the zero-copy planner returns None for this, and the buffered fallback must still
    // behave exactly like it always has.
    let beyond_hw = client
        .fetch(topic, partition, total_records as u64, 4096)
        .await
        .unwrap();
    assert!(beyond_hw.is_empty());
}

/// End-to-end tombstone + `delete.retention.ms` + `min.cleanable.dirty.ratio` compaction,
/// driven entirely through the real client (`AlterConfigs` + `ProduceBatch` + `Fetch`) and
/// the background-GC-equivalent `PartitionManager::apply_retention()` call — same real
/// entry points a client/operator would use, not `SegmentManager` internals directly (see
/// the focused per-branch unit tests in `src/segment/manager.rs` for that level of detail).
#[tokio::test]
async fn test_scenario_36_tombstone_and_dirty_ratio_compaction() {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!(
        "tombstone_compaction_test_{}_{}_{}",
        std::process::id(),
        nanos,
        count
    ));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();

    // Tiny max_segment_bytes forces auto-rotation after each small record, so every
    // record lands in its own segment — deterministic layout without needing a manual
    // rotate hook through the client.
    let config = EngineConfig {
        data_dir: data_dir.clone(),
        max_segment_bytes: 40,
        bind_addr: "127.0.0.1:0".to_string(),
        ..EngineConfig::default()
    };
    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine.clone());
    let (listener, addr) = server.bind().unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.run_with_listener(listener).await;
    });

    let mut client = TestClient::connect(addr).await.unwrap();
    let topic = "tombstone-compaction-topic";
    client.create_topic(topic, 1).await.unwrap();
    client
        .alter_configs(
            topic,
            &[
                ("cleanup.policy".to_string(), "compact".to_string()),
                ("delete.retention.ms".to_string(), "1".to_string()),
                ("min.cleanable.dirty.ratio".to_string(), "0.0".to_string()),
            ],
        )
        .await
        .expect("AlterConfigs should succeed");

    client
        .produce_single(topic, "", None, 1, "userA:v1")
        .await
        .unwrap(); // offset 0 — stale
    client
        .produce_single(topic, "", None, 1, "userA:v2")
        .await
        .unwrap(); // offset 1 — current value for userA
    client
        .produce_single(topic, "", None, 1, "userB:")
        .await
        .unwrap(); // offset 2 — tombstone for userB
    client
        .produce_single(topic, "", None, 1, "zzz:filler")
        .await
        .unwrap(); // offset 3 — pushes offset 2's segment into history

    // delete.retention.ms=1 means the tombstone is expired almost as soon as it's
    // written; this sleep just makes that unambiguous regardless of scheduling jitter.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let pm = engine.get_or_create_partition(topic, 0).unwrap();
    let compacted = pm.apply_retention().unwrap();
    assert!(
        compacted >= 2,
        "offset 0 (stale userA:v1) and offset 2 (expired userB tombstone) should both be dropped, got {}",
        compacted
    );

    // The surviving current value for userA must still be exactly as produced.
    let remaining = client.fetch(topic, 0, 1, 4096).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].offset, 1);
    assert_eq!(remaining[0].payload.as_ref(), b"userA:v2");

    server_task.abort();
    let _ = std::fs::remove_dir_all(&data_dir);
}

fn encode_wire_request(req: &hermes::WireRequest) -> Vec<u8> {
    use bytes::BufMut;
    let mut body = Vec::new();
    match &req.payload {
        hermes::RequestPayload::ShareFetch {
            group_id,
            member_id,
            topic,
            partition,
            max_records,
            max_bytes,
            lock_timeout_ms,
            acknowledgements,
        } => {
            hermes::protocol::wire::write_pascal_string(&mut body, group_id);
            hermes::protocol::wire::write_pascal_string(&mut body, member_id);
            hermes::protocol::wire::write_pascal_string(&mut body, topic);
            body.put_u32(*partition);
            body.put_u32(*max_records);
            body.put_u32(*max_bytes);
            body.put_u32(*lock_timeout_ms);
            body.put_u32(acknowledgements.len() as u32);
            for ack in acknowledgements {
                body.put_u64(ack.first_offset);
                body.put_u64(ack.last_offset);
                body.put_u8(ack.ack_type as u8);
            }
        }
        hermes::RequestPayload::ShareAcknowledge {
            group_id,
            member_id,
            topic,
            partition,
            acknowledgements,
        } => {
            hermes::protocol::wire::write_pascal_string(&mut body, group_id);
            hermes::protocol::wire::write_pascal_string(&mut body, member_id);
            hermes::protocol::wire::write_pascal_string(&mut body, topic);
            body.put_u32(*partition);
            body.put_u32(acknowledgements.len() as u32);
            for ack in acknowledgements {
                body.put_u64(ack.first_offset);
                body.put_u64(ack.last_offset);
                body.put_u8(ack.ack_type as u8);
            }
        }
        hermes::RequestPayload::ShareGroupHeartbeat {
            group_id,
            member_id,
        } => {
            hermes::protocol::wire::write_pascal_string(&mut body, group_id);
            hermes::protocol::wire::write_pascal_string(&mut body, member_id);
        }
        hermes::RequestPayload::ShareGroupDescribe { group_id } => {
            hermes::protocol::wire::write_pascal_string(&mut body, group_id);
        }
        _ => panic!("Unsupported in test encode"),
    }

    let mut buf = Vec::new();
    buf.put_u8(req.cmd as u8);
    buf.put_u32(body.len() as u32);
    buf.extend_from_slice(&body);
    buf
}

async fn read_wire_response<S>(stream: &mut S) -> hermes::WireResponse
where
    S: tokio::io::AsyncReadExt + Unpin,
{
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    let status = header[0];
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).await.unwrap();
    }
    hermes::WireResponse { status, payload }
}

/// A read must never bring a topic into existence. `Fetch` (and the offset/seek probes
/// that share its path) used to run through `get_or_create_partition`, so simply naming a
/// topic that had never been produced to created its directory tree on disk — turning an
/// unauthenticated read of a nonexistent topic into a write, and letting a request loop
/// exhaust inodes with state that outlived the connection.
#[tokio::test]
async fn test_scenario_38_fetch_of_unknown_topic_creates_no_state() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    // Fetch, seek and latest-offset against a topic that was never created.
    let fetched = client
        .fetch("never_created_topic", 0, 0, 4096)
        .await
        .unwrap();
    assert!(fetched.is_empty(), "unknown topic must read back empty");

    // Nothing on disk, and nothing registered in the engine's partition map.
    let stray: Vec<_> = std::fs::read_dir(&env.data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with("never_created_topic"))
        .collect();
    assert!(
        stray.is_empty(),
        "a read must not create partition directories, found: {:?}",
        stray
    );
    assert!(
        !env.engine
            .list_topics()
            .contains(&"never_created_topic".to_string()),
        "a read must not register the topic"
    );

    // A produce to the same topic still works (auto-creation is a write-path decision).
    client
        .produce_single("never_created_topic", "k", None, 1, "v")
        .await
        .unwrap();
    assert!(
        env.engine
            .list_topics()
            .contains(&"never_created_topic".to_string()),
        "producing should create the topic"
    );
}

/// `acks=all` must wait for **every** replica currently in the ISR, not merely until
/// `min_insync_replicas` acknowledgements have arrived.
///
/// The old behavior counted acks and returned as soon as it hit the `min.insync.replicas`
/// floor, which inverts what that setting is for: with an ISR of 3 and a floor of 2, a
/// write was acknowledged to the producer once any 2 replicas had it, so losing those 2
/// lost data the producer had been told was fully replicated.
#[tokio::test]
async fn test_scenario_39_acks_all_waits_for_every_isr_member() {
    let env = start_test_server().await;
    let engine = env.engine.clone();
    engine.create_topic("isr_topic", 1).await.unwrap();
    let pm = engine.get_or_create_partition("isr_topic", 0).unwrap();

    let self_id = engine.config().node_id;
    let (follower_a, follower_b) = (self_id + 10, self_id + 20);
    let addr_a = "127.0.0.1:19301".to_string();
    let addr_b = "127.0.0.1:19302".to_string();
    engine.register_broker_address(follower_a, addr_a.clone());
    engine.register_broker_address(follower_b, addr_b.clone());

    // An ISR of three (this leader plus two followers) with a floor of 2. Reaching the
    // floor must NOT be enough to commit.
    pm.update_leadership(
        self_id,
        1,
        vec![self_id, follower_a, follower_b],
        vec![self_id, follower_a, follower_b],
    );

    let target_offset = 42u64;
    let short = std::time::Duration::from_millis(150);

    // Only follower A has acknowledged: 2 of 3 ISR members hold the record, which
    // satisfies min.insync.replicas=2 but is NOT the full ISR.
    engine
        .replication()
        .update_replica_watermark("isr_topic", 0, &addr_a, target_offset);
    let reached_floor_only = engine
        .await_full_isr_ack(&pm, "isr_topic", 0, target_offset, short)
        .await;
    assert!(
        reached_floor_only.is_err(),
        "hitting min.insync.replicas must not commit while an ISR member is still behind"
    );

    // Once the last ISR member acknowledges, the write commits.
    engine
        .replication()
        .update_replica_watermark("isr_topic", 0, &addr_b, target_offset);
    let full_isr = engine
        .await_full_isr_ack(&pm, "isr_topic", 0, target_offset, short)
        .await;
    assert!(
        full_isr.is_ok(),
        "commit once every ISR member has acknowledged: {:?}",
        full_isr.err()
    );

    // Shrinking the ISR below the floor must fail fast rather than wait out the timeout —
    // no amount of waiting can make the write as durable as acks=all promises. Raise the
    // topic's floor to 2 so a single-member ISR is genuinely under-replicated.
    engine
        .alter_configs(
            "isr_topic",
            vec![("min.insync.replicas".to_string(), "2".to_string())],
        )
        .await
        .expect("failed to set min.insync.replicas");
    pm.update_leadership(
        self_id,
        2,
        vec![self_id, follower_a, follower_b],
        vec![self_id],
    );
    let started = std::time::Instant::now();
    let below_floor = engine
        .await_full_isr_ack(&pm, "isr_topic", 0, target_offset + 1, short)
        .await;
    assert!(
        below_floor.is_err(),
        "ISR below min.insync.replicas must be rejected"
    );
    assert!(
        started.elapsed() < short,
        "an under-replicated ISR should fail fast, not block for the timeout"
    );
}

/// Members that join together must land in ONE generation.
///
/// `JoinGroup` used to reply immediately and form a fresh generation per arrival, so each
/// new member invalidated the assignment just handed to the previous one and forced it to
/// rejoin — a group of N members starting together produced roughly N rebalances, and
/// members could be knocked out of a generation before processing a single record.
#[tokio::test]
async fn test_scenario_40_join_group_barrier_forms_one_generation() {
    let env = start_test_server().await;
    let engine = env.engine.clone();

    // Five members join concurrently, exactly as a consumer group does on startup.
    let mut joins = Vec::new();
    for i in 0..5 {
        let engine = engine.clone();
        joins.push(tokio::spawn(async move {
            engine
                .join_group_awaited(
                    "barrier_group",
                    &format!("member-{}", i),
                    None,
                    vec!["range".to_string()],
                )
                .await
        }));
    }

    let mut generations = Vec::new();
    let mut leaders = 0usize;
    for join in joins {
        let (_member_id, generation, is_leader, _protocol) = join.await.unwrap().unwrap();
        generations.push(generation);
        if is_leader {
            leaders += 1;
        }
    }

    let first = generations[0];
    assert!(
        generations.iter().all(|g| *g == first),
        "all members joining the same window must share one generation, got {:?}",
        generations
    );
    assert_eq!(
        leaders, 1,
        "exactly one member must be told it is the group leader, got {}",
        leaders
    );

    // And the group really does hold all five members in that generation.
    let described = engine
        .group_coordinator()
        .describe_group("barrier_group")
        .expect("group should exist");
    assert_eq!(
        described.members.len(),
        5,
        "every member of the window must be in the group"
    );
}

/// A malformed config value must be REJECTED, never silently applied as "unset".
///
/// `retention.ms` was parsed with `.ok()`, so a typo evaluated to `None` and turned
/// retention *off* — the topic then grew without bound while the client saw a success
/// response, and the resulting state was indistinguishable from a deliberate "unlimited".
#[tokio::test]
async fn test_scenario_41_alter_configs_rejects_invalid_values() {
    let env = start_test_server().await;
    let engine = env.engine.clone();
    engine.create_topic("cfg_topic", 1).await.unwrap();

    // Establish a real retention first, so we can prove a failed update doesn't clear it.
    engine
        .alter_configs(
            "cfg_topic",
            vec![("retention.ms".to_string(), "60000".to_string())],
        )
        .await
        .expect("valid config should apply");

    for (key, bad_value) in [
        ("retention.ms", "not_a_number"),
        ("retention.bytes", "12x"),
        ("min.insync.replicas", "0"),
        ("min.cleanable.dirty.ratio", "5.0"),
        ("cleanup.policy", "recycle"),
        ("compression.type", "bzip2"),
    ] {
        let result = engine
            .alter_configs("cfg_topic", vec![(key.to_string(), bad_value.to_string())])
            .await;
        assert!(
            result.is_err(),
            "'{}={}' must be rejected, not silently applied",
            key,
            bad_value
        );
    }

    // The previously-set value survived every rejected update.
    let configs = engine.describe_configs("cfg_topic");
    let retention = configs
        .iter()
        .find(|(k, _)| k == "retention.ms")
        .map(|(_, v)| v.clone());
    assert_eq!(
        retention,
        Some("60000".to_string()),
        "a rejected update must leave the existing value untouched"
    );

    // An explicitly empty value is still the way to clear a setting.
    engine
        .alter_configs(
            "cfg_topic",
            vec![("retention.ms".to_string(), String::new())],
        )
        .await
        .expect("clearing a setting with an empty value should be allowed");
}

/// Push and pull replication must never both deliver the same records to the same peer.
///
/// The two used to run unconditionally side by side: every record crossed the wire and hit
/// the follower's append path twice, a pushed and a pulled batch covering the same offsets
/// could race on append, and follower progress was written by two independent paths so ISR
/// decisions read a value neither owned. A first fix excluded pull-covered peers from push;
/// once partition assignment became universal (issue #40) that exclusion emptied the push
/// target list for every data partition on its own, so push was removed from the produce
/// path outright (issue #22) rather than left calling a filter that always returned empty.
///
/// There is no push target list left to inspect for a data topic now, so this proves the
/// same "no peer is ever delivered the same records twice" invariant at the wire level
/// instead: a raw peer standing in for an assigned replica must never see a single byte
/// from a data-topic produce (push simply never fires), while it must see an actual 0xAA
/// push packet the moment a metadata mutation happens, since `__cluster_metadata` has no
/// pull fetcher and is push-only by design.
#[tokio::test]
async fn test_scenario_42_push_never_duplicates_pull_and_metadata_still_pushes() {
    use hermes::config::ProcessRole;
    use std::sync::atomic::AtomicBool;
    use tokio::io::AsyncReadExt;

    let test_dir = std::env::temp_dir().join(format!(
        "hermes_test_no_dup_push_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&test_dir);

    // A raw, non-Hermes TCP peer standing in for a replica: nothing but a leader-side
    // wire call (replication push *or* the leader heartbeat, which shares the same
    // per-peer pooled connection) could ever make a connection land here. Only the
    // replication-push magic byte (0xAA) counts as proof of a push; the leader heartbeat
    // (0xAC) also legitimately connects here (`start_leader_heartbeat_loop`, started for
    // any Leader with a non-empty `peer_addrs`) and must be ignored.
    //
    // The harness doesn't implement either wire protocol's full reply — it just reads the
    // magic byte and drops the connection. Both `send_replication_push_pooled` and
    // `send_leader_heartbeat_pooled` treat a dropped/EOF reply as a failed call, clear
    // their pooled connection, and reconnect fresh next time, so a new accept() here
    // observes every subsequent call as its own connection.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = listener.local_addr().unwrap().to_string();

    let magic_seen = std::sync::Arc::new(AtomicBool::new(false));
    let magic_seen_task = magic_seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut magic = [0u8; 1];
            if stream.read_exact(&mut magic).await.is_ok() && magic[0] == 0xAA {
                magic_seen_task.store(true, Ordering::SeqCst);
            }
            // Deliberately no reply: dropping here is what makes the client treat this as
            // a failed call and reconnect fresh for its next one (see comment above).
        }
    });

    // Controller-only role (no Broker) with an explicitly empty controller peer set
    // decouples the metadata-commit quorum (majority of 1, satisfied by this sole
    // controller alone) from `peer_addrs` below, which drives push targeting. Without this
    // split, giving the node a non-empty `peer_addrs` — needed so push has somewhere to go
    // at all, mirroring a real multi-node deployment — would also force the metadata write
    // to wait on a majority ack from the fake peer.
    //
    // The periodic ISR/failover sweep independently calls `catch_up_follower_metadata`,
    // which would also push to this peer once the metadata log is non-empty (it is, from
    // the leader's own startup bootstrap record) — set the interval far beyond this test's
    // runtime so that sweep can't fire and manufacture a false-positive push.
    let config = EngineConfig {
        node_id: 1,
        role: hermes::NodeRole::Leader,
        data_dir: test_dir.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        peer_addrs: vec![peer_addr.clone()],
        roles: vec![ProcessRole::Controller],
        controller_peer_addrs: Vec::new(),
        isr_check_interval_ms: 60_000,
        ..EngineConfig::default()
    };
    let engine = StorageEngine::new(config).unwrap();

    // An assigned data partition: manually set leadership/replicas so this looks exactly
    // like the common case push used to special-case (an assigned replica), rather than
    // the auto-created/unassigned case push was originally kept around for.
    let topic = "no_dup_push_topic";
    let pm = engine.get_or_create_partition(topic, 0).unwrap();
    let self_id = engine.config().node_id;
    let follower_id = self_id + 1;
    engine.register_broker_address(follower_id, peer_addr.clone());
    pm.update_leadership(
        self_id,
        1,
        vec![self_id, follower_id],
        vec![self_id, follower_id],
    );
    assert!(engine.is_partition_leader(topic, 0));

    // Produce to the assigned data partition. If push still fired here, the fake peer
    // would see a connection carrying 0xAA.
    let records = vec![bytes::Bytes::from("no-push-payload")];
    let params = hermes::server::engine::ProduceBatchParams {
        topic,
        key: "",
        transaction_id: None,
        num_partitions: 1,
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: 0,
        records: &records,
    };
    engine.produce_batch(params).await.unwrap();

    // Give any (wrongly) spawned push task time to connect before checking.
    sleep(Duration::from_millis(300)).await;
    assert!(
        !magic_seen.load(Ordering::SeqCst),
        "a data-topic produce must never push to an assigned replica — pull is now the \
         only replication mechanism for data topics"
    );

    // Now a metadata mutation on the same node: `__cluster_metadata` has no pull fetcher
    // by design, so it must still push.
    engine
        .upsert_scram_user_with_mechanism(
            "no_dup_push_test_user",
            "irrelevant-password",
            hermes::scram::ScramMechanism::Sha256,
        )
        .await
        .unwrap();

    let mut pushed = false;
    for _ in 0..40 {
        if magic_seen.load(Ordering::SeqCst) {
            pushed = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        pushed,
        "__cluster_metadata must still replicate via leader-push — it has no pull fetcher \
         to fall back on"
    );

    let _ = std::fs::remove_dir_all(&test_dir);
}

/// A user may hold a SHA-256 and a SHA-512 credential at once, each verifying only under
/// its own mechanism — and deleting the user must remove both.
///
/// Credentials used to be keyed by username alone, so creating one under a second
/// mechanism silently replaced the first.
#[tokio::test]
async fn test_scenario_43_scram_credentials_are_per_mechanism() {
    use hermes::scram::ScramMechanism;

    let env = start_test_server().await;
    let engine = env.engine.clone();

    engine
        .upsert_scram_user_with_mechanism("dual", "pw-256", ScramMechanism::Sha256)
        .await
        .unwrap();
    engine
        .upsert_scram_user_with_mechanism("dual", "pw-512", ScramMechanism::Sha512)
        .await
        .unwrap();

    // Both survive: adding the second did not replace the first.
    assert_eq!(
        engine.scram_user_mechanisms("dual"),
        vec![ScramMechanism::Sha512, ScramMechanism::Sha256],
        "a user must be able to hold both mechanisms at once"
    );

    // Deleting the user removes every mechanism, not just one.
    assert!(engine.delete_scram_user("dual").await.unwrap());
    assert!(!engine.has_scram_user("dual"));
    assert!(
        engine.scram_user_mechanisms("dual").is_empty(),
        "deleting a user must not leave them able to authenticate under another hash"
    );
}

/// A partition created implicitly by a produce must end up with a real replica assignment.
///
/// `get_or_create_partition` opens the local log and writes no metadata, so cluster
/// metadata never learned such a partition existed: no follower knew to fetch it, the ISR
/// sweep had no membership to manage, and failover had nothing to promote. The controller
/// now retrofits an assignment onto it without moving data.
#[tokio::test]
async fn test_scenario_44_unassigned_partitions_get_a_replica_assignment() {
    let env = start_test_server().await;
    let engine = env.engine.clone();
    let self_id = engine.config().node_id;

    // Two extra brokers, so a replication factor above 1 is actually satisfiable.
    engine.register_broker_address(self_id + 1, "127.0.0.1:19601".to_string());
    engine.register_broker_address(self_id + 2, "127.0.0.1:19602".to_string());

    // Implicit creation: exactly what a produce to an unknown topic does.
    let pm = engine.get_or_create_partition("implicit_topic", 0).unwrap();
    assert!(
        engine.describe_topic("implicit_topic").is_some(),
        "the partition exists locally"
    );
    // ...but cluster metadata has no assignment for it yet.
    assert!(
        !engine.has_partition_assignment("implicit_topic", 0),
        "precondition: an implicitly created partition starts unassigned"
    );

    engine.reconcile_unassigned_partitions_for_test().await;

    assert!(
        engine.has_partition_assignment("implicit_topic", 0),
        "the sweep must publish an assignment for an unassigned partition"
    );

    // Leadership stayed put — assignment must not move data.
    assert_eq!(
        pm.leader_id(),
        self_id,
        "the broker already holding the partition must remain its leader"
    );

    // A real roster spanning the available brokers, with only the leader in-sync: the new
    // replicas hold none of the data yet.
    let replicas = pm.replicas();
    assert!(
        replicas.len() > 1,
        "expected a multi-broker roster, got {:?}",
        replicas
    );
    assert!(
        replicas.contains(&self_id),
        "the leader must be on the roster"
    );
    assert_eq!(
        pm.isr(),
        vec![self_id],
        "only the leader is in-sync immediately after assignment"
    );

    // Idempotent: a second sweep must not churn the assignment.
    let epoch_before = pm.leader_epoch();
    engine.reconcile_unassigned_partitions_for_test().await;
    assert_eq!(
        pm.leader_epoch(),
        epoch_before,
        "an already-assigned partition must not be reassigned on every sweep"
    );
}

/// End-to-end proof that pull replication delivers data-topic records on its own, now that
/// push has been removed from the produce path entirely (issue #22).
///
/// An implicitly-created partition gets a replica assignment from the controller sweep, the
/// follower's fetcher starts as a result, and records produced *after* the assignment reach
/// the follower purely by being pulled — there is no push left to deliver them.
#[tokio::test]
async fn test_scenario_45_follower_pulls_after_assignment() {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let base_dir = std::env::temp_dir().join(format!(
        "pull_after_assign_{}_{}",
        std::process::id(),
        count
    ));
    let _ = std::fs::remove_dir_all(&base_dir);

    // Follower (node 2)
    let engine_node2 = StorageEngine::new(EngineConfig {
        node_id: 2,
        role: hermes::NodeRole::Follower,
        data_dir: base_dir.join("node2"),
        bind_addr: "127.0.0.1:0".to_string(),
        ..EngineConfig::default()
    })
    .unwrap();
    let server_node2 = Server::new(engine_node2.clone());
    let (listener_node2, addr_node2) = server_node2.bind().unwrap();
    let task2 = tokio::spawn(async move {
        let _ = server_node2.run_with_listener(listener_node2).await;
    });

    // Leader (node 1), with node 2 as its peer
    let engine_node1 = StorageEngine::new(EngineConfig {
        node_id: 1,
        role: hermes::NodeRole::Leader,
        data_dir: base_dir.join("node1"),
        bind_addr: "127.0.0.1:0".to_string(),
        peer_addrs: vec![addr_node2.to_string()],
        ..EngineConfig::default()
    })
    .unwrap();
    let server_node1 = Server::new(engine_node1.clone());
    let (listener_node1, addr_node1) = server_node1.bind().unwrap();
    let task1 = tokio::spawn(async move {
        let _ = server_node1.run_with_listener(listener_node1).await;
    });

    sleep(Duration::from_millis(150)).await;

    // Each node needs the other's address to replicate. Normally learned from heartbeat
    // ACKs, which run on a 10s interval — far too slow for a test, so seed it directly.
    engine_node1.register_broker_address(2, addr_node2.to_string());
    engine_node2.register_broker_address(1, addr_node1.to_string());

    // 1. Produce to a topic that does not exist yet. Because this lands on the controller,
    //    the auto-create hook registers the topic and assigns replicas *before* the record
    //    is written — so unlike when this test was first written, the partition is already
    //    assigned by the time the produce returns.
    let mut client = TestClient::connect(addr_node1).await.unwrap();
    client
        .produce_single("pull_topic", "k1", None, 1, "before-assignment")
        .await
        .unwrap();

    // 2. The sweep is idempotent here: it has nothing left to assign. Running it anyway
    //    keeps this test honest about the end state rather than about which path produced
    //    it, since a produce landing on a non-controller would still arrive unassigned and
    //    be repaired by exactly this sweep.
    engine_node1
        .reconcile_unassigned_partitions_for_test()
        .await;
    assert!(
        engine_node1.has_partition_assignment("pull_topic", 0),
        "the partition must carry a replica assignment before pull can engage"
    );
    let replicas = engine_node1
        .get_or_create_partition("pull_topic", 0)
        .unwrap()
        .replicas();
    assert!(
        replicas.contains(&2),
        "the follower must be assigned as a replica, got {:?}",
        replicas
    );

    // 3. The leader back-fills the metadata the follower is missing.
    //
    // The follower joined after the leader had already written bootstrap records, which
    // are produced locally and never replicated, so its metadata log is empty and it
    // rejects any later record as a Gap. The catch-up sweep re-sends from the follower's
    // last acked offset (offset 0 here, since it has never acked) and heals it.
    engine_node1.catch_up_follower_metadata_for_test().await;

    // Re-assert the real addresses: the replayed `BrokerRegister` records carry each node's
    // *configured* bind address, which is "127.0.0.1:0" under an ephemeral-port test setup,
    // and applying them overwrites the resolved ones.
    engine_node1.register_broker_address(2, addr_node2.to_string());
    engine_node2.register_broker_address(1, addr_node1.to_string());

    let mut assigned_on_follower = false;
    for _ in 0..80 {
        if engine_node2.has_partition_assignment("pull_topic", 0) {
            assigned_on_follower = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        assigned_on_follower,
        "the follower must receive the assignment via replicated __cluster_metadata"
    );

    // 4. Produce AFTER assignment. Data-topic replication no longer pushes at all (see
    //    `test_scenario_42_push_never_duplicates_pull_and_metadata_still_pushes`), so this
    //    record can only reach the follower by being pulled.
    client
        .produce_single("pull_topic", "k2", None, 1, "after-assignment")
        .await
        .unwrap();

    let mut pulled = Vec::new();
    for _ in 0..100 {
        pulled = engine_node2
            .fetch("pull_topic", 0, 0, 65536)
            .await
            .unwrap_or_default();
        if pulled.len() >= 2 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let payloads: Vec<String> = pulled
        .iter()
        .map(|f| String::from_utf8_lossy(&f.payload).to_string())
        .collect();
    assert!(
        payloads.iter().any(|p| p.contains("after-assignment")),
        "the post-assignment record must reach the follower by pull; follower has {:?}",
        payloads
    );

    task1.abort();
    task2.abort();
    let _ = std::fs::remove_dir_all(&base_dir);
}

/// An explicitly requested replication factor is a durability contract: if the cluster
/// cannot satisfy it the topic must not be created, rather than silently degrading to
/// fewer replicas. Replication factor is fixed at creation and never raised automatically,
/// so a silent downgrade would leave the caller believing data is replicated for the whole
/// life of the topic.
#[tokio::test]
async fn test_scenario_46_explicit_replication_factor_is_not_silently_degraded() {
    let env = start_test_server().await;
    let engine = env.engine.clone();

    // Single broker: RF 3 cannot be satisfied.
    let rejected = engine
        .create_topic_with_replication_factor("rf_strict", 1, 3)
        .await;
    let err = rejected.expect_err("RF above the broker count must be rejected");
    assert!(
        err.to_string().contains("INVALID_REPLICATION_FACTOR"),
        "expected an explicit replication-factor error, got: {}",
        err
    );
    assert!(
        !engine.list_topics().contains(&"rf_strict".to_string()),
        "a rejected creation must not leave the topic half-created"
    );

    // A satisfiable factor succeeds.
    engine
        .create_topic_with_replication_factor("rf_ok", 1, 1)
        .await
        .expect("RF within the broker count should be accepted");
    assert!(engine.list_topics().contains(&"rf_ok".to_string()));

    // The implicit path still clamps rather than failing, so a single-node deployment can
    // create topics with the default factor of 3.
    engine
        .create_topic("rf_default", 1)
        .await
        .expect("the default factor must be clamped, not rejected");
    assert!(engine.list_topics().contains(&"rf_default".to_string()));
}

/// A follower that joins a leader which has already written metadata must still receive
/// all of it.
///
/// The leader's bootstrap records are produced locally and never replicated (they are
/// written in the synchronous `StorageEngine::new`, which cannot call the async
/// `propose_metadata`), so a fresh follower sits at offset 0 while the leader is further
/// along. The next pushed record then lands at a non-zero offset, the follower reports a
/// Gap and rejects it, and — with no pull fetcher for `__cluster_metadata` — nothing could
/// ever re-send the missing prefix. The gap was permanent, which meant followers never
/// learned partition assignments and data-topic replication silently did not happen.
#[tokio::test]
async fn test_scenario_47_metadata_catch_up_heals_a_joining_follower() {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let base_dir =
        std::env::temp_dir().join(format!("meta_catchup_{}_{}", std::process::id(), count));
    let _ = std::fs::remove_dir_all(&base_dir);

    let engine_follower = StorageEngine::new(EngineConfig {
        node_id: 2,
        role: hermes::NodeRole::Follower,
        data_dir: base_dir.join("f"),
        bind_addr: "127.0.0.1:0".to_string(),
        ..EngineConfig::default()
    })
    .unwrap();
    let server_f = Server::new(engine_follower.clone());
    let (listener_f, addr_f) = server_f.bind().unwrap();
    let task_f = tokio::spawn(async move {
        let _ = server_f.run_with_listener(listener_f).await;
    });

    let engine_leader = StorageEngine::new(EngineConfig {
        node_id: 1,
        role: hermes::NodeRole::Leader,
        data_dir: base_dir.join("l"),
        bind_addr: "127.0.0.1:0".to_string(),
        peer_addrs: vec![addr_f.to_string()],
        ..EngineConfig::default()
    })
    .unwrap();
    sleep(Duration::from_millis(150)).await;

    // The leader has bootstrap metadata; the follower has none of it.
    let leader_leo = engine_leader
        .latest_offset("__cluster_metadata", 0)
        .unwrap();
    assert!(
        leader_leo > 0,
        "precondition: the leader must have written bootstrap metadata"
    );
    assert_eq!(
        engine_follower
            .latest_offset("__cluster_metadata", 0)
            .unwrap(),
        0,
        "precondition: the follower joined with an empty metadata log"
    );

    // A record proposed now lands at a non-zero offset — the Gap the follower cannot bridge.
    engine_leader
        .create_topic("gap_topic", 1)
        .await
        .expect("topic creation should succeed on the leader");

    // The catch-up sweep re-sends from the follower's last acked offset (never acked → 0).
    engine_leader.catch_up_follower_metadata_for_test().await;

    let mut healed = false;
    for _ in 0..40 {
        if engine_follower
            .latest_offset("__cluster_metadata", 0)
            .unwrap_or(0)
            > 0
        {
            healed = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        healed,
        "the follower must receive the metadata prefix it was missing"
    );

    // It caught up on the *content*, not merely on some offset: the topic created after it
    // joined is now known to it.
    let mut knows_topic = false;
    for _ in 0..40 {
        if engine_follower
            .list_topics()
            .contains(&"gap_topic".to_string())
        {
            knows_topic = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        knows_topic,
        "the follower should know about a topic created before it caught up; \
         follower metadata LEO {} vs leader {}",
        engine_follower
            .latest_offset("__cluster_metadata", 0)
            .unwrap_or(0),
        engine_leader
            .latest_offset("__cluster_metadata", 0)
            .unwrap_or(0)
    );

    // Idempotent: a second pass must not re-send anything already applied.
    let before = engine_follower
        .latest_offset("__cluster_metadata", 0)
        .unwrap();
    engine_leader.catch_up_follower_metadata_for_test().await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        engine_follower
            .latest_offset("__cluster_metadata", 0)
            .unwrap(),
        before,
        "re-running catch-up must not duplicate records"
    );

    task_f.abort();
    let _ = std::fs::remove_dir_all(&base_dir);
}

/// A produce to an unknown topic on the controller must create it *through the metadata
/// path*, with replicas assigned before the partition holds any data.
///
/// Implicit creation used to call `get_or_create_partition` and nothing else: the local
/// directories appeared but no metadata record was written, so cluster metadata never
/// learned the partition existed. No follower knew to replicate it, the ISR sweep had no
/// membership to manage, and failover had nothing to promote. Assigning before byte one is
/// also what makes the full-ISR start correct — every replica is at LEO = HW = 0, so they
/// are all genuinely in sync.
#[tokio::test]
async fn test_scenario_48_controller_assigns_new_topics_before_first_write() {
    let env = start_test_server().await;
    let engine = env.engine.clone();
    assert!(engine.is_leader(), "the test broker is its own controller");

    assert!(!engine.topic_is_registered("born_assigned"));

    let mut client = TestClient::connect(env.addr).await.unwrap();
    client
        .produce_single("born_assigned", "k", None, 1, "first record")
        .await
        .unwrap();

    // Registered in cluster metadata, not merely present as directories on disk.
    assert!(
        engine.topic_is_registered("born_assigned"),
        "a produce to an unknown topic must register it through the metadata path"
    );
    assert!(
        engine.has_partition_assignment("born_assigned", 0),
        "the partition must carry a replica assignment, not just exist locally"
    );

    // The assignment names a real leader and a non-empty roster.
    let pm = engine.get_or_create_partition("born_assigned", 0).unwrap();
    assert_eq!(pm.leader_id(), engine.config().node_id);
    assert!(!pm.replicas().is_empty(), "roster must not be empty");
    assert_eq!(
        pm.isr(),
        pm.replicas(),
        "a brand-new partition starts with every replica in the ISR: all are at LEO = HW = 0 \
         and therefore genuinely in sync"
    );

    // And the record itself landed.
    let fetched = client.fetch("born_assigned", 0, 0, 65536).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(&fetched[0].payload[..], b"first record");

    // Idempotent: producing again must not re-create or re-assign the topic.
    let epoch_before = pm.leader_epoch();
    client
        .produce_single("born_assigned", "k", None, 1, "second record")
        .await
        .unwrap();
    assert_eq!(
        pm.leader_epoch(),
        epoch_before,
        "an existing topic must not be re-created on every produce"
    );
}

/// Read isolation must be selectable on the ordinary `Fetch` path, not only by calling a
/// different command.
///
/// `Fetch` had no isolation level at all: committed-only reads required `FetchCommitted`,
/// so a client had to decide which command to use up front and could not express isolation
/// as the per-request property it actually is. It is now a tagged field on the request
/// envelope, which is exactly the kind of optional per-request field the envelope exists to
/// carry — an older broker skips the tag rather than misparsing the request.
#[tokio::test]
async fn test_scenario_51_fetch_honours_requested_isolation_level() {
    use hermes::protocol::wire::IsolationLevel;

    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();
    let topic = "isolation_flag_topic";

    client
        .produce_single(topic, "", None, 1, "committed_record")
        .await
        .unwrap();

    client.begin_transaction("tx_iso", 77).await.unwrap();
    client
        .produce_single(topic, "", Some("tx_iso"), 1, "aborted_record")
        .await
        .unwrap();
    client.abort_transaction("tx_iso").await.unwrap();

    // Read-uncommitted is the historical behavior and stays the default: an unset tag must
    // behave exactly as a legacy `Fetch` always did.
    let legacy = client.fetch(topic, 0, 0, 65536).await.unwrap();
    let explicit_uncommitted = client
        .fetch_with_isolation(topic, 0, 0, 65536, IsolationLevel::ReadUncommitted)
        .await
        .unwrap();
    assert_eq!(
        legacy.len(),
        explicit_uncommitted.len(),
        "an explicit read-uncommitted must match legacy Fetch exactly"
    );
    assert!(
        legacy.len() >= 2,
        "read-uncommitted should expose the aborted record, got {} frames",
        legacy.len()
    );

    // Read-committed over the same command must hide what the abort invalidated.
    let committed = client
        .fetch_with_isolation(topic, 0, 0, 65536, IsolationLevel::ReadCommitted)
        .await
        .unwrap();
    let payloads: Vec<String> = committed
        .iter()
        .map(|f| String::from_utf8_lossy(&f.payload).to_string())
        .collect();
    assert!(
        !payloads.iter().any(|p| p.contains("aborted_record")),
        "read-committed must not return an aborted record, got {:?}",
        payloads
    );
    for frame in &committed {
        assert_ne!(
            frame.magic, 0xAD,
            "read-committed must not return control markers"
        );
    }
    assert!(
        committed.len() < legacy.len(),
        "read-committed ({}) must be strictly narrower than read-uncommitted ({})",
        committed.len(),
        legacy.len()
    );

    // And it agrees with the dedicated command it replaces.
    let via_command = client.fetch_committed(topic, 0, 0, 65536).await.unwrap();
    assert_eq!(
        committed.len(),
        via_command.len(),
        "the isolation flag must agree with the FetchCommitted command"
    );
}

/// Metadata must not take effect until a majority of the controller quorum has it.
///
/// A record used to be applied the instant it was appended locally, before any peer had
/// seen it. A record that only ever reached a minority was still acted upon, so if
/// leadership then moved to a node that never received it, the cluster ended up with two
/// divergent views — different topic configs, partition assignments and ACLs — with nothing
/// to detect or reconcile the split. Because metadata drives authorization and placement,
/// that divergence changed who was allowed to write and where data landed.
#[tokio::test]
async fn test_scenario_49_metadata_requires_majority_before_taking_effect() {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let base_dir =
        std::env::temp_dir().join(format!("meta_quorum_{}_{}", std::process::id(), count));
    let _ = std::fs::remove_dir_all(&base_dir);

    // A controller whose quorum peer does not exist: nothing can ever acknowledge, so no
    // metadata record can reach a majority.
    let engine = StorageEngine::new(EngineConfig {
        node_id: 1,
        role: hermes::NodeRole::Leader,
        data_dir: base_dir.join("lonely"),
        bind_addr: "127.0.0.1:0".to_string(),
        // Port 1 is not listening; the peer is unreachable by construction.
        peer_addrs: vec!["127.0.0.1:1".to_string()],
        ..EngineConfig::default()
    })
    .unwrap();
    assert!(
        engine.is_leader(),
        "it is still the controller, just without a quorum"
    );

    let started = std::time::Instant::now();
    let result = engine.create_topic("needs_quorum", 1).await;
    let elapsed = started.elapsed();

    assert!(
        result.is_err(),
        "a metadata write that cannot reach a majority must fail, not silently apply"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("NOT_ENOUGH_CONTROLLERS"),
        "expected a quorum error naming the cause, got: {}",
        err
    );

    // And critically: it must not have taken effect locally.
    assert!(
        !engine.topic_is_registered("needs_quorum"),
        "an uncommitted metadata record must not be applied on the leader"
    );

    // It failed by timing out on the quorum, not by refusing instantly for some other
    // reason — the wait is what gives a reachable peer the chance to acknowledge.
    assert!(
        elapsed >= std::time::Duration::from_secs(1),
        "expected the commit gate to wait for acknowledgement, returned in {:?}",
        elapsed
    );

    let _ = std::fs::remove_dir_all(&base_dir);
}

/// A sole controller is its own majority, so it must not be blocked by the commit gate —
/// otherwise every single-node deployment would be unable to write metadata at all.
#[tokio::test]
async fn test_scenario_50_sole_controller_commits_immediately() {
    let env = start_test_server().await;
    let engine = env.engine.clone();

    let started = std::time::Instant::now();
    engine
        .create_topic("solo_quorum", 1)
        .await
        .expect("a sole controller is a majority of one");
    assert!(
        engine.topic_is_registered("solo_quorum"),
        "the record must take effect immediately"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "a sole controller must not wait on anyone, took {:?}",
        started.elapsed()
    );
}

/// A client must be able to discover what a broker speaks before sending a real request,
/// rather than probing and interpreting the resulting error — which is indistinguishable
/// from the command failing for an ordinary reason.
#[tokio::test]
async fn test_scenario_53_clients_can_negotiate_protocol_support() {
    use hermes::protocol::wire::{
        CommandCode, IsolationLevel, PROTOCOL_VERSION_MAX, PROTOCOL_VERSION_MIN,
    };

    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.unwrap();

    let (min, max, codes) = client.negotiate_protocol().await.unwrap();
    assert_eq!(min, PROTOCOL_VERSION_MIN);
    assert_eq!(max, PROTOCOL_VERSION_MAX);
    assert!(min <= max, "advertised range must be coherent");

    // The advertised set must actually describe this broker: commands it implements are
    // listed, and a code it does not implement is not.
    assert!(codes.contains(&(CommandCode::ProduceBatch as u8)));
    assert!(codes.contains(&(CommandCode::Fetch as u8)));
    assert!(codes.contains(&(CommandCode::NegotiateProtocol as u8)));
    assert!(
        !codes.contains(&0xF0),
        "an unimplemented code must not be advertised"
    );

    // Negotiation must not require authentication or an envelope — a client that cannot
    // yet form a versioned request is exactly who needs to ask.
    assert!(client.ping().await.unwrap(), "connection still usable");

    // Having negotiated, the client can use a version-gated feature with confidence.
    client
        .produce_single("negotiated_topic", "", None, 1, "record")
        .await
        .unwrap();
    let fetched = client
        .fetch_with_isolation(
            "negotiated_topic",
            0,
            0,
            65536,
            IsolationLevel::ReadCommitted,
        )
        .await
        .unwrap();
    assert!(
        fetched.len() <= 1,
        "a negotiated versioned request must be served, got {} frames",
        fetched.len()
    );
}

/// A forwarded request must be served where it lands, never relayed onward.
///
/// Without this, forwarding ping-pongs: the controller creates a topic and forwards to the
/// newly assigned leader, which has not yet received that assignment through the metadata
/// log, sees an unknown topic, concludes it is not the leader, and forwards straight back.
/// That deadlock is what previously prevented routing new-topic produces to the controller
/// at all.
#[tokio::test]
async fn test_scenario_52_forwarded_requests_are_served_not_relayed_onward() {
    use hermes::protocol::wire::{
        strip_envelope, wrap_forwarded_request, RequestFraming, WireRequest,
    };

    // A plain client request carries no marker, so a broker is free to route it.
    let mut original = Vec::new();
    original.push(hermes::protocol::wire::CommandCode::Ping as u8);
    original.extend_from_slice(&0u32.to_be_bytes());
    let (_req, framing, _) = WireRequest::decode_framed(&original).unwrap();
    assert!(
        !framing.is_forwarded(),
        "a client request must not look forwarded"
    );

    // Relaying it marks it, and the inner request survives byte-for-byte.
    let relayed = wrap_forwarded_request(&original).unwrap();
    let (req, relayed_framing, used) = WireRequest::decode_framed(&relayed).unwrap();
    assert!(
        relayed_framing.is_forwarded(),
        "a relayed request must be marked so the receiver serves it"
    );
    assert_eq!(req.cmd, hermes::protocol::wire::CommandCode::Ping);
    assert_eq!(used, relayed.len());
    let (inner, _) = strip_envelope(&relayed).unwrap();
    assert_eq!(inner, &original[..], "the inner request must be preserved");

    // Relaying an already-enveloped request must REPLACE the envelope, not nest it —
    // a nested envelope would leave the receiver reading 0xF1 where a command code belongs.
    let twice = wrap_forwarded_request(&relayed).unwrap();
    let (req2, framing2, used2) = WireRequest::decode_framed(&twice).unwrap();
    assert_eq!(req2.cmd, hermes::protocol::wire::CommandCode::Ping);
    assert!(framing2.is_forwarded());
    assert_eq!(
        used2,
        twice.len(),
        "a re-relayed request must still parse whole"
    );

    // Recognised tags survive the relay, so a forwarded fetch keeps the isolation the
    // client asked for rather than silently reverting to the default.
    let mut enveloped = Vec::new();
    enveloped.push(hermes::protocol::wire::VERSIONED_ENVELOPE_MAGIC);
    enveloped.extend_from_slice(&hermes::protocol::wire::PROTOCOL_VERSION_MAX.to_be_bytes());
    enveloped.extend_from_slice(&99u32.to_be_bytes());
    enveloped.push(1);
    enveloped.push(hermes::protocol::wire::tags::ISOLATION_LEVEL);
    enveloped.extend_from_slice(&1u16.to_be_bytes());
    enveloped.push(hermes::protocol::wire::IsolationLevel::ReadCommitted.to_byte());
    enveloped.extend_from_slice(&original);

    let relayed_iso = wrap_forwarded_request(&enveloped).unwrap();
    let (_r, iso_framing, _) = WireRequest::decode_framed(&relayed_iso).unwrap();
    assert!(iso_framing.is_forwarded());
    assert_eq!(
        iso_framing.isolation_level(),
        hermes::protocol::wire::IsolationLevel::ReadCommitted,
        "relaying must carry the client's isolation level across the hop"
    );

    // A response from the relay hop is framed; re-framing it for a legacy client must strip
    // that envelope, or the client reads the magic byte as the response status.
    let leader_reply = hermes::protocol::wire::WireResponse::ok(vec![7, 7]).encode_framed(
        &RequestFraming::Versioned {
            api_version: hermes::protocol::wire::PROTOCOL_VERSION_MAX,
            correlation_id: 5,
            tags: Default::default(),
        },
    );
    let for_legacy_client =
        hermes::protocol::wire::relay_response(&leader_reply, &RequestFraming::Legacy);
    assert_eq!(
        for_legacy_client,
        hermes::protocol::wire::WireResponse::ok(vec![7, 7]).encode(),
        "a legacy client must receive an unwrapped response"
    );
}

/// A static member's restart must cost the group nothing.
///
/// A consumer that declares a `group.instance.id` names a *slot* in the group rather than
/// a process. Restarting the process holding that slot is not a member leaving and a
/// member arriving, so the group must not form a new generation, must not move the
/// member's partitions, and must not stop the other members while it happens — otherwise a
/// rolling restart of an N-member deployment costs 2N rebalances, each one halting
/// consumption group-wide.
#[tokio::test]
async fn test_scenario_54_static_member_restart_does_not_rebalance() {
    let env = start_test_server().await;
    let engine = env.engine.clone();
    let group_id = "static_group";

    // Two static members start up together and land in one generation.
    let mut first_join = Vec::new();
    for instance in ["worker-0", "worker-1"] {
        let engine = engine.clone();
        first_join.push(tokio::spawn(async move {
            engine
                .join_group_awaited(group_id, "", Some(instance), vec!["range".to_string()])
                .await
        }));
    }
    let mut members = Vec::new();
    for join in first_join {
        members.push(join.await.unwrap().unwrap());
    }
    let generation = members[0].1;
    assert!(
        members.iter().all(|m| m.1 == generation),
        "both members must share one generation, got {:?}",
        members.iter().map(|m| m.1).collect::<Vec<_>>()
    );

    // The leader assigns a partition to each member.
    let (leader_id, _, _, _) = members
        .iter()
        .find(|m| m.2)
        .expect("one member must lead")
        .clone();
    let assignments: Vec<hermes::protocol::wire::MemberAssignment> = members
        .iter()
        .enumerate()
        .map(
            |(i, (member_id, _, _, _))| hermes::protocol::wire::MemberAssignment {
                member_id: member_id.clone(),
                topic: "static_topic".to_string(),
                partitions: vec![i as u32],
            },
        )
        .collect();
    engine
        .group_coordinator()
        .sync_group(group_id, generation, &leader_id, assignments)
        .expect("leader must be able to submit the assignment");

    // Find the member holding worker-0's slot and what it owns, then restart that process:
    // it comes back knowing its instance id but NOT the member id it previously held.
    let (worker0_member_id, ..) = members
        .iter()
        .find(|(member_id, ..)| member_id.starts_with("worker-0"))
        .expect("worker-0 must hold a slot")
        .clone();
    let owned_before = engine
        .group_coordinator()
        .sync_group(group_id, generation, &worker0_member_id, Vec::new())
        .expect("worker-0 must be able to read its assignment");

    let (restarted_member_id, restarted_generation, _, _) = engine
        .join_group_awaited(group_id, "", Some("worker-0"), vec!["range".to_string()])
        .await
        .expect("a restarting static member must be able to rejoin");

    assert_eq!(
        restarted_generation, generation,
        "a static member's restart must not form a new generation"
    );
    assert_ne!(
        restarted_member_id, worker0_member_id,
        "the returning process must be issued a fresh member id, so a predecessor that is \
         somehow still running cannot keep acting as this member"
    );

    let owned_after = engine
        .group_coordinator()
        .sync_group(group_id, generation, &restarted_member_id, Vec::new())
        .expect("the restarted member must be able to read its assignment");
    assert_eq!(
        owned_after, owned_before,
        "a static member must get its own partitions back, not a redistributed set"
    );

    // The predecessor's id is gone, so a process that outlived its replacement cannot
    // keep heartbeating — and therefore cannot keep consuming — under the same identity.
    assert!(
        engine
            .group_coordinator()
            .heartbeat(group_id, generation, &worker0_member_id)
            .is_err(),
        "the fenced member id must no longer be accepted"
    );

    // And the group still has exactly two members: the restart replaced a slot rather
    // than adding one.
    let described = engine
        .group_coordinator()
        .describe_group(group_id)
        .expect("group should exist");
    assert_eq!(
        described.members.len(),
        2,
        "a restart must not leave the group holding a stale extra member"
    );
}

/// The contrast that gives the scenario above its meaning: without an instance id, a
/// restart *is* a new arrival, and the group rebalances around it.
#[tokio::test]
async fn test_scenario_55_dynamic_member_restart_does_rebalance() {
    let env = start_test_server().await;
    let engine = env.engine.clone();
    let group_id = "dynamic_group";

    let (_, first_generation, _, _) = engine
        .join_group_awaited(group_id, "member-a", None, vec!["range".to_string()])
        .await
        .unwrap();

    // Same consumer, restarted: with no stable identity to present, it is indistinguishable
    // from a member that has never been seen.
    let (_, second_generation, _, _) = engine
        .join_group_awaited(
            group_id,
            "member-a-restarted",
            None,
            vec!["range".to_string()],
        )
        .await
        .unwrap();

    assert!(
        second_generation > first_generation,
        "a dynamic member's arrival must rebalance the group ({} -> {})",
        first_generation,
        second_generation
    );
}

/// A static member that is being decommissioned rather than bounced must be able to give
/// its slot up, naming itself by instance id — it has no other way to say which member it
/// is, since that id is reissued on every join.
#[tokio::test]
async fn test_scenario_56_static_member_can_retire_its_instance() {
    let env = start_test_server().await;
    let engine = env.engine.clone();
    let group_id = "retire_group";

    engine
        .join_group_awaited(group_id, "", Some("worker-0"), vec!["range".to_string()])
        .await
        .unwrap();
    engine
        .join_group_awaited(group_id, "", Some("worker-1"), vec!["range".to_string()])
        .await
        .unwrap();

    engine
        .group_coordinator()
        .leave_group(group_id, "", Some("worker-0"))
        .expect("an instance id must be enough to leave the group");

    let described = engine
        .group_coordinator()
        .describe_group(group_id)
        .expect("group should exist");
    assert_eq!(
        described.members.len(),
        1,
        "the retired instance must be gone, not waiting out a session timeout"
    );

    // The slot is released, so the instance coming back is a genuinely new member and the
    // group rebalances for it — the reservation did not outlive the departure.
    let generation_before_return = engine
        .group_coordinator()
        .join_result(group_id, "")
        .expect("group should exist")
        .0;
    let (_, generation_after_return, _, _) = engine
        .join_group_awaited(group_id, "", Some("worker-0"), vec!["range".to_string()])
        .await
        .unwrap();

    assert!(
        generation_after_return > generation_before_return,
        "a retired instance returning must be treated as a new member ({} -> {})",
        generation_before_return,
        generation_after_return
    );
    assert_eq!(
        engine
            .group_coordinator()
            .describe_group(group_id)
            .expect("group should exist")
            .members
            .len(),
        2
    );
}

/// The same guarantee, driven the way a real consumer drives it: over the wire, through a
/// reconnect. Proves the instance id survives encoding and reaches the coordinator, which
/// the coordinator-level scenarios above cannot show.
#[tokio::test]
async fn test_scenario_57_static_membership_survives_a_reconnect() {
    let env = start_test_server().await;
    let group_id = "wire_static_group";
    let instance_id = "worker-7";

    let mut client = TestClient::connect(env.addr).await.unwrap();
    let first = client
        .join_group_static(group_id, "", Some(instance_id), &["range"])
        .await
        .expect("static join must be accepted");

    client
        .sync_group(
            group_id,
            first.generation_id,
            &first.member_id,
            &[hermes::protocol::wire::MemberAssignment {
                member_id: first.member_id.clone(),
                topic: "wire_static_topic".to_string(),
                partitions: vec![0, 1],
            }],
        )
        .await
        .expect("leader must be able to submit the assignment");

    // The process goes away entirely — connection and all — and comes back with nothing
    // but its instance id.
    drop(client);
    let mut restarted = TestClient::connect(env.addr).await.unwrap();
    let second = restarted
        .join_group_static(group_id, "", Some(instance_id), &["range"])
        .await
        .expect("the returning process must be recognised as the member already there");

    assert_eq!(
        second.generation_id, first.generation_id,
        "reconnecting under a known instance id must not rebalance the group"
    );

    let assignment = restarted
        .sync_group(group_id, second.generation_id, &second.member_id, &[])
        .await
        .expect("the returned member must be able to read its assignment");
    assert_eq!(
        assignment,
        vec![("wire_static_topic".to_string(), vec![0, 1])],
        "the reconnected member must resume the partitions it already owned"
    );

    // Heartbeating under the new id works; the id it held before the restart does not.
    restarted
        .heartbeat(group_id, second.generation_id, &second.member_id)
        .await
        .expect("the current member id must be accepted");
    assert!(
        restarted
            .heartbeat(group_id, second.generation_id, &first.member_id)
            .await
            .is_err(),
        "the pre-restart member id must be fenced"
    );
}

/// Finds a produce key that `hash_key` routes to `target` out of `num_partitions`
/// partitions, so a test can deterministically produce records to every partition of a
/// topic without depending on how many keys it takes to land on each one.
fn key_for_partition(target: u32, num_partitions: u32) -> String {
    for i in 0u32..100_000 {
        let candidate = format!("k{}", i);
        if hash_key(candidate.as_bytes(), num_partitions as usize) == target {
            return candidate;
        }
    }
    panic!(
        "could not find a key hashing to partition {} of {}",
        target, num_partitions
    );
}

/// A `GroupConsumer`'s assignment — not a `--partition` flag — must be what decides which
/// partitions get consumed. This is the fix for issue #51: a consumer that never joined its
/// group used to just fetch whatever partition it was told to, silently ignoring everything
/// else in the topic. This proves the sole member of a 4-partition topic's group ends up
/// owning every partition, and that polling it actually surfaces every record produced
/// across all four.
#[tokio::test]
async fn test_scenario_58_consumer_group_assignment_drives_what_is_consumed() {
    let env = start_test_server().await;
    let topic = "assignment_drives_topic";
    let group_id = "assignment_drives_group";
    let num_partitions = 4u32;

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client
        .create_topic(topic, num_partitions)
        .await
        .unwrap();

    let mut expected_payloads: std::collections::HashSet<String> = std::collections::HashSet::new();
    for partition in 0..num_partitions {
        let key = key_for_partition(partition, num_partitions);
        for i in 0..3 {
            let payload = format!("p{}-r{}", partition, i);
            setup_client
                .produce_single(topic, &key, None, num_partitions, payload.as_bytes())
                .await
                .unwrap();
            expected_payloads.insert(payload);
        }
    }

    let consumer_client = TestClient::connect(env.addr).await.unwrap();
    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        ..GroupConsumerConfig::default()
    };
    let mut consumer = GroupConsumer::join(consumer_client, config).await.unwrap();

    assert_eq!(
        consumer.assignment().to_vec(),
        vec![0u32, 1, 2, 3],
        "the sole member of the group must own every partition of the topic"
    );

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < expected_payloads.len() && std::time::Instant::now() < deadline {
        let records = consumer.poll().await.unwrap();
        for (_, frame) in records {
            seen.insert(String::from_utf8_lossy(&frame.payload).to_string());
        }
        if seen.len() < expected_payloads.len() {
            sleep(Duration::from_millis(20)).await;
        }
    }

    assert_eq!(
        seen, expected_payloads,
        "every produced payload across all 4 partitions must eventually be consumed; saw {:?}",
        seen
    );

    consumer.commit().await.unwrap();
}

/// Two members of the same group on the same topic must not both read the same partition:
/// each ends up with a disjoint, non-empty slice of the assignment that together covers
/// every partition, and neither ever receives a record from a partition it does not own.
#[tokio::test]
async fn test_scenario_59_two_consumers_split_the_partitions_disjointly() {
    let env = start_test_server().await;
    let topic = "split_partitions_topic";
    let group_id = "split_partitions_group";
    let num_partitions = 4u32;

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client
        .create_topic(topic, num_partitions)
        .await
        .unwrap();

    let mut payloads_by_partition: std::collections::HashMap<u32, Vec<String>> =
        std::collections::HashMap::new();
    for partition in 0..num_partitions {
        let key = key_for_partition(partition, num_partitions);
        let mut payloads = Vec::new();
        for i in 0..3 {
            let payload = format!("p{}-r{}", partition, i);
            setup_client
                .produce_single(topic, &key, None, num_partitions, payload.as_bytes())
                .await
                .unwrap();
            payloads.push(payload);
        }
        payloads_by_partition.insert(partition, payloads);
    }

    let client1 = TestClient::connect(env.addr).await.unwrap();
    let client2 = TestClient::connect(env.addr).await.unwrap();
    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        ..GroupConsumerConfig::default()
    };

    // Both members join at once so they land in the same rebalance window and generation
    // (see `start_test_server`'s short `group_initial_rebalance_delay_ms`), producing a
    // genuine two-way split rather than one member owning everything followed by a later
    // rebalance — that sequencing is scenario 60.
    let (c1, c2) = tokio::join!(
        GroupConsumer::join(client1, config.clone()),
        GroupConsumer::join(client2, config),
    );
    let mut c1 = c1.unwrap();
    let mut c2 = c2.unwrap();

    let mut a1 = c1.assignment().to_vec();
    let mut a2 = c2.assignment().to_vec();
    a1.sort_unstable();
    a2.sort_unstable();

    assert!(
        !a1.is_empty(),
        "consumer 1 must own at least one partition, got {:?}",
        a1
    );
    assert!(
        !a2.is_empty(),
        "consumer 2 must own at least one partition, got {:?}",
        a2
    );
    let a1_set: std::collections::HashSet<u32> = a1.iter().copied().collect();
    let a2_set: std::collections::HashSet<u32> = a2.iter().copied().collect();
    assert!(
        a1_set.is_disjoint(&a2_set),
        "the two consumers' assignments must not overlap: {:?} vs {:?}",
        a1,
        a2
    );
    let mut combined: Vec<u32> = a1.iter().chain(a2.iter()).copied().collect();
    combined.sort_unstable();
    assert_eq!(
        combined,
        vec![0, 1, 2, 3],
        "together the two consumers must cover every partition"
    );

    let expected1: std::collections::HashSet<String> = a1
        .iter()
        .flat_map(|p| payloads_by_partition[p].clone())
        .collect();
    let expected2: std::collections::HashSet<String> = a2
        .iter()
        .flat_map(|p| payloads_by_partition[p].clone())
        .collect();
    let mut seen1: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen2: std::collections::HashSet<String> = std::collections::HashSet::new();

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while (seen1.len() < expected1.len() || seen2.len() < expected2.len())
        && std::time::Instant::now() < deadline
    {
        for (partition, frame) in c1.poll().await.unwrap() {
            assert!(
                a1.contains(&partition),
                "consumer 1 received a record from partition {} which it does not own ({:?})",
                partition,
                a1
            );
            seen1.insert(String::from_utf8_lossy(&frame.payload).to_string());
        }
        for (partition, frame) in c2.poll().await.unwrap() {
            assert!(
                a2.contains(&partition),
                "consumer 2 received a record from partition {} which it does not own ({:?})",
                partition,
                a2
            );
            seen2.insert(String::from_utf8_lossy(&frame.payload).to_string());
        }
        if seen1.len() < expected1.len() || seen2.len() < expected2.len() {
            sleep(Duration::from_millis(20)).await;
        }
    }

    assert_eq!(
        seen1, expected1,
        "consumer 1 must see every payload from its owned partitions; saw {:?}",
        seen1
    );
    assert_eq!(
        seen2, expected2,
        "consumer 2 must see every payload from its owned partitions; saw {:?}",
        seen2
    );
}

/// A member that stops noticing the group has moved on must catch up on its own — and must
/// hand back what it already processed on the partitions it loses, not just abandon it. This
/// joins one consumer alone (it owns everything), has it consume every produced record
/// *without* explicitly committing (so the offsets sit in `pending_commits`), lets its
/// generation settle, then joins a second — which bumps the generation the first member is
/// still heartbeating under. The first member's `poll()` must notice its heartbeat failing,
/// rejoin by itself, end up with a shrunk assignment that is disjoint from (and together with
/// the second member's, covers) the whole topic, and — the point of this scenario — must have
/// committed the partitions it gave up on the way out, so whoever inherits them does not
/// re-read what the first member already consumed.
#[tokio::test]
async fn test_scenario_60_a_stale_generation_makes_a_consumer_rejoin() {
    let env = start_test_server().await;
    let topic = "stale_generation_topic";
    let group_id = "stale_generation_group";
    let num_partitions = 4u32;
    let records_per_partition = 3u64;

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client
        .create_topic(topic, num_partitions)
        .await
        .unwrap();

    // Produce a few records to every partition before anyone joins, so c1's initial
    // assignment has something real to consume (and later, to commit on revoke).
    let mut expected_payloads: std::collections::HashSet<String> = std::collections::HashSet::new();
    for partition in 0..num_partitions {
        let key = key_for_partition(partition, num_partitions);
        for i in 0..records_per_partition {
            let payload = format!("p{}-r{}", partition, i);
            setup_client
                .produce_single(topic, &key, None, num_partitions, payload.as_bytes())
                .await
                .unwrap();
            expected_payloads.insert(payload);
        }
    }

    let client1 = TestClient::connect(env.addr).await.unwrap();
    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        ..GroupConsumerConfig::default()
    };
    let mut c1 = GroupConsumer::join(client1, config.clone()).await.unwrap();
    assert_eq!(
        c1.assignment().to_vec(),
        vec![0u32, 1, 2, 3],
        "the sole member must start out owning every partition"
    );

    // Consume everything without ever calling `commit()` — the point is to leave the
    // offsets sitting in `pending_commits` so the later revoke has something to flush.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < expected_payloads.len() && std::time::Instant::now() < deadline {
        let records = c1.poll().await.unwrap();
        for (_, frame) in records {
            seen.insert(String::from_utf8_lossy(&frame.payload).to_string());
        }
        if seen.len() < expected_payloads.len() {
            sleep(Duration::from_millis(20)).await;
        }
    }
    assert_eq!(
        seen, expected_payloads,
        "consumer 1 must see every produced payload before anything is revoked; saw {:?}",
        seen
    );

    // Nothing has been committed yet — every partition's committed offset is still
    // untouched (u64::MAX == nothing committed).
    for partition in 0..num_partitions {
        let committed = setup_client
            .fetch_offset(group_id, topic, partition)
            .await
            .unwrap();
        assert_eq!(
            committed,
            u64::MAX,
            "partition {} must have nothing committed yet, since commit() was never called",
            partition
        );
    }

    // Let the first member's generation fully settle before the second arrives, so the two
    // do not land in the same rebalance window (that would just be scenario 59 again).
    sleep(Duration::from_millis(150)).await;

    // The second member's join opens a new rebalance window and bumps the generation
    // immediately. Its own `SyncGroup` cannot complete until the (still-)leader — c1 — has
    // submitted the new assignment, which only happens once c1 calls `poll()` again and
    // notices its heartbeat failing. So the two must run concurrently.
    let client2 = TestClient::connect(env.addr).await.unwrap();
    let join_config = config.clone();
    let c2_handle = tokio::spawn(async move { GroupConsumer::join(client2, join_config).await });

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while c1.assignment().len() == num_partitions as usize && std::time::Instant::now() < deadline {
        let _ = c1.poll().await.unwrap();
        if c1.assignment().len() == num_partitions as usize {
            sleep(Duration::from_millis(20)).await;
        }
    }

    let c2 = c2_handle
        .await
        .unwrap()
        .expect("the second member must be able to join and sync");

    let mut a1 = c1.assignment().to_vec();
    let mut a2 = c2.assignment().to_vec();
    a1.sort_unstable();
    a2.sort_unstable();

    assert!(
        a1.len() < num_partitions as usize,
        "consumer 1's assignment must shrink once it notices the stale generation and \
         rejoins, got {:?}",
        a1
    );
    let a1_set: std::collections::HashSet<u32> = a1.iter().copied().collect();
    let a2_set: std::collections::HashSet<u32> = a2.iter().copied().collect();
    assert!(
        a1_set.is_disjoint(&a2_set),
        "after the rejoin the two consumers' assignments must not overlap: {:?} vs {:?}",
        a1,
        a2
    );
    let mut combined: Vec<u32> = a1.iter().chain(a2.iter()).copied().collect();
    combined.sort_unstable();
    assert_eq!(
        combined,
        vec![0, 1, 2, 3],
        "together the two consumers must still cover every partition after the rejoin"
    );

    // The point of this scenario: every partition consumer 1 gave up must have been
    // committed at the last offset it actually consumed on the way out, not abandoned at
    // whatever it had committed before (nothing) or silently dropped.
    let revoked: Vec<u32> = (0..num_partitions).filter(|p| !a1.contains(p)).collect();
    assert!(
        !revoked.is_empty(),
        "consumer 1 must have given up at least one partition, got {:?}",
        a1
    );
    for partition in revoked {
        let committed = setup_client
            .fetch_offset(group_id, topic, partition)
            .await
            .unwrap();
        assert_eq!(
            committed,
            records_per_partition - 1,
            "partition {} was revoked from consumer 1 but its committed offset ({}) does not \
             match the last offset consumer 1 actually consumed ({}) — it was abandoned \
             instead of committed on the way out",
            partition,
            committed,
            records_per_partition - 1
        );
    }
}

/// A fresh consumer joining the same group on the same topic must resume from committed
/// offsets, not from the beginning — otherwise every restart of a consumer group re-reads
/// its whole backlog.
#[tokio::test]
async fn test_scenario_61_a_consumer_resumes_from_its_committed_offsets() {
    let env = start_test_server().await;
    let topic = "resume_committed_topic";
    let group_id = "resume_committed_group";
    let num_partitions = 4u32;

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client
        .create_topic(topic, num_partitions)
        .await
        .unwrap();

    let mut all_payloads: std::collections::HashSet<String> = std::collections::HashSet::new();
    for partition in 0..num_partitions {
        let key = key_for_partition(partition, num_partitions);
        for i in 0..3 {
            let payload = format!("p{}-r{}", partition, i);
            setup_client
                .produce_single(topic, &key, None, num_partitions, payload.as_bytes())
                .await
                .unwrap();
            all_payloads.insert(payload);
        }
    }

    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        ..GroupConsumerConfig::default()
    };

    let client1 = TestClient::connect(env.addr).await.unwrap();
    let mut c1 = GroupConsumer::join(client1, config.clone()).await.unwrap();
    assert_eq!(c1.assignment().to_vec(), vec![0u32, 1, 2, 3]);

    // Consume and commit every record.
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < all_payloads.len() && std::time::Instant::now() < deadline {
        let records = c1.poll().await.unwrap();
        for (_, frame) in &records {
            seen.insert(String::from_utf8_lossy(&frame.payload).to_string());
        }
        if !records.is_empty() {
            c1.commit().await.unwrap();
        } else {
            sleep(Duration::from_millis(20)).await;
        }
    }
    assert_eq!(
        seen, all_payloads,
        "the first consumer must see every produced payload before committing; saw {:?}",
        seen
    );

    // Leave outright rather than just dropping the connection: the group's session timeout
    // is a fixed 10s, so a bare disconnect would leave this member's slot (and leadership)
    // lingering for that long, and a fresh dynamic member joining in the meantime would
    // only get a share of the partitions rather than the whole topic. Leaving releases the
    // slot immediately, which is what actually exercises "a fresh consumer resumes cleanly"
    // rather than "a fresh consumer waits out a stale predecessor".
    c1.leave().await.unwrap();
    drop(c1);

    // A fresh consumer, same group, same topic — resumes from committed offsets.
    let client2 = TestClient::connect(env.addr).await.unwrap();
    let mut c2 = GroupConsumer::join(client2, config).await.unwrap();
    assert_eq!(
        c2.assignment().to_vec(),
        vec![0u32, 1, 2, 3],
        "the sole member must again own every partition"
    );

    // There is nothing left to wait for — the assertion is that delivery never happens, so
    // unlike the positive checks above this is a bounded number of polls rather than a
    // deadline loop waiting on a condition that must not occur.
    let mut redelivered: Vec<String> = Vec::new();
    for _ in 0..10 {
        let records = c2.poll().await.unwrap();
        for (_, frame) in records {
            redelivered.push(String::from_utf8_lossy(&frame.payload).to_string());
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        redelivered.is_empty(),
        "a fresh consumer must not re-deliver already-committed records, but got {:?}",
        redelivered
    );
}

/// `auto.create.topics.enable = false` must still be honored now that implicit creation
/// is routed through `StorageEngine::ensure_topic_created` — the controller-mediated path
/// added to fix unassigned auto-created partitions must not reopen the door that gate was
/// there to close.
///
/// Covers the flip side of `test_scenario_48_controller_assigns_new_topics_before_first_write`:
/// that test proves an unknown topic is born fully assigned when auto-creation is allowed,
/// this one proves nothing is born — on disk or in cluster metadata — when it isn't.
#[tokio::test]
async fn test_scenario_62_auto_create_disabled_rejects_unknown_topic_without_registering_it() {
    use hermes::config::EngineConfig;

    struct TestDataDirGuard {
        pub path: std::path::PathBuf,
    }

    impl TestDataDirGuard {
        fn new(prefix: &str) -> Self {
            let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("hermes_test_{}_{}", prefix, count));
            let _ = std::fs::create_dir_all(&path);
            Self { path }
        }
    }

    impl Drop for TestDataDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    let dir_guard = TestDataDirGuard::new("auto_create_disabled_test");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let cfg = EngineConfig {
        node_id: 1,
        data_dir: dir_guard.path.clone(),
        bind_addr: addr.to_string(),
        auto_create_topics_enable: false,
        ..Default::default()
    };

    let engine = hermes::server::StorageEngine::new(cfg).unwrap();
    assert!(engine.is_leader(), "the test broker is its own controller");
    let server = hermes::server::Server::new(engine.clone());

    tokio::spawn(async move {
        server.run_with_listener(listener).await.unwrap();
    });

    let mut client = TestClient::connect(addr).await.unwrap();

    let err = client
        .produce_single("never_allowed_topic", "k", None, 1, "v")
        .await
        .expect_err("a produce to an unknown topic must be rejected when auto-create is off");
    assert!(
        err.to_string()
            .contains("auto.create.topics.enable is false"),
        "unexpected error message: {}",
        err
    );

    // Rejected before the controller-mediated path ever proposes a `TopicCreated` record —
    // cluster metadata must not know this topic.
    assert!(
        !engine.topic_is_registered("never_allowed_topic"),
        "a rejected auto-create must not register the topic in cluster metadata"
    );
    assert!(
        !engine
            .list_topics()
            .contains(&"never_allowed_topic".to_string()),
        "a rejected auto-create must not appear in ListTopics"
    );

    // Nor must it leave an orphan partition directory on disk — the exact failure mode
    // this issue exists to prevent, just reached from the other direction (creation
    // refused outright rather than retrofitted later).
    let stray: Vec<_> = std::fs::read_dir(&dir_guard.path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with("never_allowed_topic"))
        .collect();
    assert!(
        stray.is_empty(),
        "a rejected auto-create must not create partition directories, found: {:?}",
        stray
    );
}

/// Issue #53: liveness must no longer be a side effect of the application calling
/// `poll()`. This is the core claim of the fix — a consumer that never polls at all must
/// still be a live group member once the background heartbeat task takes over, for at
/// least as long as its session timeout (`GroupCoordinator::join_group`). Before this
/// change, `poll()` was the only thing that ever sent a heartbeat, so this exact scenario
/// would have gotten the consumer evicted.
///
/// Uses a short, explicitly configured `session_timeout` rather than the 10s default: the
/// coordinator now actually honors what a member asks for (issue #53 follow-up — see
/// `GroupCoordinator::resolve_session_timeout`), which is what lets this scenario prove
/// the same property in just over a second of real time instead of the ~11s it used to
/// take waiting out a timeout the client had no way to shorten.
#[tokio::test]
async fn test_scenario_63_background_heartbeat_keeps_membership_alive_without_polling() {
    let env = start_test_server().await;
    let topic = "background_heartbeat_topic";
    let group_id = "background_heartbeat_group";

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client.create_topic(topic, 2).await.unwrap();

    let consumer_client = TestClient::connect(env.addr).await.unwrap();
    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        // Comfortably inside the coordinator's [200ms, 300s] clamp range, and a 5x
        // margin over `heartbeat_interval` (well past `validate`'s minimum 3x) so
        // ordinary scheduling jitter under CI load can't false-fail this.
        session_timeout: Duration::from_secs(1),
        heartbeat_interval: Duration::from_millis(200),
        ..GroupConsumerConfig::default()
    };
    let consumer = GroupConsumer::join(consumer_client, config).await.unwrap();
    let member_id = consumer.member_id().to_string();
    assert_eq!(
        consumer.assignment().to_vec(),
        vec![0u32, 1],
        "the sole member must start out owning every partition"
    );

    // `consumer.poll()` is never called again anywhere in this test — that omission is the
    // entire point. Membership is checked out-of-band, over a separate connection, via
    // DescribeGroup, which prunes expired members before answering
    // (`GroupCoordinator::describe_group`) — so a member it reports present really is
    // still present, not just not-yet-swept.
    let mut observer = TestClient::connect(env.addr).await.unwrap();

    // Wait comfortably past the 1s session timeout, checking throughout — not just at the
    // end — that the member never drops out along the way.
    let deadline = std::time::Instant::now() + Duration::from_millis(1_600);
    let mut checks = 0u32;
    while std::time::Instant::now() < deadline {
        let (_, members) = observer.describe_group(group_id).await.unwrap();
        assert!(
            members.iter().any(|m| m.member_id == member_id),
            "the consumer must still be a group member after {} check(s), having never \
             called poll()",
            checks
        );
        checks += 1;
        sleep(Duration::from_millis(80)).await;
    }
    assert!(
        checks >= 15,
        "the wait must actually span the session timeout with many checks along the way, \
         only ran {}",
        checks
    );

    drop(consumer);
}

/// Issue #53, part two: a heartbeat rejected for a stale generation must surface to the
/// consume loop as a rejoin, never be silently dropped by the background task. This
/// exercises the same underlying event as
/// `test_scenario_60_a_stale_generation_makes_a_consumer_rejoin`, but from the new angle
/// that change introduces: the rejection is now observed by the background heartbeat task
/// on its own connection, with the application never calling `poll()` in between, and only
/// a shared `needs_rejoin` flag carries that observation over to the next `poll()` call.
#[tokio::test]
async fn test_scenario_64_a_background_stale_generation_surfaces_as_rejoin() {
    let env = start_test_server().await;
    let topic = "background_stale_generation_topic";
    let group_id = "background_stale_generation_group";
    let num_partitions = 4u32;

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client
        .create_topic(topic, num_partitions)
        .await
        .unwrap();

    // A fast heartbeat interval so the background task's rejected heartbeat is observed in
    // well under a second, rather than needing to wait out a session timeout the way
    // scenario 63 does.
    //
    // `session_timeout` is deliberately generous (not tight against the interval like
    // scenario 63's) now that the coordinator actually honors it (issue #53 follow-up,
    // formerly a fixed, unaffected 10s): once c2's join bumps the generation, c1's eager
    // (non-cooperative) heartbeats are rejected and, by design, do NOT refresh
    // `last_heartbeat` — see `GroupCoordinator::heartbeat` — so c1 gets no heartbeat
    // credit for the ~800ms this test deliberately waits before calling `c1.poll()` to
    // observe the already-set `needs_rejoin` flag. A short session timeout here would
    // have the coordinator evict c1 for real before the test ever gets to that
    // assertion, which is a different scenario (session-timeout eviction) than the one
    // this test is actually about (a stale-generation rejoin). What this test exercises
    // is the generation-mismatch rejoin path, so the timeout just needs to comfortably
    // outlast that wait.
    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        session_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_millis(100),
        ..GroupConsumerConfig::default()
    };

    let client1 = TestClient::connect(env.addr).await.unwrap();
    let mut c1 = GroupConsumer::join(client1, config.clone()).await.unwrap();
    assert_eq!(
        c1.assignment().to_vec(),
        vec![0u32, 1, 2, 3],
        "the sole member must start out owning every partition"
    );
    let generation_before = c1.generation_id();

    // Let c1's generation fully settle before c2 arrives, so the two don't land in the same
    // rebalance window (matching scenario 60's setup for the same reason).
    sleep(Duration::from_millis(150)).await;

    // The second member's join bumps the group's generation immediately
    // (`GroupCoordinator::join_group`), out from under c1, which hasn't rejoined. c1 is
    // still the group's leader, so c2's own `SyncGroup` cannot complete until c1 rejoins
    // and submits the new assignment — hence running the two concurrently.
    let client2 = TestClient::connect(env.addr).await.unwrap();
    let join_config = config.clone();
    let c2_handle = tokio::spawn(async move { GroupConsumer::join(client2, join_config).await });

    // Give the background heartbeat task several ticks — at 100ms apart — to hit the
    // rejection and set `needs_rejoin`, entirely without `c1.poll()` being called. This is
    // the crux of the test: detection must not depend on the application driving the poll
    // loop at all.
    sleep(Duration::from_millis(800)).await;

    // A single `poll()` call must observe the already-set flag and rejoin immediately,
    // rather than fetching under the stale generation or silently doing nothing.
    let records = c1.poll().await.unwrap();
    assert!(
        records.is_empty(),
        "the round that performs the rejoin must not also return fetched records"
    );
    assert_ne!(
        c1.generation_id(),
        generation_before,
        "the stale generation the background task observed must have surfaced as a \
         rejoin — c1 must not still be sitting on the old generation"
    );

    let c2 = c2_handle
        .await
        .unwrap()
        .expect("the second member must be able to join and sync once c1 rejoins as leader");

    let mut a1 = c1.assignment().to_vec();
    let mut a2 = c2.assignment().to_vec();
    a1.sort_unstable();
    a2.sort_unstable();
    assert!(
        a1.len() < num_partitions as usize,
        "c1's assignment must shrink once the stale generation surfaces and it rejoins, \
         got {:?}",
        a1
    );
    let a1_set: std::collections::HashSet<u32> = a1.iter().copied().collect();
    let a2_set: std::collections::HashSet<u32> = a2.iter().copied().collect();
    assert!(
        a1_set.is_disjoint(&a2_set),
        "after the rejoin the two consumers' assignments must not overlap: {:?} vs {:?}",
        a1,
        a2
    );
    let mut combined: Vec<u32> = a1.iter().chain(a2.iter()).copied().collect();
    combined.sort_unstable();
    assert_eq!(
        combined,
        vec![0u32, 1, 2, 3],
        "together the two members must still cover every partition exactly once"
    );
}

/// The session-timeout follow-up to issue #53: the coordinator must actually use the
/// session timeout a client requests via `JoinGroup`'s `SESSION_TIMEOUT_MS` tagged field,
/// not the historical hardcoded 10s it used to ignore the client's own value for entirely
/// (`GroupCoordinator::resolve_session_timeout`).
///
/// Proven here by an outright-dead member — background heartbeat task stopped, no
/// `leave()` — getting evicted within its own short configured timeout, nowhere near the
/// old fixed 10s. `test_scenario_63` already proves the flip side (a short timeout doesn't
/// cause a live member to be evicted early); this proves the timeout is real rather than
/// decorative.
#[tokio::test]
async fn test_scenario_65_the_coordinator_honors_a_short_requested_session_timeout() {
    let env = start_test_server().await;
    let topic = "honored_session_timeout_topic";
    let group_id = "honored_session_timeout_group";

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client.create_topic(topic, 1).await.unwrap();

    let consumer_client = TestClient::connect(env.addr).await.unwrap();
    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        session_timeout: Duration::from_millis(700),
        heartbeat_interval: Duration::from_millis(150),
        ..GroupConsumerConfig::default()
    };
    // Captured *before* the join round trip, not after: the coordinator stamps
    // `last_heartbeat` early in `JoinGroup` handling, well before `GroupConsumer::join`
    // returns (which additionally waits out the rebalance barrier and, as leader, runs
    // `SyncGroup`). Starting the clock only once all of that has finished would make the
    // measured gap to eviction look shorter than it really is relative to the
    // coordinator's own clock, and could spuriously trip the lower-bound check below.
    let started = std::time::Instant::now();
    let consumer = GroupConsumer::join(consumer_client, config).await.unwrap();
    let member_id = consumer.member_id().to_string();

    let mut observer = TestClient::connect(env.addr).await.unwrap();
    let (_, members) = observer.describe_group(group_id).await.unwrap();
    assert!(
        members.iter().any(|m| m.member_id == member_id),
        "the member must be present immediately after joining"
    );

    // Dropped without `leave()` — this stops the background heartbeat task
    // (`GroupConsumer::drop`) without telling the coordinator, simulating a process that
    // died outright rather than one that shut down cleanly.
    drop(consumer);

    // Bounded poll for the eviction rather than a single fixed sleep, so the assertion is
    // both prompt (fails fast if it never happens at all) and tolerant of scheduling
    // jitter around exactly when it happens.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut evicted_after = None;
    while std::time::Instant::now() < deadline {
        let (_, members) = observer.describe_group(group_id).await.unwrap();
        if !members.iter().any(|m| m.member_id == member_id) {
            evicted_after = Some(started.elapsed());
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let evicted_after = evicted_after.expect(
        "a dead member must eventually be evicted — the coordinator must not still be \
         waiting out the old hardcoded 10s",
    );
    assert!(
        evicted_after >= Duration::from_millis(700),
        "must not be evicted before its own configured session timeout elapsed, got {:?}",
        evicted_after
    );
    assert!(
        evicted_after < Duration::from_secs(5),
        "must be evicted comfortably under the legacy hardcoded 10s, not just eventually \
         — took {:?}",
        evicted_after
    );
}

/// Issue #54, core scenario: once heartbeating runs on its own background schedule (#53),
/// a member that keeps heartbeating but has stopped calling `poll()` looks exactly like a
/// healthy one to a liveness check based on heartbeats alone. This proves the coordinator
/// catches it anyway — via fetch-progress attribution (`tags::GROUP_MEMBER`,
/// `GroupCoordinator::record_progress`) and `max.poll.interval.ms` — while a member that
/// keeps fetching is left alone, and that the stalled member's partitions actually get
/// redistributed once it's gone rather than sitting abandoned.
#[tokio::test]
async fn test_scenario_66_a_heartbeating_but_non_consuming_member_is_evicted_and_redistributed() {
    let env = start_test_server_with_max_poll_interval(500).await;
    let topic = "stalled_consumption_topic";
    let group_id = "stalled_consumption_group";
    let num_partitions = 4u32;

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client
        .create_topic(topic, num_partitions)
        .await
        .unwrap();

    let client_active = TestClient::connect(env.addr).await.unwrap();
    let client_stalled = TestClient::connect(env.addr).await.unwrap();
    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        // A session timeout heartbeats alone keep both members comfortably inside for the
        // whole test, so the only thing that can evict `stalled` here is the stalled-
        // consumption check, never a session timeout.
        session_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_millis(100),
        ..GroupConsumerConfig::default()
    };

    // Both join at once, landing in the same generation — same reasoning as scenario 59.
    let (active, stalled) = tokio::join!(
        GroupConsumer::join(client_active, config.clone()),
        GroupConsumer::join(client_stalled, config.clone()),
    );
    let mut active = active.unwrap();
    let stalled = stalled.unwrap();
    let active_member_id = active.member_id().to_string();
    let stalled_member_id = stalled.member_id().to_string();

    let mut a_active = active.assignment().to_vec();
    let mut a_stalled = stalled.assignment().to_vec();
    a_active.sort_unstable();
    a_stalled.sort_unstable();
    assert!(
        !a_active.is_empty() && !a_stalled.is_empty(),
        "both members must start out owning partitions, got {:?} / {:?}",
        a_active,
        a_stalled
    );

    // `stalled` is never polled again anywhere in this test — its background heartbeat
    // task keeps reporting it alive, but nothing ever fetches on its behalf. That is the
    // entire scenario: heartbeating without consuming.
    //
    // `active` keeps polling throughout: partly to keep proving it must never be evicted,
    // and partly because a live member's own background heartbeat task is what notices a
    // generation bump and flags a rejoin (see scenario 64) — needed for the redistribution
    // phase below.
    let mut observer = TestClient::connect(env.addr).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut stalled_evicted = false;
    while std::time::Instant::now() < deadline {
        let _ = active.poll().await.unwrap();

        let (_, members) = observer.describe_group(group_id).await.unwrap();
        assert!(
            members.iter().any(|m| m.member_id == active_member_id),
            "the actively-fetching member must never be evicted"
        );

        if !members.iter().any(|m| m.member_id == stalled_member_id) {
            stalled_evicted = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        stalled_evicted,
        "the heartbeating-but-non-consuming member must eventually be evicted"
    );

    // Distinguishable from a session-timeout eviction, not just "gone for some reason".
    let evictions = env.engine.group_coordinator().recent_evictions(group_id);
    let stalled_record = evictions
        .iter()
        .find(|e| e.member_id == stalled_member_id)
        .expect("the stalled member's eviction must be recorded");
    assert_eq!(
        stalled_record.reason,
        hermes::server::coordinator::EvictionReason::StalledConsumption,
        "a heartbeating-but-non-consuming member must be evicted as stalled, not as a \
         session timeout"
    );

    // Pruning alone never bumps the generation, so nothing about `stalled` disappearing
    // by itself redistributes its partitions — exactly like a session-timeout eviction, a
    // new member's `JoinGroup` is what actually forces the rebalance that hands them out
    // (see scenario 60/64). This is what proves "redistributed", not just "vacated".
    let client_newcomer = TestClient::connect(env.addr).await.unwrap();
    let newcomer_config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        session_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_millis(100),
        ..GroupConsumerConfig::default()
    };
    let newcomer_handle =
        tokio::spawn(async move { GroupConsumer::join(client_newcomer, newcomer_config).await });

    // Watch `active`'s generation rather than its assignment *length* — with `stalled`
    // gone, 4 partitions split 2-and-2 across `active` and the newcomer is the same count
    // `active` already had, so a length check would never observe the rejoin at all and
    // would just spin for the full deadline.
    let generation_before_newcomer = active.generation_id();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while active.generation_id() == generation_before_newcomer
        && std::time::Instant::now() < deadline
    {
        let _ = active.poll().await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }
    assert_ne!(
        active.generation_id(),
        generation_before_newcomer,
        "active must have observed the newcomer's join and rejoined under the new \
         generation"
    );

    let newcomer = newcomer_handle
        .await
        .unwrap()
        .expect("the newcomer must be able to join and sync once active rejoins as leader");

    let mut a1 = active.assignment().to_vec();
    let mut a2 = newcomer.assignment().to_vec();
    a1.sort_unstable();
    a2.sort_unstable();
    let a1_set: std::collections::HashSet<u32> = a1.iter().copied().collect();
    let a2_set: std::collections::HashSet<u32> = a2.iter().copied().collect();
    assert!(
        a1_set.is_disjoint(&a2_set),
        "the surviving member and the newcomer must not overlap: {:?} vs {:?}",
        a1,
        a2
    );
    let mut combined: Vec<u32> = a1.iter().chain(a2.iter()).copied().collect();
    combined.sort_unstable();
    assert_eq!(
        combined,
        (0..num_partitions).collect::<Vec<u32>>(),
        "together the surviving member and the newcomer must cover every partition — \
         proving the stalled member's partitions were actually redistributed, not left \
         abandoned"
    );
}

/// Issue #54: a session-timeout eviction and a stalled-consumption eviction must be
/// distinguishable, not just two paths that both silently make a member disappear. Proven
/// end-to-end over the wire: one member never heartbeats at all (dies outright), another
/// heartbeats normally but never fetches (stalls) — same group, same pruning pass — and
/// `GroupCoordinator::recent_evictions` must report the right reason for each.
#[tokio::test]
async fn test_scenario_67_session_timeout_and_stalled_consumption_evictions_are_distinguishable() {
    let env = start_test_server_with_max_poll_interval(300).await;
    let topic = "distinguishable_eviction_topic";
    let group_id = "distinguishable_eviction_group";

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client.create_topic(topic, 1).await.unwrap();

    // `dead`: a raw client that joins with a short session timeout and then never sends
    // another heartbeat — nothing keeps it alive at all.
    let mut dead_client = TestClient::connect(env.addr).await.unwrap();
    let dead_join = dead_client
        .join_group_with_session_timeout(
            group_id,
            "",
            None,
            &["range"],
            Some(Duration::from_millis(250)),
        )
        .await
        .unwrap();
    let dead_member_id = dead_join.member_id;

    // `stalled`: a real `GroupConsumer`, heartbeating normally on its own background
    // schedule, but never polled — so it keeps looking alive while never consuming.
    let stalled_client = TestClient::connect(env.addr).await.unwrap();
    let stalled_config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        session_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_millis(100),
        ..GroupConsumerConfig::default()
    };
    let stalled = GroupConsumer::join(stalled_client, stalled_config)
        .await
        .unwrap();
    let stalled_member_id = stalled.member_id().to_string();

    let mut observer = TestClient::connect(env.addr).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let (_, members) = observer.describe_group(group_id).await.unwrap();
        let dead_gone = !members.iter().any(|m| m.member_id == dead_member_id);
        let stalled_gone = !members.iter().any(|m| m.member_id == stalled_member_id);
        if dead_gone && stalled_gone {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "both members must eventually be evicted — dead_gone={} stalled_gone={}",
            dead_gone,
            stalled_gone
        );
        sleep(Duration::from_millis(40)).await;
    }

    let evictions = env.engine.group_coordinator().recent_evictions(group_id);
    let dead_record = evictions
        .iter()
        .find(|e| e.member_id == dead_member_id)
        .expect("the dead member's eviction must be recorded");
    let stalled_record = evictions
        .iter()
        .find(|e| e.member_id == stalled_member_id)
        .expect("the stalled member's eviction must be recorded");

    assert_eq!(
        dead_record.reason,
        hermes::server::coordinator::EvictionReason::SessionTimeout,
        "a member that never heartbeats must be evicted as a session timeout"
    );
    assert_eq!(
        stalled_record.reason,
        hermes::server::coordinator::EvictionReason::StalledConsumption,
        "a member that heartbeats but never fetches must be evicted as stalled \
         consumption"
    );
    assert_ne!(
        dead_record.reason, stalled_record.reason,
        "the two eviction causes must be distinguishable from one another"
    );

    drop(stalled);
}

/// Waits for a child process to exit, with a hard deadline, draining stdout/stderr
/// concurrently so a full pipe buffer can never block the child.
///
/// If the child hasn't exited by `deadline`, it is SIGKILLed and this panics with a clear
/// message plus whatever output was captured so far -- turning a hang into a readable test
/// failure instead of blocking the whole suite (and CI) for however long the runner allows.
///
/// Synchronous by design: run it inside `tokio::task::spawn_blocking` rather than awaiting
/// it directly, so the polling loop below doesn't starve the (single-threaded, by default)
/// test runtime that the in-process test server also runs on.
#[cfg(unix)]
fn wait_for_child_with_deadline(
    mut child: std::process::Child,
    deadline: Duration,
) -> std::process::Output {
    use std::io::Read;

    let pid = child.id();
    let mut stdout_pipe = child.stdout.take();
    let stdout_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(pipe) = stdout_pipe.as_mut() {
            let _ = pipe.read_to_end(&mut buf);
        }
        buf
    });
    let mut stderr_pipe = child.stderr.take();
    let stderr_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(pipe) = stderr_pipe.as_mut() {
            let _ = pipe.read_to_end(&mut buf);
        }
        buf
    });

    let start = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("failed to poll child status") {
            break status;
        }
        if start.elapsed() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let stdout =
                String::from_utf8_lossy(&stdout_handle.join().unwrap_or_default()).into_owned();
            let stderr =
                String::from_utf8_lossy(&stderr_handle.join().unwrap_or_default()).into_owned();
            panic!(
                "child process (pid {}) did not exit within {:?} and was SIGKILLed -- it \
                 hung instead of shutting down gracefully.\nstdout so far: {}\nstderr so \
                 far: {}",
                pid, deadline, stdout, stderr
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };

    let stdout = stdout_handle.join().expect("stdout reader thread panicked");
    let stderr = stderr_handle.join().expect("stderr reader thread panicked");

    std::process::Output {
        status,
        stdout,
        stderr,
    }
}

/// Issue #60: SIGTERM must reach the same graceful-shutdown path as SIGINT (Ctrl+C).
/// Docker, Kubernetes and systemd stop a managed process with SIGTERM, not SIGINT, so a
/// shutdown handler that only ever waited on `ctrl_c()` never ran under those supervisors
/// -- silently skipping the consumer's final offset commit on every container restart, and
/// reprocessing everything since the last periodic commit.
///
/// This drives the real `hermes_cli` binary as a child process and sends it an actual
/// SIGTERM, rather than exercising the `select!` arm in-process, because the bug was
/// specifically about which OS signal reaches that arm -- an in-process test would not have
/// caught it. Unix-only: Windows has no SIGTERM to send.
#[cfg(unix)]
#[tokio::test]
async fn test_scenario_68_sigterm_triggers_the_same_graceful_shutdown_as_sigint() {
    let env = start_test_server().await;
    let topic = "sigterm_shutdown_topic";
    let group_id = "sigterm_shutdown_group";
    let num_records = 5u64;

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client.create_topic(topic, 1).await.unwrap();
    for i in 0..num_records {
        setup_client
            .produce_single(topic, "k", None, 1, format!("msg-{}", i).as_bytes())
            .await
            .unwrap();
    }

    let cli_path = env!("CARGO_BIN_EXE_hermes_cli");
    let child = std::process::Command::new(cli_path)
        .args([
            "group-consume",
            "--server",
            &env.addr.to_string(),
            "--group",
            group_id,
            "--topic",
            topic,
            "--interval",
            "50",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn hermes_cli group-consume");
    let pid = child.id();

    // Wait for the consumer to actually join, consume every produced record, and
    // auto-commit at least once -- so SIGTERM lands on a process that has something to
    // lose if the graceful-shutdown path is skipped. Frame offsets are 0-indexed, so the
    // last of `num_records` frames commits offset `num_records - 1`.
    let expected_committed = num_records - 1;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(committed) = setup_client.fetch_offset(group_id, topic, 0).await {
            if committed == expected_committed {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the CLI consumer must consume and auto-commit every produced record before \
             we send it SIGTERM"
        );
        sleep(Duration::from_millis(20)).await;
    }

    // Send SIGTERM the same way Docker/Kubernetes/systemd would -- not SIGKILL, not
    // SIGINT -- via the system `kill` binary rather than a new crate dependency.
    let kill_status = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("failed to invoke `kill`");
    assert!(
        kill_status.success(),
        "`kill -TERM {}` itself must succeed",
        pid
    );

    let output = tokio::task::spawn_blocking(move || {
        wait_for_child_with_deadline(child, Duration::from_secs(5))
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "hermes_cli must exit successfully after SIGTERM, proving it went through the \
         graceful-shutdown path instead of being killed outright by the OS default action; \
         status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Graceful shutdown signal received"),
        "SIGTERM must drive the same graceful-shutdown log line SIGINT already used; \
         stdout was: {}",
        stdout
    );
}

/// Regression test for the swallowed-signal bug itself (as opposed to scenario 68, which
/// exercises SIGTERM in general but -- being timing-dependent -- passed even with the bug
/// present, which is exactly why CI caught this and local runs didn't).
///
/// `wait_for_shutdown_signal()` used to be constructed fresh on every loop iteration of the
/// `tokio::select!` in `group-consume`. Whichever branch didn't win had its future dropped,
/// so a signal delivered while the *other* branch's body was still running (i.e. while
/// `consumer.poll().await` was in flight, with no live signal listener registered) was lost
/// for good -- not merely delayed, since tokio's `signal()` had already replaced the OS
/// default disposition, so the process didn't die from that either. It just hung forever.
///
/// To land SIGTERM deterministically inside that window rather than racing a sub-millisecond
/// gap, this configures a very low fetch byte-rate quota and produces enough data that the
/// consumer's first `poll()` is server-side throttled for several seconds -- turning the
/// race into a wide, reliable window instead of a coin flip. The signal is sent well inside
/// that window, so a fix that keeps the shutdown listener alive across iterations must still
/// observe it (and exit once the in-flight poll finishes), while the old per-iteration
/// reconstruction would drop it and hang, which `wait_for_child_with_deadline`'s bounded
/// wait turns into a clean panic instead of stalling the suite.
#[cfg(unix)]
#[tokio::test]
async fn test_scenario_69_sigterm_delivered_mid_poll_is_not_swallowed() {
    // 100 bytes/sec, 100-byte burst capacity. Five records well over that in one fetch
    // response throttle the first `poll()` for (500 - 100) / 100 = 4 seconds -- long
    // enough to comfortably send SIGTERM into the middle of it without racing.
    let fetch_quota_bytes_per_sec = 100u64;
    let env = start_test_server_with_quota(None, Some(fetch_quota_bytes_per_sec)).await;
    let topic = "sigterm_mid_poll_topic";
    let group_id = "sigterm_mid_poll_group";
    let num_records = 5u64;
    let payload = vec![b'x'; 100];

    let mut setup_client = TestClient::connect(env.addr).await.unwrap();
    setup_client.create_topic(topic, 1).await.unwrap();
    for _ in 0..num_records {
        setup_client
            .produce_single(topic, "k", None, 1, payload.as_slice())
            .await
            .unwrap();
    }

    let cli_path = env!("CARGO_BIN_EXE_hermes_cli");
    let child = std::process::Command::new(cli_path)
        .args([
            "group-consume",
            "--server",
            &env.addr.to_string(),
            "--group",
            group_id,
            "--topic",
            topic,
            "--interval",
            "50",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn hermes_cli group-consume");
    let pid = child.id();

    // The first `sleep(50ms)` branch wins (no signal has been sent yet), which starts the
    // one and only `poll()` this test needs -- fetching all 5 records in a single request
    // that the quota throttles for ~4s before the server responds. Waiting 1s here lands
    // well past the 50ms sleep (so the CLI is certainly inside that throttled `poll().await`
    // by then) and well before the ~4s throttle expires (so it's certainly still there).
    sleep(Duration::from_millis(1_000)).await;

    // Send SIGTERM the same way Docker/Kubernetes/systemd would.
    let kill_status = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("failed to invoke `kill`");
    assert!(
        kill_status.success(),
        "`kill -TERM {}` itself must succeed",
        pid
    );

    // The signal can only be acted on once the in-flight throttled poll (~4s from its
    // start, ~3s remaining from here) returns control to the `select!` loop, so give it
    // generous headroom before declaring a hang.
    let output = tokio::task::spawn_blocking(move || {
        wait_for_child_with_deadline(child, Duration::from_secs(10))
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "hermes_cli must exit successfully after SIGTERM sent mid-poll, proving the \
         shutdown listener survives across select! iterations instead of being dropped and \
         losing the signal; status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Graceful shutdown signal received"),
        "SIGTERM sent mid-poll must still drive the graceful-shutdown log line; stdout was: {}",
        stdout
    );
}

// ─────────────────────────────────────────────────────────
// Issue #62: broker discovery deadlock — advertised address (Commit 1)
// ─────────────────────────────────────────────────────────

/// Issue #62 / Commit 1: a node configured with an ephemeral `bind_addr` (`:0`) must
/// advertise the real port the OS assigned it, not the literal `:0` it was configured
/// with. Before this fix, `ReplicationManager` captured `EngineConfig::bind_addr` at
/// construction time — before the TCP listener ever bound — so every identity this node
/// published (heartbeats, heartbeat ACKs, its own `BrokerRegister`) carried an unusable
/// placeholder no peer could ever dial back.
#[tokio::test]
async fn test_scenario_70_advertised_addr_resolves_to_real_bound_address() {
    let dir = TestDataDirGuard::new("advertised_addr_real");
    let config = EngineConfig {
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        ..EngineConfig::default()
    };
    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine.clone());
    let (_listener, addr) = server.bind().unwrap();

    let advertised = engine.replication().advertised_addr();
    assert_ne!(
        advertised, "127.0.0.1:0",
        "must not still be advertising the configured ephemeral placeholder"
    );
    assert_eq!(
        advertised,
        addr.to_string(),
        "advertised address must be exactly the real address the OS bound"
    );
    let port: u16 = advertised.rsplit(':').next().unwrap().parse().unwrap();
    assert_ne!(
        port, 0,
        "advertised port must be the real, non-zero OS-assigned port"
    );
}

/// Issue #62 / Commit 1: an explicit `advertised_addr` override (Kafka's
/// `advertised.listeners`) takes precedence over the real bound address — needed behind
/// NAT / a load balancer, where the locally bound address isn't what a peer should dial.
#[tokio::test]
async fn test_scenario_71_advertised_addr_override_takes_precedence() {
    let dir = TestDataDirGuard::new("advertised_addr_override");
    let config = EngineConfig {
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        advertised_addr: Some("203.0.113.5:9092".to_string()),
        ..EngineConfig::default()
    };
    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine.clone());
    let (_listener, addr) = server.bind().unwrap();

    assert_ne!(
        addr.to_string(),
        "203.0.113.5:9092",
        "sanity check: the real bound loopback address must differ from the override"
    );
    assert_eq!(
        engine.replication().advertised_addr(),
        "203.0.113.5:9092",
        "the configured override must win over the real bound address"
    );
}

/// Issue #62 / Commit 1: refusing to advertise a wildcard identity. A node bound to
/// `0.0.0.0` (with no `advertised_addr` override) has no real address to fall back on —
/// `local_addr()` after such a bind still reports the wildcard IP, since the OS never
/// picks a concrete interface for it. Kafka refuses to start in the equivalent case;
/// here that surfaces as `Server::bind` returning `Err` (a hard failure — `main.rs`
/// propagates it straight out of `run()` and the process exits without ever starting the
/// listener loop).
#[tokio::test]
async fn test_scenario_72_wildcard_advertised_addr_refuses_to_bind() {
    let dir = TestDataDirGuard::new("advertised_addr_wildcard");
    let config = EngineConfig {
        data_dir: dir.path.clone(),
        bind_addr: "0.0.0.0:0".to_string(),
        ..EngineConfig::default()
    };
    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine);

    let result = server.bind();
    assert!(
        result.is_err(),
        "binding to a wildcard host with no advertised_addr override must be refused"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("advertised.listeners") || err_msg.contains("advertised"),
        "the error must name the config key to fix; got: {}",
        err_msg
    );
}

// ─────────────────────────────────────────────────────────
// Issue #62: broker discovery deadlock — heartbeat allowlist (Commit 2)
// ─────────────────────────────────────────────────────────

/// Issue #62 / Commit 2: this is the actual deadlock topology — a node with the default
/// *empty* `peer_addrs` must accept a heartbeat from a distinct sender address rather than
/// rejecting every sender forever. Before this fix, an empty allowlist meant "nothing is
/// ever whitelisted", so a follower configured this way (the common case for a node whose
/// peers' addresses aren't known ahead of time — e.g. an ephemeral port) could never be
/// discovered by its leader.
#[tokio::test]
async fn test_scenario_73_empty_peer_allowlist_accepts_heartbeat_from_any_distinct_leader() {
    let dir = TestDataDirGuard::new("heartbeat_empty_allowlist");
    let cluster_id = EngineConfig::default().cluster_id;
    let config = EngineConfig {
        node_id: 9,
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        // peer_addrs left at the default: empty.
        ..EngineConfig::default()
    };
    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine.clone());
    let (listener, addr) = server.bind().unwrap();
    let _task = tokio::spawn(async move {
        let _ = server.run_with_listener(listener).await;
    });

    let result = hermes::replication::send_leader_heartbeat(
        &addr.to_string(),
        &cluster_id,
        42,                // a leader node_id distinct from this node's own (9)
        1,                 // term
        "127.0.0.1:19999", // a leader address never configured anywhere
    )
    .await;

    assert!(
        result.is_ok(),
        "a heartbeat from a distinct, unlisted address must be accepted when no allowlist \
         is configured; got {:?}",
        result.err()
    );
    let (follower_id, follower_addr, _roles) = result.unwrap();
    assert_eq!(follower_id, 9);
    assert_eq!(
        follower_addr,
        addr.to_string(),
        "the ACK must carry this node's real advertised address"
    );
}

/// Issue #62 / Commit 2: a *non-empty* `peer_addrs` must keep exactly its previous
/// behavior — a heartbeat whose claimed leader address isn't one of the configured peers
/// is still rejected, and one that is a configured peer is still accepted.
#[tokio::test]
async fn test_scenario_74_nonempty_peer_allowlist_still_rejects_unlisted_leader() {
    let dir = TestDataDirGuard::new("heartbeat_nonempty_allowlist");
    let cluster_id = EngineConfig::default().cluster_id;
    let whitelisted_peer = "127.0.0.1:55001".to_string();
    let config = EngineConfig {
        node_id: 9,
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        peer_addrs: vec![whitelisted_peer.clone()],
        ..EngineConfig::default()
    };
    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine.clone());
    let (listener, addr) = server.bind().unwrap();
    let _task = tokio::spawn(async move {
        let _ = server.run_with_listener(listener).await;
    });

    let rejected = hermes::replication::send_leader_heartbeat(
        &addr.to_string(),
        &cluster_id,
        42,
        1,
        "127.0.0.1:55002", // not the whitelisted peer
    )
    .await;
    assert!(
        rejected.is_err(),
        "a heartbeat claiming a leader address outside the configured allowlist must still \
         be rejected"
    );

    let accepted = hermes::replication::send_leader_heartbeat(
        &addr.to_string(),
        &cluster_id,
        42,
        1,
        &whitelisted_peer,
    )
    .await;
    assert!(
        accepted.is_ok(),
        "a heartbeat claiming a whitelisted leader address must still be accepted; got {:?}",
        accepted.err()
    );
}

/// Issue #62 / Commit 2 / CRIT-03: a peer claiming to be this node's own advertised
/// address must be rejected even with no allowlist configured — this is what stops a peer
/// from advertising our own address as "the leader" to make forwarded produces loop back
/// to us. The allowlist becoming permissive when empty must not weaken this.
#[tokio::test]
async fn test_scenario_75_crit03_self_address_rejected_even_with_empty_allowlist() {
    let dir = TestDataDirGuard::new("heartbeat_crit03_empty_allowlist");
    let cluster_id = EngineConfig::default().cluster_id;
    let config = EngineConfig {
        node_id: 9,
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        // peer_addrs left at the default: empty.
        ..EngineConfig::default()
    };
    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine.clone());
    let (listener, addr) = server.bind().unwrap();
    let self_advertised_addr = engine.replication().advertised_addr();
    assert_eq!(
        self_advertised_addr,
        addr.to_string(),
        "sanity check: this node's advertised address should be its real bound address"
    );
    let _task = tokio::spawn(async move {
        let _ = server.run_with_listener(listener).await;
    });

    let result = hermes::replication::send_leader_heartbeat(
        &addr.to_string(),
        &cluster_id,
        42,
        1,
        &self_advertised_addr, // claims to *be* this node
    )
    .await;

    assert!(
        result.is_err(),
        "a peer claiming our own advertised address as the leader must be rejected even \
         with an empty allowlist"
    );
}

// ─────────────────────────────────────────────────────────
// Issue #20: client-side request pipelining
// ─────────────────────────────────────────────────────────
//
// The Kafka protocol guide is explicit that the broker does *not* answer out of order:
// "The server guarantees that on a single TCP connection, requests will be processed in
// the order they are sent and responses will return in that order as well." Hermes's
// broker already relies on exactly that guarantee (`WireResponse::encode_framed_into`
// echoes each request's correlation id, and the connection loop in `server/handler.rs`
// drains and answers back-to-back requests off one read in order) — so pipelining is
// purely a client-side change: send several requests without waiting for each response,
// bounded by `TestClient::send_pipelined`'s `max_in_flight`, and let the broker's existing
// in-order guarantee do the matching.

/// The key end-to-end proof: pipeline more requests than the default max-in-flight bound
/// (5) over one connection and confirm every response comes back correctly correlated, in
/// the same order the requests were sent, carrying the right payload for its request. This
/// is what exercises the broker's in-order-response contract under an actual bounded
/// sliding window rather than one request at a time.
#[tokio::test]
async fn test_scenario_76_pipelined_requests_return_correlated_in_order_responses() {
    use bytes::BufMut;

    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr)
        .await
        .expect("Failed to connect");

    // N > hermes::client::DEFAULT_MAX_IN_FLIGHT_REQUESTS (5), so the pipeline must refill
    // its window at least once mid-flight rather than emptying it in a single pass.
    const N: usize = 8;
    let mut expected_watermarks = Vec::with_capacity(N);
    for i in 0..N {
        let topic = format!("pipeline_topic_{}", i);
        client.create_topic(&topic, 1).await.expect("create topic");
        // A distinct record count per topic so a response landing at the wrong index would
        // be caught by a wrong watermark, not just a coincidentally-matching one.
        let record_count = (i as u64) + 1;
        for r in 0..record_count {
            client
                .produce_single(
                    &topic,
                    "k",
                    None,
                    1,
                    format!("pipeline-payload-{}-{}", i, r),
                )
                .await
                .expect("produce");
        }
        expected_watermarks.push(record_count);
    }

    let requests: Vec<_> = (0..N)
        .map(|i| {
            let topic = format!("pipeline_topic_{}", i);
            let mut inner = Vec::new();
            hermes::protocol::wire::write_pascal_string(&mut inner, &topic);
            inner.put_u32(0); // partition
            (hermes::CommandCode::LatestOffset, Vec::new(), inner)
        })
        .collect();

    let responses = client
        .send_pipelined(requests, hermes::client::DEFAULT_MAX_IN_FLIGHT_REQUESTS)
        .await
        .expect("pipelined send failed");

    assert_eq!(
        responses.len(),
        N,
        "must get back exactly one response per pipelined request"
    );
    for (i, resp) in responses.iter().enumerate() {
        assert_eq!(
            resp.status, 0,
            "request {} came back with an error status",
            i
        );
        assert!(
            resp.payload.len() >= 8,
            "LatestOffset response {} payload too short",
            i
        );
        let watermark = u64::from_be_bytes(resp.payload[0..8].try_into().unwrap());
        assert_eq!(
            watermark, expected_watermarks[i],
            "response {} does not match its request — pipelined responses came back \
             mis-correlated or out of order",
            i
        );
    }
}

/// `InFlightWindow` is the structure [`TestClient::send_pipelined`] uses to decide when it
/// must stop writing and wait for a response: this proves that structure actually refuses
/// to exceed its bound, in FIFO order, rather than growing unboundedly.
#[test]
fn test_scenario_77_in_flight_window_enforces_max_bound() {
    let mut window = hermes::client::InFlightWindow::new(3);

    assert!(window.try_push(1));
    assert!(window.try_push(2));
    assert!(window.try_push(3));
    assert!(window.is_full());
    assert_eq!(window.len(), 3);
    assert!(
        !window.try_push(4),
        "a 4th outstanding request must be refused once the bound of 3 is reached"
    );
    assert_eq!(
        window.len(),
        3,
        "a refused push must not have been recorded"
    );

    // Draining the oldest frees exactly one slot, in the order requests were sent.
    assert_eq!(window.pop_front(), Some(1));
    assert!(!window.is_full());
    assert!(window.try_push(4));
    assert!(window.is_full());

    assert_eq!(window.pop_front(), Some(2));
    assert_eq!(window.pop_front(), Some(3));
    assert_eq!(window.pop_front(), Some(4));
    assert_eq!(window.pop_front(), None);
    assert!(window.is_empty());
}

/// A correlation id mismatch means a protocol bug or wire corruption and must be a loud
/// error, never a silently mismatched result. Hermes's broker always echoes the correct id
/// (this is the guarantee issue #20 depends on), so a real mismatch cannot be provoked by
/// talking to a well-behaved broker — this instead tests the matching logic itself, the
/// same `verify_correlation_id` both `send_versioned` and `send_pipelined` check every
/// response against.
#[test]
fn test_scenario_78_correlation_id_mismatch_is_reported_as_error() {
    hermes::client::verify_correlation_id(42, 42).expect("matching ids must be accepted");

    let err = hermes::client::verify_correlation_id(42, 43)
        .expect_err("a mismatched correlation id must be reported as an error, not accepted");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("correlation id mismatch"),
        "error should name the actual problem; got: {}",
        err
    );
}

/// The key claim of incremental cooperative rebalancing (issue #66, Kafka's KIP-429):
/// when a new member joins a `cooperative-sticky` group, partitions that are NOT moving
/// keep their in-memory consumption position across the rebalance — the existing member
/// (a real `GroupConsumer`, exercising the actual `rejoin()` implementation, not a
/// hand-simulated stand-in) resumes exactly where it left off, with no re-delivery and no
/// gap — while the partition that does move is picked up by its new owner from exactly
/// where the old owner's automatic revoke-commit left it. This is deliberately stronger
/// than asserting the final assignment is correct (an eager rebalance would produce that
/// too): it checks actual record continuity, at the offset level, on both the kept and
/// the moved partition.
///
/// The new member's own join is driven with a raw `TestClient` rather than a second
/// `GroupConsumer`: the round-one-only intermediate state this feature produces exists
/// for a network-round-trip-scale window (the leader chains straight from round one into
/// requesting round two), far too narrow for a second, independently-scheduled
/// `GroupConsumer`'s background heartbeat to reliably observe without an inherently flaky
/// race — that specific protocol-level property (round one hands out nothing) is instead
/// proven deterministically in scenario 80. This test's job is the data-level guarantee,
/// which only needs the new member's *final* state.
///
/// The timing of production matters here: r1/r2 are held back until the coordinator has
/// *already* fully settled the rebalance (checked via a spectator connection, not via
/// A's own client) and A's background heartbeat has had a full interval to notice and
/// flag `needs_rejoin` — otherwise A's own poll loop, driven by this test to detect the
/// rebalance, would race ahead and fetch r1/r2 under the old assignment before the
/// rejoin ever happens, leaving nothing left to distinguish "resumed correctly" from
/// "resumed from scratch".
#[tokio::test]
async fn test_scenario_79_cooperative_rebalance_revokes_before_reassigning_and_preserves_position()
{
    let env = start_test_server().await;
    let topic = "coop_key_topic";
    let group_id = "coop_key_group";
    let num_partitions = 4u32;

    let mut setup = TestClient::connect(env.addr).await.unwrap();
    setup.create_topic(topic, num_partitions).await.unwrap();

    // Only r0 on every partition up front.
    for partition in 0..num_partitions {
        let key = key_for_partition(partition, num_partitions);
        let payload = format!("p{partition}-r0");
        setup
            .produce_single(topic, &key, None, num_partitions, payload.as_bytes())
            .await
            .unwrap();
    }

    let heartbeat_interval = Duration::from_millis(60);
    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        protocols: vec!["cooperative-sticky".to_string()],
        heartbeat_interval,
        session_timeout: Duration::from_secs(3),
        ..GroupConsumerConfig::default()
    };

    let client_a = TestClient::connect(env.addr).await.unwrap();
    let mut a = GroupConsumer::join(client_a, config.clone()).await.unwrap();
    let mut a_assignment = a.assignment().to_vec();
    a_assignment.sort_unstable();
    assert_eq!(
        a_assignment,
        vec![0u32, 1, 2, 3],
        "A must start out owning every partition"
    );

    // A consumes r0 from every partition, but never commits — its in-memory position
    // moves ahead of the broker's committed offset, and stays that way for whichever
    // partitions it ends up keeping.
    let mut consumed_first: std::collections::HashMap<u32, String> =
        std::collections::HashMap::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while consumed_first.len() < num_partitions as usize && std::time::Instant::now() < deadline {
        let records = a.poll().await.unwrap();
        for (partition, frame) in records {
            consumed_first
                .entry(partition)
                .or_insert_with(|| String::from_utf8_lossy(&frame.payload).to_string());
        }
        if consumed_first.len() < num_partitions as usize {
            sleep(Duration::from_millis(20)).await;
        }
    }
    for partition in 0..num_partitions {
        assert_eq!(
            consumed_first.get(&partition),
            Some(&format!("p{partition}-r0")),
            "A must have consumed exactly r0 of partition {partition} before B joins"
        );
    }

    // B joins. A brand-new member's JoinGroup bumps the group's generation — this is
    // round one's trigger.
    let mut client_b = TestClient::connect(env.addr).await.unwrap();
    let join_b = client_b
        .join_group(group_id, "", &["cooperative-sticky"])
        .await
        .expect("B must be able to register with the group");
    assert!(!join_b.is_leader, "A must remain the leader when B joins");

    // Watch the coordinator's own bookkeeping, from a separate connection, for a moment
    // where the partition about to move is owned by neither A nor B — the handover-safety
    // property that is actually specific to cooperative rebalancing (see the rename
    // comment below). A's leader-side `rejoin()` submits round one and round two as two
    // separate, atomic `SyncGroup` calls with a real round-trip (and, for round two, a
    // full `group_initial_rebalance_delay_ms` join window) between them, so this window
    // is not a hair's-breadth race: it is open for tens of milliseconds, plenty for this
    // background poll loop to land inside it before A's own poll loop (below) observes
    // the settled, final state. If the intersection narrowing in `cooperative_round_one`
    // were disabled, A's single `SyncGroup` call would move every partition in one atomic
    // step and this sum would never dip below `num_partitions` at all.
    let a_member_id = a.member_id().to_string();
    let b_member_id = join_b.member_id.clone();
    let watch_group_id = group_id.to_string();
    let watch_topic = topic.to_string();
    let watch_addr = env.addr;
    let handover_witness: tokio::task::JoinHandle<Option<(Vec<u32>, Vec<u32>)>> =
        tokio::spawn(async move {
            let mut spectator = TestClient::connect(watch_addr).await.ok()?;
            // Bounded well under B's session timeout (10s by default, since B is a raw
            // `TestClient` here and never heartbeats): the genuine window this is
            // watching for is tens of milliseconds wide (see the comment above), so 3s is
            // generous headroom for it, while still failing fast — rather than stalling
            // long enough for B's own session to expire and produce a confusing,
            // unrelated failure further down — if the property genuinely never appears.
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while std::time::Instant::now() < deadline {
                if let Ok((_, members)) = spectator.describe_group(&watch_group_id).await {
                    let mut a_partitions: Vec<u32> = Vec::new();
                    let mut b_partitions: Vec<u32> = Vec::new();
                    for m in members {
                        let mut partitions: Vec<u32> = m
                            .assigned_partitions
                            .into_iter()
                            .filter(|(t, _)| t == &watch_topic)
                            .map(|(_, p)| p)
                            .collect();
                        partitions.sort_unstable();
                        if m.member_id == a_member_id {
                            a_partitions = partitions;
                        } else if m.member_id == b_member_id {
                            b_partitions = partitions;
                        }
                    }
                    if a_partitions.len() + b_partitions.len() < num_partitions as usize {
                        return Some((a_partitions, b_partitions));
                    }
                }
                sleep(Duration::from_millis(2)).await;
            }
            None
        });

    // Drive A's own poll loop until it notices B (via its background heartbeat
    // failing on the now-stale generation) and rebalances. A is the leader, and its
    // `rejoin()` performs both cooperative rounds synchronously once it fires, so
    // this loop only ever needs to observe the *final* state — there's no
    // intermediate value to catch here. This is safe to do with plain `poll()` calls
    // rather than racing an eager fetch: r1/r2 don't exist on the broker yet (they're
    // produced further down, only once this settles), so there is nothing for an
    // ordinary fetch to prematurely slurp up in the meantime.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let _ = a.poll().await.unwrap();
        if !a.assignment().is_empty() && a.assignment().len() < num_partitions as usize {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "A never rebalanced after B joined; still owns {:?}",
            a.assignment()
        );
        sleep(Duration::from_millis(20)).await;
    }

    // --- The key assertion: cooperative rebalancing must have actually passed through
    // an intermediate state where the moving partition belonged to neither A nor B,
    // rather than jumping directly from A to B in one atomic step. This is what an
    // eager-equivalent bug — e.g. `cooperative_round_one` hand out the full target
    // instead of the intersection with what a member already owned — would remove: it
    // would collapse the two-round handover into a single round, so the watcher above
    // would never see the partition count dip below `num_partitions` at all. ---
    let handover_witness = handover_witness
        .await
        .expect("the handover-watcher task must not panic");
    let (witnessed_a, witnessed_b) = handover_witness.expect(
        "must observe a moment where the moving partition is owned by neither A nor B — \
         cooperative rebalancing must revoke it from A (round one) before it is ever \
         handed to B (round two), never both in the same atomic SyncGroup submission",
    );
    assert!(
        witnessed_b.is_empty(),
        "B is a brand-new member with nothing to intersect against, so round one's \
         keep-set for it must be empty — B must own nothing at this intermediate point, \
         got {witnessed_b:?}"
    );
    assert!(
        witnessed_a.len() < num_partitions as usize,
        "A must have already released at least one partition by this intermediate point, \
         got {witnessed_a:?}"
    );

    // Only now produce r1/r2 — after the rebalance has fully settled and A's own
    // position for whatever it kept is confirmed untouched since r0.
    for partition in 0..num_partitions {
        let key = key_for_partition(partition, num_partitions);
        for i in 1..3 {
            let payload = format!("p{partition}-r{i}");
            setup
                .produce_single(topic, &key, None, num_partitions, payload.as_bytes())
                .await
                .unwrap();
        }
    }

    let mut a_final = a.assignment().to_vec();
    a_final.sort_unstable();
    assert!(
        !a_final.is_empty(),
        "A must still own at least one partition"
    );
    assert!(
        a_final.len() < num_partitions as usize,
        "A must have given up at least one partition to B"
    );

    // B's confirmed, final share — read via the *same* generation A (the real
    // `GroupConsumer`) ended up on, with no further JoinGroup call from B: B is
    // already a known member, so a redundant JoinGroup here — while the group is
    // already Stable — would itself request an unwanted *third* round under this
    // feature's own cooperative JoinGroup semantics. A plain SyncGroup has no such
    // side effect and simply returns B's stored assignment.
    let final_generation = a.generation_id();
    let b_assignment = client_b
        .sync_group(group_id, final_generation, &join_b.member_id, &[])
        .await
        .expect("B's sync at the group's final, current generation must succeed");
    let mut b_final: Vec<u32> = b_assignment
        .into_iter()
        .find(|(t, _)| t == topic)
        .map(|(_, p)| p)
        .unwrap_or_default();
    b_final.sort_unstable();
    assert!(
        !b_final.is_empty(),
        "B must have gained at least one partition"
    );

    let mut combined: Vec<u32> = a_final.iter().chain(b_final.iter()).copied().collect();
    combined.sort_unstable();
    assert_eq!(
        combined,
        vec![0u32, 1, 2, 3],
        "together the two members must cover every partition exactly once"
    );

    // --- A regression guard, not proof of this feature: non-moving partitions resume
    // exactly where A left off, picking up r1/r2 (produced after the rebalance settled
    // but never yet fetched) without re-reading r0. This behavior — `next_offsets`
    // surviving a rebalance for whatever a member still owns — predates cooperative
    // rebalancing entirely (it came from #51's original `GroupConsumer`, untouched by
    // this feature) and holds just as well for an eager rebalance, so it passes even
    // with the intersection narrowing above disabled. Worth keeping as a guard against a
    // different regression, but the property that actually distinguishes cooperative
    // from eager is the handover-safety assertion above. ---
    let mut seen_on_kept: std::collections::HashMap<u32, Vec<String>> =
        std::collections::HashMap::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen_on_kept.values().map(Vec::len).sum::<usize>() < a_final.len() * 2
        && std::time::Instant::now() < deadline
    {
        let records = a.poll().await.unwrap();
        for (partition, frame) in records {
            seen_on_kept
                .entry(partition)
                .or_default()
                .push(String::from_utf8_lossy(&frame.payload).to_string());
        }
        sleep(Duration::from_millis(20)).await;
    }
    for &partition in &a_final {
        assert_eq!(
            seen_on_kept.get(&partition).cloned().unwrap_or_default(),
            vec![format!("p{partition}-r1"), format!("p{partition}-r2")],
            "partition {partition} never moved and must resume exactly where A left off \
             — no re-delivery of r0, no gap on r1/r2 either"
        );
    }

    // --- Moved partitions resume from A's committed offset: r0 (committed via the
    // automatic commit at the top of A's own rejoin) must not be re-delivered to B,
    // but r1 and r2 (which A never touched) must both still arrive. Read directly via
    // raw fetch/offset calls under B's own member identity — this is exactly what a
    // real consuming `GroupConsumer` would do with this assignment, just without a
    // second background-heartbeat actor racing the narrow round-one window. ---
    for &partition in &b_final {
        let committed = client_b
            .fetch_offset(group_id, topic, partition)
            .await
            .expect("fetching the committed offset must succeed");
        assert_eq!(
            committed, 0,
            "partition {partition} must be committed exactly through r0 — A's automatic \
             revoke-commit, not 0 records and not r1/r2 (which A never fetched)"
        );
        let start = committed + 1;
        let frames = client_b
            .fetch_as_member(
                topic,
                partition,
                start,
                64 * 1024,
                group_id,
                &join_b.member_id,
            )
            .await
            .expect("B's fetch of its newly-owned partition must succeed");
        let payloads: Vec<String> = frames
            .iter()
            .filter(|f| !f.is_control_marker())
            .map(|f| String::from_utf8_lossy(&f.payload).to_string())
            .collect();
        assert_eq!(
            payloads,
            vec![format!("p{partition}-r1"), format!("p{partition}-r2")],
            "partition {partition} moved to B and must resume from A's committed offset \
             — neither re-delivering r0 nor skipping r1/r2"
        );
    }
}

/// Complements scenario 79 from the giver's side: when a member's target loses exactly
/// one of its two partitions, round one's SyncGroup submission must revoke exactly that
/// one partition — not both (an eager-equivalent bug would hand out an empty keep-set
/// for everyone), and not neither (a no-op bug would never free anything up for round
/// two). Driven with raw `TestClient` calls, computing the expected round-one payload
/// with the real library functions (`assign_sticky` / `cooperative_round_one`) — exactly
/// what `GroupConsumer::rejoin`'s leader branch does internally — so this test is
/// checking the coordinator's actual protocol state, deterministically, rather than
/// racing a background heartbeat.
#[tokio::test]
async fn test_scenario_80_cooperative_round_one_revokes_only_the_moving_partition() {
    let env = start_test_server().await;
    let group_id = "coop-revoke-group";
    let topic = "coop-revoke-topic";

    let mut setup = TestClient::connect(env.addr).await.unwrap();
    setup.create_topic(topic, 2).await.unwrap();

    let mut client_a = TestClient::connect(env.addr).await.unwrap();
    let mut client_b = TestClient::connect(env.addr).await.unwrap();

    // A forms the group alone, holding both partitions.
    let join_a = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .expect("A must be able to join");
    assert_eq!(join_a.generation_id, 1);
    assert!(join_a.is_leader);
    let a_full = vec![hermes::protocol::wire::MemberAssignment {
        member_id: "member-a".to_string(),
        topic: topic.to_string(),
        partitions: vec![0, 1],
    }];
    let a_assignment = client_a
        .sync_group(group_id, 1, "member-a", &a_full)
        .await
        .expect("A's initial sync must succeed");
    assert_eq!(a_assignment, vec![(topic.to_string(), vec![0, 1])]);

    // B joins — a brand-new member always bumps the generation.
    let join_b = client_b
        .join_group(group_id, "member-b", &["cooperative-sticky"])
        .await
        .expect("B must be able to join");
    assert_eq!(join_b.generation_id, 2);
    assert!(!join_b.is_leader);

    // A rejoins to learn generation 2 (an already-known member joining a window that's
    // still open — existing, unchanged behavior), then computes round one exactly like
    // `GroupConsumer::rejoin`'s leader branch does.
    let rejoin_a = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .expect("A's rejoin must succeed");
    assert_eq!(rejoin_a.generation_id, 2);
    assert!(rejoin_a.is_leader);

    let previous: std::collections::HashMap<String, Vec<u32>> = std::collections::HashMap::from([
        ("member-a".to_string(), vec![0, 1]),
        ("member-b".to_string(), vec![]),
    ]);
    let member_ids = vec!["member-a".to_string(), "member-b".to_string()];
    let target = hermes::consumer::assign_sticky(2, &member_ids, &previous);
    let (round_one, needs_second_round) =
        hermes::consumer::cooperative_round_one(&target, &previous);
    assert!(
        needs_second_round,
        "one partition must be moving from A to B, so a second round is required"
    );

    let round_one_map: std::collections::HashMap<String, Vec<u32>> =
        round_one.iter().cloned().collect();
    let a_keep = round_one_map.get("member-a").cloned().unwrap_or_default();
    let b_keep = round_one_map.get("member-b").cloned().unwrap_or_default();

    // The distinguishing property under test: A's keep-set is exactly one partition
    // (the one NOT moving) — not both (nothing withheld at all) and not empty
    // (everything withheld, the eager-equivalent behavior).
    assert_eq!(
        a_keep.len(),
        1,
        "A must keep exactly one of its two partitions in round one, got {a_keep:?}"
    );
    assert!(
        b_keep.is_empty(),
        "B must receive nothing in round one — it never owned anything yet, got {b_keep:?}"
    );
    let a_revoked: Vec<u32> = vec![0u32, 1]
        .into_iter()
        .filter(|p| !a_keep.contains(p))
        .collect();
    assert_eq!(
        a_revoked.len(),
        1,
        "exactly one partition must be revoked from A, not both"
    );

    let round_one_assignments: Vec<hermes::protocol::wire::MemberAssignment> = round_one
        .iter()
        .map(
            |(member_id, partitions)| hermes::protocol::wire::MemberAssignment {
                member_id: member_id.clone(),
                topic: topic.to_string(),
                partitions: partitions.clone(),
            },
        )
        .collect();
    client_a
        .sync_group(group_id, 2, "member-a", &round_one_assignments)
        .await
        .expect("A's round-one submission must succeed");

    // Inspect what actually landed, from a separate connection, so the assertion is
    // about the coordinator's real state rather than what the test computed locally.
    let mut spectator = TestClient::connect(env.addr).await.unwrap();
    let (_, members) = spectator.describe_group(group_id).await.unwrap();
    let by_id: std::collections::HashMap<String, Vec<u32>> = members
        .into_iter()
        .map(|m| {
            let mut partitions: Vec<u32> = m
                .assigned_partitions
                .into_iter()
                .filter(|(t, _)| t == topic)
                .map(|(_, p)| p)
                .collect();
            partitions.sort_unstable();
            (m.member_id, partitions)
        })
        .collect();
    let mut expected_a_keep = a_keep.clone();
    expected_a_keep.sort_unstable();
    assert_eq!(
        by_id.get("member-a"),
        Some(&expected_a_keep),
        "the coordinator must reflect exactly A's computed keep-set, not everything and \
         not nothing"
    );
    assert_eq!(
        by_id.get("member-b"),
        Some(&Vec::new()),
        "B must own nothing in the coordinator's state after round one"
    );

    // B's own follower sync now succeeds (the group is Stable) and confirms it got
    // nothing either.
    let b_assignment = client_b
        .sync_group(group_id, 2, "member-b", &[])
        .await
        .expect("B's follower sync must succeed once A has submitted round one");
    assert_eq!(b_assignment, vec![(topic.to_string(), Vec::new())]);
}

/// Proves the two-round cooperative dance actually completes in exactly two rounds and
/// converges to the sticky target — not one (which would mean cooperative silently
/// degraded to eager) and not three-or-more (which would mean it oscillates instead of
/// converging). Continues directly from where scenario 80 leaves off, driving round two
/// through raw `TestClient` calls for the same determinism.
#[tokio::test]
async fn test_scenario_81_cooperative_rebalance_converges_in_exactly_two_rounds() {
    let env = start_test_server().await;
    let group_id = "coop-converge-group";
    let topic = "coop-converge-topic";

    let mut client_a = TestClient::connect(env.addr).await.unwrap();
    let mut client_b = TestClient::connect(env.addr).await.unwrap();
    TestClient::connect(env.addr)
        .await
        .unwrap()
        .create_topic(topic, 2)
        .await
        .unwrap();

    let join_a = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .unwrap();
    assert_eq!(join_a.generation_id, 1);
    let a_full = vec![hermes::protocol::wire::MemberAssignment {
        member_id: "member-a".to_string(),
        topic: topic.to_string(),
        partitions: vec![0, 1],
    }];
    client_a
        .sync_group(group_id, 1, "member-a", &a_full)
        .await
        .unwrap();

    let join_b = client_b
        .join_group(group_id, "member-b", &["cooperative-sticky"])
        .await
        .unwrap();
    assert_eq!(
        join_b.generation_id, 2,
        "round one: B's join bumps the generation once"
    );

    let rejoin_a = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .unwrap();
    assert_eq!(rejoin_a.generation_id, 2);

    let previous: std::collections::HashMap<String, Vec<u32>> = std::collections::HashMap::from([
        ("member-a".to_string(), vec![0, 1]),
        ("member-b".to_string(), vec![]),
    ]);
    let member_ids = vec!["member-a".to_string(), "member-b".to_string()];
    let target = hermes::consumer::assign_sticky(2, &member_ids, &previous);
    let (round_one, needs_second_round) =
        hermes::consumer::cooperative_round_one(&target, &previous);
    assert!(needs_second_round);

    let round_one_assignments: Vec<hermes::protocol::wire::MemberAssignment> = round_one
        .iter()
        .map(
            |(member_id, partitions)| hermes::protocol::wire::MemberAssignment {
                member_id: member_id.clone(),
                topic: topic.to_string(),
                partitions: partitions.clone(),
            },
        )
        .collect();
    client_a
        .sync_group(group_id, 2, "member-a", &round_one_assignments)
        .await
        .unwrap();
    // The group is Stable again at generation 2 — round one is done.

    // Round two: A (the leader), already a known member, requests a fresh round by
    // calling JoinGroup again while the group is Stable — the coordinator change this
    // feature needed (`GroupCoordinator::join_group`'s cooperative branch) bumps the
    // generation for exactly this call.
    let round_two_join = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .expect("A must be able to request a second round");
    assert_eq!(
        round_two_join.generation_id, 3,
        "round two: the generation must advance again, with no membership change at all"
    );
    assert!(round_two_join.is_leader);

    // Round two reuses `target` as-is (see `GroupConsumer::rejoin`'s reasoning) rather
    // than recomputing it.
    let target_assignments: Vec<hermes::protocol::wire::MemberAssignment> = target
        .iter()
        .map(
            |(member_id, partitions)| hermes::protocol::wire::MemberAssignment {
                member_id: member_id.clone(),
                topic: topic.to_string(),
                partitions: partitions.clone(),
            },
        )
        .collect();
    let a_final = client_a
        .sync_group(group_id, 3, "member-a", &target_assignments)
        .await
        .expect("A's round-two submission must succeed");

    let target_map: std::collections::HashMap<String, Vec<u32>> = target.iter().cloned().collect();
    assert_eq!(
        a_final,
        vec![(topic.to_string(), target_map["member-a"].clone())]
    );

    // B notices the new generation and rejoins, then syncs to get its final share.
    let b_rejoin = client_b
        .join_group(group_id, "member-b", &["cooperative-sticky"])
        .await
        .expect("B must be able to rejoin for round two");
    assert_eq!(b_rejoin.generation_id, 3);
    let b_final = client_b
        .sync_group(group_id, 3, "member-b", &[])
        .await
        .expect("B's round-two sync must succeed");
    assert_eq!(
        b_final,
        vec![(topic.to_string(), target_map["member-b"].clone())]
    );

    // Convergence: the group's final state exactly matches the sticky target, and
    // re-running round one's own logic against that final state shows nothing further
    // would need to be withheld — i.e. a third round is never triggered.
    let mut spectator = TestClient::connect(env.addr).await.unwrap();
    let (state_str, members) = spectator.describe_group(group_id).await.unwrap();
    assert_eq!(state_str, "Stable");
    let final_ownership: std::collections::HashMap<String, Vec<u32>> = members
        .into_iter()
        .map(|m| {
            let mut partitions: Vec<u32> = m
                .assigned_partitions
                .into_iter()
                .filter(|(t, _)| t == topic)
                .map(|(_, p)| p)
                .collect();
            partitions.sort_unstable();
            (m.member_id, partitions)
        })
        .collect();
    assert_eq!(
        final_ownership, target_map,
        "the group must land exactly on the sticky target"
    );

    let (_, no_third_round) = hermes::consumer::cooperative_round_one(&target, &final_ownership);
    assert!(
        !no_third_round,
        "the final state already matches the target, so nothing more should ever be \
         withheld — a third round must never be triggered"
    );
}

/// Eager groups (`range`, `roundrobin`, plain `sticky`) must rebalance exactly as they
/// did before this feature existed: one generation bump, one `SyncGroup` submission,
/// full target handed out immediately — no intersection narrowing, no withheld
/// partitions, no second round. `GroupConsumer::rejoin`'s cooperative branch must be
/// entered only for a negotiated `cooperative-sticky` protocol.
#[tokio::test]
async fn test_scenario_82_eager_group_still_rebalances_in_a_single_round() {
    let env = start_test_server().await;
    let topic = "eager_unaffected_topic";
    let group_id = "eager_unaffected_group";
    let num_partitions = 4u32;

    let mut setup = TestClient::connect(env.addr).await.unwrap();
    setup.create_topic(topic, num_partitions).await.unwrap();

    let config = GroupConsumerConfig {
        group_id: group_id.to_string(),
        topic: topic.to_string(),
        protocols: vec!["sticky".to_string()],
        heartbeat_interval: Duration::from_millis(60),
        session_timeout: Duration::from_secs(3),
        ..GroupConsumerConfig::default()
    };

    let client_a = TestClient::connect(env.addr).await.unwrap();
    let mut a = GroupConsumer::join(client_a, config.clone()).await.unwrap();
    let mut a_assignment = a.assignment().to_vec();
    a_assignment.sort_unstable();
    assert_eq!(a_assignment, vec![0u32, 1, 2, 3]);
    let generation_after_bootstrap = a.generation_id();

    let client_b = TestClient::connect(env.addr).await.unwrap();
    let join_config = config.clone();
    let b_handle = tokio::spawn(async move { GroupConsumer::join(client_b, join_config).await });

    // Drive A's poll loop until it notices B and rebalances.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let _ = a.poll().await.unwrap();
        if a.assignment().len() < num_partitions as usize {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "A never rebalanced after B joined"
        );
        sleep(Duration::from_millis(20)).await;
    }

    // Exactly one generation bump for this membership change — no cooperative second
    // round exists for an eager protocol.
    assert_eq!(
        a.generation_id(),
        generation_after_bootstrap + 1,
        "an eager group must advance the generation exactly once per membership change"
    );

    let b = b_handle
        .await
        .unwrap()
        .expect("B must be able to join and sync in a single round");

    // The distinguishing assertion: B's very first join+sync cycle already carries its
    // full, final share — not an empty round-one placeholder. This is what would break
    // if the cooperative narrowing logic leaked into the eager path.
    assert!(
        !b.assignment().is_empty(),
        "an eager group must hand the new member its real assignment immediately, not \
         an empty round-one placeholder"
    );

    let mut a_final = a.assignment().to_vec();
    a_final.sort_unstable();
    let mut b_final = b.assignment().to_vec();
    b_final.sort_unstable();
    let mut combined: Vec<u32> = a_final.iter().chain(b_final.iter()).copied().collect();
    combined.sort_unstable();
    assert_eq!(combined, vec![0u32, 1, 2, 3]);
}

/// Generation fencing: two cooperative rounds mean the group's generation advances
/// twice for the same membership change. A member still holding round one's now-stale
/// generation id must be rejected — not served round two's assignment, and not treated
/// as still current — once round two has landed.
#[tokio::test]
async fn test_scenario_83_stale_round_one_generation_is_rejected_after_round_two() {
    let env = start_test_server().await;
    let group_id = "coop-fencing-group";
    let topic = "coop-fencing-topic";

    let mut client_a = TestClient::connect(env.addr).await.unwrap();
    let mut client_b = TestClient::connect(env.addr).await.unwrap();
    TestClient::connect(env.addr)
        .await
        .unwrap()
        .create_topic(topic, 2)
        .await
        .unwrap();

    let join_a = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .unwrap();
    let a_full = vec![hermes::protocol::wire::MemberAssignment {
        member_id: "member-a".to_string(),
        topic: topic.to_string(),
        partitions: vec![0, 1],
    }];
    client_a
        .sync_group(group_id, join_a.generation_id, "member-a", &a_full)
        .await
        .unwrap();

    let join_b = client_b
        .join_group(group_id, "member-b", &["cooperative-sticky"])
        .await
        .unwrap();
    let round_one_generation = join_b.generation_id;

    let rejoin_a = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .unwrap();
    assert_eq!(rejoin_a.generation_id, round_one_generation);

    let previous: std::collections::HashMap<String, Vec<u32>> = std::collections::HashMap::from([
        ("member-a".to_string(), vec![0, 1]),
        ("member-b".to_string(), vec![]),
    ]);
    let member_ids = vec!["member-a".to_string(), "member-b".to_string()];
    let target = hermes::consumer::assign_sticky(2, &member_ids, &previous);
    let (round_one, _) = hermes::consumer::cooperative_round_one(&target, &previous);
    let round_one_assignments: Vec<hermes::protocol::wire::MemberAssignment> = round_one
        .iter()
        .map(
            |(member_id, partitions)| hermes::protocol::wire::MemberAssignment {
                member_id: member_id.clone(),
                topic: topic.to_string(),
                partitions: partitions.clone(),
            },
        )
        .collect();
    client_a
        .sync_group(
            group_id,
            round_one_generation,
            "member-a",
            &round_one_assignments,
        )
        .await
        .unwrap();

    // Round two: A requests and drives a fresh round, advancing the generation again.
    let round_two_join = client_a
        .join_group(group_id, "member-a", &["cooperative-sticky"])
        .await
        .unwrap();
    let round_two_generation = round_two_join.generation_id;
    assert_ne!(
        round_two_generation, round_one_generation,
        "round two must be a genuinely new generation"
    );
    let target_assignments: Vec<hermes::protocol::wire::MemberAssignment> = target
        .iter()
        .map(
            |(member_id, partitions)| hermes::protocol::wire::MemberAssignment {
                member_id: member_id.clone(),
                topic: topic.to_string(),
                partitions: partitions.clone(),
            },
        )
        .collect();
    client_a
        .sync_group(
            group_id,
            round_two_generation,
            "member-a",
            &target_assignments,
        )
        .await
        .unwrap();

    // The group has now fully stabilized at round two's generation. A follower still
    // presenting round one's stale generation id must be fenced, not served.
    let stale_sync = client_b
        .sync_group(group_id, round_one_generation, "member-b", &[])
        .await;
    assert!(
        stale_sync.is_err(),
        "SyncGroup at round one's stale generation must not succeed once round two has \
         landed"
    );
    assert!(
        stale_sync
            .unwrap_err()
            .to_string()
            .contains("Generation mismatch"),
        "the failure must be a recognisable generation mismatch, not some other error"
    );

    let stale_heartbeat = client_b
        .heartbeat(group_id, round_one_generation, "member-b")
        .await;
    assert!(
        stale_heartbeat.is_err(),
        "a heartbeat at round one's stale generation must not succeed once the group has \
         fully stabilized past it — even for a cooperative group, once state is Stable \
         there is no further grace period for a generation that old"
    );

    // Confirm the group's real state never regressed to round one's assignment: B's
    // real (round two) generation still works and returns its final share.
    let b_current = client_b
        .sync_group(group_id, round_two_generation, "member-b", &[])
        .await
        .expect("B's real, current generation must still work");
    let target_map: std::collections::HashMap<String, Vec<u32>> = target.into_iter().collect();
    assert_eq!(
        b_current,
        vec![(topic.to_string(), target_map["member-b"].clone())]
    );
}

/// Sabotages the OS-level file descriptor backing `path` so the next `sync()` through any
/// `std::fs::File` still pointing at it fails, without ever fully closing the fd. Used to
/// force a real I/O failure in a durability-sensitive step without any fault-injection seam
/// in production code.
///
/// Finds the fd via `/proc/self/fd`, then `dup2`s a pipe's write end onto it after closing
/// the pipe's read end: a write end whose reader is gone still accepts `close()` normally,
/// but `fsync`/`fdatasync` on a pipe always fails with `EINVAL` (pipes aren't syncable
/// objects). Redirecting instead of outright closing matters: closing a fd the standard
/// library still owns is exactly the double-close pattern its IO-safety hardening watches
/// for, and it aborts the whole process the moment the owning `File`'s `Drop` tries to close
/// the same fd again. `dup2` keeps the fd number continuously open (just repointed), so that
/// `Drop` closes a live fd like any other and nothing aborts.
///
/// Linux-only: relies on `/proc/self/fd` and `libc`, unavailable on the Windows CI target.
#[cfg(target_os = "linux")]
fn sabotage_fd_for(path: &std::path::Path) {
    let target = std::fs::canonicalize(path).expect("path must exist to steal its fd");
    let fd_dir = std::fs::read_dir("/proc/self/fd").expect("/proc/self/fd must be readable");
    let mut target_fd: Option<i32> = None;
    for entry in fd_dir.flatten() {
        let fd_path = entry.path();
        if std::fs::read_link(&fd_path).ok().as_deref() != Some(target.as_path()) {
            continue;
        }
        target_fd = fd_path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.parse().ok());
        break;
    }
    let target_fd = target_fd.unwrap_or_else(|| panic!("no open fd found for {:?}", target));

    unsafe {
        let mut pipe_fds = [0i32; 2];
        assert_eq!(libc::pipe(pipe_fds.as_mut_ptr()), 0, "pipe() failed");
        let [read_end, write_end] = pipe_fds;
        assert_eq!(libc::close(read_end), 0, "closing pipe read end failed");
        assert_eq!(
            libc::dup2(write_end, target_fd),
            target_fd,
            "dup2 onto the target fd failed"
        );
        assert_eq!(
            libc::close(write_end),
            0,
            "closing spare pipe write end failed"
        );
    }
}

/// Issue #24's remaining item, leader side: `await_metadata_commit`'s doc comment says "the
/// leader counts itself: the record is already durable in its own log by the time this is
/// called" — but under `FlushPolicy::AsyncPeriodic` (the default), the leader's own append
/// via `produce_frame` was never actually forced durable, so that claim was false. This
/// proves the leader's own append is now genuinely on the critical path: when it cannot be
/// made durable, `create_topic` must fail outright rather than silently counting an unsynced
/// local copy toward the majority and applying it anyway.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_scenario_84_leader_metadata_append_must_be_durable_before_it_counts_itself() {
    let dir = TestDataDirGuard::new("leader_meta_durability");
    // A sole controller (no peer_addrs) with the default `FlushPolicy::AsyncPeriodic` —
    // exactly the configuration under which the leader's self-append durability claim was
    // false before this fix.
    let engine = StorageEngine::new(EngineConfig {
        node_id: 1,
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        ..EngineConfig::default()
    })
    .unwrap();
    assert!(engine.is_leader(), "a sole node is its own controller");

    // Prime `__cluster_metadata`'s on-disk files — they don't exist before the first write.
    engine
        .create_topic("prime", 1)
        .await
        .expect("priming write must succeed with the fd intact");

    let index_path = dir
        .path
        .join("__cluster_metadata-0")
        .join(format!("{:020}.index", 0u64));
    sabotage_fd_for(&index_path);

    let result = engine.create_topic("should_not_apply", 1).await;
    assert!(
        result.is_err(),
        "the leader's own metadata append must fail loudly when it cannot be made durable, \
         not silently succeed on an unsynced local copy"
    );
    assert!(
        !engine.topic_is_registered("should_not_apply"),
        "a metadata record whose durability could not be confirmed must not be applied, even \
         on the leader that proposed it"
    );
}

/// Issue #18 stage 1b-ii, headline claim: `produce.record.batches.enable` must be
/// perfectly transparent to a client. The same produce request, sent against two
/// otherwise-identical engines that differ only in this flag, must land at the same
/// offsets and be fetchable back to the same records either way — one engine is writing
/// a `RecordFrame` per record on disk, the other one `RecordBatch` for the whole
/// request, and nothing outside `StorageEngine`/`SegmentManager` should be able to tell
/// the difference. Asserted as equality between the two modes' results, not as two
/// separate checks, per the plan's explicit ask.
#[tokio::test]
async fn test_scenario_85_produce_batch_flag_on_and_off_are_indistinguishable_to_a_client() {
    let dir_off = TestDataDirGuard::new("batch_flag_off_roundtrip");
    let dir_on = TestDataDirGuard::new("batch_flag_on_roundtrip");

    let engine_off = StorageEngine::new(EngineConfig {
        data_dir: dir_off.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        produce_record_batches_enable: false,
        ..EngineConfig::default()
    })
    .unwrap();
    let engine_on = StorageEngine::new(EngineConfig {
        data_dir: dir_on.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        produce_record_batches_enable: true,
        ..EngineConfig::default()
    })
    .unwrap();

    let topic = "batch_flag_roundtrip_topic";
    let records: Vec<bytes::Bytes> = (0..7)
        .map(|i| bytes::Bytes::from(format!("record-payload-{i}")))
        .collect();

    fn make_params<'a>(
        topic: &'a str,
        records: &'a [bytes::Bytes],
    ) -> hermes::server::engine::ProduceBatchParams<'a> {
        hermes::server::engine::ProduceBatchParams {
            topic,
            key: "",
            transaction_id: None,
            num_partitions: 1,
            producer_id: 555,
            producer_epoch: 2,
            base_sequence: 0,
            records,
        }
    }

    let (partition_off, first_off, last_off) = engine_off
        .produce_batch(make_params(topic, &records))
        .await
        .unwrap();
    let (partition_on, first_on, last_on) = engine_on
        .produce_batch(make_params(topic, &records))
        .await
        .unwrap();

    assert_eq!(
        (partition_off, first_off, last_off),
        (partition_on, first_on, last_on),
        "partition assignment and offset range must be identical whichever path wrote it"
    );

    let fetched_off = engine_off
        .fetch(topic, partition_off, 0, 1024 * 1024)
        .await
        .unwrap();
    let fetched_on = engine_on
        .fetch(topic, partition_on, 0, 1024 * 1024)
        .await
        .unwrap();

    assert_eq!(fetched_off.len(), records.len());
    assert_eq!(fetched_on.len(), records.len());
    for i in 0..records.len() {
        assert_eq!(
            fetched_off[i].offset, fetched_on[i].offset,
            "offset mismatch at record {i}"
        );
        assert_eq!(
            fetched_off[i].decompress_payload().unwrap(),
            fetched_on[i].decompress_payload().unwrap(),
            "decoded payload mismatch at record {i}"
        );
        assert_eq!(
            fetched_off[i].decompress_payload().unwrap().as_ref(),
            records[i].as_ref(),
            "decoded payload must match what was produced, record {i}"
        );
    }
}

/// A batch is atomic on disk, so serving a fetch whose `start_offset` lands in the
/// middle of one requires decoding the whole batch and filtering — never returning
/// records the caller didn't ask for, and never returning nothing just because the
/// requested offset isn't the batch's own base offset.
#[tokio::test]
async fn test_scenario_86_fetch_from_an_offset_inside_a_batch_returns_only_records_from_there_on() {
    let dir = TestDataDirGuard::new("batch_mid_fetch");
    let engine = StorageEngine::new(EngineConfig {
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        produce_record_batches_enable: true,
        ..EngineConfig::default()
    })
    .unwrap();

    let topic = "batch_mid_fetch_topic";
    let records: Vec<bytes::Bytes> = (0..6)
        .map(|i| bytes::Bytes::from(format!("rec-{i}")))
        .collect();
    let params = hermes::server::engine::ProduceBatchParams {
        topic,
        key: "",
        transaction_id: None,
        num_partitions: 1,
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: 0,
        records: &records,
    };
    let (partition, first_offset, last_offset) = engine.produce_batch(params).await.unwrap();
    assert_eq!(first_offset, 0);
    assert_eq!(last_offset, 5, "all six records must land in one batch");

    for start in 0u64..=5 {
        let fetched = engine
            .fetch(topic, partition, start, 1024 * 1024)
            .await
            .unwrap();
        let offsets: Vec<u64> = fetched.iter().map(|f| f.offset).collect();
        let expected: Vec<u64> = (start..=5).collect();
        assert_eq!(
            offsets, expected,
            "fetch starting at offset {start} (inside the batch) must return exactly the \
             records from there onward"
        );
        for (i, frame) in fetched.iter().enumerate() {
            assert_eq!(
                frame.payload.as_ref(),
                records[(start as usize) + i].as_ref()
            );
        }
    }
}

/// With the flag on, the `RecordBatch` actually written to disk must carry the
/// producer id, producer epoch, and base sequence from `ProduceBatchParams`, plus the
/// partition's own `leader_epoch()` — that's the entire point of giving the batch
/// header those fields in stage 1a. Verified by reading the segment file directly and
/// decoding it, bypassing `fetch` (which synthesizes plain frames and would hide
/// exactly what this test needs to see).
#[tokio::test]
async fn test_scenario_87_batch_on_disk_carries_producer_and_leader_epoch_metadata() {
    let dir = TestDataDirGuard::new("batch_metadata");
    let engine = StorageEngine::new(EngineConfig {
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        produce_record_batches_enable: true,
        ..EngineConfig::default()
    })
    .unwrap();

    let topic = "batch_metadata_topic";
    // Force the partition to exist with a distinctive, non-default leader_epoch first,
    // so the assertion below can't pass by coincidence with a zero-valued default.
    let pm = engine.get_or_create_partition(topic, 0).unwrap();
    pm.update_leadership(1, 42, vec![1], vec![1]);

    let records: Vec<bytes::Bytes> = vec![
        bytes::Bytes::from_static(b"a"),
        bytes::Bytes::from_static(b"b"),
        bytes::Bytes::from_static(b"c"),
    ];
    let params = hermes::server::engine::ProduceBatchParams {
        topic,
        key: "",
        transaction_id: None,
        num_partitions: 1,
        producer_id: 777_888,
        producer_epoch: 5,
        base_sequence: 3,
        records: &records,
    };
    engine.produce_batch(params).await.unwrap();

    let log_path = dir
        .path
        .join(format!("{}-0", topic))
        .join(format!("{:020}.log", 0u64));
    let raw = std::fs::read(&log_path).unwrap();
    let (entry, consumed) = hermes::segment::decode_entry(&raw).unwrap();
    assert_eq!(
        consumed,
        raw.len(),
        "exactly one entry — the whole batch — should be on disk"
    );
    let batch = match entry {
        hermes::segment::LogEntry::Batch(b) => b,
        hermes::segment::LogEntry::Frame(_) => {
            panic!("expected a RecordBatch on disk with produce.record.batches.enable on")
        }
    };

    assert_eq!(batch.producer_id, 777_888);
    assert_eq!(batch.producer_epoch, 5);
    assert_eq!(batch.base_sequence, 3);
    assert_eq!(
        batch.leader_epoch, 42,
        "leader_epoch must come from the partition's own leader_epoch(), not a default"
    );
    assert_eq!(batch.record_count, 3);
}

/// With the flag off (the default), every entry written to disk must still be a plain
/// `RecordFrame` — this is the "verified beyond existing tests pass" check the plan
/// asks for: it inspects the actual on-disk bytes rather than trusting that no other
/// test happened to notice a `RecordBatch` sneak in.
#[tokio::test]
async fn test_scenario_88_batch_flag_off_still_writes_only_plain_frames_on_disk() {
    let dir = TestDataDirGuard::new("batch_flag_off_raw");
    let engine = StorageEngine::new(EngineConfig {
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        produce_record_batches_enable: false,
        ..EngineConfig::default()
    })
    .unwrap();
    assert!(
        !engine.config().produce_record_batches_enable,
        "false must be the config default even when not set explicitly"
    );

    let topic = "batch_flag_off_raw_topic";
    let records: Vec<bytes::Bytes> = (0..4)
        .map(|i| bytes::Bytes::from(format!("r{i}")))
        .collect();
    let params = hermes::server::engine::ProduceBatchParams {
        topic,
        key: "",
        transaction_id: None,
        num_partitions: 1,
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: 0,
        records: &records,
    };
    let (partition, first_offset, last_offset) = engine.produce_batch(params).await.unwrap();
    assert_eq!(first_offset, 0);
    assert_eq!(last_offset, 3);

    let log_path = dir
        .path
        .join(format!("{}-{}", topic, partition))
        .join(format!("{:020}.log", 0u64));
    let raw = std::fs::read(&log_path).unwrap();
    let mut cursor = 0usize;
    let mut decoded_offsets = Vec::new();
    while cursor < raw.len() {
        let (entry, consumed) = hermes::segment::decode_entry(&raw[cursor..]).unwrap();
        match entry {
            hermes::segment::LogEntry::Frame(f) => decoded_offsets.push(f.offset),
            hermes::segment::LogEntry::Batch(_) => panic!(
                "the flag is off — every on-disk entry must still be a plain RecordFrame, \
                 exactly as before this stage; a RecordBatch appearing here means the gate leaked"
            ),
        }
        cursor += consumed;
    }
    assert_eq!(decoded_offsets, vec![0, 1, 2, 3]);
}

/// `PartitionManager::truncate_after` end-to-end: truncating at a point inside a batch
/// must leave LEO and the committed high watermark consistent with what's actually left
/// on disk (the batch's base offset), not the literal offset that was requested — the
/// same divergence bug fixed in `SegmentManager::truncate_after`, one layer up.
#[tokio::test]
async fn test_scenario_89_partition_truncate_after_mid_batch_leaves_consistent_watermarks() {
    let dir = TestDataDirGuard::new("batch_truncate_partition");
    let engine = StorageEngine::new(EngineConfig {
        data_dir: dir.path.clone(),
        bind_addr: "127.0.0.1:0".to_string(),
        produce_record_batches_enable: true,
        ..EngineConfig::default()
    })
    .unwrap();

    let topic = "batch_truncate_partition_topic";
    let records: Vec<bytes::Bytes> = (0..5)
        .map(|i| bytes::Bytes::from(format!("v{i}")))
        .collect();
    let params = hermes::server::engine::ProduceBatchParams {
        topic,
        key: "",
        transaction_id: None,
        num_partitions: 1,
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: 0,
        records: &records,
    };
    let (partition, first_offset, last_offset) = engine.produce_batch(params).await.unwrap();
    assert_eq!((first_offset, last_offset), (0, 4), "one 5-record batch");

    let pm = engine.get_or_create_partition(topic, partition).unwrap();
    assert_eq!(pm.latest_offset(), 5);

    // Truncate at offset 3 — inside the batch [0, 4]. The whole batch must be removed,
    // and LEO must reflect that (0), not the literal requested offset (3).
    pm.truncate_after(3).unwrap();
    assert_eq!(
        pm.latest_offset(),
        0,
        "LEO must drop to the batch's base offset, not the requested mid-batch offset"
    );
    assert_eq!(
        pm.high_watermark(),
        0,
        "committed HW must never exceed LEO after truncation"
    );

    // The freed range is reusable and the log is genuinely empty of the old batch.
    let refill: Vec<bytes::Bytes> = vec![bytes::Bytes::from_static(b"replacement")];
    let refill_params = hermes::server::engine::ProduceBatchParams {
        topic,
        key: "",
        transaction_id: None,
        num_partitions: 1,
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: 0,
        records: &refill,
    };
    let (_, refill_first, refill_last) = engine.produce_batch(refill_params).await.unwrap();
    assert_eq!((refill_first, refill_last), (0, 0));
}
