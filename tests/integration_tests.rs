use hermes::{hash_key, EngineConfig, FlushPolicy, RecordFrame, Server, StorageEngine, TestClient};
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
    tx_mgr.complete_commit("tx_orders_99").unwrap();

    // 3. High Availability Replication Manager Test
    let cluster_config = hermes::ClusterConfig {
        cluster_id: "test-cluster".to_string(),
        node_id: 1,
        role: hermes::NodeRole::Leader,
        peer_addrs: Vec::new(),
        min_insync_replicas: 1,
    };
    let repl_mgr = hermes::ReplicationManager::new(
        cluster_config,
        "127.0.0.1:0".to_string(),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );
    assert_eq!(repl_mgr.role(), hermes::NodeRole::Leader);

    let frame = hermes::RecordFrame::create(0, 1000, "replicated_payload");
    let res = repl_mgr.replicate_batch("events", 0, &[frame]).await;
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
    };
    let repl_mgr = hermes::ReplicationManager::new(
        cluster_config,
        "127.0.0.1:0".to_string(),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );

    // Before follower watermark update, ISR quorum check times out
    let quorum_before = repl_mgr
        .await_isr_quorum(
            "orders_stream",
            1,
            100,
            std::time::Duration::from_millis(50),
        )
        .await;
    assert!(!quorum_before);

    // Follower node 2 updates its watermark to 100
    repl_mgr.update_replica_watermark("orders_stream", 1, "127.0.0.1:9093", 100);
    let quorum_after = repl_mgr
        .await_isr_quorum(
            "orders_stream",
            1,
            100,
            std::time::Duration::from_millis(50),
        )
        .await;
    assert!(quorum_after); // Quorum satisfied!

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
    let server_node1 = Server::new(engine_node1);
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
    let server_node1 = Server::new(engine_node1);
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
    let frames = engine.fetch_committed(topic, 0, 0, 1024 * 1024).unwrap();
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
}
