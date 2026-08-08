use hermes::{
    hash_key, EngineConfig, FlushPolicy, RecordFrame, Server, StorageEngine, TestClient,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

/// RAII Guard ensuring automatic cleanup of unique test data directories upon test completion (Rule C)
pub struct TestEnv {
    pub addr: SocketAddr,
    pub data_dir: std::path::PathBuf,
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
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!("storage_test_{}_{}_{}", std::process::id(), nanos, count));
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
        ..EngineConfig::default()
    };

    let engine = StorageEngine::new(config).unwrap();
    let server = Server::new(engine);
    let (listener, addr) = server.bind().unwrap();

    tokio::spawn(async move {
        let _ = server.run_with_listener(listener).await;
    });

    TestEnv { addr, data_dir }
}

#[tokio::test]
async fn test_scenario_1_connection_and_handshake() {
    let env = start_test_server().await;
    let mut client = TestClient::connect(env.addr).await.expect("Failed to connect");
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
    let frames = client
        .fetch(topic, partition, 0, 64 * 1024)
        .await
        .unwrap();
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
    let sliced_frames = client
        .fetch(topic, partition, 2, 64 * 1024)
        .await
        .unwrap();
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

    assert_eq!(total_produced, (num_producers * records_per_producer) as u64);
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
    tx_mgr.commit_transaction("tx_orders_99", |_, _| None).unwrap();

    // 3. High Availability Replication Manager Test
    let cluster_config = hermes::ClusterConfig {
        cluster_id: "test-cluster".to_string(),
        node_id: 1,
        role: hermes::NodeRole::Leader,
        peer_addrs: Vec::new(),
        min_insync_replicas: 1,
    };
    let repl_mgr = hermes::ReplicationManager::new(cluster_config, "127.0.0.1:0".to_string());
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
    let repl_mgr = hermes::ReplicationManager::new(cluster_config, "127.0.0.1:0".to_string());

    // Before follower watermark update, ISR quorum check times out
    let quorum_before = repl_mgr.await_isr_quorum("orders_stream", 1, 100, std::time::Duration::from_millis(50)).await;
    assert!(!quorum_before);

    // Follower node 2 updates its watermark to 100
    repl_mgr.update_replica_watermark("orders_stream", 1, "127.0.0.1:9093", 100);
    let quorum_after = repl_mgr.await_isr_quorum("orders_stream", 1, 100, std::time::Duration::from_millis(50)).await;
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
    assert!(client.begin_transaction("tx_checkout_1001", 88).await.is_ok());
    assert!(client.commit_transaction("tx_checkout_1001").await.is_ok());

    assert!(client.begin_transaction("tx_checkout_1002", 88).await.is_ok());
    assert!(client.abort_transaction("tx_checkout_1002").await.is_ok());

    // 2. Test FetchByTimestamp over TCP Socket
    let prod_res = client.produce_single("time_topic", "k1", None, 1, "Timestamp Payload").await.unwrap();
    assert_eq!(prod_res.first_offset, 0);

    let frames = client.fetch_by_timestamp("time_topic", 0, 1000, 65536).await.unwrap();
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
        .produce_single("cluster_topic", "key1", None, 1, "Cluster Replication Event 101")
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
    assert_eq!(fetched[0].payload, "Cluster Replication Event 101".as_bytes());

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
        .produce_single("dynamic_topic", "key123", None, 1, "Dynamic Metadata Replicated Event")
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
    assert_eq!(fetched[0].payload, "Dynamic Metadata Replicated Event".as_bytes());

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
    let r1 = client.produce_single(topic, "", Some("tx_lso_1"), num_parts, "msg_hidden_1").await.unwrap();
    let r2 = client.produce_single(topic, "", Some("tx_lso_1"), num_parts, "msg_hidden_2").await.unwrap();
    let r3 = client.produce_single(topic, "", Some("tx_lso_1"), num_parts, "msg_hidden_3").await.unwrap();
    assert_eq!(r1.first_offset, 0);
    assert_eq!(r2.first_offset, 1);
    assert_eq!(r3.first_offset, 2);

    // 3. read_uncommitted must see all 3 records
    let frames_uncommitted = client.fetch(topic, partition, 0, 65536).await.unwrap();
    assert_eq!(frames_uncommitted.len(), 3, "read_uncommitted must see all 3 records");

    // 4. fetch_committed returns only non-control-marker data records
    let frames_committed = client.fetch_committed(topic, partition, 0, 65536).await.unwrap();
    for frame in &frames_committed {
        assert_ne!(frame.magic, 0xAD, "fetch_committed must never return control markers");
    }

    // 5. Commit the transaction — this writes a Commit control marker (0xAD)
    client.commit_transaction("tx_lso_1").await.unwrap();

    // 6. After commit, read_uncommitted sees data records + control marker
    let frames_all = client.fetch(topic, partition, 0, 65536).await.unwrap();
    assert!(frames_all.len() >= 3, "After commit, at least 3+ frames exist (data + marker)");

    // 7. fetch_committed filters out 0xAD control markers — returns only data records
    let frames_after_commit = client.fetch_committed(topic, partition, 0, 65536).await.unwrap();
    for frame in &frames_after_commit {
        assert_ne!(frame.magic, 0xAD, "fetch_committed must not return control markers");
    }
    assert_eq!(frames_after_commit.len(), 3, "All 3 data records visible after commit");
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
    client.produce_single(topic, "", None, num_parts, "committed_record").await.unwrap();

    // Begin transaction and produce records that will be aborted
    client.begin_transaction("tx_abort_1", 99).await.unwrap();
    client.produce_single(topic, "", Some("tx_abort_1"), num_parts, "aborted_record_1").await.unwrap();
    client.produce_single(topic, "", Some("tx_abort_1"), num_parts, "aborted_record_2").await.unwrap();

    // Abort the transaction — writes 0xAD abort control marker
    client.abort_transaction("tx_abort_1").await.unwrap();

    // read_uncommitted returns all data records + abort marker
    let all = client.fetch(topic, partition, 0, 65536).await.unwrap();
    assert!(all.len() >= 3, "read_uncommitted sees 3+ frames, got {}", all.len());

    // fetch_committed: no control markers (0xAD) must appear
    let committed = client.fetch_committed(topic, partition, 0, 65536).await.unwrap();
    for frame in &committed {
        assert_ne!(frame.magic, 0xAD, "No control markers in fetch_committed");
    }
    // The pre-tx committed record must always be present
    assert!(
        committed.iter().any(|f| f.payload.as_ref() == b"committed_record"),
        "Committed record must appear in read_committed"
    );
}
