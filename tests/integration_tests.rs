use cli_file_appender::{
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
async fn start_test_server() -> TestEnv {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!("storage_test_{}", nanos));
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
    let resp1 = client
        .produce_single(topic, key1, num_partitions, "payload_alpha")
        .await
        .unwrap();

    assert_eq!(resp1.assigned_partition, expected_partition1);
    assert_eq!(resp1.first_offset, 0);
    assert_eq!(resp1.last_offset, 0);

    // 2. Batched message produce with same key
    let batch = vec!["payload_beta_1", "payload_beta_2", "payload_beta_3"];
    let resp2 = client
        .produce_batch(topic, key1, num_partitions, &batch)
        .await
        .unwrap();

    assert_eq!(resp2.assigned_partition, expected_partition1);
    assert_eq!(resp2.first_offset, 1);
    assert_eq!(resp2.last_offset, 3);

    // 3. Batched message produce with different key
    let key2 = "user_9999";
    let expected_partition2 = hash_key(key2.as_bytes(), num_partitions as usize);
    let resp3 = client
        .produce_batch(topic, key2, num_partitions, &["msg_x", "msg_y"])
        .await
        .unwrap();

    assert_eq!(resp3.assigned_partition, expected_partition2);
    if expected_partition1 == expected_partition2 {
        assert_eq!(resp3.first_offset, 4);
        assert_eq!(resp3.last_offset, 5);
    } else {
        assert_eq!(resp3.first_offset, 0);
        assert_eq!(resp3.last_offset, 1);
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
        .produce_batch(topic, "", 1, &records)
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
    client
        .produce_batch(topic, "", 1, &["m1", "m2", "m3"])
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
                    .produce_single(&t_name, &key, num_partitions, payload)
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
    let _ = client.send_raw_bytes(&truncated_frame).await;

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
        .produce_single("reconnect_topic", "k1", 1, "post_reconnect_msg")
        .await
        .unwrap();
    assert_eq!(prod_res.first_offset, 0);

    let fetched = client.fetch("reconnect_topic", 0, 0, 1024).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].payload, "post_reconnect_msg".as_bytes());
}
