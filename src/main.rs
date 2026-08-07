use bytes::BufMut;
use cli_file_appender::{
    hash_key, CommandCode, EngineConfig, FlushPolicy, RecordFrame, Server, StorageEngine,
};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("=== Starting Milestone 2 Event Engine: Multi-Partition & Consumer Groups ===");

    let data_dir = PathBuf::from("./data_m2");
    let config = EngineConfig {
        data_dir: data_dir.clone(),
        max_segment_bytes: 512 * 1024, // 512 KB per segment for demonstration
        index_interval_bytes: 1024,    // 1 KB sparse index interval
        flush_policy: FlushPolicy::AsyncPeriodic {
            interval: Duration::from_millis(5),
            max_bytes: 64 * 1024,
        },
        preallocate_segments: true,
        bind_addr: "127.0.0.1:9092".to_string(),
        retention_bytes: Some(10 * 1024 * 1024), // 10 MB retention limit
        retention_millis: Some(86400 * 1000),     // 24 hours
        retention_check_interval: Duration::from_secs(5),
    };

    let engine = StorageEngine::new(config.clone())?;
    let server = Server::new(engine.clone());

    tokio::spawn(async move {
        if let Err(err) = server.run().await {
            tracing::error!("Server error: {}", err);
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    run_milestone2_demo(&config.bind_addr).await?;

    tracing::info!("=== Milestone 2 Verification Suite Passed Successfully ===");
    Ok(())
}

async fn run_milestone2_demo(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    tracing::info!("Client connected to Milestone 2 TCP server at {}", addr);

    let topic = "order_events";
    let num_partitions = 3u32;
    let group_id = "payment_processor_group";

    // 1. Hash Routing & Produce Batch
    tracing::info!("1. Producing batch of records routed across {} partitions...", num_partitions);
    let sample_keys = vec!["user_8812", "user_9941", "user_1042", "user_8812", "user_3321"];

    for (idx, key) in sample_keys.iter().enumerate() {
        let routed_partition = hash_key(key.as_bytes(), num_partitions as usize);
        tracing::info!("  Record Key '{}' routed to Partition ID {}", key, routed_partition);

        let records = vec![
            format!("Order Payment Event A for {} (seq: {})", key, idx).into_bytes(),
            format!("Order Fulfillment Event B for {} (seq: {})", key, idx).into_bytes(),
        ];

        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::ProduceBatch as u8);

        let mut inner = Vec::new();
        cli_file_appender::protocol::wire::write_pascal_string(&mut inner, topic);
        cli_file_appender::protocol::wire::write_pascal_string(&mut inner, key);
        inner.put_u32(num_partitions);
        inner.put_u32(records.len() as u32);
        for rec in &records {
            inner.put_u32(rec.len() as u32);
            inner.put_slice(rec);
        }

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        stream.write_all(&req_buf).await?;

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let status = resp_header[0];
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        if status == 0 {
            let assigned_part = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap());
            let first_off = u64::from_be_bytes(resp_payload[4..12].try_into().unwrap());
            let last_off = u64::from_be_bytes(resp_payload[12..20].try_into().unwrap());
            tracing::info!(
                "  Assigned Partition: {}, Offsets: {}..={}",
                assigned_part,
                first_off,
                last_off
            );
        } else {
            return Err(format!("ProduceBatch failed: {}", String::from_utf8_lossy(&resp_payload)).into());
        }
    }

    // 2. Commit Consumer Group Offset
    let target_partition = 0u32;
    let commit_offset_val = 42u64;
    tracing::info!(
        "2. Committing offset {} for group '{}' on topic '{}' partition {}...",
        commit_offset_val,
        group_id,
        topic,
        target_partition
    );
    {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::CommitOffset as u8);

        let mut inner = Vec::new();
        cli_file_appender::protocol::wire::write_pascal_string(&mut inner, group_id);
        cli_file_appender::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(target_partition);
        inner.put_u64(commit_offset_val);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        stream.write_all(&req_buf).await?;

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        if resp_header[0] == 0 {
            tracing::info!("  Offset committed successfully.");
        } else {
            return Err("CommitOffset failed".into());
        }
    }

    // 3. Fetch Consumer Group Offset
    tracing::info!(
        "3. Fetching committed offset for group '{}' on topic '{}' partition {}...",
        group_id,
        topic,
        target_partition
    );
    {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::FetchOffset as u8);

        let mut inner = Vec::new();
        cli_file_appender::protocol::wire::write_pascal_string(&mut inner, group_id);
        cli_file_appender::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(target_partition);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        stream.write_all(&req_buf).await?;

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        let fetched_offset = u64::from_be_bytes(resp_payload[0..8].try_into().unwrap());
        tracing::info!("  Fetched Committed Offset: {}", fetched_offset);
        assert_eq!(fetched_offset, commit_offset_val);
    }

    // 4. Fetch Records
    tracing::info!("4. Fetching records from partition 0 starting at offset 0...");
    {
        let mut req_buf = Vec::new();
        req_buf.put_u8(CommandCode::Fetch as u8);

        let mut inner = Vec::new();
        cli_file_appender::protocol::wire::write_pascal_string(&mut inner, topic);
        inner.put_u32(target_partition);
        inner.put_u64(0);
        inner.put_u32(64 * 1024);

        req_buf.put_u32(inner.len() as u32);
        req_buf.extend_from_slice(&inner);

        stream.write_all(&req_buf).await?;

        let mut resp_header = [0u8; 5];
        stream.read_exact(&mut resp_header).await?;
        let payload_len = u32::from_be_bytes(resp_header[1..5].try_into().unwrap()) as usize;
        let mut resp_payload = vec![0u8; payload_len];
        stream.read_exact(&mut resp_payload).await?;

        let count = u32::from_be_bytes(resp_payload[0..4].try_into().unwrap()) as usize;
        tracing::info!("  Fetched {} record frames from partition 0.", count);

        let mut cursor = 4usize;
        for i in 0..count {
            let (frame, consumed) = RecordFrame::decode(&resp_payload[cursor..])?;
            cursor += consumed;
            tracing::info!(
                "    Record #{}: Offset = {}, CRC32 = 0x{:08X}, Payload = '{}'",
                i,
                frame.offset,
                frame.crc,
                String::from_utf8_lossy(&frame.payload)
            );
        }
    }

    Ok(())
}