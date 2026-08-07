use hermes::{EngineConfig, FlushPolicy, Server, StorageEngine};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize production tracing subscriber
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("============================================================");
    tracing::info!("  HERMES: Production Event Streaming Storage Engine Server  ");
    tracing::info!("============================================================");

    let bind_addr = std::env::var("HERMES_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let data_dir_path = std::env::var("HERMES_DATA_DIR").unwrap_or_else(|_| "./data_store".to_string());
    let data_dir = PathBuf::from(data_dir_path);

    let config = EngineConfig {
        data_dir: data_dir.clone(),
        max_segment_bytes: 10 * 1024 * 1024, // 10 MB segment size
        index_interval_bytes: 4096,          // 4 KB sparse index interval
        flush_policy: FlushPolicy::AsyncPeriodic {
            interval: Duration::from_millis(5),
            max_bytes: 64 * 1024,
        },
        preallocate_segments: true,
        bind_addr: bind_addr.clone(),
        retention_bytes: Some(100 * 1024 * 1024), // 100 MB total log retention
        retention_millis: Some(86400 * 1000),      // 24 hours retention
        retention_check_interval: Duration::from_secs(10),
    };

    tracing::info!("Configuration:");
    tracing::info!("  Storage Directory: {}", data_dir.display());
    tracing::info!("  TCP Bind Address:  {}", bind_addr);
    tracing::info!("  Segment Size Limit: 10 MB");
    tracing::info!("  Durability Policy:  AsyncPeriodic (5ms / 64KB)");

    let engine = StorageEngine::new(config)?;
    let server = Server::new(engine);

    tracing::info!("Starting TCP Listener Loop. Press Ctrl+C to stop.");
    server.run().await?;

    Ok(())
}