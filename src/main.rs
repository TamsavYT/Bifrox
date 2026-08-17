use hermes::{EngineConfig, Server, StorageEngine};
use std::env;
use std::fs;
use std::path::PathBuf;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Builds the Tokio runtime by hand rather than via `#[tokio::main]` so that
/// `num.network.threads` from the config file can actually size the worker pool — the
/// attribute macro hardcodes Tokio's default (one worker per logical CPU) with no way to
/// read a config value first, since the config is only loaded *inside* `main`.
///
/// This means the config file has to be located and parsed before the runtime exists, so
/// `real_main` does the config load synchronously and only then enters the async part.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(threads) = config.num_network_threads {
        builder.worker_threads(threads);
    }
    let runtime = builder.build()?;

    runtime.block_on(run(config))
}

fn load_config() -> Result<EngineConfig, Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let config_path_opt = if args.len() > 1 && !args[1].starts_with('-') {
        Some(args[1].clone())
    } else if let Ok(path) = env::var("HERMES_CONFIG") {
        Some(path)
    } else if std::path::Path::new("server.properties").exists() {
        Some("server.properties".to_string())
    } else {
        None
    };

    let config = if let Some(config_path) = config_path_opt {
        EngineConfig::from_properties_file(config_path)?
    } else {
        let bind_addr =
            env::var("HERMES_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
        let data_dir_path =
            env::var("HERMES_DATA_DIR").unwrap_or_else(|_| "./data_store".to_string());
        let log_file_dir_path = env::var("HERMES_LOG_DIR").unwrap_or_else(|_| "./logs".to_string());
        EngineConfig {
            data_dir: PathBuf::from(data_dir_path),
            log_file_dir: PathBuf::from(log_file_dir_path),
            bind_addr,
            ..Default::default()
        }
    };

    Ok(config)
}

async fn run(config: EngineConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Configure non-blocking daily rolling log file appender using config.log_file_dir
    let log_dir = config.log_file_dir.clone();
    fs::create_dir_all(&log_dir)?;
    let file_appender = tracing_appender::rolling::daily(&log_dir, "hermes-server.log");
    let (non_blocking_file, _guard) = tracing_appender::non_blocking(file_appender);

    // Set up dual subscriber: Log to both stdout (console) and log file (log_file_dir/hermes-server.log)
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stdout))
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(non_blocking_file),
        )
        .try_init();

    tracing::info!("============================================================");
    tracing::info!("  HERMES: Production Event Streaming Storage Engine Server  ");
    tracing::info!("============================================================");

    tracing::info!("Configuration Details:");
    tracing::info!("  Cluster ID:         {}", config.cluster_id);
    tracing::info!("  Node ID:            {}", config.node_id);
    tracing::info!("  HA Cluster Role:    {:?}", config.role);
    tracing::info!("  TCP Bind Address:   {}", config.bind_addr);
    tracing::info!("  Storage Directory:  {}", config.data_dir.display());
    tracing::info!("  Log File Directory: {}", config.log_file_dir.display());
    tracing::info!("  Segment Size Limit: {} bytes", config.max_segment_bytes);
    tracing::info!("  Peer Nodes (HA):    {:?}", config.peer_addrs);
    tracing::info!("  Min ISR Replicas:   {}", config.min_insync_replicas);
    tracing::info!("  Process Roles:      {:?}", config.roles);
    tracing::info!(
        "  Segment Roll Time:  {}",
        config
            .segment_ms
            .map(|ms| format!("{} ms", ms))
            .unwrap_or_else(|| "disabled (size-based rolling only)".to_string())
    );
    tracing::info!(
        "  Max Message Bytes:  {}",
        config
            .message_max_bytes
            .map(|b| b.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    tracing::info!(
        "  Network Threads:    {}",
        config
            .num_network_threads
            .map(|t| t.to_string())
            .unwrap_or_else(|| "auto (one per logical CPU)".to_string())
    );
    tracing::info!("  Txn Timeout:        {} ms", config.transaction_timeout_ms);
    if !config.controller_peer_addrs.is_empty() {
        tracing::info!(
            "  Controller Quorum:  {:?}",
            config.controller_peer_addrs
        );
    }
    if config.is_controller_role() && !config.is_broker_role() {
        tracing::info!("  Deployment Mode:    Controller-only (metadata quorum, no data partitions)");
    } else if config.is_broker_role() && !config.is_controller_role() {
        tracing::info!("  Deployment Mode:    Broker-only (data partitions, never metadata leader)");
    } else {
        tracing::info!("  Deployment Mode:    Combined (controller + broker, historical default)");
    }
    if config.role == hermes::NodeRole::Leader {
        tracing::info!("  Behavior:           Handles produces, replicates to followers");
    } else {
        tracing::info!("  Behavior:           Forwards produces to Leader, serves local fetches");
    }

    let engine = StorageEngine::new(config)?;
    let server = Server::new(engine);

    tracing::info!("Starting TCP Listener Loop. Press Ctrl+C to stop.");
    server.run().await?;

    Ok(())
}
