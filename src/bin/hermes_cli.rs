use hermes::TestClient;
use std::net::ToSocketAddrs;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    let server_str = get_arg_val(&args, "--server")
        .or_else(|| get_arg_val(&args, "-s"))
        .unwrap_or_else(|| "127.0.0.1:9092".to_string());

    let server_addr = match server_str.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(a) => a,
            None => {
                eprintln!("Error: Invalid server address '{}'", server_str);
                return Ok(());
            }
        },
        Err(e) => {
            eprintln!("Error: Could not resolve server address '{}': {}", server_str, e);
            return Ok(());
        }
    };

    let command = match args.iter().skip(1).find(|a| !a.starts_with('-')) {
        Some(cmd) => cmd.to_lowercase(),
        None => {
            print_usage();
            return Ok(());
        }
    };

    let mut client = match TestClient::connect(server_addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: Could not connect to Hermes server at {}: {}", server_addr, e);
            eprintln!("Make sure the server is running on {} (run: cargo run --bin hermes -- config/server-node1.properties)", server_addr);
            return Ok(());
        }
    };

    match command.as_str() {
        "produce" => {
            let topic = get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let key = get_arg_val(&args, "--key").unwrap_or_default();
            let msg = get_arg_val(&args, "--message").unwrap_or_else(|| "Sample Hermes Event Payload".to_string());
            let num_partitions: u32 = get_arg_val(&args, "--partitions")
                .and_then(|s| s.parse().ok())
                .unwrap_or(3);

            let res = client
                .produce_single(&topic, &key, None, num_partitions, msg.as_bytes())
                .await?;

            println!("✅ Produced 1 Record successfully!");
            println!("  Server Address:     {}", server_addr);
            println!("  Topic:              {}", topic);
            println!("  Assigned Partition: {}", res.assigned_partition);
            println!("  Logical Offset:     {}", res.first_offset);
        }
        "fetch" | "consume" => {
            let topic = get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let group_opt = get_arg_val(&args, "--group");
            let from_beginning = has_flag(&args, "--from-beginning");

            let start_offset: u64 = if let Some(explicit_off) = get_arg_val(&args, "--offset").and_then(|s| s.parse().ok()) {
                explicit_off
            } else if from_beginning {
                0
            } else if let Some(ref group_id) = group_opt {
                let committed = client.fetch_offset(group_id, &topic, partition).await?;
                if committed == u64::MAX {
                    0
                } else {
                    committed + 1
                }
            } else {
                0
            };

            let max_bytes: u32 = get_arg_val(&args, "--max-bytes")
                .and_then(|s| s.parse().ok())
                .unwrap_or(64 * 1024);

            let frames = client.fetch(&topic, partition, start_offset, max_bytes).await?;

            if let Some(ref group_id) = group_opt {
                println!(
                    "📥 Kafka-Style Group Fetch ('{}') from Server {} [Topic '{}' Partition {}]:",
                    group_id, server_addr, topic, partition
                );
            } else {
                println!(
                    "📥 Fetched {} record frame(s) from Server {} [Topic '{}' Partition {}]:",
                    frames.len(), server_addr, topic, partition
                );
            }
            println!("------------------------------------------------------------------");

            for (idx, frame) in frames.iter().enumerate() {
                let payload_str = String::from_utf8_lossy(&frame.payload);
                println!(
                    "  [{:03}] Offset: {:<6} | CRC32: 0x{:08X} | Timestamp: {} | Payload: '{}'",
                    idx, frame.offset, frame.crc, frame.timestamp, payload_str
                );
            }

            // Auto-commit if group argument was provided
            if let Some(ref group_id) = group_opt {
                if let Some(last_frame) = frames.last() {
                    let commit_off = last_frame.offset;
                    client.commit_offset(group_id, &topic, partition, commit_off).await?;
                    println!("\n  📌 Auto-committed Offset {} for Group '{}' to __consumer_offsets.log", commit_off, group_id);
                }
            }
        }
        "group-consume" => {
            let group_id = get_arg_val(&args, "--group").unwrap_or_else(|| "my_consumer_group".to_string());
            let topic = get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let poll_interval_ms: u64 = get_arg_val(&args, "--interval")
                .and_then(|s| s.parse().ok())
                .unwrap_or(500);

            let committed = client.fetch_offset(&group_id, &topic, partition).await?;
            let mut next_offset = if committed == u64::MAX {
                0
            } else {
                committed + 1
            };

            println!("============================================================");
            println!("   HERMES CONSUMER GROUP POLLING LOOP: '{}'", group_id);
            println!("============================================================");
            println!("  Topic:               {}", topic);
            println!("  Partition:           {}", partition);
            println!("  Starting Offset:     {}", next_offset);
            println!("  Poll Interval:       {} ms", poll_interval_ms);
            println!("Polling for messages. Press Ctrl+C to stop.\n");

            loop {
                match client.fetch(&topic, partition, next_offset, 64 * 1024).await {
                    Ok(frames) if !frames.is_empty() => {
                        for frame in &frames {
                            let payload_str = String::from_utf8_lossy(&frame.payload);
                            println!(
                                "📥 Group '{}' consumed Offset {:<6} | Timestamp: {} | Payload: '{}'",
                                group_id, frame.offset, frame.timestamp, payload_str
                            );
                            next_offset = frame.offset + 1;
                        }

                        let last_offset = frames.last().unwrap().offset;
                        if let Err(e) = client.commit_offset(&group_id, &topic, partition, last_offset).await {
                            eprintln!("Failed to commit offset {}: {}", last_offset, e);
                        } else {
                            println!("  📌 Auto-committed offset {} to __consumer_offsets.log", last_offset);
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Error polling server: {}. Reconnecting...", e);
                        let _ = client.reconnect().await;
                    }
                }

                sleep(Duration::from_millis(poll_interval_ms)).await;
            }
        }
        "latest-offset" | "watermark" => {
            let topic = get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let offset = client.latest_offset(&topic, partition).await?;
            println!("📊 Topic '{}' Partition {} High Watermark Offset: {}", topic, partition, offset);
        }
        "commit-offset" => {
            let group_id = get_arg_val(&args, "--group").unwrap_or_else(|| "my_consumer_group".to_string());
            let topic = get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let offset: u64 = get_arg_val(&args, "--offset")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            client.commit_offset(&group_id, &topic, partition, offset).await?;
            println!(
                "✅ Committed Offset {} for Consumer Group '{}' on Topic '{}' Partition {} (persisted to __consumer_offsets.log)",
                offset, group_id, topic, partition
            );
        }
        "fetch-offset" => {
            let group_id = get_arg_val(&args, "--group").unwrap_or_else(|| "my_consumer_group".to_string());
            let topic = get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let offset = client.fetch_offset(&group_id, &topic, partition).await?;
            if offset == u64::MAX {
                println!("⚠️ No committed offset found for Group '{}' on Topic '{}' Partition {}", group_id, topic, partition);
            } else {
                println!("📌 Group '{}' Committed Offset on Topic '{}' Partition {}: {}", group_id, topic, partition, offset);
            }
        }
        "seek" => {
            let topic = get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let offset: u64 = get_arg_val(&args, "--offset")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let seek_res = client.seek(&topic, partition, offset).await?;
            println!(
                "🔍 Sparse Index Seek for Offset {}: Base Segment Offset = {}, Physical Byte Position = {}",
                offset, seek_res.base_offset, seek_res.physical_position
            );
        }
        _ => {
            eprintln!("Unknown command: '{}'", command);
            print_usage();
        }
    }

    Ok(())
}

fn get_arg_val(args: &[String], flag: &str) -> Option<String> {
    for i in 0..args.len() {
        if (args[i] == flag || (flag.starts_with("--") && args[i] == &flag[1..])) && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|a| a == flag)
}

fn print_usage() {
    println!("============================================================");
    println!("               HERMES EVENT STREAMING CLI                   ");
    println!("============================================================");
    println!("Usage:");
    println!("  cargo run --bin hermes_cli -- [--server <IP:PORT>] <COMMAND> [FLAGS]\n");
    println!("Commands:");
    println!("  produce         Produce an event payload to a topic");
    println!("                  --topic <NAME> --message <MSG> [--key <KEY>] [--partitions <N>]");
    println!("  fetch / consume Kafka-style fetch (supports consumer group tracking & --from-beginning)");
    println!("                  --topic <NAME> [--group <GROUP>] [--partition <ID>] [--offset <N>] [--from-beginning]");
    println!("  group-consume   Kafka-style Consumer Group continuous loop (auto-fetches & commits)");
    println!("                  --group <GROUP> --topic <NAME> [--partition <ID>] [--interval <MS>]");
    println!("  latest-offset   Get partition high watermark offset");
    println!("                  --topic <NAME> [--partition <ID>]");
    println!("  commit-offset   Commit consumer group offset");
    println!("                  --group <GROUP> --topic <NAME> [--partition <ID>] --offset <N>");
    println!("  fetch-offset    Get committed consumer group offset");
    println!("                  --group <GROUP> --topic <NAME> [--partition <ID>]");
    println!("  seek            Seek physical disk byte position for offset");
    println!("                  --topic <NAME> [--partition <ID>] --offset <N>");
    println!("\nGlobal Flag:");
    println!("  --server / -s   Server address (default: 127.0.0.1:9092 or localhost:9092)");
}
