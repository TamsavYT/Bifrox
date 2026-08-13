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
            eprintln!(
                "Error: Could not resolve server address '{}': {}",
                server_str, e
            );
            return Ok(());
        }
    };

    let known_flags_with_val = [
        "--server",
        "-s",
        "--topic",
        "--message",
        "--key",
        "--partitions",
        "--partition",
        "--group",
        "--offset",
        "--interval",
        "--max-bytes",
        "--sasl-user",
        "--sasl-pass",
        "--resource-type",
        "--resource-name",
        "--principal",
        "--operation",
        "--permission",
        "--node-id",
        "--endpoint",
        "--ca-path",
        "--cert-path",
        "--key-path",
        "--sni",
        "--server-name",
    ];

    let mut command_opt = None;
    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if known_flags_with_val.contains(&arg.as_str()) {
            i += 2; // skip flag and its value
        } else if arg.starts_with('-') {
            i += 1; // skip standalone boolean flag
        } else {
            command_opt = Some(arg.to_lowercase());
            break;
        }
    }

    let command = match command_opt {
        Some(cmd) => cmd,
        None => {
            print_usage();
            return Ok(());
        }
    };

    let use_tls = has_flag(&args, "--tls") || has_flag(&args, "--ssl");
    let ca_path_opt = get_arg_val(&args, "--ca-path");
    let cert_path_opt = get_arg_val(&args, "--cert-path");
    let key_path_opt = get_arg_val(&args, "--key-path");
    let sni_opt = get_arg_val(&args, "--sni").or_else(|| get_arg_val(&args, "--server-name"));
    let insecure = has_flag(&args, "--insecure");
    let sasl_user_opt = get_arg_val(&args, "--sasl-user");
    let sasl_pass_opt = get_arg_val(&args, "--sasl-pass").unwrap_or_default();

    let mut client = if use_tls {
        let ca_p = ca_path_opt.as_ref().map(std::path::Path::new);
        let client_auth = match (&cert_path_opt, &key_path_opt) {
            (Some(c), Some(k)) => Some((std::path::Path::new(c), std::path::Path::new(k))),
            _ => None,
        };
        let skip_verify = insecure;

        match TestClient::connect_tls_full_with_domain(
            server_addr,
            ca_p,
            client_auth,
            skip_verify,
            sni_opt.as_deref(),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "Error: Could not connect via TLS/SSL to Hermes server at {}: {}",
                    server_addr, e
                );
                if !insecure {
                    eprintln!("Hint: For SAN hostname/IP validation, use --sni <hostname>.");
                    eprintln!(
                        "Windows Guidance: System CAs (Let's Encrypt, DigiCert, etc.) are verified automatically."
                    );
                    eprintln!(
                        "For custom internal CAs on Windows, export your root cert to PEM ('certutil -encode ca.crt ca.pem') and pass '--ca-path C:\\path\\to\\ca.pem'."
                    );
                }
                return Ok(());
            }
        }
    } else {
        match TestClient::connect(server_addr).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "Error: Could not connect to Hermes server at {}: {}",
                    server_addr, e
                );
                eprintln!("Make sure the server is running on {} (run: cargo run --bin hermes -- config/server-node1.properties)", server_addr);
                return Ok(());
            }
        }
    };

    if let Some(user) = sasl_user_opt {
        let (err_code, mechs) = client.sasl_handshake("PLAIN").await?;
        if err_code != 0 {
            eprintln!("Error: SASL Handshake failed with error code {}", err_code);
            return Ok(());
        }
        let auth_payload = format!("\0{}\0{}", user, sasl_pass_opt);
        let auth_err = client.sasl_authenticate(auth_payload.as_bytes()).await?;
        if auth_err != 0 {
            eprintln!(
                "Error: SASL Authentication failed for user '{}' (error code {})",
                user, auth_err
            );
            return Ok(());
        }
        println!(
            "🔐 SASL Authentication succeeded for user '{}' (Mechanisms: {:?})",
            user, mechs
        );
    }

    match command.as_str() {
        "produce" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let key = get_arg_val(&args, "--key").unwrap_or_default();
            let msg = get_arg_val(&args, "--message")
                .unwrap_or_else(|| "Sample Hermes Event Payload".to_string());
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
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let group_opt = get_arg_val(&args, "--group");
            let from_beginning = has_flag(&args, "--from-beginning");

            let start_offset: u64 = if let Some(explicit_off) =
                get_arg_val(&args, "--offset").and_then(|s| s.parse().ok())
            {
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

            let frames = client
                .fetch(&topic, partition, start_offset, max_bytes)
                .await?;

            if let Some(ref group_id) = group_opt {
                println!(
                    "📥 Kafka-Style Group Fetch ('{}') from Server {} [Topic '{}' Partition {}]:",
                    group_id, server_addr, topic, partition
                );
            } else {
                println!(
                    "📥 Fetched {} record frame(s) from Server {} [Topic '{}' Partition {}]:",
                    frames.len(),
                    server_addr,
                    topic,
                    partition
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
                    client
                        .commit_offset(group_id, &topic, partition, commit_off)
                        .await?;
                    println!(
                        "\n  📌 Auto-committed Offset {} for Group '{}' to __consumer_offsets.log",
                        commit_off, group_id
                    );
                }
            }
        }
        "group-consume" => {
            let group_id =
                get_arg_val(&args, "--group").unwrap_or_else(|| "my_consumer_group".to_string());
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
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
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        println!("\n🛑 Graceful shutdown signal received. Exiting consumer group loop.");
                        break;
                    }
                    _ = sleep(Duration::from_millis(poll_interval_ms)) => {
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
                    }
                }
            }
        }
        "latest-offset" | "watermark" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let offset = client.latest_offset(&topic, partition).await?;
            println!(
                "📊 Topic '{}' Partition {} High Watermark Offset: {}",
                topic, partition, offset
            );
        }
        "commit-offset" => {
            let group_id =
                get_arg_val(&args, "--group").unwrap_or_else(|| "my_consumer_group".to_string());
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let offset: u64 = get_arg_val(&args, "--offset")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            client
                .commit_offset(&group_id, &topic, partition, offset)
                .await?;
            println!(
                "✅ Committed Offset {} for Consumer Group '{}' on Topic '{}' Partition {} (persisted to __consumer_offsets.log)",
                offset, group_id, topic, partition
            );
        }
        "fetch-offset" => {
            let group_id =
                get_arg_val(&args, "--group").unwrap_or_else(|| "my_consumer_group".to_string());
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let offset = client.fetch_offset(&group_id, &topic, partition).await?;
            if offset == u64::MAX {
                println!(
                    "⚠️ No committed offset found for Group '{}' on Topic '{}' Partition {}",
                    group_id, topic, partition
                );
            } else {
                println!(
                    "📌 Group '{}' Committed Offset on Topic '{}' Partition {}: {}",
                    group_id, topic, partition, offset
                );
            }
        }
        "seek" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
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
        "ping" => {
            let ok = client.ping().await?;
            if ok {
                println!("✅ PONG received from server {}", server_addr);
            } else {
                println!("❌ Ping failed");
            }
        }
        "list-topics" => {
            let topics = client.list_topics().await?;
            println!("📋 Active Topics on Server {}:", server_addr);
            for t in topics {
                println!("  - {}", t);
            }
        }
        "delete-topic" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            client.delete_topic(&topic).await?;
            println!("🗑️ Topic '{}' deleted successfully.", topic);
        }
        "create-acl" => {
            let res_type_str =
                get_arg_val(&args, "--resource-type").unwrap_or_else(|| "topic".to_string());
            let res_name = get_arg_val(&args, "--resource-name")
                .unwrap_or_else(|| "default_topic".to_string());
            let principal =
                get_arg_val(&args, "--principal").unwrap_or_else(|| "User:*".to_string());
            let op_str = get_arg_val(&args, "--operation").unwrap_or_else(|| "read".to_string());
            let perm_str =
                get_arg_val(&args, "--permission").unwrap_or_else(|| "allow".to_string());

            let binding = hermes::AclBinding {
                resource_type: parse_resource_type(&res_type_str),
                resource_name: res_name.clone(),
                pattern_type: 3, // Literal
                principal: principal.clone(),
                host: "*".to_string(),
                operation: parse_operation(&op_str),
                permission_type: parse_permission(&perm_str),
            };

            client.create_acl(&binding).await?;
            println!("🛡️ Created ACL Binding: Principal='{}', Resource={}:{}, Operation={}, Permission={}",
                principal, res_type_str, res_name, op_str, perm_str);
        }
        "describe-acls" => {
            let res_type_str =
                get_arg_val(&args, "--resource-type").unwrap_or_else(|| "any".to_string());
            let res_name = get_arg_val(&args, "--resource-name").unwrap_or_else(|| "*".to_string());
            let principal = get_arg_val(&args, "--principal").unwrap_or_else(|| "*".to_string());

            let filter = hermes::AclBinding {
                resource_type: parse_resource_type(&res_type_str),
                resource_name: res_name,
                pattern_type: 3,
                principal,
                host: "*".to_string(),
                operation: 1,       // Any
                permission_type: 1, // Any
            };

            let acls = client.describe_acls(&filter).await?;
            println!(
                "🛡️ Active ACL Bindings on Server {}: ({})",
                server_addr,
                acls.len()
            );
            for (idx, a) in acls.iter().enumerate() {
                println!("  [{:02}] Principal: {:<15} | ResourceType: {:<2} | ResourceName: {:<15} | Op: {:<2} | Perm: {:<2}",
                    idx, a.principal, a.resource_type, a.resource_name, a.operation, a.permission_type);
            }
        }
        "register-broker" => {
            let node_id: u32 = get_arg_val(&args, "--node-id")
                .and_then(|s| s.parse().ok())
                .unwrap_or(2);
            let endpoint =
                get_arg_val(&args, "--endpoint").unwrap_or_else(|| "127.0.0.1:9093".to_string());
            client.register_broker(node_id, &endpoint).await?;
            println!(
                "🌐 Registered Broker node_id={} endpoint='{}' in cluster catalog",
                node_id, endpoint
            );
        }
        "unregister-broker" => {
            let node_id: u32 = get_arg_val(&args, "--node-id")
                .and_then(|s| s.parse().ok())
                .unwrap_or(2);
            client.unregister_broker(node_id).await?;
            println!(
                "🌐 Unregistered Broker node_id={} from cluster catalog",
                node_id
            );
        }
        _ => {
            eprintln!("Unknown command: '{}'", command);
            print_usage();
        }
    }

    Ok(())
}

fn parse_resource_type(s: &str) -> u8 {
    match s.to_lowercase().as_str() {
        "topic" => hermes::ResourceType::Topic as u8,
        "group" => hermes::ResourceType::Group as u8,
        "cluster" => hermes::ResourceType::Cluster as u8,
        "tx" | "transactional_id" => hermes::ResourceType::TransactionalId as u8,
        "user" => hermes::ResourceType::User as u8,
        _ => hermes::ResourceType::Any as u8,
    }
}

fn parse_operation(s: &str) -> u8 {
    match s.to_lowercase().as_str() {
        "read" => hermes::AclOperation::Read as u8,
        "write" => hermes::AclOperation::Write as u8,
        "create" => hermes::AclOperation::Create as u8,
        "delete" => hermes::AclOperation::Delete as u8,
        "alter" => hermes::AclOperation::Alter as u8,
        "describe" => hermes::AclOperation::Describe as u8,
        "all" => hermes::AclOperation::All as u8,
        _ => hermes::AclOperation::Any as u8,
    }
}

fn parse_permission(s: &str) -> u8 {
    match s.to_lowercase().as_str() {
        "deny" => hermes::AclPermissionType::Deny as u8,
        _ => hermes::AclPermissionType::Allow as u8,
    }
}

fn get_arg_val(args: &[String], flag: &str) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == flag && i + 1 < args.len() {
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
    println!(
        "  fetch / consume Kafka-style fetch (supports consumer group tracking & --from-beginning)"
    );
    println!("                  --topic <NAME> [--group <GROUP>] [--partition <ID>] [--offset <N>] [--from-beginning]");
    println!(
        "  group-consume   Kafka-style Consumer Group continuous loop (auto-fetches & commits)"
    );
    println!(
        "                  --group <GROUP> --topic <NAME> [--partition <ID>] [--interval <MS>]"
    );
    println!("  latest-offset   Get partition high watermark offset");
    println!("                  --topic <NAME> [--partition <ID>]");
    println!("  commit-offset   Commit consumer group offset");
    println!("                  --group <GROUP> --topic <NAME> [--partition <ID>] --offset <N>");
    println!("  fetch-offset    Get committed consumer group offset");
    println!("                  --group <GROUP> --topic <NAME> [--partition <ID>]");
    println!("  seek            Seek physical disk byte position for offset");
    println!("                  --topic <NAME> [--partition <ID>] --offset <N>");
    println!("  ping            Send health check PING to server");
    println!("  list-topics     List active topics on server");
    println!("  delete-topic    Delete a topic and its partition files");
    println!("                  --topic <NAME>");
    println!("\nGlobal Flag:");
    println!("  --server / -s   Server address (default: 127.0.0.1:9092 or localhost:9092)");
}
