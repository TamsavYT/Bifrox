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
        "--num-records",
        "--record-size",
        "--batch-size",
        "--throughput",
        "--messages",
        "--tx-id",
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
        "perf-produce" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "perf-test-topic".to_string());
            let key = get_arg_val(&args, "--key").unwrap_or_default();
            let num_partitions: u32 = get_arg_val(&args, "--partitions")
                .and_then(|s| s.parse().ok())
                .unwrap_or(3);
            let num_records: u64 = get_arg_val(&args, "--num-records")
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000);
            let record_size: usize = get_arg_val(&args, "--record-size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);
            let batch_size: u64 = get_arg_val(&args, "--batch-size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(100)
                .max(1);
            // Matches Kafka's kafka-producer-perf-test.sh --throughput: target records/sec,
            // -1 (or any non-positive value) means unthrottled — send as fast as possible.
            let throughput: i64 = get_arg_val(&args, "--throughput")
                .and_then(|s| s.parse().ok())
                .unwrap_or(-1);

            let payload: Vec<u8> = vec![b'x'; record_size.max(1)];

            println!("============================================================");
            println!("   HERMES PRODUCER PERFORMANCE TEST");
            println!("============================================================");
            println!("  Server:        {}", server_addr);
            println!("  Topic:         {}", topic);
            println!("  Records:       {}", num_records);
            println!("  Record Size:   {} bytes", record_size);
            println!("  Batch Size:    {} records/request", batch_size);
            println!(
                "  Target Rate:   {}",
                if throughput > 0 {
                    format!("{} records/sec", throughput)
                } else {
                    "unlimited".to_string()
                }
            );
            println!("------------------------------------------------------------------");

            let mut sent: u64 = 0;
            let mut sent_bytes: u64 = 0;
            let mut request_latencies_ms: Vec<f64> = Vec::new();
            let test_start = std::time::Instant::now();
            let mut last_report = test_start;
            let mut last_report_count: u64 = 0;

            while sent < num_records {
                let this_batch = batch_size.min(num_records - sent) as usize;
                let records: Vec<&[u8]> =
                    std::iter::repeat_n(payload.as_slice(), this_batch).collect();

                let req_start = std::time::Instant::now();
                let res = client
                    .produce_batch(&topic, &key, None, num_partitions, &records)
                    .await;
                let elapsed_ms = req_start.elapsed().as_secs_f64() * 1000.0;

                match res {
                    Ok(_) => {
                        request_latencies_ms.push(elapsed_ms);
                        sent += this_batch as u64;
                        sent_bytes += (this_batch * record_size) as u64;
                    }
                    Err(e) => {
                        eprintln!("❌ Produce request failed after {} records: {}", sent, e);
                        break;
                    }
                }

                // Simple target-rate pacing (matches Kafka perf test's --throughput):
                // sleep just enough to keep cumulative send rate at or below the target,
                // rather than pacing every individual request independently.
                if throughput > 0 {
                    let expected_elapsed_secs = sent as f64 / throughput as f64;
                    let actual_elapsed_secs = test_start.elapsed().as_secs_f64();
                    if actual_elapsed_secs < expected_elapsed_secs {
                        sleep(Duration::from_secs_f64(
                            expected_elapsed_secs - actual_elapsed_secs,
                        ))
                        .await;
                    }
                }

                let now = std::time::Instant::now();
                let since_report = now.duration_since(last_report).as_secs_f64();
                if since_report >= 1.0 {
                    let interval_records = sent - last_report_count;
                    let rate = interval_records as f64 / since_report;
                    let mb_rate =
                        (interval_records * record_size as u64) as f64 / (1024.0 * 1024.0) / since_report;
                    println!(
                        "  {:>10} records sent, {:>10.1} records/sec ({:>6.2} MB/sec)",
                        sent, rate, mb_rate
                    );
                    last_report = now;
                    last_report_count = sent;
                }
            }

            let total_elapsed = test_start.elapsed().as_secs_f64().max(0.000_001);
            let overall_rate = sent as f64 / total_elapsed;
            let overall_mb_rate = sent_bytes as f64 / (1024.0 * 1024.0) / total_elapsed;

            request_latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let avg_latency = if request_latencies_ms.is_empty() {
                0.0
            } else {
                request_latencies_ms.iter().sum::<f64>() / request_latencies_ms.len() as f64
            };
            let max_latency = request_latencies_ms.last().copied().unwrap_or(0.0);

            println!("------------------------------------------------------------------");
            println!(
                "✅ {} records sent, {:.1} records/sec ({:.2} MB/sec) in {:.2}s",
                sent, overall_rate, overall_mb_rate, total_elapsed
            );
            println!(
                "   Request latency (per {}-record produce_batch call): avg {:.2} ms, p50 {:.2} ms, p95 {:.2} ms, p99 {:.2} ms, max {:.2} ms",
                batch_size,
                avg_latency,
                percentile(&request_latencies_ms, 50.0),
                percentile(&request_latencies_ms, 95.0),
                percentile(&request_latencies_ms, 99.0),
                max_latency,
            );
        }
        "perf-consume" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "perf-test-topic".to_string());
            let partition: u32 = get_arg_val(&args, "--partition")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let target_messages: u64 = get_arg_val(&args, "--messages")
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_000);
            // `--from-beginning` is accepted for symmetry with the other consume commands
            // but needs no binding here: this path already starts at 0 unless `--offset`
            // says otherwise.
            let max_bytes: u32 = get_arg_val(&args, "--max-bytes")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1024 * 1024);

            // `--from-beginning` and the no-flag default both start at 0, so only an
            // explicit `--offset` changes the starting point.
            let mut next_offset: u64 = get_arg_val(&args, "--offset")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            println!("============================================================");
            println!("   HERMES CONSUMER PERFORMANCE TEST");
            println!("============================================================");
            println!("  Server:        {}", server_addr);
            println!("  Topic:         {}", topic);
            println!("  Partition:     {}", partition);
            println!("  Target:        {} messages", target_messages);
            println!("  Start Offset:  {}", next_offset);
            println!("------------------------------------------------------------------");

            let mut consumed: u64 = 0;
            let mut consumed_bytes: u64 = 0;
            let test_start = std::time::Instant::now();
            let mut last_report = test_start;
            let mut last_report_count: u64 = 0;
            let mut empty_fetch_streak: u32 = 0;
            // Give up waiting for more data after ~2s of consecutive empty fetches
            // (100ms backoff each) rather than spinning forever if the target message
            // count exceeds what's actually been produced.
            const MAX_EMPTY_FETCH_STREAK: u32 = 20;

            while consumed < target_messages {
                match client.fetch(&topic, partition, next_offset, max_bytes).await {
                    Ok(frames) if !frames.is_empty() => {
                        empty_fetch_streak = 0;
                        for frame in &frames {
                            next_offset = frame.offset + 1;
                            // Match the plain fetch/consume command: control markers
                            // occupy real offsets (advance next_offset) but aren't
                            // real records for throughput accounting.
                            if !frame.is_control_marker() {
                                consumed += 1;
                                consumed_bytes += frame.payload.len() as u64;
                            }
                        }
                    }
                    Ok(_) => {
                        empty_fetch_streak += 1;
                        if empty_fetch_streak > MAX_EMPTY_FETCH_STREAK {
                            println!(
                                "\n⚠️  No new data after {} empty fetches — stopping early at {} of {} messages.",
                                empty_fetch_streak, consumed, target_messages
                            );
                            break;
                        }
                        sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        eprintln!("❌ Fetch failed after {} records: {}", consumed, e);
                        break;
                    }
                }

                let now = std::time::Instant::now();
                let since_report = now.duration_since(last_report).as_secs_f64();
                if since_report >= 1.0 {
                    let interval_records = consumed - last_report_count;
                    let rate = interval_records as f64 / since_report;
                    println!("  {:>10} messages consumed, {:>10.1} msg/sec", consumed, rate);
                    last_report = now;
                    last_report_count = consumed;
                }
            }

            let total_elapsed = test_start.elapsed().as_secs_f64().max(0.000_001);
            let overall_rate = consumed as f64 / total_elapsed;
            let overall_mb_rate = consumed_bytes as f64 / (1024.0 * 1024.0) / total_elapsed;

            println!("------------------------------------------------------------------");
            println!(
                "✅ {} messages consumed, {:.1} msg/sec ({:.2} MB/sec) in {:.2}s",
                consumed, overall_rate, overall_mb_rate, total_elapsed
            );
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
            // Plain Fetch returns raw frames, including transaction commit/abort control
            // markers — the server exposes them at the wire level (same as real Kafka)
            // and expects the client to skip them. Filter them out here so the CLI never
            // prints a control marker's raw payload as if it were a real record.
            let frames: Vec<_> = frames
                .into_iter()
                .filter(|f| !f.is_control_marker())
                .collect();

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
                                    // Advance past every frame (control markers occupy
                                    // real offsets too), but only print real records —
                                    // see the one-shot fetch command above for why.
                                    if !frame.is_control_marker() {
                                        let payload_str = String::from_utf8_lossy(&frame.payload);
                                        println!(
                                            "📥 Group '{}' consumed Offset {:<6} | Timestamp: {} | Payload: '{}'",
                                            group_id, frame.offset, frame.timestamp, payload_str
                                        );
                                    }
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
        "create-topic" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let num_partitions: u32 = get_arg_val(&args, "--partitions")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            client.create_topic(&topic, num_partitions).await?;
            println!(
                "✅ Created topic '{}' with {} partition(s) on server {}",
                topic, num_partitions, server_addr
            );
        }
        "delete-topic" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            client.delete_topic(&topic).await?;
            println!("🗑️ Topic '{}' deleted successfully.", topic);
        }
        "describe-topic" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let (res_topic, partitions) = client.describe_topic(&topic).await?;
            println!("📋 Topic '{}' on Server {}:", res_topic, server_addr);
            println!("  Partitions: {}", partitions.len());
            println!("------------------------------------------------------------------");
            println!(
                "  {:<10} | {:<16} | {:<9} | REPLICAS",
                "PARTITION", "HIGH WATERMARK", "LEADER"
            );
            for p in &partitions {
                println!(
                    "  {:<10} | {:<16} | {:<9} | {:?}",
                    p.partition_id, p.high_watermark, p.leader_id, p.replicas
                );
            }
        }
        "describe-group" => {
            let group_id =
                get_arg_val(&args, "--group").unwrap_or_else(|| "my_consumer_group".to_string());
            let (state, members) = client.describe_group(&group_id).await?;
            println!("👥 Consumer Group '{}' on Server {}:", group_id, server_addr);
            println!("  State:   {}", state);
            println!("  Members: {}", members.len());
            println!("------------------------------------------------------------------");
            for m in &members {
                println!("  Member '{}':", m.member_id);
                if m.assigned_partitions.is_empty() {
                    println!("    (no assigned partitions)");
                }
                for (topic, partition) in &m.assigned_partitions {
                    println!("    - {} partition {}", topic, partition);
                }
            }
        }
        "produce-batch" => {
            let topic =
                get_arg_val(&args, "--topic").unwrap_or_else(|| "default_topic".to_string());
            let key = get_arg_val(&args, "--key").unwrap_or_default();
            let num_partitions: u32 = get_arg_val(&args, "--partitions")
                .and_then(|s| s.parse().ok())
                .unwrap_or(3);
            let tx_id = get_arg_val(&args, "--tx-id");

            // Records come either from repeated/comma-separated `--messages`, or — when
            // `--stdin` is passed — one record per line read from standard input, so a
            // batch can be piped in rather than squeezed onto the command line.
            let records: Vec<Vec<u8>> = if has_flag(&args, "--stdin") {
                use std::io::BufRead;
                let stdin = std::io::stdin();
                stdin
                    .lock()
                    .lines()
                    .map_while(Result::ok)
                    .filter(|l| !l.is_empty())
                    .map(|l| l.into_bytes())
                    .collect()
            } else {
                get_arg_val(&args, "--messages")
                    .unwrap_or_else(|| "msg-1,msg-2,msg-3".to_string())
                    .split(',')
                    .map(|s| s.trim().as_bytes().to_vec())
                    .filter(|b| !b.is_empty())
                    .collect()
            };

            if records.is_empty() {
                eprintln!("Error: no records to produce (use --messages a,b,c or --stdin).");
                return Ok(());
            }

            // A transactional batch has to be bracketed by an explicit begin/commit —
            // the server rejects a produce naming a transaction that isn't Ongoing.
            if let Some(ref tx) = tx_id {
                client.begin_transaction(tx, 0).await?;
            }

            let res = client
                .produce_batch(&topic, &key, tx_id.as_deref(), num_partitions, &records)
                .await?;

            if let Some(ref tx) = tx_id {
                client.commit_transaction(tx).await?;
            }

            println!("✅ Produced {} record(s) successfully!", records.len());
            println!("  Server Address:     {}", server_addr);
            println!("  Topic:              {}", topic);
            println!("  Assigned Partition: {}", res.assigned_partition);
            println!("  First Offset:       {}", res.first_offset);
            println!("  Last Offset:        {}", res.last_offset);
            if let Some(ref tx) = tx_id {
                println!("  Transaction:        '{}' (committed)", tx);
            }
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

/// Nearest-rank percentile over an already-sorted (ascending) sample set. Returns 0.0 for
/// an empty set rather than panicking, since a perf test that fails its very first request
/// should still print a (zeroed) summary rather than crash on the stats line.
fn percentile(sorted_samples: &[f64], p: f64) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted_samples.len() - 1) as f64).round() as usize;
    sorted_samples[idx.min(sorted_samples.len() - 1)]
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
    println!("  produce-batch   Produce multiple records in one request");
    println!(
        "                  --topic <NAME> [--messages a,b,c | --stdin] [--key <KEY>] [--partitions <N>] [--tx-id <ID>]"
    );
    println!(
        "  perf-produce    Producer performance test (like kafka-producer-perf-test.sh)"
    );
    println!(
        "                  --topic <NAME> [--num-records <N>] [--record-size <BYTES>] [--batch-size <N>] [--throughput <N>] [--partitions <N>]"
    );
    println!(
        "  perf-consume    Consumer performance test (like kafka-consumer-perf-test.sh)"
    );
    println!(
        "                  --topic <NAME> [--partition <ID>] [--messages <N>] [--offset <N> | --from-beginning] [--max-bytes <N>]"
    );
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
    println!("  create-topic    Create a topic with an explicit partition count");
    println!("                  --topic <NAME> [--partitions <N>]");
    println!("  delete-topic    Delete a topic and its partition files");
    println!("                  --topic <NAME>");
    println!("  describe-topic  Show a topic's partitions, watermarks, leaders and replicas");
    println!("                  --topic <NAME>");
    println!("  describe-group  Show a consumer group's state, members and assignments");
    println!("                  --group <GROUP>");
    println!("\nGlobal Flag:");
    println!("  --server / -s   Server address (default: 127.0.0.1:9092 or localhost:9092)");
}
