use crate::protocol::{RequestPayload, WireRequest, WireResponse};
use crate::server::engine::StorageEngine;
use bytes::BufMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Handles incoming TCP client connections asynchronously using Tokio IOCP
pub async fn handle_connection(mut socket: TcpStream, engine: StorageEngine) {
    let mut buffer = vec![0u8; 64 * 1024];
    let mut read_bytes_total = 0usize;

    loop {
        let read_count = match socket.read(&mut buffer[read_bytes_total..]).await {
            Ok(0) => break, // Connection closed cleanly by client
            Ok(n) => n,
            Err(e) => {
                tracing::debug!("TCP socket connection read error: {}", e);
                break;
            }
        };

        read_bytes_total += read_count;
        let mut cursor = 0usize;

        while cursor < read_bytes_total {
            let slice = &buffer[cursor..read_bytes_total];
            match WireRequest::decode(slice) {
                Ok((req, consumed)) => {
                    cursor += consumed;
                    let response = process_request(&engine, req);
                    let encoded_resp = response.encode();

                    if let Err(e) = socket.write_all(&encoded_resp).await {
                        tracing::error!("Failed to send TCP response: {}", e);
                        return;
                    }
                }
                Err(crate::protocol::WireError::Incomplete { .. }) => {
                    break;
                }
                Err(err) => {
                    tracing::warn!("Malformed TCP request frame: {}", err);
                    let resp = WireResponse::error(&format!("Protocol Error: {}", err));
                    let _ = socket.write_all(&resp.encode()).await;
                    return;
                }
            }
        }

        if cursor > 0 {
            buffer.copy_within(cursor..read_bytes_total, 0);
            read_bytes_total -= cursor;
        }

        if read_bytes_total == buffer.len() {
            buffer.resize(buffer.len() * 2, 0);
        }
    }
}

fn process_request(engine: &StorageEngine, req: WireRequest) -> WireResponse {
    match req.payload {
        RequestPayload::ProduceBatch {
            topic,
            key,
            num_partitions,
            records,
        } => match engine.produce_batch(&topic, &key, num_partitions, &records) {
            Ok((assigned_partition, first_offset, last_offset)) => {
                let mut resp_buf = Vec::with_capacity(20);
                resp_buf.put_u32(assigned_partition);
                resp_buf.put_u64(first_offset);
                resp_buf.put_u64(last_offset);
                WireResponse::ok(resp_buf)
            }
            Err(e) => WireResponse::error(&format!("ProduceBatch failed: {}", e)),
        },
        RequestPayload::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
        } => match engine.fetch(&topic, partition, offset, max_bytes) {
            Ok(frames) => {
                let mut resp_buf = Vec::new();
                resp_buf.put_u32(frames.len() as u32);
                for frame in frames {
                    frame.encode_into(&mut resp_buf);
                }
                WireResponse::ok(resp_buf)
            }
            Err(e) => WireResponse::error(&format!("Fetch failed: {}", e)),
        },
        RequestPayload::CommitOffset {
            group_id,
            topic,
            partition,
            offset,
        } => match engine.commit_offset(&group_id, &topic, partition, offset) {
            Ok(()) => WireResponse::ok(Vec::new()),
            Err(e) => WireResponse::error(&format!("CommitOffset failed: {}", e)),
        },
        RequestPayload::FetchOffset {
            group_id,
            topic,
            partition,
        } => {
            let offset = engine
                .fetch_offset(&group_id, &topic, partition)
                .unwrap_or(u64::MAX);
            let mut resp_buf = Vec::with_capacity(8);
            resp_buf.put_u64(offset);
            WireResponse::ok(resp_buf)
        }
        RequestPayload::Seek {
            topic,
            partition,
            offset,
        } => match engine.seek(&topic, partition, offset) {
            Ok(Some((base_offset, physical_pos))) => {
                let mut resp_buf = Vec::with_capacity(16);
                resp_buf.put_u64(base_offset);
                resp_buf.put_u64(physical_pos);
                WireResponse::ok(resp_buf)
            }
            Ok(None) => WireResponse::error("Offset not found in index"),
            Err(e) => WireResponse::error(&format!("Seek failed: {}", e)),
        },
        RequestPayload::LatestOffset { topic, partition } => {
            match engine.latest_offset(&topic, partition) {
                Ok(watermark) => {
                    let mut resp_buf = Vec::with_capacity(8);
                    resp_buf.put_u64(watermark);
                    WireResponse::ok(resp_buf)
                }
                Err(e) => WireResponse::error(&format!("LatestOffset failed: {}", e)),
            }
        }
    }
}
