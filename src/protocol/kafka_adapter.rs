use crate::protocol::wire::{CommandCode, RequestPayload, WireError, WireRequest};
use bytes::{Buf, BufMut, Bytes};

/// Kafka API Key Constants matching standard Kafka Wire Protocol Specification
pub const KAFKA_API_PRODUCE: i16 = 0;
pub const KAFKA_API_FETCH: i16 = 1;
pub const KAFKA_API_LIST_OFFSETS: i16 = 2;
pub const KAFKA_API_METADATA: i16 = 3;
pub const KAFKA_API_OFFSET_COMMIT: i16 = 8;
pub const KAFKA_API_OFFSET_FETCH: i16 = 9;
pub const KAFKA_API_JOIN_GROUP: i16 = 11;
pub const KAFKA_API_HEARTBEAT: i16 = 12;
pub const KAFKA_API_LEAVE_GROUP: i16 = 13;
pub const KAFKA_API_SYNC_GROUP: i16 = 14;
pub const KAFKA_API_SASL_HANDSHAKE: i16 = 17;
pub const KAFKA_API_API_VERSIONS: i16 = 18;
pub const KAFKA_API_SASL_AUTHENTICATE: i16 = 36;

#[derive(Debug, Clone)]
pub struct KafkaHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

pub struct KafkaWireAdapter;

impl KafkaWireAdapter {
    /// Decodes a standard Kafka binary protocol request header and converts the request
    /// payload into Hermes' internal WireRequest representation.
    pub fn decode_kafka_request(
        src: &[u8],
    ) -> Result<(KafkaHeader, WireRequest, usize), WireError> {
        if src.len() < 8 {
            return Err(WireError::Incomplete {
                needed: 8,
                available: src.len(),
            });
        }

        let mut buf = src;
        let total_size = buf.get_u32() as usize;

        if src.len() < 4 + total_size {
            return Err(WireError::Incomplete {
                needed: 4 + total_size,
                available: src.len(),
            });
        }

        let payload_slice = &src[4..4 + total_size];
        let mut p_buf = payload_slice;

        let api_key = p_buf.get_i16();
        let api_version = p_buf.get_i16();
        let correlation_id = p_buf.get_i32();

        let client_id_len = p_buf.get_i16();
        let client_id = if client_id_len > 0 && p_buf.len() >= client_id_len as usize {
            let str_bytes = &p_buf[..client_id_len as usize];
            p_buf = &p_buf[client_id_len as usize..];
            String::from_utf8(str_bytes.to_vec()).ok()
        } else {
            None
        };

        let header = KafkaHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        };

        let wire_req = match api_key {
            KAFKA_API_PRODUCE => {
                let topic = if !p_buf.is_empty() {
                    let len = p_buf.get_i16() as usize;
                    if p_buf.len() >= len {
                        let t = String::from_utf8_lossy(&p_buf[..len]).to_string();
                        p_buf = &p_buf[len..];
                        t
                    } else {
                        "default".to_string()
                    }
                } else {
                    "default".to_string()
                };

                let record_bytes = Bytes::copy_from_slice(p_buf);
                WireRequest {
                    cmd: CommandCode::ProduceBatch,
                    payload: RequestPayload::ProduceBatch {
                        topic,
                        key: "".to_string(),
                        transaction_id: "".to_string(),
                        num_partitions: 1,
                        producer_id: 0,
                        producer_epoch: 0,
                        base_sequence: 0,
                        records: vec![record_bytes],
                    },
                }
            }
            KAFKA_API_FETCH => {
                let topic = if !p_buf.is_empty() {
                    let len = p_buf.get_i16() as usize;
                    if p_buf.len() >= len {
                        let t = String::from_utf8_lossy(&p_buf[..len]).to_string();
                        p_buf = &p_buf[len..];
                        t
                    } else {
                        "default".to_string()
                    }
                } else {
                    "default".to_string()
                };

                let offset = if p_buf.len() >= 8 { p_buf.get_u64() } else { 0 };
                let max_bytes = if p_buf.len() >= 4 {
                    p_buf.get_u32()
                } else {
                    1048576
                };

                WireRequest {
                    cmd: CommandCode::Fetch,
                    payload: RequestPayload::Fetch {
                        topic,
                        partition: 0,
                        offset,
                        max_bytes,
                    },
                }
            }
            KAFKA_API_METADATA => WireRequest {
                cmd: CommandCode::ListTopics,
                payload: RequestPayload::ListTopics,
            },
            KAFKA_API_API_VERSIONS => WireRequest {
                cmd: CommandCode::DescribeCluster,
                payload: RequestPayload::DescribeCluster,
            },
            KAFKA_API_SASL_HANDSHAKE => {
                let mechanism = if p_buf.len() >= 2 {
                    let len = p_buf.get_i16() as usize;
                    if p_buf.len() >= len {
                        String::from_utf8_lossy(&p_buf[..len]).to_string()
                    } else {
                        "PLAIN".to_string()
                    }
                } else {
                    "PLAIN".to_string()
                };
                WireRequest {
                    cmd: CommandCode::SaslHandshake,
                    payload: RequestPayload::SaslHandshake { mechanism },
                }
            }
            KAFKA_API_SASL_AUTHENTICATE => WireRequest {
                cmd: CommandCode::SaslAuthenticate,
                payload: RequestPayload::SaslAuthenticate {
                    auth_bytes: p_buf.to_vec(),
                },
            },
            _ => WireRequest {
                cmd: CommandCode::Ping,
                payload: RequestPayload::Ping,
            },
        };

        Ok((header, wire_req, 4 + total_size))
    }

    /// Formats a standard Kafka Wire Protocol response binary frame given correlation_id and payload.
    pub fn encode_kafka_response(correlation_id: i32, payload: &[u8]) -> Vec<u8> {
        let total_size = 4 + payload.len();
        let mut buf = Vec::with_capacity(4 + total_size);
        buf.put_u32(total_size as u32);
        buf.put_i32(correlation_id);
        buf.extend_from_slice(payload);
        buf
    }
}
