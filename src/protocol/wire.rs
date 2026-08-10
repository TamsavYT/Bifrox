use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandCode {
    ProduceBatch = 0x01,
    Fetch = 0x02,
    CommitOffset = 0x03,
    FetchOffset = 0x04,
    Seek = 0x05,
    LatestOffset = 0x06,
    BeginTx = 0x07,
    CommitTx = 0x08,
    AbortTx = 0x09,
    FetchByTimestamp = 0x0A,
    /// P1: Read-committed fetch — hides uncommitted and aborted records
    FetchCommitted = 0x0B,
    Ping = 0x0C,
    ListTopics = 0x0D,
    DescribeCluster = 0x0E,
    DeleteTopic = 0x0F,
}

impl TryFrom<u8> for CommandCode {
    type Error = WireError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(CommandCode::ProduceBatch),
            0x02 => Ok(CommandCode::Fetch),
            0x03 => Ok(CommandCode::CommitOffset),
            0x04 => Ok(CommandCode::FetchOffset),
            0x05 => Ok(CommandCode::Seek),
            0x06 => Ok(CommandCode::LatestOffset),
            0x07 => Ok(CommandCode::BeginTx),
            0x08 => Ok(CommandCode::CommitTx),
            0x09 => Ok(CommandCode::AbortTx),
            0x0A => Ok(CommandCode::FetchByTimestamp),
            0x0B => Ok(CommandCode::FetchCommitted),
            0x0C => Ok(CommandCode::Ping),
            0x0D => Ok(CommandCode::ListTopics),
            0x0E => Ok(CommandCode::DescribeCluster),
            0x0F => Ok(CommandCode::DeleteTopic),
            _ => Err(WireError::UnknownCommand(value)),
        }
    }
}

pub const MAX_REQUEST_PAYLOAD_BYTES: usize = 64 * 1024 * 1024; // 64MB cap (SEC-01)

#[derive(Debug, Error)]
pub enum WireError {
    #[error("Unknown wire command code: 0x{0:02X}")]
    UnknownCommand(u8),
    #[error("Insufficient wire buffer bytes: needed {needed}, available {available}")]
    Incomplete { needed: usize, available: usize },
    #[error("Protocol error: {0}")]
    InvalidProtocol(String),
}

/// Request payloads received from clients over TCP
#[derive(Debug, Clone)]
pub enum RequestPayload {
    ProduceBatch {
        topic: String,
        key: String,
        transaction_id: String,
        num_partitions: u32,
        records: Vec<Bytes>,
    },
    Fetch {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    },
    CommitOffset {
        group_id: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    FetchOffset {
        group_id: String,
        topic: String,
        partition: u32,
    },
    Seek {
        topic: String,
        partition: u32,
        offset: u64,
    },
    LatestOffset {
        topic: String,
        partition: u32,
    },
    BeginTx {
        transaction_id: String,
        producer_id: u64,
    },
    CommitTx {
        transaction_id: String,
    },
    AbortTx {
        transaction_id: String,
    },
    FetchByTimestamp {
        topic: String,
        partition: u32,
        target_timestamp: u64,
        max_bytes: u32,
    },
    /// P1: Same wire shape as Fetch, but triggers read-committed LSO filtering
    FetchCommitted {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    },
    Ping,
    ListTopics,
    DescribeCluster,
    DeleteTopic {
        topic: String,
    },
}

#[derive(Debug, Clone)]
pub struct WireRequest {
    pub cmd: CommandCode,
    pub payload: RequestPayload,
}

impl WireRequest {
    /// Decode wire request from buffer: `[Cmd: 1b] | [Payload Len: 4b] | [Payload Bytes]`
    pub fn decode(mut src: &[u8]) -> Result<(Self, usize), WireError> {
        if src.len() < 5 {
            return Err(WireError::Incomplete {
                needed: 5,
                available: src.len(),
            });
        }

        let raw_cmd = src.get_u8();
        let cmd = CommandCode::try_from(raw_cmd)?;
        let payload_len = src.get_u32() as usize;

        if payload_len > MAX_REQUEST_PAYLOAD_BYTES {
            return Err(WireError::InvalidProtocol(format!(
                "Payload length {} exceeds maximum allowed limit of 64MB",
                payload_len
            )));
        }

        if src.len() < payload_len {
            return Err(WireError::Incomplete {
                needed: payload_len,
                available: src.len(),
            });
        }

        let mut payload_buf = &src[..payload_len];
        let req_payload = match cmd {
            CommandCode::ProduceBatch => {
                let topic = read_pascal_string(&mut payload_buf)?;
                let key = read_pascal_string(&mut payload_buf)?;
                let transaction_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 8 {
                    return Err(WireError::Incomplete {
                        needed: 8,
                        available: payload_buf.len(),
                    });
                }
                let num_partitions = payload_buf.get_u32();
                let record_count = payload_buf.get_u32() as usize;

                let mut records = Vec::with_capacity(record_count);
                for _ in 0..record_count {
                    if payload_buf.len() < 4 {
                        return Err(WireError::Incomplete {
                            needed: 4,
                            available: payload_buf.len(),
                        });
                    }
                    let rec_len = payload_buf.get_u32() as usize;
                    if payload_buf.len() < rec_len {
                        return Err(WireError::Incomplete {
                            needed: rec_len,
                            available: payload_buf.len(),
                        });
                    }
                    let rec_bytes = Bytes::copy_from_slice(&payload_buf[..rec_len]);
                    payload_buf = &payload_buf[rec_len..];
                    records.push(rec_bytes);
                }

                RequestPayload::ProduceBatch {
                    topic,
                    key,
                    transaction_id,
                    num_partitions,
                    records,
                }
            }
            CommandCode::Fetch => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 16 {
                    return Err(WireError::Incomplete {
                        needed: 16,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                let max_bytes = payload_buf.get_u32();
                RequestPayload::Fetch {
                    topic,
                    partition,
                    offset,
                    max_bytes,
                }
            }
            CommandCode::CommitOffset => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 12 {
                    return Err(WireError::Incomplete {
                        needed: 12,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                RequestPayload::CommitOffset {
                    group_id,
                    topic,
                    partition,
                    offset,
                }
            }
            CommandCode::FetchOffset => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                RequestPayload::FetchOffset {
                    group_id,
                    topic,
                    partition,
                }
            }
            CommandCode::Seek => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 12 {
                    return Err(WireError::Incomplete {
                        needed: 12,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                RequestPayload::Seek {
                    topic,
                    partition,
                    offset,
                }
            }
            CommandCode::LatestOffset => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                RequestPayload::LatestOffset { topic, partition }
            }
            CommandCode::BeginTx => {
                let transaction_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 8 {
                    return Err(WireError::Incomplete {
                        needed: 8,
                        available: payload_buf.len(),
                    });
                }
                let producer_id = payload_buf.get_u64();
                RequestPayload::BeginTx {
                    transaction_id,
                    producer_id,
                }
            }
            CommandCode::CommitTx => {
                let transaction_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::CommitTx { transaction_id }
            }
            CommandCode::AbortTx => {
                let transaction_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::AbortTx { transaction_id }
            }
            CommandCode::FetchByTimestamp => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 16 {
                    return Err(WireError::Incomplete {
                        needed: 16,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let target_timestamp = payload_buf.get_u64();
                let max_bytes = payload_buf.get_u32();
                RequestPayload::FetchByTimestamp {
                    topic,
                    partition,
                    target_timestamp,
                    max_bytes,
                }
            }
            CommandCode::FetchCommitted => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 16 {
                    return Err(WireError::Incomplete {
                        needed: 16,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                let offset = payload_buf.get_u64();
                let max_bytes = payload_buf.get_u32();
                RequestPayload::FetchCommitted {
                    topic,
                    partition,
                    offset,
                    max_bytes,
                }
            }
            CommandCode::Ping => RequestPayload::Ping,
            CommandCode::ListTopics => RequestPayload::ListTopics,
            CommandCode::DescribeCluster => RequestPayload::DescribeCluster,
            CommandCode::DeleteTopic => {
                let topic = read_pascal_string(&mut payload_buf)?;
                RequestPayload::DeleteTopic { topic }
            }
        };

        let total_consumed = 5 + payload_len;
        Ok((
            WireRequest {
                cmd,
                payload: req_payload,
            },
            total_consumed,
        ))
    }
}

/// Helper function to read pascal-style strings: `[Len: 2b] | [UTF-8 bytes]`
fn read_pascal_string(buf: &mut &[u8]) -> Result<String, WireError> {
    if buf.len() < 2 {
        return Err(WireError::Incomplete {
            needed: 2,
            available: buf.len(),
        });
    }
    let len = buf.get_u16() as usize;
    if buf.len() < len {
        return Err(WireError::Incomplete {
            needed: len,
            available: buf.len(),
        });
    }
    let str_bytes = &buf[..len];
    *buf = &buf[len..];
    String::from_utf8(str_bytes.to_vec())
        .map_err(|e| WireError::InvalidProtocol(format!("Invalid UTF-8 string: {}", e)))
}

/// Helper function to write pascal-style strings: `[Len: 2b] | [UTF-8 bytes]`
pub fn write_pascal_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    // H9: Guard against silent u16 wrap-around.  Strings in this protocol (topic
    // names, cluster IDs, addresses) are always short; exceeding 65535 bytes is a
    // programming error, not a runtime condition.
    assert!(
        bytes.len() <= u16::MAX as usize,
        "write_pascal_string: string too long ({} bytes, max {})",
        bytes.len(),
        u16::MAX
    );
    buf.put_u16(bytes.len() as u16);
    buf.put_slice(bytes);
}

/// Binary response returned to clients over TCP: `[Status Code: 1b] | [Payload Len: 4b] | [Payload]`
#[derive(Debug)]
pub struct WireResponse {
    pub status: u8, // 0 = OK, 1 = Error
    pub payload: Vec<u8>,
}

impl WireResponse {
    pub fn ok(payload: Vec<u8>) -> Self {
        Self { status: 0, payload }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            status: 1,
            payload: msg.as_bytes().to_vec(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(5 + self.payload.len());
        buf.put_u8(self.status);
        buf.put_u32(self.payload.len() as u32);
        buf.put_slice(&self.payload);
        buf
    }
}
