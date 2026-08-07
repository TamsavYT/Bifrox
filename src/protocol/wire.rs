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
            _ => Err(WireError::UnknownCommand(value)),
        }
    }
}

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

    /// Serializes a WireRequest into its wire binary representation: `[Cmd: 1b] | [Payload Len: 4b] | [Payload]`
    pub fn encode(&self) -> Vec<u8> {
        let mut inner = Vec::new();
        match &self.payload {
            RequestPayload::ProduceBatch {
                topic,
                key,
                num_partitions,
                records,
            } => {
                write_pascal_string(&mut inner, topic);
                write_pascal_string(&mut inner, key);
                inner.put_u32(*num_partitions);
                inner.put_u32(records.len() as u32);
                for rec in records {
                    inner.put_u32(rec.len() as u32);
                    inner.put_slice(rec);
                }
            }
            RequestPayload::Fetch {
                topic,
                partition,
                offset,
                max_bytes,
            } => {
                write_pascal_string(&mut inner, topic);
                inner.put_u32(*partition);
                inner.put_u64(*offset);
                inner.put_u32(*max_bytes);
            }
            RequestPayload::CommitOffset {
                group_id,
                topic,
                partition,
                offset,
            } => {
                write_pascal_string(&mut inner, group_id);
                write_pascal_string(&mut inner, topic);
                inner.put_u32(*partition);
                inner.put_u64(*offset);
            }
            RequestPayload::FetchOffset {
                group_id,
                topic,
                partition,
            } => {
                write_pascal_string(&mut inner, group_id);
                write_pascal_string(&mut inner, topic);
                inner.put_u32(*partition);
            }
            RequestPayload::Seek {
                topic,
                partition,
                offset,
            } => {
                write_pascal_string(&mut inner, topic);
                inner.put_u32(*partition);
                inner.put_u64(*offset);
            }
            RequestPayload::LatestOffset { topic, partition } => {
                write_pascal_string(&mut inner, topic);
                inner.put_u32(*partition);
            }
        }

        let mut buf = Vec::with_capacity(5 + inner.len());
        buf.put_u8(self.cmd as u8);
        buf.put_u32(inner.len() as u32);
        buf.extend_from_slice(&inner);
        buf
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
    buf.put_u16(bytes.len() as u16);
    buf.put_slice(bytes);
}

/// Binary response returned to clients over TCP: `[Status Code: 1b] | [Payload Len: 4b] | [Payload]`
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// Decodes a WireResponse from a byte buffer: `[Status: 1b] | [Payload Len: 4b] | [Payload]`
    pub fn decode(mut src: &[u8]) -> Result<(Self, usize), WireError> {
        if src.len() < 5 {
            return Err(WireError::Incomplete {
                needed: 5,
                available: src.len(),
            });
        }
        let status = src.get_u8();
        let payload_len = src.get_u32() as usize;
        if src.len() < payload_len {
            return Err(WireError::Incomplete {
                needed: payload_len,
                available: src.len(),
            });
        }
        let payload = src[..payload_len].to_vec();
        Ok((Self { status, payload }, 5 + payload_len))
    }
}
