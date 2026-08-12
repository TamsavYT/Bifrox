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
    JoinGroup = 0x10,
    SyncGroup = 0x11,
    Heartbeat = 0x12,
    LeaveGroup = 0x13,
    CreateTopic = 0x14,
    DescribeTopic = 0x15,
    ListGroups = 0x16,
    DescribeGroup = 0x17,
    InitProducerId = 0x18,
    AddPartitionsToTxn = 0x19,
    EndTxn = 0x1A,
    OffsetCommit = 0x1B,
    OffsetFetch = 0x1C,
    SaslHandshake = 0x1D,
    SaslAuthenticate = 0x1E,
    DescribeAcls = 0x1F,
    CreateAcls = 0x20,
    DeleteAcls = 0x21,
    RegisterBroker = 0x22,
    UnregisterBroker = 0x23,
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
            0x10 => Ok(CommandCode::JoinGroup),
            0x11 => Ok(CommandCode::SyncGroup),
            0x12 => Ok(CommandCode::Heartbeat),
            0x13 => Ok(CommandCode::LeaveGroup),
            0x14 => Ok(CommandCode::CreateTopic),
            0x15 => Ok(CommandCode::DescribeTopic),
            0x16 => Ok(CommandCode::ListGroups),
            0x17 => Ok(CommandCode::DescribeGroup),
            0x18 => Ok(CommandCode::InitProducerId),
            0x19 => Ok(CommandCode::AddPartitionsToTxn),
            0x1A => Ok(CommandCode::EndTxn),
            0x1B => Ok(CommandCode::OffsetCommit),
            0x1C => Ok(CommandCode::OffsetFetch),
            0x1D => Ok(CommandCode::SaslHandshake),
            0x1E => Ok(CommandCode::SaslAuthenticate),
            0x1F => Ok(CommandCode::DescribeAcls),
            0x20 => Ok(CommandCode::CreateAcls),
            0x21 => Ok(CommandCode::DeleteAcls),
            0x22 => Ok(CommandCode::RegisterBroker),
            0x23 => Ok(CommandCode::UnregisterBroker),
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
        producer_id: u64,
        producer_epoch: i16,
        base_sequence: i32,
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
    JoinGroup {
        group_id: String,
        member_id: String,
        protocols: Vec<String>,
    },
    SyncGroup {
        group_id: String,
        generation_id: u32,
        member_id: String,
        assignments: Vec<MemberAssignment>,
    },
    Heartbeat {
        group_id: String,
        generation_id: u32,
        member_id: String,
    },
    LeaveGroup {
        group_id: String,
        member_id: String,
    },
    CreateTopic {
        topic: String,
        partitions: u32,
    },
    DescribeTopic {
        topic: String,
    },
    ListGroups,
    DescribeGroup {
        group_id: String,
    },
    InitProducerId {
        transactional_id: String,
    },
    AddPartitionsToTxn {
        transactional_id: String,
        producer_id: u64,
        producer_epoch: i16,
        topics: Vec<(String, Vec<u32>)>,
    },
    EndTxn {
        transactional_id: String,
        producer_id: u64,
        producer_epoch: i16,
        committed: bool,
    },
    OffsetCommit {
        group_id: String,
        topic: String,
        partition: u32,
        offset: u64,
        metadata: String,
    },
    OffsetFetch {
        group_id: String,
        topic: String,
        partition: u32,
    },
    SaslHandshake {
        mechanism: String,
    },
    SaslAuthenticate {
        auth_bytes: Vec<u8>,
    },
    DescribeAcls {
        resource_type: u8,
        resource_name: String,
        pattern_type: u8,
        principal: String,
        host: String,
        operation: u8,
        permission_type: u8,
    },
    CreateAcls {
        resource_type: u8,
        resource_name: String,
        pattern_type: u8,
        principal: String,
        host: String,
        operation: u8,
        permission_type: u8,
    },
    DeleteAcls {
        resource_type: u8,
        resource_name: String,
        pattern_type: u8,
        principal: String,
        host: String,
        operation: u8,
        permission_type: u8,
    },
    RegisterBroker {
        node_id: u32,
        endpoint: String,
    },
    UnregisterBroker {
        node_id: u32,
    },
}

#[derive(Debug, Clone)]
pub struct MemberAssignment {
    pub member_id: String,
    pub topic: String,
    pub partitions: Vec<u32>,
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
                if payload_buf.len() < 22 {
                    return Err(WireError::Incomplete {
                        needed: 22,
                        available: payload_buf.len(),
                    });
                }
                let num_partitions = payload_buf.get_u32();
                let producer_id = payload_buf.get_u64();
                let producer_epoch = payload_buf.get_i16();
                let base_sequence = payload_buf.get_i32();
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
                    producer_id,
                    producer_epoch,
                    base_sequence,
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
            CommandCode::JoinGroup => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let member_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let proto_count = payload_buf.get_u32() as usize;
                let mut protocols = Vec::with_capacity(proto_count);
                for _ in 0..proto_count {
                    protocols.push(read_pascal_string(&mut payload_buf)?);
                }
                RequestPayload::JoinGroup {
                    group_id,
                    member_id,
                    protocols,
                }
            }
            CommandCode::SyncGroup => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let generation_id = payload_buf.get_u32();
                let member_id = read_pascal_string(&mut payload_buf)?;

                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let assign_count = payload_buf.get_u32() as usize;
                let mut assignments = Vec::with_capacity(assign_count);
                for _ in 0..assign_count {
                    let a_member_id = read_pascal_string(&mut payload_buf)?;
                    let a_topic = read_pascal_string(&mut payload_buf)?;
                    if payload_buf.len() < 4 {
                        return Err(WireError::Incomplete {
                            needed: 4,
                            available: payload_buf.len(),
                        });
                    }
                    let p_count = payload_buf.get_u32() as usize;
                    if payload_buf.len() < p_count * 4 {
                        return Err(WireError::Incomplete {
                            needed: p_count * 4,
                            available: payload_buf.len(),
                        });
                    }
                    let mut partitions = Vec::with_capacity(p_count);
                    for _ in 0..p_count {
                        partitions.push(payload_buf.get_u32());
                    }
                    assignments.push(MemberAssignment {
                        member_id: a_member_id,
                        topic: a_topic,
                        partitions,
                    });
                }
                RequestPayload::SyncGroup {
                    group_id,
                    generation_id,
                    member_id,
                    assignments,
                }
            }
            CommandCode::Heartbeat => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let generation_id = payload_buf.get_u32();
                let member_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::Heartbeat {
                    group_id,
                    generation_id,
                    member_id,
                }
            }
            CommandCode::LeaveGroup => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let member_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::LeaveGroup {
                    group_id,
                    member_id,
                }
            }
            CommandCode::CreateTopic => {
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let partitions = payload_buf.get_u32();
                RequestPayload::CreateTopic { topic, partitions }
            }
            CommandCode::DescribeTopic => {
                let topic = read_pascal_string(&mut payload_buf)?;
                RequestPayload::DescribeTopic { topic }
            }
            CommandCode::ListGroups => RequestPayload::ListGroups,
            CommandCode::DescribeGroup => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::DescribeGroup { group_id }
            }
            CommandCode::InitProducerId => {
                let transactional_id = read_pascal_string(&mut payload_buf)?;
                RequestPayload::InitProducerId { transactional_id }
            }
            CommandCode::AddPartitionsToTxn => {
                let transactional_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 10 {
                    return Err(WireError::Incomplete {
                        needed: 10,
                        available: payload_buf.len(),
                    });
                }
                let producer_id = payload_buf.get_u64();
                let producer_epoch = payload_buf.get_i16();

                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let topic_count = payload_buf.get_u32() as usize;
                let mut topics = Vec::with_capacity(topic_count);

                for _ in 0..topic_count {
                    let t_name = read_pascal_string(&mut payload_buf)?;
                    if payload_buf.len() < 4 {
                        return Err(WireError::Incomplete {
                            needed: 4,
                            available: payload_buf.len(),
                        });
                    }
                    let p_count = payload_buf.get_u32() as usize;
                    if payload_buf.len() < p_count * 4 {
                        return Err(WireError::Incomplete {
                            needed: p_count * 4,
                            available: payload_buf.len(),
                        });
                    }
                    let mut parts = Vec::with_capacity(p_count);
                    for _ in 0..p_count {
                        parts.push(payload_buf.get_u32());
                    }
                    topics.push((t_name, parts));
                }
                RequestPayload::AddPartitionsToTxn {
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    topics,
                }
            }
            CommandCode::EndTxn => {
                let transactional_id = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 11 {
                    return Err(WireError::Incomplete {
                        needed: 11,
                        available: payload_buf.len(),
                    });
                }
                let producer_id = payload_buf.get_u64();
                let producer_epoch = payload_buf.get_i16();
                let committed = payload_buf.get_u8() != 0;
                RequestPayload::EndTxn {
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    committed,
                }
            }
            CommandCode::OffsetCommit => {
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
                let metadata = if !payload_buf.is_empty() {
                    read_pascal_string(&mut payload_buf)?
                } else {
                    String::new()
                };
                RequestPayload::OffsetCommit {
                    group_id,
                    topic,
                    partition,
                    offset,
                    metadata,
                }
            }
            CommandCode::OffsetFetch => {
                let group_id = read_pascal_string(&mut payload_buf)?;
                let topic = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let partition = payload_buf.get_u32();
                RequestPayload::OffsetFetch {
                    group_id,
                    topic,
                    partition,
                }
            }
            CommandCode::SaslHandshake => {
                let mechanism = read_pascal_string(&mut payload_buf)?;
                RequestPayload::SaslHandshake { mechanism }
            }
            CommandCode::SaslAuthenticate => {
                let auth_bytes = payload_buf.to_vec();
                RequestPayload::SaslAuthenticate { auth_bytes }
            }
            CommandCode::DescribeAcls => {
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let resource_type = payload_buf.get_u8();
                let resource_name = read_pascal_string(&mut payload_buf)?;
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let pattern_type = payload_buf.get_u8();
                let principal = read_pascal_string(&mut payload_buf)?;
                let host = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 2 {
                    return Err(WireError::Incomplete {
                        needed: 2,
                        available: payload_buf.len(),
                    });
                }
                let operation = payload_buf.get_u8();
                let permission_type = payload_buf.get_u8();
                RequestPayload::DescribeAcls {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                }
            }
            CommandCode::CreateAcls => {
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let resource_type = payload_buf.get_u8();
                let resource_name = read_pascal_string(&mut payload_buf)?;
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let pattern_type = payload_buf.get_u8();
                let principal = read_pascal_string(&mut payload_buf)?;
                let host = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 2 {
                    return Err(WireError::Incomplete {
                        needed: 2,
                        available: payload_buf.len(),
                    });
                }
                let operation = payload_buf.get_u8();
                let permission_type = payload_buf.get_u8();
                RequestPayload::CreateAcls {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                }
            }
            CommandCode::DeleteAcls => {
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let resource_type = payload_buf.get_u8();
                let resource_name = read_pascal_string(&mut payload_buf)?;
                if payload_buf.is_empty() {
                    return Err(WireError::Incomplete {
                        needed: 1,
                        available: 0,
                    });
                }
                let pattern_type = payload_buf.get_u8();
                let principal = read_pascal_string(&mut payload_buf)?;
                let host = read_pascal_string(&mut payload_buf)?;
                if payload_buf.len() < 2 {
                    return Err(WireError::Incomplete {
                        needed: 2,
                        available: payload_buf.len(),
                    });
                }
                let operation = payload_buf.get_u8();
                let permission_type = payload_buf.get_u8();
                RequestPayload::DeleteAcls {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                }
            }
            CommandCode::RegisterBroker => {
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let node_id = payload_buf.get_u32();
                let endpoint = read_pascal_string(&mut payload_buf)?;
                RequestPayload::RegisterBroker { node_id, endpoint }
            }
            CommandCode::UnregisterBroker => {
                if payload_buf.len() < 4 {
                    return Err(WireError::Incomplete {
                        needed: 4,
                        available: payload_buf.len(),
                    });
                }
                let node_id = payload_buf.get_u32();
                RequestPayload::UnregisterBroker { node_id }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroupMember {
    pub member_id: String,
    pub assigned_partitions: Vec<(String, u32)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedPartition {
    pub partition_id: u32,
    pub high_watermark: u64,
    pub leader_id: u32,
    pub replicas: Vec<u32>,
}

/// Encodes DescribeTopic binary payload: `[Topic: pascal] | [NumPartitions: 4b] | { [PartitionID: 4b] | [HighWatermark: 8b] | [LeaderID: 4b] | [ReplicasLen: 4b] | [Replicas...] }...`
pub fn encode_describe_topic_response(topic: &str, partitions: &[DescribedPartition]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_pascal_string(&mut buf, topic);
    buf.put_u32(partitions.len() as u32);
    for p in partitions {
        buf.put_u32(p.partition_id);
        buf.put_u64(p.high_watermark);
        buf.put_u32(p.leader_id);
        buf.put_u32(p.replicas.len() as u32);
        for &r in &p.replicas {
            buf.put_u32(r);
        }
    }
    buf
}

/// Encodes DescribeGroup binary payload: `[GroupState: pascal] | [MemberCount: 4b] | { [MemberID: pascal] | [NumAssignments: 4b] | { [Topic: pascal] | [Partition: 4b] }... }...`
pub fn encode_describe_group_response(state: &str, members: &[DescribedGroupMember]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_pascal_string(&mut buf, state);
    buf.put_u32(members.len() as u32);
    for member in members {
        write_pascal_string(&mut buf, &member.member_id);
        buf.put_u32(member.assigned_partitions.len() as u32);
        for (topic, partition) in &member.assigned_partitions {
            write_pascal_string(&mut buf, topic);
            buf.put_u32(*partition);
        }
    }
    buf
}

/// Encodes OffsetFetch binary payload: `[Offset: 8b] | [Metadata: pascal]`
pub fn encode_offset_fetch_response(offset: u64, metadata: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10 + metadata.len());
    buf.put_u64(offset);
    write_pascal_string(&mut buf, metadata);
    buf
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
