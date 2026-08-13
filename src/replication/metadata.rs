use bytes::{Buf, BufMut};
use std::io::{Error, ErrorKind, Result as IoResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataRecord {
    TopicPartition {
        topic: String,
        partition: u32,
        leader_id: u32,
        replicas: Vec<u32>,
    },
    BrokerRegister {
        node_id: u32,
        bind_addr: String,
    },
    TopicCreated {
        topic: String,
        num_partitions: u32,
        replication_factor: u16,
    },
    TopicDeleted {
        topic: String,
    },
    PartitionLeadershipChange {
        topic: String,
        partition: u32,
        leader_id: u32,
        leader_epoch: u32,
        isr: Vec<u32>,
    },
    AclCreated {
        binding: crate::server::acl::AclBinding,
    },
    AclDeleted {
        binding: crate::server::acl::AclBinding,
    },
    BrokerUnregister {
        node_id: u32,
    },
    ScramCredentialUpsert {
        username: String,
        iterations: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    },
    ScramCredentialDelete {
        username: String,
    },
    TransactionalProducerRegistration {
        transactional_id: String,
        producer_id: u64,
        producer_epoch: i16,
    },
}

impl MetadataRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            MetadataRecord::TopicPartition {
                topic,
                partition,
                leader_id,
                replicas,
            } => {
                buf.put_u8(0x01); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, topic);
                buf.put_u32(*partition);
                buf.put_u32(*leader_id);
                buf.put_u32(replicas.len() as u32);
                for &r in replicas {
                    buf.put_u32(r);
                }
            }
            MetadataRecord::BrokerRegister { node_id, bind_addr } => {
                buf.put_u8(0x02); // record type
                buf.put_u32(*node_id);
                crate::protocol::wire::write_pascal_string(&mut buf, bind_addr);
            }
            MetadataRecord::TopicCreated {
                topic,
                num_partitions,
                replication_factor,
            } => {
                buf.put_u8(0x03); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, topic);
                buf.put_u32(*num_partitions);
                buf.put_u16(*replication_factor);
            }
            MetadataRecord::TopicDeleted { topic } => {
                buf.put_u8(0x04); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, topic);
            }
            MetadataRecord::PartitionLeadershipChange {
                topic,
                partition,
                leader_id,
                leader_epoch,
                isr,
            } => {
                buf.put_u8(0x05); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, topic);
                buf.put_u32(*partition);
                buf.put_u32(*leader_id);
                buf.put_u32(*leader_epoch);
                buf.put_u32(isr.len() as u32);
                for &id in isr {
                    buf.put_u32(id);
                }
            }
            MetadataRecord::AclCreated { binding } => {
                buf.put_u8(0x06); // record type
                buf.put_u8(binding.resource_type);
                crate::protocol::wire::write_pascal_string(&mut buf, &binding.resource_name);
                buf.put_u8(binding.pattern_type);
                crate::protocol::wire::write_pascal_string(&mut buf, &binding.principal);
                crate::protocol::wire::write_pascal_string(&mut buf, &binding.host);
                buf.put_u8(binding.operation);
                buf.put_u8(binding.permission_type);
            }
            MetadataRecord::AclDeleted { binding } => {
                buf.put_u8(0x07); // record type
                buf.put_u8(binding.resource_type);
                crate::protocol::wire::write_pascal_string(&mut buf, &binding.resource_name);
                buf.put_u8(binding.pattern_type);
                crate::protocol::wire::write_pascal_string(&mut buf, &binding.principal);
                crate::protocol::wire::write_pascal_string(&mut buf, &binding.host);
                buf.put_u8(binding.operation);
                buf.put_u8(binding.permission_type);
            }
            MetadataRecord::BrokerUnregister { node_id } => {
                buf.put_u8(0x08); // record type
                buf.put_u32(*node_id);
            }
            MetadataRecord::ScramCredentialUpsert {
                username,
                iterations,
                salt,
                stored_key,
                server_key,
            } => {
                buf.put_u8(0x09); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, username);
                buf.put_u32(*iterations);
                buf.put_u16(salt.len() as u16);
                buf.extend_from_slice(salt);
                buf.put_u16(stored_key.len() as u16);
                buf.extend_from_slice(stored_key);
                buf.put_u16(server_key.len() as u16);
                buf.extend_from_slice(server_key);
            }
            MetadataRecord::ScramCredentialDelete { username } => {
                buf.put_u8(0x0A); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, username);
            }
            MetadataRecord::TransactionalProducerRegistration {
                transactional_id,
                producer_id,
                producer_epoch,
            } => {
                buf.put_u8(0x0B); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, transactional_id);
                buf.put_u64(*producer_id);
                buf.put_i16(*producer_epoch);
            }
        }
        buf
    }

    pub fn decode(mut src: &[u8]) -> IoResult<Self> {
        if src.is_empty() {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "Empty metadata record",
            ));
        }
        let record_type = src.get_u8();
        match record_type {
            0x01 => {
                let topic = read_pascal_string_io(&mut src)?;
                if src.len() < 12 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete topic metadata",
                    ));
                }
                let partition = src.get_u32();
                let leader_id = src.get_u32();
                let replicas_len = src.get_u32() as usize;
                if src.len() < replicas_len * 4 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete replicas list",
                    ));
                }
                let mut replicas = Vec::with_capacity(replicas_len);
                for _ in 0..replicas_len {
                    replicas.push(src.get_u32());
                }
                Ok(MetadataRecord::TopicPartition {
                    topic,
                    partition,
                    leader_id,
                    replicas,
                })
            }
            0x02 => {
                if src.len() < 6 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete broker metadata",
                    ));
                }
                let node_id = src.get_u32();
                let bind_addr = read_pascal_string_io(&mut src)?;
                Ok(MetadataRecord::BrokerRegister { node_id, bind_addr })
            }
            0x03 => {
                let topic = read_pascal_string_io(&mut src)?;
                if src.len() < 6 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete TopicCreated metadata",
                    ));
                }
                let num_partitions = src.get_u32();
                let replication_factor = src.get_u16();
                Ok(MetadataRecord::TopicCreated {
                    topic,
                    num_partitions,
                    replication_factor,
                })
            }
            0x04 => {
                let topic = read_pascal_string_io(&mut src)?;
                Ok(MetadataRecord::TopicDeleted { topic })
            }
            0x05 => {
                let topic = read_pascal_string_io(&mut src)?;
                if src.len() < 16 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete PartitionLeadershipChange metadata",
                    ));
                }
                let partition = src.get_u32();
                let leader_id = src.get_u32();
                let leader_epoch = src.get_u32();
                let isr_len = src.get_u32() as usize;
                if src.len() < isr_len * 4 {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete ISR list"));
                }
                let mut isr = Vec::with_capacity(isr_len);
                for _ in 0..isr_len {
                    isr.push(src.get_u32());
                }
                Ok(MetadataRecord::PartitionLeadershipChange {
                    topic,
                    partition,
                    leader_id,
                    leader_epoch,
                    isr,
                })
            }
            0x06 | 0x07 => {
                if src.is_empty() {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete ACL metadata",
                    ));
                }
                let resource_type = src.get_u8();
                let resource_name = read_pascal_string_io(&mut src)?;
                if src.is_empty() {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete ACL pattern_type",
                    ));
                }
                let pattern_type = src.get_u8();
                let principal = read_pascal_string_io(&mut src)?;
                let host = read_pascal_string_io(&mut src)?;
                if src.len() < 2 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete ACL operations",
                    ));
                }
                let operation = src.get_u8();
                let permission_type = src.get_u8();
                let binding = crate::server::acl::AclBinding {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission_type,
                };
                if record_type == 0x06 {
                    Ok(MetadataRecord::AclCreated { binding })
                } else {
                    Ok(MetadataRecord::AclDeleted { binding })
                }
            }
            0x08 => {
                if src.len() < 4 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete BrokerUnregister node_id",
                    ));
                }
                let node_id = src.get_u32();
                Ok(MetadataRecord::BrokerUnregister { node_id })
            }
            0x09 => {
                let username = read_pascal_string_io(&mut src)?;
                if src.len() < 4 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete SCRAM credential iterations",
                    ));
                }
                let iterations = src.get_u32();
                let salt = read_len_prefixed_bytes(&mut src, "salt")?;
                let stored_key = read_len_prefixed_bytes(&mut src, "stored_key")?;
                let server_key = read_len_prefixed_bytes(&mut src, "server_key")?;
                Ok(MetadataRecord::ScramCredentialUpsert {
                    username,
                    iterations,
                    salt,
                    stored_key,
                    server_key,
                })
            }
            0x0A => {
                let username = read_pascal_string_io(&mut src)?;
                Ok(MetadataRecord::ScramCredentialDelete { username })
            }
            0x0B => {
                let transactional_id = read_pascal_string_io(&mut src)?;
                if src.len() < 10 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Incomplete transactional producer state",
                    ));
                }
                let producer_id = src.get_u64();
                let producer_epoch = src.get_i16();
                Ok(MetadataRecord::TransactionalProducerRegistration {
                    transactional_id,
                    producer_id,
                    producer_epoch,
                })
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unknown metadata record type: 0x{:02X}", record_type),
            )),
        }
    }
}

fn read_pascal_string_io(buf: &mut &[u8]) -> IoResult<String> {
    if buf.len() < 2 {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Incomplete pascal string length",
        ));
    }
    let len = buf.get_u16() as usize;
    if buf.len() < len {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Incomplete pascal string bytes",
        ));
    }
    let str_bytes = &buf[..len];
    *buf = &buf[len..];
    String::from_utf8(str_bytes.to_vec()).map_err(|e| {
        Error::new(
            ErrorKind::InvalidData,
            format!("Invalid UTF-8 metadata: {}", e),
        )
    })
}

fn read_len_prefixed_bytes(buf: &mut &[u8], field_name: &str) -> IoResult<Vec<u8>> {
    if buf.len() < 2 {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!("Incomplete {} length", field_name),
        ));
    }
    let len = buf.get_u16() as usize;
    if buf.len() < len {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!("Incomplete {} bytes", field_name),
        ));
    }
    let bytes = buf[..len].to_vec();
    *buf = &buf[len..];
    Ok(bytes)
}
