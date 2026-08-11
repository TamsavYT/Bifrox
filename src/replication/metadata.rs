use bytes::{Buf, BufMut};
use std::io::{Result as IoResult, Error, ErrorKind};

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
}

impl MetadataRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            MetadataRecord::TopicPartition { topic, partition, leader_id, replicas } => {
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
            MetadataRecord::TopicCreated { topic, num_partitions, replication_factor } => {
                buf.put_u8(0x03); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, topic);
                buf.put_u32(*num_partitions);
                buf.put_u16(*replication_factor);
            }
            MetadataRecord::TopicDeleted { topic } => {
                buf.put_u8(0x04); // record type
                crate::protocol::wire::write_pascal_string(&mut buf, topic);
            }
        }
        buf
    }

    pub fn decode(mut src: &[u8]) -> IoResult<Self> {
        if src.is_empty() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Empty metadata record"));
        }
        let record_type = src.get_u8();
        match record_type {
            0x01 => {
                let topic = read_pascal_string_io(&mut src)?;
                if src.len() < 12 {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete topic metadata"));
                }
                let partition = src.get_u32();
                let leader_id = src.get_u32();
                let replicas_len = src.get_u32() as usize;
                if src.len() < replicas_len * 4 {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete replicas list"));
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
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete broker metadata"));
                }
                let node_id = src.get_u32();
                let bind_addr = read_pascal_string_io(&mut src)?;
                Ok(MetadataRecord::BrokerRegister {
                    node_id,
                    bind_addr,
                })
            }
            0x03 => {
                let topic = read_pascal_string_io(&mut src)?;
                if src.len() < 6 {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete TopicCreated metadata"));
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
            _ => Err(Error::new(ErrorKind::InvalidData, format!("Unknown metadata record type: 0x{:02X}", record_type))),
        }
    }
}

fn read_pascal_string_io(buf: &mut &[u8]) -> IoResult<String> {
    if buf.len() < 2 {
        return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete pascal string length"));
    }
    let len = buf.get_u16() as usize;
    if buf.len() < len {
        return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete pascal string bytes"));
    }
    let str_bytes = &buf[..len];
    *buf = &buf[len..];
    String::from_utf8(str_bytes.to_vec())
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Invalid UTF-8 metadata: {}", e)))
}
