use crate::config::EngineConfig;
use crate::consumer_group::ConsumerGroupManager;
use crate::protocol::RecordFrame;
use crate::server::partition::PartitionManager;
use bytes::Bytes;
use dashmap::DashMap;
use std::io::Result as IoResult;
use std::sync::Arc;

/// Hash router using CRC32 to determine target partition ID for a given record key
pub fn hash_key(key: &[u8], num_partitions: usize) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    let hash = crc32fast::hash(key);
    (hash as usize % num_partitions) as u32
}

/// StorageEngine maintaining multi-topic concurrent partition routing and consumer group offset management
#[derive(Debug, Clone)]
pub struct StorageEngine {
    config: EngineConfig,
    partitions: Arc<DashMap<(String, u32), Arc<PartitionManager>>>,
    consumer_groups: ConsumerGroupManager,
}

impl StorageEngine {
    pub fn new(config: EngineConfig) -> IoResult<Self> {
        let consumer_groups = ConsumerGroupManager::open(&config.data_dir)?;
        Ok(Self {
            config,
            partitions: Arc::new(DashMap::new()),
            consumer_groups,
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn consumer_groups(&self) -> &ConsumerGroupManager {
        &self.consumer_groups
    }

    /// Retrieve existing partition or dynamically initialize directory `data/{topic}-{partition}` on demand
    pub fn get_or_create_partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> IoResult<Arc<PartitionManager>> {
        let key = (topic.to_string(), partition);
        if let Some(pm) = self.partitions.get(&key) {
            return Ok(pm.value().clone());
        }

        // Layout: data/{topic}-{partition}/
        let partition_dir = self
            .config
            .data_dir
            .join(format!("{}-{}", topic, partition));

        let pm = Arc::new(PartitionManager::open(
            partition_dir,
            topic,
            partition,
            self.config.clone(),
        )?);

        self.partitions.insert(key, pm.clone());
        Ok(pm)
    }

    /// Produce a batch of records to a routed partition. If key is non-empty, routes using hash_key.
    pub fn produce_batch(
        &self,
        topic: &str,
        key: &str,
        num_partitions: u32,
        records: &[Bytes],
    ) -> IoResult<(u32, u64, u64)> {
        let partition_id = if !key.is_empty() && num_partitions > 0 {
            hash_key(key.as_bytes(), num_partitions as usize)
        } else {
            0
        };

        let pm = self.get_or_create_partition(topic, partition_id)?;
        let mut first_offset = 0u64;
        let mut last_offset = 0u64;

        for (idx, record) in records.iter().enumerate() {
            let offset = pm.produce(record)?;
            if idx == 0 {
                first_offset = offset;
            }
            last_offset = offset;
        }

        Ok((partition_id, first_offset, last_offset))
    }

    pub fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> IoResult<Vec<RecordFrame>> {
        let pm = self.get_or_create_partition(topic, partition)?;
        pm.fetch(offset, max_bytes)
    }

    pub fn seek(&self, topic: &str, partition: u32, offset: u64) -> IoResult<Option<(u64, u64)>> {
        let pm = self.get_or_create_partition(topic, partition)?;
        Ok(pm.seek(offset))
    }

    pub fn latest_offset(&self, topic: &str, partition: u32) -> IoResult<u64> {
        let pm = self.get_or_create_partition(topic, partition)?;
        Ok(pm.latest_offset())
    }

    pub fn commit_offset(&self, group_id: &str, topic: &str, partition: u32, offset: u64) -> IoResult<()> {
        self.consumer_groups.commit_offset(group_id, topic, partition, offset)
    }

    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: u32) -> Option<u64> {
        self.consumer_groups.fetch_offset(group_id, topic, partition)
    }

    /// Triggers retention garbage collection across all open partitions
    pub fn apply_retention_all(&self) -> IoResult<usize> {
        let mut total_removed = 0;
        for entry in self.partitions.iter() {
            total_removed += entry.value().apply_retention()?;
        }
        Ok(total_removed)
    }

    pub fn flush_all(&self) -> IoResult<()> {
        for entry in self.partitions.iter() {
            entry.value().flush()?;
        }
        Ok(())
    }
}
