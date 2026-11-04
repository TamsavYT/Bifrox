//! Controller-serialized transaction decisions and authenticated partition-leader commands.
use super::*;
use crate::replication::{FrameType, MetadataRecord};
use crate::server::transaction::TransactionState;
use bytes::{Buf, BufMut};
use std::io::{Error, ErrorKind};

impl StorageEngine {
    pub(super) fn distributed_transactions(&self) -> bool {
        !self.config.peer_addrs.is_empty() || !self.transaction_revisions.is_empty()
    }
    pub(super) fn require_transaction_controller(&self) -> IoResult<()> {
        if !self.is_leader() {
            return Err(Error::other("NOT_CONTROLLER"));
        }
        if self
            .transaction_uncertain
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::other("Uncertain transaction proposal; restart/recover coordinator before further mutations"));
        }
        let pm = self.get_or_create_partition("__cluster_metadata", 0)?;
        if pm.latest_offset() != pm.high_watermark() {
            return Err(Error::other(
                "Unresolved metadata log tail; coordinator recovery required",
            ));
        }
        Ok(())
    }
    pub(super) fn install_replicated_transaction(
        &self,
        revision: u64,
        created_at_ms: u64,
        raw: &[u8],
    ) -> IoResult<()> {
        let (status, producer_id, id, partitions) = decode_tx_state_record(raw)
            .ok_or_else(|| Error::other("invalid transaction decision"))?;
        if self
            .transaction_revisions
            .get(&id)
            .is_some_and(|v| *v >= revision)
        {
            return Ok(());
        }
        // Persist aborted ranges on every local replica before releasing any LSO pin.
        if status == TxStatus::Aborted {
            for (topic, part, start, end) in &partitions {
                if *start != u64::MAX {
                    if let Some(pm) = self.get_partition(topic, *part) {
                        pm.append_aborted_txn(producer_id, *start, *end)?;
                        pm.flush_durable()?;
                    }
                }
            }
        }
        self.transactions.install_state(TransactionState {
            transaction_id: id.clone(),
            producer_id,
            status,
            partitions,
            created_at_ms,
        });
        self.transaction_revisions.insert(id, revision);
        Ok(())
    }
    async fn save_transaction(&self, state: &TransactionState) -> IoResult<u64> {
        self.require_transaction_controller()?;
        let term = self.replication.get_epoch();
        let revision = self
            .get_or_create_partition("__cluster_metadata", 0)?
            .latest_offset()
            + 1;
        let result = self
            .propose_metadata(MetadataRecord::TransactionState {
                revision,
                created_at_ms: state.created_at_ms,
                state: encode_tx_state_record(
                    state.status,
                    state.producer_id,
                    &state.transaction_id,
                    &state.partitions,
                ),
            })
            .await;
        if result.is_err() {
            self.transaction_uncertain
                .store(true, std::sync::atomic::Ordering::Release);
        }
        result?;
        if !self.is_leader() || self.replication.get_epoch() != term {
            return Err(Error::other("Controller term changed"));
        }
        Ok(revision)
    }
    pub async fn begin_transaction_coordinated(&self, id: &str, pid: u64) -> Result<(), String> {
        if !self.distributed_transactions() {
            return self.begin_transaction(id, pid);
        }
        let _guard = self.transaction_gate.lock().await;
        self.require_transaction_controller()
            .map_err(|e| e.to_string())?;
        if self
            .transactions
            .state(id)
            .is_some_and(|s| !matches!(s.status, TxStatus::Committed | TxStatus::Aborted))
        {
            return Err("Transaction already active".into());
        }
        let state = TransactionState {
            transaction_id: id.into(),
            producer_id: pid,
            status: TxStatus::Ongoing,
            partitions: vec![],
            created_at_ms: now_ms(),
        };
        self.save_transaction(&state)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    pub async fn add_partitions_coordinated(
        &self,
        id: &str,
        pid: u64,
        epoch: i16,
        topics: &[(String, Vec<u32>)],
    ) -> Result<(), String> {
        if !self.distributed_transactions() {
            return self.add_partitions_to_txn(id, pid, epoch, topics);
        }
        let _guard = self.transaction_gate.lock().await;
        if self.transactions.has_transactional_producer(id) {
            self.transactions
                .validate_transactional_producer(id, pid, epoch)?;
        }
        let mut state = self.transactions.state(id).unwrap_or(TransactionState {
            transaction_id: id.into(),
            producer_id: pid,
            status: TxStatus::Ongoing,
            partitions: vec![],
            created_at_ms: now_ms(),
        });
        if state.producer_id != pid || state.status != TxStatus::Ongoing {
            return Err("Transaction not ongoing or producer fenced".into());
        }
        for (topic, parts) in topics {
            validate_client_topic_name(topic).map_err(|e| e.to_string())?;
            for part in parts {
                if !self
                    .topic_registry
                    .get(topic)
                    .is_some_and(|t| t.partitions.contains_key(part))
                {
                    return Err("Unknown topic partition".into());
                }
                if !state
                    .partitions
                    .iter()
                    .any(|(t, p, _, _)| t == topic && p == part)
                {
                    state
                        .partitions
                        .push((topic.clone(), *part, u64::MAX, u64::MAX));
                }
            }
        }
        self.save_transaction(&state)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    pub async fn finish_transaction_coordinated(
        &self,
        id: &str,
        commit: bool,
    ) -> Result<(), String> {
        if !self.distributed_transactions() {
            return if commit {
                self.commit_transaction(id)
            } else {
                self.abort_transaction(id)
            };
        }
        let _guard = self.transaction_gate.lock().await;
        self.finish_transaction_locked(id, commit).await
    }
    pub async fn end_transaction_coordinated(
        &self,
        id: &str,
        pid: u64,
        epoch: i16,
        commit: bool,
    ) -> Result<(), String> {
        if !self.distributed_transactions() {
            return self.end_transaction(id, pid, epoch, commit);
        }
        let _guard = self.transaction_gate.lock().await;
        self.transactions
            .validate_transactional_producer(id, pid, epoch)?;
        self.finish_transaction_locked(id, commit).await
    }
    pub(super) async fn finish_transaction_locked(
        &self,
        id: &str,
        commit: bool,
    ) -> Result<(), String> {
        self.require_transaction_controller()
            .map_err(|e| e.to_string())?;
        let mut state = self.transactions.state(id).ok_or("Transaction not found")?;
        let prepared = if commit {
            TxStatus::PrepareCommit
        } else {
            TxStatus::PrepareAbort
        };
        let terminal = if commit {
            TxStatus::Committed
        } else {
            TxStatus::Aborted
        };
        if state.status == terminal {
            return Ok(());
        }
        if state.status != TxStatus::Ongoing && state.status != prepared {
            return Err("Transaction has a different durable decision".into());
        }
        if state.status == TxStatus::Ongoing {
            state.status = prepared;
            self.save_transaction(&state)
                .await
                .map_err(|e| e.to_string())?;
        }
        let revision = *self
            .transaction_revisions
            .get(id)
            .ok_or("Missing decision revision")?;
        for range in &mut state.partitions {
            if range.2 == u64::MAX {
                continue;
            }
            let mut command = vec![2];
            put_string(&mut command, &range.0);
            command.put_u32(range.1);
            put_string(&mut command, id);
            command.put_u64(revision);
            let reply = self
                .transaction_partition_command(&range.0, range.1, &command)
                .await
                .map_err(|e| e.to_string())?;
            let mut reader = Reader(&reply);
            range.3 = reader.u64().map_err(|e| e.to_string())?;
            reader.done().map_err(|e| e.to_string())?;
        }
        state.status = terminal;
        self.save_transaction(&state)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    pub(super) async fn produce_transaction_coordinated(
        &self,
        params: ProduceBatchParams<'_>,
    ) -> IoResult<(u32, u64, u64)> {
        let _guard = self.transaction_gate.lock().await;
        self.require_transaction_controller()?;
        let id = params
            .transaction_id
            .ok_or_else(|| Error::other("missing transaction id"))?;
        let mut state = self
            .transactions
            .state(id)
            .ok_or_else(|| Error::other("Transaction not found"))?;
        if state.status != TxStatus::Ongoing {
            return Err(Error::other("Transaction not ongoing"));
        }
        if params.batch.producer_id != state.producer_id {
            return Err(Error::other("Producer mismatch"));
        }
        if self.transactions.has_transactional_producer(id) {
            self.transactions
                .validate_transactional_producer(
                    id,
                    params.batch.producer_id,
                    params.batch.producer_epoch,
                )
                .map_err(Error::other)?;
        }
        validate_client_topic_name(params.topic)?;
        if params.num_partitions == 0
            || params.num_partitions > self.config.max_partitions_per_topic
        {
            return Err(Error::other("Invalid partition count"));
        }
        let part = if params.key.is_empty() {
            0
        } else {
            hash_key(params.key.as_bytes(), params.num_partitions as usize)
        };
        if !state
            .partitions
            .iter()
            .any(|(t, p, start, _)| t == params.topic && *p == part && *start != u64::MAX)
        {
            let mut command = vec![0];
            put_string(&mut command, params.topic);
            command.put_u32(part);
            let reply = self
                .transaction_partition_command(params.topic, part, &command)
                .await?;
            let mut reader = Reader(&reply);
            let start = reader.u64()?;
            reader.done()?;
            if let Some(range) = state
                .partitions
                .iter_mut()
                .find(|(t, p, _, _)| t == params.topic && *p == part)
            {
                range.2 = start;
            } else {
                state
                    .partitions
                    .push((params.topic.into(), part, start, u64::MAX));
            }
            self.save_transaction(&state).await?;
        }
        let revision = *self
            .transaction_revisions
            .get(id)
            .ok_or_else(|| Error::other("missing transaction revision"))?;
        // Every replica must durably know the read-committed pin before any copy of
        // the batch can arrive. Otherwise a lagging metadata consumer could expose it.
        let replicas = self
            .get_partition(params.topic, part)
            .ok_or_else(|| Error::other("Unknown partition"))?
            .replicas();
        if replicas.is_empty() {
            return Err(Error::other("No assigned replicas"));
        }
        let mut barrier = vec![3];
        put_string(&mut barrier, params.topic);
        barrier.put_u32(part);
        put_string(&mut barrier, id);
        barrier.put_u64(revision);
        for replica in replicas {
            self.transaction_command_to_node(replica, &barrier).await?;
        }
        let mut command = vec![1];
        put_string(&mut command, params.topic);
        command.put_u32(part);
        put_string(&mut command, id);
        command.put_u64(revision);
        let mut batch = Vec::new();
        params.batch.encode_into(&mut batch);
        command.put_u32(batch.len() as u32);
        command.extend_from_slice(&batch);
        let reply = self
            .transaction_partition_command(params.topic, part, &command)
            .await?;
        let mut reader = Reader(&reply);
        let first = reader.u64()?;
        let last = reader.u64()?;
        reader.done()?;
        Ok((part, first, last))
    }
    async fn transaction_partition_command(
        &self,
        topic: &str,
        part: u32,
        command: &[u8],
    ) -> IoResult<Vec<u8>> {
        self.require_transaction_controller()?;
        let leader = self
            .partition_leader_id(topic, part)
            .ok_or_else(|| Error::other("Unknown partition leader"))?;
        self.transaction_command_to_node(leader, command).await
    }
    async fn transaction_command_to_node(&self, leader: u32, command: &[u8]) -> IoResult<Vec<u8>> {
        self.require_transaction_controller()?;
        if leader == self.config.node_id {
            return self
                .execute_transaction_command(command, self.replication.get_epoch())
                .await;
        }
        let addr = self
            .get_broker_address(leader)
            .ok_or_else(|| Error::other("Unknown broker address"))?;
        let mut payload = Vec::new();
        put_string(&mut payload, &self.config.cluster_id);
        payload.put_u32(self.config.node_id);
        payload.put_u64(self.replication.get_epoch());
        payload.extend_from_slice(command);
        let request = crate::replication::encode_frame(FrameType::TransactionCommand, &payload);
        let mut stream = crate::replication::transport::PeerTransport::from_config(&self.config)?
            .connect(&addr)
            .await?;
        let response = crate::replication::envelope::exchange_frame(
            &mut stream,
            &request,
            FrameType::TransactionResponse,
        )
        .await?;
        if response.first() != Some(&0) {
            return Err(Error::other(
                String::from_utf8_lossy(response.get(1..).unwrap_or_default()).into_owned(),
            ));
        }
        Ok(response[1..].to_vec())
    }
    pub(crate) async fn handle_transaction_command(&self, payload: &[u8]) -> Vec<u8> {
        let result = async {
            let mut r = Reader(payload);
            let cluster = r.string()?;
            let node = r.u32()?;
            let term = r.u64()?;
            if cluster != self.config.cluster_id
                || term != self.replication.get_epoch()
                || !self
                    .get_broker_address(node)
                    .is_some_and(|addr| self.leader_addr().as_ref() == Some(&addr))
            {
                return Err(Error::other("Stale or unauthorized transaction controller"));
            }
            self.execute_transaction_command(r.0, term).await
        }
        .await;
        let payload = match result {
            Ok(mut bytes) => {
                bytes.insert(0, 0);
                bytes
            }
            Err(error) => {
                let mut bytes = vec![1];
                bytes.extend_from_slice(error.to_string().as_bytes());
                bytes
            }
        };
        crate::replication::encode_frame(FrameType::TransactionResponse, &payload)
    }
    async fn execute_transaction_command(
        &self,
        command: &[u8],
        controller_term: u64,
    ) -> IoResult<Vec<u8>> {
        let mut r = Reader(command);
        let op = r.byte()?;
        let topic = r.string()?;
        let part = r.u32()?;
        validate_client_topic_name(&topic)?;
        if op != 3 && !self.is_partition_leader(&topic, part) {
            return Err(Error::other("NotLeaderForPartition"));
        }
        if op == 3 && !self.is_partition_replica(&topic, part) {
            return Err(Error::other("NotReplicaForPartition"));
        }
        let pm = self.get_or_create_partition(&topic, part)?;
        if op == 0 {
            r.done()?;
            return Ok(pm.latest_offset().to_be_bytes().to_vec());
        }
        let id = r.string()?;
        let revision = r.u64()?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !self
            .transaction_revisions
            .get(&id)
            .is_some_and(|v| *v >= revision)
        {
            if std::time::Instant::now() >= deadline {
                return Err(Error::other(
                    "Transaction decision not yet committed locally",
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let _guard = self.transaction_partition_gate.lock().await;
        if self.replication.get_epoch() != controller_term {
            return Err(Error::other("Stale controller term"));
        }
        let state = self
            .transactions
            .state(&id)
            .ok_or_else(|| Error::other("Missing transaction"))?;
        if self.transaction_revisions.get(&id).map(|v| *v) != Some(revision) {
            return Err(Error::other("Stale transaction command"));
        }
        let range = state
            .partitions
            .iter()
            .find(|(t, p, _, _)| *t == topic && *p == part)
            .ok_or_else(|| Error::other("Partition not enlisted"))?;
        if op == 3 {
            r.done()?;
            if state.status != TxStatus::Ongoing || range.2 == u64::MAX {
                return Err(Error::other("Replica has no active read-committed pin"));
            }
            return Ok(Vec::new());
        }
        if op == 1 {
            if state.status != TxStatus::Ongoing || range.2 == u64::MAX {
                return Err(Error::other("Transaction not ongoing or unpinned"));
            }
            let size = r.u32()? as usize;
            let (batch, used) = RecordBatch::decode(r.take(size)?).map_err(Error::other)?;
            if used != size {
                return Err(Error::other("Invalid batch length"));
            }
            r.done()?;
            if batch.producer_id != state.producer_id {
                return Err(Error::other("Producer mismatch"));
            }
            let (_, first, last) = self
                .produce_batch_local(
                    ProduceBatchParams {
                        topic: &topic,
                        key: "",
                        transaction_id: Some(&id),
                        num_partitions: part + 1,
                        batch,
                    },
                    true,
                    Some(part),
                )
                .await?;
            if self.replication.get_epoch() != controller_term {
                return Err(Error::other("Stale controller term"));
            }
            let mut reply = Vec::new();
            reply.put_u64(first);
            reply.put_u64(last);
            Ok(reply)
        } else if op == 2 {
            r.done()?;
            let control = match state.status {
                TxStatus::PrepareCommit => crate::server::transaction::CTRL_COMMIT,
                TxStatus::PrepareAbort => crate::server::transaction::CTRL_ABORT,
                _ => return Err(Error::other("Missing prepared decision")),
            };
            let marker = pm.produce_control_marker_uncommitted(control, state.producer_id, &id)?;
            pm.flush_durable()?;
            self.await_transaction_replication(&pm, &topic, part, marker.base_offset)
                .await?;
            if self.replication.get_epoch() != controller_term {
                return Err(Error::other("Stale controller term"));
            }
            pm.advance_committed_hw(marker.base_offset + 1)?;
            Ok(marker.base_offset.to_be_bytes().to_vec())
        } else {
            Err(Error::other("Unknown transaction operation"))
        }
    }
    pub(super) async fn await_transaction_replication(
        &self,
        pm: &Arc<PartitionManager>,
        topic: &str,
        part: u32,
        offset: u64,
    ) -> IoResult<()> {
        let replicas = pm.replicas();
        if replicas.is_empty() {
            return Err(Error::other("Transaction requires an assigned replica set"));
        }
        let required = (replicas.len() / 2 + 1).max(self.effective_min_insync_replicas(topic));
        let epoch = pm.leader_epoch();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            if !pm.is_leader(self.config.node_id) || pm.leader_epoch() != epoch {
                return Err(Error::other("Partition leadership changed"));
            }
            let acked = replicas
                .iter()
                .filter(|id| {
                    **id == self.config.node_id
                        || self
                            .get_broker_address(**id)
                            .and_then(|addr| self.replication.replica_watermark(topic, part, &addr))
                            .is_some_and(|w| w >= offset)
                })
                .count();
            if acked >= required {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::new(
                    ErrorKind::TimedOut,
                    "Transaction data/marker did not reach replica quorum",
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    pub async fn recover_transactions_coordinated(&self) {
        if !self.distributed_transactions() {
            self.sweep_expired_transactions();
            return;
        }
        if !self.is_leader() {
            return;
        }
        for state in self.transactions.all_states() {
            let decision = match state.status {
                TxStatus::PrepareCommit => Some(true),
                TxStatus::PrepareAbort => Some(false),
                TxStatus::Ongoing
                    if now_ms().saturating_sub(state.created_at_ms)
                        >= self.config.transaction_timeout_ms =>
                {
                    Some(false)
                }
                _ => None,
            };
            if let Some(commit) = decision {
                if let Err(error) = self
                    .finish_transaction_coordinated(&state.transaction_id, commit)
                    .await
                {
                    tracing::warn!("Transaction recovery will retry: {}", error);
                }
            }
        }
    }
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn put_string(out: &mut Vec<u8>, s: &str) {
    crate::protocol::wire::write_pascal_string(out, s);
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> IoResult<&'a [u8]> {
        if self.0.len() < n {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "Truncated transaction command",
            ));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn byte(&mut self) -> IoResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> IoResult<u32> {
        Ok(self.take(4)?.get_u32())
    }
    fn u64(&mut self) -> IoResult<u64> {
        Ok(self.take(8)?.get_u64())
    }
    fn string(&mut self) -> IoResult<String> {
        let size = u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize;
        String::from_utf8(self.take(size)?.to_vec()).map_err(Error::other)
    }
    fn done(&self) -> IoResult<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::other("Trailing transaction command bytes"))
        }
    }
}
