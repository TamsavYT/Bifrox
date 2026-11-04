# Transaction durability and remaining limits

Transaction begin, partition enlistment/registration, and prepare records must persist before the coordinator publishes the corresponding state in memory. Storage failures propagate to the caller. Transactional produce reserves its first-offset pin before writing data and serializes with completion. Completion flushes data markers and persists the terminal record before releasing transaction state; aborted-index failures propagate too.

A prepared commit is retried as a commit after restart or timeout. It cannot switch to abort or accept new partitions. Repeating a failed completion may append another control marker; callers must treat an error as an uncertain result and retry the same decision.

Network InitProducerId now runs on the controller through the metadata proposal/quorum path. Its new epoch is published after commit, and followers wait for the committed watermark before applying metadata. The synchronous embedded InitProducerId API is limited to a sole controller. Use `init_producer_id_coordinated(...).await` in clustered code.

This change does not complete distributed transactions. `__transaction_state` and transaction control markers still use local durability; there is no complete coordinator protocol to route markers to remote partition leaders, replicate state/decisions to a recovery quorum, or recover the entire transaction on another broker. A successful local commit is not proof of a cluster-wide exactly-once guarantee. Multi-broker transaction support requires that remaining work and crash/failover tests before it can be considered safe.
