# Commit checkpoints and Raft hard state

Partitions now persist `committed.offset` separately from the log end. The checkpoint
contains one big-endian u64: the next offset after the last committed record. Before
publishing a newer committed watermark, the broker syncs the log, writes a temporary
checkpoint, syncs it, renames it, and syncs its parent directory on Unix. A failed write
leaves the published watermark unchanged. This adds synchronous I/O to commit advancement;
measure throughput and latency before rollout.

Recovery rejects malformed checkpoints and checkpoints past the recovered log end.
A fresh empty partition receives a zero checkpoint. A nonempty older partition without
one is rejected: its log bytes cannot establish which records reached quorum. Do not
invent a checkpoint from the log end. Preserve the old store and obtain the authoritative
committed position from a verified backup or the surviving cluster before migration.
There is no automatic legacy-store migration in this change. Do not delete existing
stores to work around this check.

The data directory also contains `raft-hard-state`. It persists the term and the node
voted for, before acknowledgement. Restarted clustered nodes begin as followers, even
when their configured bootstrap role was leader. A hard-state write failure fences that
node from further voting until restart. Invalid hard state fails startup.

The supplied restart/failure tests cover local commit checkpoints, missing legacy
checkpoints, vote restoration, same-term heartbeat handling, and write-error fencing.
They do not replace multi-node crash, power-loss, divergent-log, or transaction failover
validation. Inter-node authentication/TLS and transaction coordination are separate work.
