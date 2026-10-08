# LDBC SPB Resilience Profile

TripleStore implements the pinned SPB full-backup milestones as a coordinated,
quiesced-write backup and a restore into a fresh embedded store. The harness
records backup and restore duration, backup bytes and file count, graph-context
preservation, derived-fact count, the dataset-manifest identity, and a canonical
accepted-answer probe before and after restoration.

The pinned LDBC SPB 2.0.2 source at commit
`ce6323c0936306729408233dc70d26f2389b34c6` defines the following audit-only
enterprise actions. Each action depends on a vendor implementation.

| Driver action | Meaning in the pinned catalog | TripleStore support |
| --- | --- | --- |
| `benchmarkOnlineReplicationAndBackup` | Measure under online replication and backup | Unsupported; no score output |
| `full_backup_start` | Start a full backup | Coordinated local backup |
| `full_backup_restore` | Restore a full backup | Fresh-path local restore |
| `system_shutdown` | Stop the benchmark database | Unsupported as a failover action |
| `system_start` | Start the benchmark database | Unsupported as a failover action |

Backup and restore do not constitute replication or failover. TripleStore has no
replica topology, replication log, promotion protocol, or consistency contract
between nodes. The harness rejects `:online_replication` and `:failover` before
measurement and emits no availability score. Adding either profile would change
the embedded-library deployment contract and requires a separate accepted ADR
before an adapter or scoring path is implemented.

The current backup implementation copies the RocksDB directory. The benchmark
harness therefore requires writes to be quiesced during backup. It does not claim
measured read or write availability while the copy runs. The source store remains
caller-owned; the harness closes every restored store it opens and starts no
scheduled-backup helper.
