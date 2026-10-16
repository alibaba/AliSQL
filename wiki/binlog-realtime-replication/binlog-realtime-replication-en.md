# Binlog Realtime Replication (BRR)

[ [AliSQL](../../README.md) | [English](./binlog-realtime-replication-en.md) | [中文](./binlog-realtime-replication-zh.md) ]

BRR overlaps replication work with execution on the source. Eligible large transactions can send cached binlog events before commit. Dedicated replica workers can also begin supported DDL and transaction work before the source commits. This reduces the work left after source commit; the benefit depends on transaction size, DDL duration, network capacity, and replica resources.

BRR is disabled by default. The settings below describe this AliSQL source tree; RDS product defaults and similarly named legacy variables must not be substituted for them.

## Transmission and Early Apply

| Mode | Source | Replica |
|------|--------|---------|
| Large-transaction transmission | Reads eligible transaction cache files while the transaction is running | Receives BRR cache files; normal replication can apply the completed transaction |
| DDL early apply | Sends a supported ALTER/rebuild operation once it exceeds the DDL time threshold | BRR workers execute ahead, then wait for the source's commit or rollback outcome |
| Large-transaction early apply | Includes dependency information when `binlog_realtime_apply_long_trx_enabled=ON` | BRR workers apply eligible events ahead and wait for the source transaction outcome |

Early transmission and execution do not mean early commit. BRR uses GTID/dependency information and the source outcome to complete or roll back the speculative work. It does not turn asynchronous replication into synchronous replication or guarantee zero lag. Transactions not assigned to BRR workers are handled by the ordinary replication path.

## Prerequisites

The examples assume an existing, healthy, GTID-based source/replica pair with unique server IDs, binary logging, `gtid_mode=ON`, `enforce_gtid_consistency=ON`, and `binlog_format=ROW`. Provision a consistent replica and configure replication credentials before enabling BRR.

- Both servers must support the same BRR protocol. Check `SHOW GLOBAL STATUS LIKE 'brr_version'`; this implementation reports `3`.
- BRR negotiation supports only the default channel, whose name is empty. Named channels and Group Replication channels are outside this implementation's supported scope.
- Large-transaction transmission requires a named transaction cache file with reserved header space. Enable `binlog_cache_free_flush` on the source, even when the final commit itself falls back to normal group commit.
- Transactions must exceed the configured size threshold and spill to disk. Compressed binlog transactions and transactions using the nontransactional statement cache do not use this large-transaction path.

## Enable Large-Transaction Transmission

On the source:

```sql
SET GLOBAL binlog_cache_free_flush = ON;
SET GLOBAL binlog_realtime_transmit_source_enabled = ON;

-- Example threshold: 256 MiB. The actual default is 512 GiB.
SET GLOBAL binlog_realtime_replica_long_transaction_limit_size = 268435456;
```

Despite its name, `binlog_realtime_replica_long_transaction_limit_size` controls eligibility on the **source**. Do not use the deprecated `binlog_realtime_transmit_long_transaction_limit_size`; it does not update the active threshold. Use uncompressed transactions (`binlog_transaction_compression=OFF`) and new transactions after changing source settings.

On the replica, reconnect the replication receiver so it negotiates BRR:

```sql
STOP REPLICA;
SET GLOBAL binlog_realtime_transmit_replica_enabled = ON;
START REPLICA;
```

With `binlog_realtime_apply_workers=0`, this enables transmission without starting dedicated early-apply workers. Verify normal replication and BRR reception before enabling early apply. `SET GLOBAL` settings are not persisted across server restarts; keep the selected settings in each server's configuration or persist them separately.

## Enable Early Apply

Early apply requires GTIDs and parallel replication. On the default replica channel, `PRIVILEGE_CHECKS_USER` must be `NULL`, `ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS` must be `OFF`, `REQUIRE_TABLE_PRIMARY_KEY_CHECK` must be `STREAM`, and replication filters must be absent. The BRR worker pool is separate from `replica_parallel_workers`.

Configure the replica while replication is stopped:

```sql
STOP REPLICA;
SET GLOBAL replica_parallel_type = 'LOGICAL_CLOCK';
SET GLOBAL replica_parallel_workers = 4;
SET GLOBAL replica_preserve_commit_order = OFF;
SET GLOBAL binlog_realtime_apply_workers = 4;
CHANGE REPLICATION SOURCE TO REQUIRE_ROW_FORMAT = 1;
START REPLICA;
```

The worker count is sampled when the SQL applier starts. Changing the global variable alone does not resize running BRR workers. If BRR workers were already active, wait for `replica_brr_worker_size` to reach zero after stopping replication before starting it again. Check the error log if the requested workers do not start.

Then enable the desired early-apply modes on the source:

```sql
SET GLOBAL binlog_realtime_apply_ddl_source_enabled = ON;
SET GLOBAL binlog_realtime_apply_long_trx_enabled = ON;
```

DDL transmission uses `binlog_realtime_ddl_time_limit`, which defaults to 1000 ms; a value of zero removes the delay. It covers supported transactional ALTER TABLE/rebuild paths, including applicable OPTIMIZE TABLE operations. Instant DDL, nontransactional DDL, DDL executed by a replication applier, and DDL excluded from the binlog do not enter this path. External XA transactions do not use large-transaction early apply.

`optimize_table_write_binlog_advance` is a separate, OFF-by-default option. It moves OPTIMIZE binlogging to after successful table recreation, before the subsequent ANALYZE. Enabling it changes failure behavior after recreation; it is not required merely to enable BRR.

## Variables

All variables below are global and dynamically assignable. Receiver negotiation and worker-count changes take effect on the corresponding replication-thread restart as described above. Byte ranges use inclusive bounds.

| Variable | Configure on | Default | Range / meaning |
|----------|--------------|---------|-----------------|
| `binlog_realtime_transmit_source_enabled` | Source | `OFF` | Enable BRR transmission |
| `binlog_realtime_transmit_replica_enabled` | Replica | `OFF` | Negotiate BRR when the receiver connects |
| `binlog_realtime_replica_long_transaction_limit_size` | Source | 512 GiB (`549755813888`) | 10 MiB to `ULLONG_MAX` bytes; transaction cache must exceed it |
| `binlog_realtime_transmit_read_block_size` | Source | 2 MiB (`2097152`) | 4096 to `ULLONG_MAX` bytes; accumulated new cache data before a flush/update |
| `binlog_realtime_transmit_max_send_size` | Source | 2 MiB (`2097152`) | 1024 to `ULLONG_MAX` bytes; send budget per batch; oversized events are not sent early |
| `binlog_realtime_apply_ddl_source_enabled` | Source | `OFF` | Enable eligible DDL transmission |
| `binlog_realtime_ddl_time_limit` | Source | 1000 ms | 0 to `UINT_MAX` ms; zero removes the delay |
| `binlog_realtime_apply_long_trx_enabled` | Source | `OFF` | Include metadata needed for large-transaction early apply |
| `binlog_realtime_apply_workers` | Replica | `0` | 0 to 1024; BRR workers created at SQL-applier start |
| `binlog_realtime_apply_worker_heartbeat_period` | Replica | 200 ms | 1 to `UINT_MAX` ms; worker polling interval |
| `binlog_realtime_enable_log_message` | Either | `OFF` | Enable detailed BRR messages in the error log |
| `optimize_table_write_binlog_advance` | Source executing OPTIMIZE | `OFF` | Write binlog after table recreation, before ANALYZE |

Two names remain for compatibility but are not active controls: `binlog_realtime_transmit_long_transaction_limit_size` (default 512 GiB) and `binlog_realtime_transmit_enable_log_message` (default `ON`). Use the active threshold and logging variables listed above.

## Monitor and Troubleshoot

On the source:

```sql
SHOW GLOBAL STATUS LIKE 'brr_version';
SHOW GLOBAL STATUS LIKE 'source_brr%';
SHOW GLOBAL STATUS LIKE 'source_sent_brr_events_count';
SELECT * FROM information_schema.BRR_SOURCE_TRX_INFO;
SELECT * FROM information_schema.BRR_SENDER_TRX_INFO;
```

On the replica:

```sql
SHOW REPLICA STATUS\G
SHOW GLOBAL STATUS LIKE 'replica_brr%';
SHOW GLOBAL STATUS LIKE 'replica_received_brr_events_count';
SELECT * FROM information_schema.BRR_REPLICA_INFO;
```

- `replica_brr_io_thread_count` indicates negotiated receivers; `replica_brr_worker_size` shows actual BRR workers.
- Sent/received event counters show transmission activity. Assigned, committed, and rolled-back counters distinguish early apply from normal fallback.
- `source_brr_exceed_max_send_size_count` identifies events excluded by the send-size limit.
- Information Schema tables describe current work; they can be empty while the system is idle. Cache/worker counts are gauges, while event/transaction counters accumulate over the server process lifetime; compare deltas over an observation interval.
- Source/replica protocol mismatch disables BRR negotiation. Check the error log and normal replication state.
- If no large transaction is transmitted, check the source threshold, cache spill, Free Flush cache setup, compression, and source-applier restriction. If data arrives but is not applied early, check worker counts and all early-apply prerequisites.

Enable `binlog_realtime_enable_log_message` temporarily when diagnosing an issue; detailed per-transaction logging can be verbose.

## Boundaries and Related Features

- This guide's configuration targets an InnoDB source/replica pair. Validate other engine combinations against their own replication restrictions.
- DuckDB Multi-Trx Batch requires `replica_parallel_workers=0`, while BRR early apply requires parallel workers. The [DuckDB batch-replica setup](../duckdb/how-to-setup-duckdb-node-en.md) must not be combined with the early-apply example unchanged.
- Source replication-applier transactions do not originate this early-transmission path. Do not assume cascaded replication forwards BRR before its upstream transaction commits.
- BRR reuses the cache-file infrastructure of [Binlog Cache Free Flush](../binlog-cache-free-flush/binlog-cache-free-flush-en.md). A successful BRR transfer does not prove the Free Flush commit optimization ran; its transaction checks still apply. The earlier extra-2PC-engine fallback guard is absent in this branch; see that guide for the current DuckDB-mode behavior and recovery considerations.
- BRR cache files use disk space and early workers consume CPU, I/O, and locks before source commit. Size the replica for concurrent source work and its ordinary replication workload.
- Stopping the receiver or losing its connection can roll back in-flight early work. Normal replication and reconnection determine subsequent replay. Do not remove BRR cache files manually.

To disable BRR, first stop originating early work on the source, then stop replication on the replica and wait for its BRR workers to exit:

```sql
-- Source
SET GLOBAL binlog_realtime_apply_ddl_source_enabled = OFF;
SET GLOBAL binlog_realtime_apply_long_trx_enabled = OFF;
SET GLOBAL binlog_realtime_transmit_source_enabled = OFF;

-- Replica
STOP REPLICA;
SET GLOBAL binlog_realtime_transmit_replica_enabled = OFF;
SET GLOBAL binlog_realtime_apply_workers = 0;
-- Confirm replica_brr_worker_size is zero before restarting.
START REPLICA;
```

Retain or restore ordinary replication settings according to the topology, and verify `SHOW REPLICA STATUS`. Update persisted/startup configuration as well.

Implementation references: [variables](../../sql/sys_vars_ext.cc), [source defaults and DDL thresholds](../../sql/rpl_binlog_sender_ext.h), [receiver negotiation and worker eligibility](../../sql/rpl_replica.cc), and [transaction cache eligibility](../../sql/binlog_ostream_ext.cc).
