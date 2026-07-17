# Persist Binlog Into Redo V2

[ [AliSQL](../../README.md) | [English](./binlog-in-redo-en.md) | [中文](./binlog-in-redo-zh.md) ]

Persist Binlog Into Redo V2, also called Binlog in Redo, stores eligible transaction binlog events in InnoDB redo during commit. Background writer and synchronization threads then persist the binlog file. If a crash leaves the binlog tail behind committed redo, recovery reconstructs the missing tail from redo before normal binlog recovery continues.

The settings below apply to self-managed AliSQL 8.0.44. For RDS MySQL versions and parameters, see [Alibaba Cloud RDS MySQL](#alibaba-cloud-rds-mysql).

## Commit Path

With the normal durable configuration, a foreground commit may synchronize both InnoDB redo and the binlog. Binlog in Redo carries eligible binlog data in the redo durability boundary and moves binlog write and synchronization work out of the foreground path. Unsupported transactions retain the normal binlog group-commit path.

## Configuration

Configure the server before startup:

```ini
[mysqld]
sync_binlog=1
innodb_flush_log_at_trx_commit=1
binlog_order_commits=OFF
replica_preserve_commit_order=OFF
persist_binlog_to_redo=ON
```

The current implementation selects Binlog in Redo only when `sync_binlog=1`, `binlog_order_commits=OFF`, and `replica_preserve_commit_order=OFF`. Clone operations that temporarily require commit order also force fallback.

`innodb_flush_log_at_trx_commit=1` is not an implementation gate, but it is the recommended durability baseline because eligible binlog events rely on synchronized redo for crash recovery.

Confirm the effective configuration after startup:

```sql
SHOW GLOBAL VARIABLES WHERE Variable_name IN (
  'persist_binlog_to_redo',
  'persist_binlog_to_redo_size_limit',
  'sync_binlog',
  'binlog_order_commits',
  'replica_preserve_commit_order'
);
```

## Transaction Eligibility and Fallback

An individual transaction uses Binlog in Redo only when all applicable checks pass:

- Redo is writable and the binlog is configured with `sync_binlog=1`.
- Commit ordering is not required.
- The transaction modifies only transactional tables.
- The statement binlog cache is empty.
- The transaction binlog cache does not exceed `persist_binlog_to_redo_size_limit`.
- The active 2PC configuration has one non-binlog storage engine.

The standard AliSQL build can use Binlog in Redo while `duckdb_mode=NONE`: the enablement check discounts the idle DuckDB 2PC registration. Enabling the feature is rejected while DuckDB mode is active.

When a transaction fails an eligibility check, it uses normal binlog group commit. Other eligible transactions can continue to use Binlog in Redo.

## Variables

| Variable | Scope | Default | Range | Description |
|----------|-------|---------|-------|-------------|
| `persist_binlog_to_redo` | Global, dynamic | `OFF` | `ON`, `OFF` | Enable Binlog in Redo V2 |
| `persist_binlog_to_redo_size_limit` | Global, dynamic | 1 MiB | 0 to 10 MiB | Maximum eligible transaction binlog-cache size |
| `sync_binlog_interval` | Global, dynamic | 50000 us | 1 to 100000000 us | Background binlog synchronization interval |
| `binlog_buffer_size` | Global, read-only | 20 MiB | 20 MiB to 1 GiB | Asynchronous binlog ring-buffer size; configure at startup |
| `wait_binlog_flush` | Global, dynamic | `ON` | `ON`, `OFF` | Wait for the binlog file write before returning from commit |
| `binlog_group_delay` | Global, dynamic | 100 ns | 0 to 1000000000 ns | Leader delay used to collect a larger commit group |
| `binlog_group_delay_running_threads` | Global, dynamic | `100` | 0 to 100000 | Minimum running-thread threshold for group delay |

`wait_binlog_flush=ON` waits for the corresponding binlog file write, not necessarily for a binlog file synchronization. Crash recovery for eligible commits is provided by the synchronized redo record. Changing this variable changes commit visibility and latency behavior and should be benchmarked under the intended durability policy.

`binlog_group_delay` is applied only when the configured running-thread threshold is met. Increasing it can improve group size at the cost of commit latency.

## Sizing and Operations

- `persist_binlog_to_redo_size_limit` should cover common transactions without allowing a single transaction to dominate redo and the asynchronous buffer.
- `binlog_buffer_size` is read-only and must be sized before startup for peak eligible commit traffic, not average throughput.
- Monitor both redo pressure and binlog write latency. Moving work off the foreground path does not remove the underlying I/O.
- Keep the normal group-commit path available and test mixed workloads containing nontransactional statements and transactions above the size limit.
- Validate crash recovery and replication with the exact durability settings used in production.

## Alibaba Cloud RDS MySQL

RDS MySQL also supports Binlog in Redo. Its supported versions, replication requirements, backup behavior, parameters, and performance data are documented in the official [English documentation](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-in-redo) and [Chinese documentation](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-in-redo).

Those details are specific to RDS. For a source build, use the prerequisites and variables on this page rather than copying the RDS configuration.
