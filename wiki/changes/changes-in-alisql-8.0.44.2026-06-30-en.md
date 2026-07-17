# AliSQL 8.0.44 Feature Release Notes (2026-06-30)

[ [Release Notes](../changes-in-alisql-8.0.44.md) | [English](./changes-in-alisql-8.0.44.2026-06-30-en.md) | [中文](./changes-in-alisql-8.0.44.2026-06-30-zh.md) ]

This document summarizes the public AliSQL 8.0.44 feature release: DuckDB enhancements, native vector indexing, Binlog Cache Free Flush, Persist Binlog Into Redo V2, and Native Flashback. AliSQL is based on MySQL 8.0.44, and most new execution paths are disabled by default. The Native Flashback query gate is enabled by default, while snapshot generation and undo retention remain disabled.

## Overview

| Area | Feature | Default |
|------|---------|---------|
| Analytics | DuckDB storage engine enhancements | `duckdb_mode=NONE` |
| Vector search | Native `VECTOR` type and HNSW index | `vidx_disabled=ON` |
| Large transactions | Binlog Cache Free Flush | `OFF`; optimized path inactive in the standard DuckDB build |
| Binlog durability | Persist Binlog Into Redo V2 | `persist_binlog_to_redo=OFF` |
| Historical queries | Native Flashback `AS OF TIMESTAMP` | Snapshot task `OFF` |

## 1. DuckDB Storage Engine Enhancements

AliSQL integrates DuckDB as a native analytical storage engine behind the MySQL protocol and SQL interface. This update upgrades DuckDB to v1.4.4 and extends compatibility, DDL, replication, and resource governance.

### New Features

- Optional SQL normalization for cross-database queries, temporal expressions, more MySQL functions, and prepared-statement reprepare.
- Generated-column table conversion, Latin1, BLOB/VARCHAR comparison, and improved BINARY, VARBINARY, and BIT handling.
- Separate per-query thread limits for user and replication workloads.
- Optional high-precision calculation paths.
- `INSERT ... SELECT` acceleration for DuckDB-to-DuckDB Copy DDL.
- CPU governance for DuckDB replication work.

### Reliability and Replication Fixes

- Fixed crashes in JSON paths, `CONCAT`, UPDATE/DELETE subqueries, partition Copy DDL, and read-only prepared-statement cursors.
- Fixed orphan-table cleanup and shutdown during startup conversion.
- Multi-Trx Batch requires `replica_parallel_workers=0`; change `duckdb_multi_trx_in_batch` only while replication is stopped.
- Fixed `Exec_Master_Log_Pos`, invalid `gno=0`, partial-batch rollback, and GTID release handling.
- Improved `log_replica_updates=OFF`, empty-transaction, temporary-table, cross-database, and idempotent-replay behavior.

### New Variables

| Variable | Scope | Default | Range / Description |
|----------|-------|---------|---------------------|
| `duckdb_sql_normalization` | Global, Session | `OFF` | Rewrite supported MySQL SQL for DuckDB |
| `duckdb_max_threads_per_query` | Global, Session | `1000000` | 1 to 4611686018427387904; user query thread limit |
| `duckdb_max_threads_per_query_rpl` | Global | `1000000` | Same range; replication query thread limit |
| `duckdb_psmt_cursor_send_extra_eof` | Global, Session | `ON` | Extra EOF for empty cursor results |
| `duckdb_prefer_high_precision` | Global, Session | `OFF` | Prefer high-precision calculations |
| `duckdb_convert_tables_with_generated_columns` | Global | `ON` | Allow generated-column conversion |
| `duckdb_copy_data_between_tables_use_ins_sel` | Global, Session | `OFF` | Use `INSERT ... SELECT` during Copy DDL |
| `ignore_index_hint_error` | Global | `OFF` | Warn instead of failing for missing hinted indexes |

## 2. Native Vector Index (VIDX)

VIDX adds `VECTOR(N)`, scalar vector functions, and HNSW approximate nearest-neighbor search. It supports Euclidean and cosine distance with up to 16,383 dimensions.

```sql
SET GLOBAL vidx_disabled = OFF;
SET SESSION transaction_isolation = 'READ-COMMITTED';

CREATE TABLE embeddings (
    id BIGINT PRIMARY KEY,
    embedding VECTOR(3)
) ENGINE=InnoDB;

INSERT INTO embeddings VALUES
    (1, VEC_FROMTEXT('[0.1,0.2,0.3]')),
    (2, VEC_FROMTEXT('[0.2,0.1,0.4]'));

CREATE VECTOR INDEX embedding_hnsw
    ON embeddings(embedding) M=6 DISTANCE=COSINE;

SELECT id,
       VEC_DISTANCE_COSINE(
           embedding, VEC_FROMTEXT('[0.1,0.2,0.3]')
       ) AS distance
FROM embeddings
ORDER BY distance
LIMIT 10;
```

| Variable | Scope | Default | Range |
|----------|-------|---------|-------|
| `vidx_disabled` | Global | `ON` | `ON`, `OFF` |
| `vidx_default_distance` | Global, Session | `EUCLIDEAN` | `EUCLIDEAN`, `COSINE` |
| `vidx_hnsw_default_m` | Global, Session | `6` | 3 to 200 |
| `vidx_hnsw_ef_search` | Global, Session | `20` | 1 to 10000 |
| `vidx_hnsw_cache_size` | Global | 16 MiB | 1 MiB to `ULLONG_MAX` bytes |

Vector indexes require InnoDB and `READ COMMITTED`. They do not support `ALGORITHM=INPLACE` or invisible indexes. Nullable vectors are allowed but are omitted from HNSW; scalar distance returns `NULL` for them. Randomized and heuristic HNSW construction means replicas are not guaranteed to have byte-identical graph topology.

## 3. Large TX Optimization: Binlog Cache Free Flush

Free Flush finalizes a spilled transaction binlog-cache file and renames it as the next binlog instead of copying the large cache again during commit. This reduces commit I/O amplification.

> **Current availability:** The variables and implementation are present, but the optimized path is inactive in the standard DuckDB-enabled build. When binlog is enabled, DuckDB registers a 2PC `prepare` callback even with `duckdb_mode=NONE`, so every transaction selects normal binlog group commit. A build with no additional registered 2PC engine is required to enter Free Flush.

| Variable | Scope | Default | Range |
|----------|-------|---------|-------|
| `binlog_cache_free_flush` | Global, dynamic | `OFF` | `ON`, `OFF` |
| `binlog_cache_free_flush_limit_size` | Global, dynamic | 256 MiB | 10 MiB to `ULLONG_MAX` bytes |

The cache must exceed the threshold, be spilled and finalized, contain no statement-cache event or incident, remain unencrypted, and not modify `mysql.gtid_executed`. The path supports one non-binlog 2PC engine. Any additional registered engine transparently forces normal binlog group commit. DuckDB is always such a registered participant in the standard build, regardless of `duckdb_mode`.

## 4. Persist Binlog Into Redo V2

Eligible binlog events are persisted in InnoDB redo during commit. Background flush and sync threads update the binlog file; crash recovery reconstructs a missing binlog tail from redo.

The implementation guards require `sync_binlog=1`, `binlog_order_commits=OFF`, and `replica_preserve_commit_order=OFF`. Keep `innodb_flush_log_at_trx_commit=1` for the intended full-durability baseline; it is a recommendation rather than an enablement guard.

| Variable | Scope | Default | Range / Unit |
|----------|-------|---------|--------------|
| `persist_binlog_to_redo` | Global, dynamic | `OFF` | `ON`, `OFF` |
| `persist_binlog_to_redo_size_limit` | Global, dynamic | 1 MiB | 0 to 10 MiB |
| `sync_binlog_interval` | Global, dynamic | 50000 us | 1 to 100000000 us |
| `binlog_buffer_size` | Global, read-only | 20 MiB | 20 MiB to 1 GiB |
| `wait_binlog_flush` | Global, dynamic | `ON` | `ON`, `OFF` |
| `binlog_group_delay` | Global, dynamic | 100 ns | 0 to 1000000000 ns |
| `binlog_group_delay_running_threads` | Global, dynamic | `100` | 0 to 100000 |

Only one active non-binlog 2PC engine is supported. Enabling is rejected while DuckDB mode is active. Transactions using nontransactional tables, the statement binlog cache, or a cache above the size limit transparently use normal group commit.

## 5. Native Flashback

Native Flashback reconstructs an InnoDB historical read view from retained undo and exposes it through `AS OF TIMESTAMP`.

The background thread is created if `innodb_rds_flashback_task_enabled=ON` or `innodb_undo_retention>0` at server startup, but actual snapshot generation requires both the task to be enabled and undo retention to be nonzero:

```ini
[mysqld]
innodb_rds_flashback_task_enabled=ON
innodb_undo_retention=3600
innodb_rds_flashback_interval=1
```

```sql
SELECT *
FROM orders AS OF TIMESTAMP DATE_SUB(NOW(), INTERVAL 5 MINUTE);
```

The read view is selected from retained snapshot history and may not match the requested wall-clock time exactly. `innodb_rds_flashback_allow_gap` bounds the accepted difference. For recovery, write historical rows to a separate table, validate them, and stop application traffic before replacing the original table.

| Variable | Default | Range / Unit |
|----------|---------|--------------|
| `innodb_rds_flashback_task_enabled` | `OFF` | Boolean |
| `innodb_rds_flashback_task_stop_all` | `OFF` | Boolean |
| `innodb_rds_flashback_interval` | `1` | 1 to 86400 seconds |
| `innodb_rds_flashback_enabled` | `ON` | Boolean |
| `innodb_rds_flashback_allow_gap` | `30` | 0 to `UINT32_MAX` minutes |
| `innodb_rds_flashback_print_warning` | `ON` | Boolean |
| `innodb_undo_retention` | `0` | 0 to `UINT32_MAX` seconds |
| `innodb_undo_space_supremum_size` | `10240` | 0 to `UINT32_MAX` MiB |
| `innodb_undo_space_reserved_size` | `0` | 0 to `UINT32_MAX` MiB |

Administrative procedures are `dbms_admin.analyze_flashback_snapshots()`, `dbms_admin.show_flashback_snapshots(timestamp, order, limit)`, and `dbms_admin.del_flashback_snapshots(timestamp, order)`. Analyze returns the minimum and maximum snapshot timestamps plus total undo-space usage reported by InnoDB in MiB. Show and delete require `SUPER`.

Flashback supports SELECT references to InnoDB base tables. Temporary tables, views, locking reads, and historical reads across incompatible DDL are not supported. The effective history window is constrained by both retention time and undo-space pressure.

## Alibaba Cloud RDS MySQL Commercial Offering

Alibaba Cloud RDS MySQL productizes these kernel capabilities with service-managed rollout, parameter management, product topologies, synchronization, backup, monitoring, and support. RDS parameter names, defaults, ranges, and eligibility can differ from this open-source branch; use the official product documentation for RDS instances.

| Capability | Official documentation |
|------------|------------------------|
| DuckDB analytical instances | [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) / [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) |
| Vector storage | [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/vector-storage-1) / [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/vector-storage-1) |
| Native Flashback | [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/native-flashback) / [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/native-flashback) |
| Binlog Cache Free Flush | [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) / [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) |
| Binlog in Redo | [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-in-redo) / [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-in-redo) |
