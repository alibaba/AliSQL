# AliSQL 8.0.44 Feature Release Notes

AliSQL 8.0.44 is based on MySQL 8.0.44. It includes the DuckDB storage engine, native vector indexes, binlog realtime replication (BRR), two binlog commit optimizations, and Native Flashback. Most new execution paths are disabled by default.

## Release History

| Update | English | 中文 |
|--------|---------|------|
| 2026-10-16: BRR, DuckDB snapshot upgrade, AUTO_INCREMENT, background checkpoints, and integration fixes | [Release notes](./changes/changes-in-alisql-8.0.44.2026-10-16-en.md) | [发布说明](./changes/changes-in-alisql-8.0.44.2026-10-16-zh.md) |
| 2026-06-30: DuckDB enhancements, VIDX, binlog commit optimizations, and Native Flashback | [Release notes](./changes/changes-in-alisql-8.0.44.2026-06-30-en.md) | [发布说明](./changes/changes-in-alisql-8.0.44.2026-06-30-zh.md) |

## Current Feature Overview

| Area | Feature | Default |
|------|---------|---------|
| Analytics | DuckDB storage engine and 8.0.44 enhancements | `duckdb_mode=NONE` |
| Vector search | Native `VECTOR` type and HNSW vector indexes | `vidx_disabled=ON` |
| Large transactions | Binlog Cache Free Flush | `OFF` |
| Binlog durability | Persist Binlog Into Redo V2 | `persist_binlog_to_redo=OFF` |
| Replication | BRR transmission and early apply for eligible DDL and large transactions | Transmission `OFF`; BRR workers `0` |
| Historical queries | Native Flashback with `AS OF TIMESTAMP` | Snapshot task `OFF` |

## DuckDB Storage Engine Enhancements

AliSQL integrates DuckDB as a native analytical storage engine while preserving the MySQL client protocol and SQL entry point. The 8.0.44 update includes the following improvements.

> **RDS MySQL:** Managed DuckDB analytical primary and read-only instances include service-side topology, synchronization, DDL handling, and resource isolation. See the official [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) and [Chinese](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) documentation. Those service features are not part of this source tree.

### New Features

- Upgraded the embedded DuckDB core to snapshot `v1.5.2-dev736` at `8a66223119b16000f3814fa7150cfa7370b63209`.
- Added optional AUTO_INCREMENT and background automatic checkpoints.
- Added `JSON_OBJECTAGG`, cross-schema RENAME TABLE, additional temporal SQL compatibility, and DDL/rollback fixes.
- Added optional SQL normalization for cross-database queries, temporal expressions, additional MySQL functions, and prepared-statement reprepare.
- Added generated-column conversion, Latin1 character-set support, BLOB/VARCHAR comparison, and improved BINARY, VARBINARY, and BIT default-value handling.
- Added separate per-query thread controls for user and replication workloads.
- Added an optional high-precision calculation path.
- Added an `INSERT ... SELECT` path for Copy DDL between DuckDB tables.
- Added CPU controls for DuckDB replication work.

### Replication and Reliability Fixes

- Correctly advances `Exec_Master_Log_Pos` after multi-transaction batch commits.
- Requires `replica_parallel_workers=0` for Multi-Trx Batch; change `duckdb_multi_trx_in_batch` only while replication is stopped.
- Prevents invalid GTID `gno=0` entries in multi-channel replication.
- Correctly rolls back partial batches and releases all GTIDs after DuckDB commit failures.
- Supports multi-transaction batches when `log_replica_updates=OFF` and avoids batching empty transactions.
- Improves temporary-table splitting, cross-database event handling, and idempotent replay.
- Cleans orphan DuckDB tables after failed Copy DDL and handles shutdown during startup conversion.
- Fixes crashes involving JSON functions, `CONCAT`, UPDATE/DELETE subqueries, partition Copy DDL, and read-only prepared-statement cursors.

### New DuckDB Variables

| Variable | Scope | Default | Description |
|----------|-------|---------|-------------|
| `duckdb_sql_normalization` | Global, Session | `OFF` | Rewrite supported MySQL SQL for DuckDB |
| `duckdb_max_threads_per_query` | Global, Session | `1000000` | Maximum threads for one user query; range 1 to 4611686018427387904 |
| `duckdb_max_threads_per_query_rpl` | Global | `1000000` | Maximum threads for one replication query; same range |
| `duckdb_psmt_cursor_send_extra_eof` | Global, Session | `ON` | Send an extra EOF for empty prepared-statement cursor results |
| `duckdb_prefer_high_precision` | Global, Session | `OFF` | Prefer high-precision calculation paths |
| `duckdb_convert_tables_with_generated_columns` | Global | `ON` | Allow conversion of tables containing generated columns |
| `duckdb_copy_data_between_tables_use_ins_sel` | Global, Session | `OFF` | Use `INSERT ... SELECT` for DuckDB-to-DuckDB Copy DDL |
| `ignore_index_hint_error` | Global | `OFF` | Warn instead of failing when an index hint names a missing index |
| `duckdb_support_auto_increment` | Global | `OFF` | Enable AUTO_INCREMENT support for DuckDB tables |
| `duckdb_enable_background_checkpoint` | Global | `OFF` | Run automatic checkpoints in a background thread |
| `duckdb_background_checkpoint_full_interval` | Global | `0` | Concurrent checkpoints between full checkpoints; 0 to `ULLONG_MAX`, zero means always full |

For deployment, compatibility boundaries, and the complete variable list, see the [DuckDB integration guide](./duckdb/duckdb-en.md), [setup guide](./duckdb/how-to-setup-duckdb-node-en.md), and [variables reference](./duckdb/duckdb_variables-en.md).

## Binlog Realtime Replication (BRR)

BRR overlaps source execution, network transfer, and replica work. It can transmit eligible large transactions before source commit and optionally apply supported DDL and transactions on dedicated replica workers. Final commit/rollback follows the source outcome and GTID dependencies.

- Source and receiver switches default to `OFF`; `binlog_realtime_apply_workers` defaults to `0`.
- The active source-side large-transaction threshold, `binlog_realtime_replica_long_transaction_limit_size`, defaults to 512 GiB. The DDL time threshold defaults to 1000 ms.
- Both nodes must negotiate the same BRR protocol on the default channel. Early apply requires GTIDs, parallel workers, `replica_preserve_commit_order=OFF`, and the documented channel restrictions.
- Large-transaction transmission uses Free Flush cache files, while the actual Free Flush commit optimization retains its transaction eligibility checks. Active DuckDB mode selects a dedicated commit path.
- BRR early apply and DuckDB Multi-Trx Batch have conflicting ordinary-worker requirements; their example configurations must not be combined unchanged.
- This update fixes BRR metadata-event leaks, XA recovery without a current THD, and normal OPTIMIZE binlogging after partial failures.

See the [English BRR guide](./binlog-realtime-replication/binlog-realtime-replication-en.md) or [中文指南](./binlog-realtime-replication/binlog-realtime-replication-zh.md) for configuration, parameter defaults, monitoring, limitations, and shutdown procedures.

## Native Vector Index (VIDX)

VIDX adds a native `VECTOR(N)` column type, scalar vector functions, and approximate nearest-neighbor search backed by an HNSW graph. It supports Euclidean and cosine distance for vectors with up to 16,383 dimensions.

### Quick Start

```sql
SET GLOBAL vidx_disabled = OFF;
SET SESSION transaction_isolation = 'READ-COMMITTED';

CREATE TABLE embeddings (
    id BIGINT PRIMARY KEY,
    embedding VECTOR(3)
) ENGINE=InnoDB;

INSERT INTO embeddings VALUES
    (1, VEC_FROMTEXT('[0.1,0.2,0.3]')),
    (2, VEC_FROMTEXT('[0.2,0.1,0.4]')),
    (3, NULL);

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

### Functions and Variables

- Conversion: `VEC_FROMTEXT`, `TO_VECTOR`, `STRING_TO_VECTOR`, `VEC_TOTEXT`, `FROM_VECTOR`, and `VECTOR_TO_STRING`.
- Inspection: `VECTOR_DIM`.
- Distance: `VEC_DISTANCE`, `VEC_DISTANCE_EUCLIDEAN`, and `VEC_DISTANCE_COSINE`.

| Variable | Scope | Default | Range |
|----------|-------|---------|-------|
| `vidx_disabled` | Global | `ON` | `ON`, `OFF` |
| `vidx_default_distance` | Global, Session | `EUCLIDEAN` | `EUCLIDEAN`, `COSINE` |
| `vidx_hnsw_default_m` | Global, Session | `6` | 3 to 200 |
| `vidx_hnsw_ef_search` | Global, Session | `20` | 1 to 10000 |
| `vidx_hnsw_cache_size` | Global | 16 MiB | 1 MiB to `ULLONG_MAX` bytes |

### Current Boundaries

- Vector indexes require InnoDB and `READ COMMITTED` isolation.
- Vector-index DDL does not support `ALGORITHM=INPLACE`; vector indexes cannot be invisible.
- Nullable vector columns are supported. Rows containing `NULL` are omitted from the vector index; scalar distance evaluation returns `NULL` for those rows.
- Query and indexed vectors must have the same dimension.

See the [Vector Index guide](./vidx/vidx_readme.md) for architecture and tuning details.

> **RDS MySQL:** Managed vector storage includes service-side enablement, data synchronization, backup, and recovery. See the official [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/vector-storage-1) and [Chinese](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/vector-storage-1) documentation. Supported versions and defaults may differ from this source tree.

## Large TX Optimization: Binlog Cache Free Flush

Large transactions normally spill their transaction binlog cache to a temporary file and then copy that data into the binlog again during commit. Binlog Cache Free Flush reserves the required binlog header space in that temporary file, finalizes it, and renames it as the next binlog file. This removes the second large copy and reduces commit-time I/O amplification.

Free Flush targets eligible InnoDB transactions while DuckDB mode is inactive. The BRR integration removes the earlier extra-2PC-engine registration guard, so including DuckDB in a build does not by itself force fallback. Active DuckDB mode instead selects a dedicated commit path before the Free Flush checks. See the [Free Flush guide](./binlog-cache-free-flush/binlog-cache-free-flush-en.md) for current eligibility and recovery considerations.

> **RDS MySQL:** Supported versions and the RDS `loose_binlog_cache_free_flush*` parameters are listed in the official [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) and [Chinese](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) documentation. The RDS names and ranges do not apply to the source variables below.

```sql
SET GLOBAL binlog_cache_free_flush_limit_size = 268435456;
SET GLOBAL binlog_cache_free_flush = ON;
```

| Variable | Scope | Default | Range | Description |
|----------|-------|---------|-------|-------------|
| `binlog_cache_free_flush` | Global, dynamic | `OFF` | `ON`, `OFF` | Enables the optimized commit path |
| `binlog_cache_free_flush_limit_size` | Global, dynamic | 256 MiB | 10 MiB to `ULLONG_MAX` bytes | Minimum transaction cache size for Free Flush |

Free Flush requires the transaction cache to exceed the configured limit, have spilled to a finalized unencrypted temporary file with reserved header space, contain no statement-cache events or incidents, and not modify `mysql.gtid_executed`. The binlog must be open. Active DuckDB mode selects its dedicated commit path before these checks; otherwise transactions rejected by Free Flush continue through the configured normal/Binlog-in-Redo commit paths.

Binlog encryption excludes Free Flush because the cache file and final binlog use different keys. The earlier engine-registration guard is no longer present, so crash-recovery qualification must cover the actual engine combination.

For the full mechanism, eligibility checks, build boundary, and open-source/RDS differences, see the [Binlog Cache Free Flush guide](./binlog-cache-free-flush/binlog-cache-free-flush-en.md) or its [Chinese version](./binlog-cache-free-flush/binlog-cache-free-flush-zh.md).

## Persist Binlog Into Redo V2

Persist Binlog Into Redo V2 stores eligible transaction binlog events in InnoDB redo as part of transaction commit. Dedicated background threads write and synchronize the binlog file. If a crash leaves the binlog tail behind the committed redo, recovery reconstructs the missing tail from redo before normal binlog recovery continues.

This path reduces foreground binlog I/O while retaining the durability contract for eligible InnoDB transactions.

```ini
[mysqld]
sync_binlog=1
innodb_flush_log_at_trx_commit=1
binlog_order_commits=OFF
replica_preserve_commit_order=OFF
persist_binlog_to_redo=ON
```

`innodb_flush_log_at_trx_commit=1` preserves the intended commit-durability baseline by synchronizing the redo that carries eligible binlog events. It is a durability recommendation rather than an enablement guard in the current implementation.

> **RDS MySQL:** Replication prerequisites, backup behavior, parameters, and performance references are listed in the official [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-in-redo) and [Chinese](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-in-redo) documentation. Self-managed AliSQL uses the variables and fallback rules below.

### Variables

| Variable | Scope | Default | Range | Description |
|----------|-------|---------|-------|-------------|
| `persist_binlog_to_redo` | Global, dynamic | `OFF` | `ON`, `OFF` | Enable Binlog in Redo V2 |
| `persist_binlog_to_redo_size_limit` | Global, dynamic | 1 MiB | 0 to 10 MiB | Maximum eligible transaction binlog-cache size |
| `sync_binlog_interval` | Global, dynamic | 50000 us | 1 to 100000000 us | Background binlog synchronization interval |
| `binlog_buffer_size` | Global, read-only | 20 MiB | 20 MiB to 1 GiB | Asynchronous binlog ring-buffer size |
| `wait_binlog_flush` | Global, dynamic | `ON` | `ON`, `OFF` | Wait for the binlog write before returning from commit |
| `binlog_group_delay` | Global, dynamic | 100 ns | 0 to 1000000000 ns | Leader delay used to collect a larger commit group |
| `binlog_group_delay_running_threads` | Global, dynamic | `100` | 0 to 100000 | Minimum running-thread threshold for group delay |

### Eligibility and Fallback

- Requires `sync_binlog=1`, `binlog_order_commits=OFF`, and `replica_preserve_commit_order=OFF`.
- Supports one active non-binlog 2PC storage engine. Enabling the feature is rejected while DuckDB mode is active; DuckDB registration is discounted while DuckDB mode is off.
- Transactions that modify nontransactional tables, use the statement binlog cache, or exceed `persist_binlog_to_redo_size_limit` use normal binlog group commit.
- Commit-order requirements introduced by clone operations also force the normal path.

These checks are per transaction where possible, so unsupported transactions fall back without disabling the global feature.

For configuration, sizing, durability semantics, and open-source/RDS differences, see the [Binlog in Redo guide](./binlog-in-redo/binlog-in-redo-en.md) or its [Chinese version](./binlog-in-redo/binlog-in-redo-zh.md).

## Native Flashback

Native Flashback reconstructs an InnoDB consistent read view from retained undo and exposes it through `AS OF TIMESTAMP`. It supports historical reads without restoring a backup or replaying binlogs into another instance.

> **RDS MySQL:** Managed Native Flashback has its own supported versions and retention controls. See the official [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/native-flashback) and [Chinese](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/native-flashback) documentation. Self-managed AliSQL uses the snapshot and query controls below.

### Startup Configuration

The snapshot-history background thread is created only when either `innodb_rds_flashback_task_enabled=ON` or `innodb_undo_retention>0` is set at server startup. Actual snapshot generation requires **both** the task to be enabled and undo retention to be nonzero. Configure both before startup; enabling both only after a startup with both values disabled does not create the missing thread.

```ini
[mysqld]
innodb_rds_flashback_task_enabled=ON
innodb_undo_retention=3600
innodb_rds_flashback_interval=1
```

`innodb_rds_flashback_enabled=ON` is the default query gate, but it does not by itself start snapshot generation or retain undo.

### Query Syntax

```sql
SELECT id, status
FROM orders AS OF TIMESTAMP DATE_SUB(NOW(), INTERVAL 5 MINUTE)
WHERE customer_id = 1001;

CREATE TABLE recovered_orders LIKE orders;
INSERT INTO recovered_orders
SELECT *
FROM orders AS OF TIMESTAMP '2026-07-16 10:00:00';
```

The timestamp expression must be constant for one statement execution and evaluate to a timestamp value.

The returned read view is selected from retained snapshot history and is not guaranteed to match the requested wall-clock time exactly. `innodb_rds_flashback_allow_gap` bounds the accepted difference, and `innodb_rds_flashback_print_warning` controls warnings for a non-exact match.

When using Flashback for recovery, first write the historical result into a separate table and validate it. Stop application reads and writes before replacing the original table with `RENAME TABLE`; keep the original table under a backup name until the recovered data has been verified.

### Administrative Procedures

| Procedure | Result or action |
|-----------|------------------|
| `CALL dbms_admin.analyze_flashback_snapshots()` | Returns minimum timestamp, maximum timestamp, and total undo-space usage reported by InnoDB in MiB |
| `CALL dbms_admin.show_flashback_snapshots(ts, order, limit)` | Lists snapshot transaction ID, timestamp, and read view; `order` is `ASC` or `DESC`; requires `SUPER` |
| `CALL dbms_admin.del_flashback_snapshots(ts, order)` | Deletes matching snapshot history; `order` is `ASC` or `DESC`; requires `SUPER` |

### Variables

All variables below are global, dynamic InnoDB plugin variables.

| Variable | Default | Range / unit | Description |
|----------|---------|--------------|-------------|
| `innodb_rds_flashback_task_enabled` | `OFF` | Boolean | Generate snapshot-history records |
| `innodb_rds_flashback_task_stop_all` | `OFF` | Boolean | Pause all snapshot-history tasks |
| `innodb_rds_flashback_interval` | `1` | 1 to 86400 seconds | Snapshot generation interval |
| `innodb_rds_flashback_enabled` | `ON` | Boolean | Allow `AS OF TIMESTAMP` queries |
| `innodb_rds_flashback_allow_gap` | `30` | 0 to `UINT32_MAX` minutes | Maximum gap between requested time and matched snapshot |
| `innodb_rds_flashback_print_warning` | `ON` | Boolean | Warn when a non-exact snapshot is selected |
| `innodb_undo_retention` | `0` | 0 to `UINT32_MAX` seconds | Undo retention target |
| `innodb_undo_space_supremum_size` | `10240` | 0 to `UINT32_MAX` MiB | Maximum undo space for retention before forced cleanup |
| `innodb_undo_space_reserved_size` | `0` | 0 to `UINT32_MAX` MiB | Reserved undo space threshold |

### Current Boundaries

- `AS OF TIMESTAMP` is supported only for `SELECT` table references on InnoDB base tables.
- Temporary tables, views, `FOR UPDATE`, and `LOCK IN SHARE MODE` are not supported.
- A requested time outside retained history returns `ER_SNAPSHOT_OUT_OF_RANGE`.
- DDL that changes the visible primary-key record, including some instant/inplace column changes, can make an older snapshot incompatible and return `ER_FLASHBACK_PK_INVISIBLE`.
- Undo retention is bounded by both time and configured undo-space thresholds. Size pressure can shorten the effective history window.

For complete enablement rules, recovery workflow, procedures, and open-source/RDS differences, see the [Native Flashback guide](./native-flashback/native-flashback-en.md) or its [Chinese version](./native-flashback/native-flashback-zh.md).

## Upgrade Notes

- All major new execution paths are disabled by default except the Native Flashback query gate; snapshot generation and undo retention remain disabled by default.
- Validate DuckDB SQL compatibility and vector recall/performance with production-shaped workloads before migration.
- Size `binlog_buffer_size`, undo retention, and HNSW cache limits against peak concurrency rather than average load.
- Keep the normal binlog path available. Binlog in Redo and Free Flush retain their respective transaction checks; active DuckDB mode uses its dedicated commit path. Qualify Free Flush crash recovery with the actual engine combination.

Earlier baseline: [2025-12-31 DuckDB release](./changes/changes-in-alisql-8.0.44.2025-12-31.md).

Where applicable, feature guides link to RDS MySQL documentation and describe differences from the source build.
