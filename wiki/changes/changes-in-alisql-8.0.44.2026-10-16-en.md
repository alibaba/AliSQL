# AliSQL 8.0.44 Feature Release Notes (2026-10-16)

[ [Release Notes](../changes-in-alisql-8.0.44.md) | [English](./changes-in-alisql-8.0.44.2026-10-16-en.md) | [中文](./changes-in-alisql-8.0.44.2026-10-16-zh.md) ]

This update adds binlog realtime replication (BRR), updates the embedded DuckDB source, and fixes issues found while integrating the two features. It remains based on MySQL 8.0.44.

## Overview

| Area | Update | Default |
|------|--------|---------|
| Replication | BRR transmission and optional early apply for supported DDL and large transactions | Source/receiver switches `OFF`; BRR workers `0` |
| Analytics | DuckDB snapshot `v1.5.2-dev736` at `8a66223119b16000f3814fa7150cfa7370b63209` | `duckdb_mode=NONE` |
| DuckDB DDL | AUTO_INCREMENT support | `duckdb_support_auto_increment=OFF` |
| DuckDB checkpoints | Background automatic checkpoints and full-checkpoint interval | Background mode `OFF`; interval `0` |

## Binlog Realtime Replication

BRR transmits eligible transaction-cache events before source commit and can execute supported DDL and large transactions on dedicated replica workers ahead of the commit. Final completion follows the source transaction outcome and GTID dependencies. This overlaps source execution, transfer, and replica work; it does not provide synchronous replication or a zero-lag guarantee.

- Source and replica negotiate a matching BRR protocol on the default replication channel.
- Large-transaction transmission requires source-side `binlog_cache_free_flush=ON`, an eligible disk-backed cache, and no binlog transaction compression.
- The active threshold is `binlog_realtime_replica_long_transaction_limit_size`, configured on the source. Its default is **512 GiB**, with a lower bound of 10 MiB.
- Early apply requires GTIDs, parallel replication, nonzero `binlog_realtime_apply_workers`, `replica_preserve_commit_order=OFF`, and the documented channel restrictions.
- DDL early transmission defaults to a 1000 ms threshold and covers supported transactional ALTER/rebuild paths. Instant DDL and replica-applier DDL are excluded.
- The new Information Schema tables `BRR_SOURCE_TRX_INFO`, `BRR_SENDER_TRX_INFO`, and `BRR_REPLICA_INFO`, together with BRR status counters, expose active work.
- `optimize_table_write_binlog_advance` is an independent option, default `OFF`, that changes OPTIMIZE binlog timing to after successful recreation and before ANALYZE.

See the [BRR guide](../binlog-realtime-replication/binlog-realtime-replication-en.md) for enable/disable procedures, all defaults, compatibility variables, monitoring, and fallback behavior.

## DuckDB Updates

The embedded source is a pinned development snapshot, `v1.5.2-dev736`, rather than a claim of an upstream stable v1.5.2 release. The upgrade includes integration API adaptations and expanded optimizer names while preserving AliSQL's empty `duckdb_disabled_optimizers` default.

### Features

- Optional AUTO_INCREMENT handling for DuckDB tables, including dictionary, DDL, and replication support.
- Optional background automatic checkpoints and a configurable interval between full checkpoints.
- `JSON_OBJECTAGG` support and additional temporal/string SQL compatibility.
- Cross-schema RENAME TABLE for DuckDB tables.

### Compatibility and Reliability

- Correctly quote column defaults, identifiers, and literals containing quotes, backticks, backslashes, or embedded NUL.
- Handle ADD/MODIFY/CHANGE NOT NULL with existing rows and SET NOT NULL after ADD COLUMN DEFAULT.
- Handle generated-column dependencies and explicit partition engine conversion; guard invalid partition operations on nonpartitioned tables.
- Retry startup conversions after VIEW metadata-lock deadlocks and preserve shutdown/error handling.
- Handle appender rollback exceptions and reset parser error positions between queries.
- Repair the InnoDB FIL-cache charset handling needed by failed atomic cross-engine DDL.

### New Variables

All three variables are global and dynamic:

| Variable | Default | Range | Purpose |
|----------|---------|-------|---------|
| `duckdb_support_auto_increment` | `OFF` | `ON`, `OFF` | Enable AUTO_INCREMENT support for DuckDB tables |
| `duckdb_enable_background_checkpoint` | `OFF` | `ON`, `OFF` | Execute automatic checkpoints in a background thread |
| `duckdb_background_checkpoint_full_interval` | `0` | 0 to `ULLONG_MAX` | Concurrent checkpoints between full checkpoints in background mode; zero means always full |

See the [DuckDB guide](../duckdb/duckdb-en.md) and [variables reference](../duckdb/duckdb_variables-en.md).

## Integration Fixes

- Release skipped BRR metadata events in the relay-log reader to prevent memory leaks.
- Handle metadata-lock acquisition during startup XA recovery when no current THD exists.
- Preserve normal binlogging and GTID generation for partially successful multi-table OPTIMIZE when early binlogging is disabled.
- Stabilize BRR regression synchronization, cleanup waits, event-count expectations, and test isolation.

## Upgrade and Configuration Notes

- BRR, DuckDB AUTO_INCREMENT, and DuckDB background checkpoints require explicit enablement; existing AliSQL defaults are retained.
- DuckDB Multi-Trx Batch requires `replica_parallel_workers=0` and cannot be combined unchanged with BRR early-apply settings, which require parallel workers.
- BRR uses Free Flush cache files. This integration removes the older extra-2PC-engine fallback guard; active DuckDB mode still selects a dedicated commit path. See the [Free Flush guide](../binlog-cache-free-flush/binlog-cache-free-flush-en.md) for the current behavior and recovery considerations.
- RDS-only packaging, release metadata, private submodule builds, recycle-bin integration, and the reverted DECIMAL precision-above-38 feature are not included.
- Previous features and release history are described in the [2026-06-30 notes](./changes-in-alisql-8.0.44.2026-06-30-en.md).
