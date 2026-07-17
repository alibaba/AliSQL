# Native Flashback

[ [AliSQL](../../README.md) | [English](./native-flashback-en.md) | [中文](./native-flashback-zh.md) ]

Native Flashback lets an InnoDB query read retained historical data with `AS OF TIMESTAMP`. It builds a consistent historical read view from snapshot records and retained undo, so accidental data changes can be investigated or recovered without first restoring a backup and replaying binlogs on another server.

The settings below apply to self-managed AliSQL 8.0.44. For RDS MySQL versions and parameters, see [Alibaba Cloud RDS MySQL](#alibaba-cloud-rds-mysql).

## How It Works

AliSQL periodically records an InnoDB snapshot and retains the undo needed to reconstruct older row versions. An `AS OF TIMESTAMP` query selects a snapshot from that history and uses its read view for a consistent InnoDB read.

The requested wall-clock time and the selected snapshot need not be identical. `innodb_rds_flashback_allow_gap` limits the acceptable difference, and `innodb_rds_flashback_print_warning` controls whether a non-exact match produces a warning.

## Enable Native Flashback

Snapshot generation requires **both** of the following values:

- `innodb_rds_flashback_task_enabled=ON`
- `innodb_undo_retention>0`

The background thread is created at server startup when either value is enabled. If the server starts with the task disabled and retention set to zero, changing both variables later does not create the missing thread. Configure both values before startup:

```ini
[mysqld]
innodb_rds_flashback_task_enabled=ON
innodb_undo_retention=3600
innodb_rds_flashback_interval=1
```

`innodb_rds_flashback_enabled=ON` is the query gate and is enabled by default. It does not generate snapshots or retain undo by itself. `innodb_rds_flashback_task_stop_all=ON` pauses snapshot creation and history cleanup even when the other controls are enabled.

After startup, confirm the effective values:

```sql
SHOW GLOBAL VARIABLES LIKE 'innodb_rds_flashback%';
SHOW GLOBAL VARIABLES LIKE 'innodb_undo_retention';
```

Wait for snapshot history to cover the target time before running a historical query.

## Query Historical Data

```sql
SELECT id, status
FROM orders AS OF TIMESTAMP DATE_SUB(NOW(), INTERVAL 5 MINUTE)
WHERE customer_id = 1001;
```

The timestamp expression must be constant for one statement execution and evaluate to a timestamp value.

To recover data, write the historical result into a separate table first:

```sql
CREATE TABLE recovered_orders LIKE orders;

INSERT INTO recovered_orders
SELECT *
FROM orders AS OF TIMESTAMP '2026-07-16 10:00:00';
```

Validate row counts and business invariants before replacing production data. Quiesce application reads and writes before using `RENAME TABLE`, and keep the original table under a backup name until the recovered data is verified.

## Inspect Snapshot History

| Procedure | Result or action |
|-----------|------------------|
| `CALL dbms_admin.analyze_flashback_snapshots()` | Return the minimum timestamp, maximum timestamp, and total undo-space usage reported by InnoDB in MiB |
| `CALL dbms_admin.show_flashback_snapshots(ts, order, limit)` | List snapshot transaction ID, timestamp, and read view; `order` is `ASC` or `DESC`; requires `SUPER` |
| `CALL dbms_admin.del_flashback_snapshots(ts, order)` | Delete matching snapshot history; `order` is `ASC` or `DESC`; requires `SUPER` |

Use the analysis procedure to confirm that a target timestamp is inside the retained window before recovery.

## Variables

All variables below are global, dynamic InnoDB plugin variables. Startup configuration is still required to ensure that the background thread is created.

| Variable | Default | Range or unit | Description |
|----------|---------|---------------|-------------|
| `innodb_rds_flashback_task_enabled` | `OFF` | Boolean | Participate in snapshot generation; generation also requires nonzero undo retention |
| `innodb_rds_flashback_task_stop_all` | `OFF` | Boolean | Pause all snapshot-history work |
| `innodb_rds_flashback_interval` | `1` | 1 to 86400 seconds | Snapshot generation interval |
| `innodb_rds_flashback_enabled` | `ON` | Boolean | Allow `AS OF TIMESTAMP` queries |
| `innodb_rds_flashback_allow_gap` | `30` | 0 to `UINT32_MAX` minutes | Maximum gap between the requested time and selected snapshot |
| `innodb_rds_flashback_print_warning` | `ON` | Boolean | Warn when a non-exact snapshot is selected |
| `innodb_undo_retention` | `0` | 0 to `UINT32_MAX` seconds | Undo retention target; snapshot generation also requires a nonzero value |
| `innodb_undo_space_supremum_size` | `10240` | 0 to `UINT32_MAX` MiB | Undo-space ceiling used by retention cleanup |
| `innodb_undo_space_reserved_size` | `0` | 0 to `UINT32_MAX` MiB | Reserved undo-space threshold |

## Boundaries and Operations

- `AS OF TIMESTAMP` is supported only for `SELECT` table references on InnoDB base tables.
- Temporary tables, views, `FOR UPDATE`, and `LOCK IN SHARE MODE` are not supported.
- A time outside retained history returns `ER_SNAPSHOT_OUT_OF_RANGE`.
- DDL that changes the visible primary-key record can make an older snapshot incompatible and return `ER_FLASHBACK_PK_INVISIBLE`. Historical access across arbitrary DDL is not guaranteed.
- Undo retention is a target, not an unconditional guarantee. Space thresholds and workload pressure can shorten the effective history window.
- Retaining more undo consumes storage and can increase purge work. Monitor undo-space growth and query latency with production-shaped workloads.
- Flashback is a targeted recovery tool, not a replacement for backups, binlogs, or tested disaster-recovery procedures.

## Alibaba Cloud RDS MySQL

RDS MySQL also provides Native Flashback, with managed enablement and retention settings. See the official [English documentation](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/native-flashback) and [Chinese documentation](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/native-flashback).

RDS versions, console operations, and parameter policy are product-specific. For a source build, use the startup rules, defaults, ranges, and SQL restrictions on this page.
