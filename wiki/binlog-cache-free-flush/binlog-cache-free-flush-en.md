# Binlog Cache Free Flush

[ [AliSQL](../../README.md) | [English](./binlog-cache-free-flush-en.md) | [中文](./binlog-cache-free-flush-zh.md) ]

Binlog Cache Free Flush is a large-transaction commit optimization. When a transaction binlog cache spills to a temporary file, the normal path copies that file into the binlog during commit. Free Flush reserves binlog header space in the temporary file, finalizes the file, and renames it as the next binlog file, avoiding the second large data copy.

The configuration below applies to self-managed AliSQL 8.0.44. RDS MySQL uses different parameter names and version rules; see [Alibaba Cloud RDS MySQL](#alibaba-cloud-rds-mysql).

## Support in This Release

Free Flush targets eligible large InnoDB transactions while DuckDB mode is inactive. The BRR integration removes the earlier check that rejected Free Flush solely because another non-binlog 2PC engine was registered. A build containing DuckDB can therefore enter Free Flush with `duckdb_mode=NONE`; engine registration alone no longer guarantees fallback.

When DuckDB mode is enabled, the binlog commit entry selects the dedicated DuckDB commit path before checking Free Flush eligibility. Enabling `binlog_cache_free_flush` does not turn that path into a Free Flush commit.

Free Flush closes the old binlog before renaming the cache file. Crash/restart qualification must cover that interval with the actual registered engine combination; the removed engine-count check must not be treated as a remaining recovery safeguard.

## Configuration

Configure the cache infrastructure and the InnoDB transaction size threshold:

```sql
SET GLOBAL binlog_cache_free_flush_limit_size = 268435456;
SET GLOBAL binlog_cache_free_flush = ON;
```

Both variables are dynamic. Setting them successfully means the feature was configured; it does not prove that a transaction entered the optimized path.

## Transaction Eligibility

In an eligible build, a transaction enters Free Flush only when every check passes:

- `binlog_cache_free_flush=ON`.
- The transaction binlog cache is larger than `binlog_cache_free_flush_limit_size`.
- The transaction cache has spilled to disk and has nonzero reserved header space.
- The transaction cache is finalized and contains no incident.
- The statement binlog cache is empty.
- Binlog encryption and cache-file encryption are disabled.
- The binlog is open.
- The transaction did not modify `mysql.gtid_executed`.
- The commit entry has not selected the dedicated DuckDB-mode path.

Transactions that do not enter Free Flush continue through the configured commit paths: the dedicated DuckDB path when selected, or normal/Binlog-in-Redo commit otherwise.

## Variables

| Variable | Scope | Default | Range | Description |
|----------|-------|---------|-------|-------------|
| `binlog_cache_free_flush` | Global, dynamic | `OFF` | `ON`, `OFF` | Enable Free Flush eligibility checks |
| `binlog_cache_free_flush_limit_size` | Global, dynamic | 256 MiB | 10 MiB to `ULLONG_MAX` bytes | Minimum transaction binlog-cache size for Free Flush |

The open-source lower bound is 10 MiB. Do not copy the RDS product range or `loose_` variable names into a self-managed configuration.

## Operational Guidance

### Use with Binlog Realtime Replication

[BRR large-transaction transmission](../binlog-realtime-replication/binlog-realtime-replication-en.md) also uses the named cache files and reserved header space enabled by `binlog_cache_free_flush=ON`. BRR therefore requires this source-side setting even when a transaction does not meet the Free Flush commit checks. Enabling the cache infrastructure for BRR does not imply that the rename-based commit path is eligible.

### Commit-Path Checks

- Keep normal group commit available as the fallback path.
- Check active DuckDB mode; engine registration alone does not exclude Free Flush in this release.
- Benchmark transactions above the threshold and confirm actual I/O behavior; variable state alone is not evidence that Free Flush ran.
- Binlog encryption always forces fallback because the cache temporary file and final binlog use different file keys.
- Keep crash-injection and restart recovery tests in the qualification suite for any build that enables the optimized path.

## Alibaba Cloud RDS MySQL

RDS MySQL also provides Binlog Cache Free Flush. Supported versions, parameter names, and parameter ranges are documented in the official [English documentation](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) and [Chinese documentation](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush).

RDS uses `loose_binlog_cache_free_flush*` and a product-specific threshold range. Do not copy those names or ranges into a self-managed AliSQL configuration.
