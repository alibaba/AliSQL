# Binlog Cache Free Flush

[ [AliSQL](../../README.md) | [English](./binlog-cache-free-flush-en.md) | [中文](./binlog-cache-free-flush-zh.md) ]

Binlog Cache Free Flush is a large-transaction commit optimization. When a transaction binlog cache spills to a temporary file, the normal path copies that file into the binlog during commit. Free Flush reserves binlog header space in the temporary file, finalizes the file, and renames it as the next binlog file, avoiding the second large data copy.

The configuration below applies to self-managed AliSQL 8.0.44. RDS MySQL uses different parameter names and version rules; see [Alibaba Cloud RDS MySQL](#alibaba-cloud-rds-mysql).

## Support in This Release

Free Flush supports large InnoDB transactions in the binlog-plus-InnoDB 2PC topology. The current DuckDB integration registers an additional 2PC participant, so AliSQL builds that include DuckDB use normal binlog group commit, even if `duckdb_mode=NONE`. Large-transaction optimization for DuckDB will be added in the next release.

The restriction comes from crash recovery. Free Flush currently supports the binlog plus one non-binlog 2PC storage engine. When the binlog is enabled, a build with DuckDB registers an additional 2PC `prepare` callback regardless of `duckdb_mode`.

Free Flush closes the old binlog before renaming the temporary file. If a crash occurs between those steps, the current recovery code cannot resolve the prepared transaction when more than one non-binlog 2PC engine is registered. Using normal group commit in that configuration avoids the ambiguous recovery state and does not change transaction correctness.

## Configuration

Enable Free Flush only when the binlog and InnoDB are the server's registered 2PC participants:

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
- No 2PC storage engine other than InnoDB is registered.

If any check fails, that transaction uses normal binlog group commit and can still commit successfully.

## Variables

| Variable | Scope | Default | Range | Description |
|----------|-------|---------|-------|-------------|
| `binlog_cache_free_flush` | Global, dynamic | `OFF` | `ON`, `OFF` | Enable Free Flush eligibility checks |
| `binlog_cache_free_flush_limit_size` | Global, dynamic | 256 MiB | 10 MiB to `ULLONG_MAX` bytes | Minimum transaction binlog-cache size for Free Flush |

The open-source lower bound is 10 MiB. Do not copy the RDS product range or `loose_` variable names into a self-managed configuration.

## Operational Guidance

- Keep normal group commit available as the fallback path.
- Confirm that InnoDB is the only registered non-binlog 2PC engine.
- Benchmark transactions above the threshold and confirm actual I/O behavior; variable state alone is not evidence that Free Flush ran.
- Binlog encryption always forces fallback because the cache temporary file and final binlog use different file keys.
- Keep crash-injection and restart recovery tests in the qualification suite for any build that enables the optimized path.

## Alibaba Cloud RDS MySQL

RDS MySQL also provides Binlog Cache Free Flush. Supported versions, parameter names, and parameter ranges are documented in the official [English documentation](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) and [Chinese documentation](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush).

RDS uses `loose_binlog_cache_free_flush*` and a product-specific threshold range. Do not copy those names or ranges into a self-managed AliSQL configuration.
