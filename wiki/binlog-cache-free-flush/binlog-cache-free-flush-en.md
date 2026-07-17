# Binlog Cache Free Flush

[ [AliSQL](../../README.md) | [English](./binlog-cache-free-flush-en.md) | [中文](./binlog-cache-free-flush-zh.md) ]

Binlog Cache Free Flush is a large-transaction commit optimization. When a transaction binlog cache spills to a temporary file, the normal path copies that file into the binlog during commit. Free Flush reserves binlog header space in the temporary file, finalizes the file, and renames it as the next binlog file, avoiding the second large data copy.

This document describes the open-source AliSQL 8.0.44 branch. For the managed RDS MySQL product, see [Alibaba Cloud RDS MySQL](#alibaba-cloud-rds-mysql).

## Availability in This Branch

> **The implementation and variables are present, but the optimized path is inactive in the standard DuckDB-enabled build. Every transaction falls back to normal binlog group commit.**

Free Flush crash recovery currently supports the binlog plus one non-binlog 2PC storage engine. In the standard build, DuckDB registers a 2PC `prepare` callback whenever the binlog is enabled, even with `duckdb_mode=NONE`. The resulting 2PC engine count fails the Free Flush safety check.

This is why the feature is not described as merely "limited": changing `duckdb_mode` or enabling the Free Flush variables does not make the optimized path reachable in the standard build. The path can run only in a build with no additional registered 2PC engine, unless its crash-recovery protocol is redesigned to support that engine set.

Fallback is deliberate. Free Flush cleanly closes the old binlog before the temporary file becomes the new binlog. A crash in that interval can leave a prepared transaction that the existing recovery path cannot safely resolve when multiple non-binlog 2PC engines are registered.

## Enable in a Compatible Build

Only use the following configuration after verifying that the build has no additional registered 2PC engine:

```sql
SET GLOBAL binlog_cache_free_flush_limit_size = 268435456;
SET GLOBAL binlog_cache_free_flush = ON;
```

Both variables are dynamic. Setting them successfully means the feature was configured; it does not prove that a transaction entered the optimized path.

## Transaction Eligibility

In a compatible build, an individual transaction enters Free Flush only when every check passes:

- `binlog_cache_free_flush=ON`.
- The transaction binlog cache is larger than `binlog_cache_free_flush_limit_size`.
- The transaction cache has spilled to disk and has nonzero reserved header space.
- The transaction cache is finalized and contains no incident.
- The statement binlog cache is empty.
- Binlog encryption and cache-file encryption are disabled.
- The binlog is open.
- The transaction did not modify `mysql.gtid_executed`.
- The 2PC configuration contains only the binlog and one non-binlog storage engine.

If any check fails, that transaction uses normal binlog group commit and can still commit successfully.

## Variables

| Variable | Scope | Default | Range | Description |
|----------|-------|---------|-------|-------------|
| `binlog_cache_free_flush` | Global, dynamic | `OFF` | `ON`, `OFF` | Enable Free Flush eligibility checks |
| `binlog_cache_free_flush_limit_size` | Global, dynamic | 256 MiB | 10 MiB to `ULLONG_MAX` bytes | Minimum transaction binlog-cache size for Free Flush |

The open-source lower bound is 10 MiB. Do not copy the RDS product range or `loose_` variable names into a self-managed configuration.

## Operational Guidance

- Treat normal group commit as the required fallback path.
- Verify the registered 2PC engine set before enabling the feature in a custom build.
- Benchmark transactions above the threshold and confirm actual I/O behavior; variable state alone is not evidence that Free Flush ran.
- Binlog encryption always forces fallback because the cache temporary file and final binlog use different file keys.
- Keep crash-injection and restart recovery tests in the qualification suite for any build that enables the optimized path.

## Alibaba Cloud RDS MySQL

Alibaba Cloud RDS for MySQL offers a commercially supported Free Flush rollout with product-specific kernel eligibility and parameter management. See the official [English documentation](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) or [Chinese documentation](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush).

The RDS page uses `loose_binlog_cache_free_flush*`, has a product-specific threshold range, and defines supported RDS engine versions. Those details apply to RDS instances, not to this source branch. The fact that RDS offers the optimization does not change the standard open-source build's DuckDB 2PC interaction described above.
