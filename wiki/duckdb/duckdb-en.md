# DuckDB in AliSQL
![MySQL with DuckDB](./pic/mysql_with_duckdb.png)

[ [AliSQL DuckDB 引擎](./duckdb-zh.md) | [DuckDB in AliSQL](./duckdb-en.md) ]

## What is DuckDB?

[DuckDB](https://github.com/duckdb/duckdb) is an open-source embedded analytical database system (OLAP) designed for data analysis workloads. DuckDB is rapidly becoming a popular choice in data science, BI tools, and embedded analytics scenarios due to its key characteristics:

- **Analytical Query Performance**: Columnar execution is designed for analytical workloads; the reference benchmark below compares AliSQL DuckDB with InnoDB and ClickHouse
- **Excellent Compression**: DuckDB uses columnar storage and automatically selects appropriate compression algorithms based on data types, achieving very high compression ratios
- **Embedded Design**: DuckDB is an embedded database system, naturally suitable for integration into MySQL
- **Plugin Architecture**: DuckDB uses a plugin-based design, making it very convenient for third-party development and feature extensions
- **Friendly License**: DuckDB's license allows any form of use, including commercial purposes


## Why Integrate DuckDB with AliSQL?

MySQL has long lacked an analytical query engine. While InnoDB is naturally designed for OLTP and excels in TP scenarios, its query efficiency is very low for analytical workloads. This integration enables:

- **Hybrid Workloads**: Run both OLTP (MySQL/InnoDB) and OLAP (DuckDB) queries in a single database system
- **High-Performance Analytics**: In the reference TPC-H SF100 results below, multiple queries show more than **200x speedups** over InnoDB
- **Storage Cost Reduction**: Columnar compression can reduce the storage footprint of analytical replicas; measure the ratio with production-shaped data
- **MySQL-Compatible SQL Interface**: DuckDB is integrated as a storage engine, so applications continue to use the MySQL protocol and familiar SQL. Compatibility is high but not identical; see the compatibility notes below
- **Familiar Operations**: DuckDB instances retain the MySQL protocol and can use the same operational entry points as other AliSQL instances
- **Startup Conversion**: Existing InnoDB user tables can be converted to DuckDB during a controlled server startup

**AliSQL** integrates **DuckDB** as a native AP engine, empowering users with high-performance, lightweight analytical capabilities while maintaining a seamless, MySQL-compatible experience.

## Quick Start

Enable DuckDB at startup; `duckdb_mode` is read-only after the server starts:

```ini
[mysqld]
duckdb_mode=ON
duckdb_memory_limit=2147483648
duckdb_threads=0
duckdb_temp_directory=/path/to/duckdb-tmp
```

Then create or convert an analytical table through MySQL SQL:

```sql
CREATE TABLE sales (
    id BIGINT PRIMARY KEY,
    region VARCHAR(32),
    amount DECIMAL(18,2)
) ENGINE=DuckDB;

SELECT region, SUM(amount)
FROM sales
GROUP BY region
ORDER BY SUM(amount) DESC;

ALTER TABLE existing_innodb_table ENGINE=DuckDB;
```

For production topology and initialization details, see [How to Setup a DuckDB Node](./how-to-setup-duckdb-node-en.md).

## AliSQL 8.0.44 Highlights

- DuckDB core upgraded to v1.4.4.
- Optional SQL normalization rewrites additional MySQL syntax and functions for DuckDB, including cross-database queries and prepared-statement reprepare.
- Added generated-column conversion, Latin1 support, BIT/default-value compatibility, and BLOB/VARCHAR comparison.
- Added per-query thread limits for user and replication workloads, plus an optional high-precision calculation mode.
- Added an `INSERT ... SELECT` path for faster Copy DDL between DuckDB tables.
- Improved batch replication, GTID handling, idempotent replay, crash rollback, and orphan-table cleanup.

See the [AliSQL 8.0.44 release notes](../changes-in-alisql-8.0.44.md) and [DuckDB variables reference](./duckdb_variables-en.md) for the complete list and controls.


## Architecture
### MySQL's Pluggable Storage Engine Architecture
MySQL's pluggable storage engine architecture allows it to extend its capabilities through different storage engines:

![MySQL Architecture](./pic/mysql_arch.png)

The architecture consists of four main layers:
- **Runtime Layer**: Handles MySQL runtime tasks like communication, access control, system configuration, and monitoring
- **Binlog Layer**: Manages binlog generation, replication, and application
- **SQL Layer**: Handles SQL parsing, optimization, and execution
- **Storage Engine Layer**: Manages data storage and access

### DuckDB Read-Only Instance Architecture

![DuckDB Architecture](./pic/duckdb_arch.png)

DuckDB analytical read-only instances use a read-write separation architecture:
- Analytical workloads are separated from the main instance, ensuring no mutual impact
- Data replication from the main instance via binlog mechanism (similar to regular read replicas)
- InnoDB stores only metadata and system information (accounts, configurations)
- All user data resides in the DuckDB engine

### Query Path

![Query Path](./pic/query_path.png)

1. Users connect via MySQL client
2. MySQL parses the query and performs necessary processing
3. SQL is sent to DuckDB engine for execution
4. DuckDB returns results to server layer
5. Server layer converts results to MySQL format and returns to client

**Compatibility**:
- Extended DuckDB's syntax parser to support MySQL-specific syntax
- Rewrote numerous DuckDB functions and added many MySQL functions
- Compatibility is broad but not complete. Validate application-specific SQL, data types, collations, and DDL before migration; RDS product compatibility metrics do not automatically apply to a self-managed build

### Binlog Replication Path

![Binlog Replication](./pic/binlog_replication.png)


AliSQL allows DuckDB nodes to serve as replicas via Binlog synchronization. By re-engineering the transaction commit and replay processes, AliSQL overcomes the lack of 2PC support in DuckDB, ensuring full data and metadata consistency even after abnormal crashes.

**Idempotent Replay**:
- Since DuckDB doesn't support two-phase commit, custom transaction commit and binlog replay processes ensure data consistency after instance crashes

**DML Replay Optimization**:
- DuckDB favors large transactions; frequent small transactions cause severe replication lag
- Batch replay groups small transactions to improve replication throughput and reduce commit overhead
- Multi-Trx Batch requires `replica_parallel_workers=0`, and `duckdb_multi_trx_in_batch` can be changed only while replication is stopped
- The resulting replay rate and lag depend on row shape, transaction size, hardware, and source concurrency
- Batch-write optimization also applies to the primary node: with our DML optimizations, INSERT and DELETE may achieve excellent performance on the primary.
![Batch commit](./pic/batch_commit.png)

### DDL Compatibility & Optimizations

![DDL Compatibility](./pic/ddl_support.png)

- Natively supported DDL uses Inplace/Instant execution
- For DDL operations DuckDB doesn't natively support (e.g., column reordering), implemented Copy DDL mechanism
- InnoDB-to-DuckDB conversion uses multi-threaded parallel execution to reduce migration time
![Copy DDL from InnoDB](./pic/parallel_copy_from_innodb.png)

## Operational Scope and Limitations

- DuckDB is intended for analytical workloads. Keep latency-sensitive transactional writes on InnoDB and use a DuckDB replica when workload isolation is required.
- Enable `duckdb_require_primary_key` for replicated DuckDB tables. DuckDB does not physically enforce MySQL PRIMARY/UNIQUE indexes, so source data must preserve uniqueness.
- MySQL SQL compatibility is broad but not complete. Test unsupported syntax, functions, collations, and DDL behavior against the target release.
- DuckDB does not natively implement MySQL two-phase commit. AliSQL supplies a dedicated commit and idempotent replay protocol; do not bypass the supported binlog replication path.
- `duckdb_use_direct_io` is experimental and is not recommended for production use.


## Performance Benchmarks
**Test Environment**:
- ECS Instance: 32 CPU, 128GB Memory, ESSD PL1 Cloud Disk 500GB
- Benchmark: TPC-H SF100
- Values below are query latency in seconds; `1800` denotes the test timeout and `OOM` denotes out-of-memory.

These are directional reference results supplied with the original integration material, not a performance guarantee. Exact engine versions, cache state, schema options, and run-count methodology are not recorded here. Validate performance with your own data and configuration. The `total` row reproduces the supplied aggregation; because timeout and OOM entries are censored outcomes, it is not a strict end-to-end speedup metric.

| Query ID | DuckDB | InnoDB | ClickHouse |
| --- | --- | --- | --- |
|q1|0.92|1134.25|3.47|
|q2|0.15|1800|1.52|
|q3|0.53|802.94|3.65|
|q4|0.46|1000.45|2.77|
|q5|0.5|1800|5.38|
|q6|0.22|566.73|0.73|
|q7|0.59|1800|6.06|
|q8|0.68|1800|6.99|
|q9|1.44|1800|13.29|
|q10|0.91|894.35|3.22|
|q11|0.11|79.63|1.1|
|q12|0.44|734.35|1.69|
|q13|1.59|454.15|5.85|
|q14|0.38|574.07|0.83|
|q15|0.31|568.43|1.53|
|q16|0.32|63.56|0.52|
|q17|0.89|1800|7.96|
|q18|1.59|1800|3.11|
|q19|0.8|1800|2.96|
|q20|0.51|1800|3.38|
|q21|1.64|1800|OOM|
|q22|0.33|361.4|4|
|total|15.31|25234.31|80.01|

In these reference results, DuckDB shows more than **200x speedups** over InnoDB on multiple completed queries.

## Alibaba Cloud RDS MySQL Commercial Offering

Alibaba Cloud RDS MySQL offers DuckDB analytical primary and analytical read-only instance products. The managed read-only topology adds native binlog synchronization, automatic DDL handling, and resource isolation from the transactional primary; the analytical primary is available as a separately deployed product topology.

The official RDS product page documents product-level SQL compatibility and analytical acceleration results, as well as current availability and trial options. Those metrics and lifecycle capabilities apply to RDS-managed configurations and are not performance or compatibility guarantees for arbitrary self-managed builds.

- DuckDB analytical instances: [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) / [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance)


## See also

- [DuckDB Variables Reference](./duckdb_variables-en.md)
- [How to Setup DuckDB Node](./how-to-setup-duckdb-node-en.md)
- [DuckDB GitHub Repository](https://github.com/duckdb/duckdb)
- [Detailed Article (Chinese)](https://mp.weixin.qq.com/s/_YmlV3vPc9CksumXvXWBEw)
- [AliSQL](https://github.com/alibaba/AliSQL.git)
