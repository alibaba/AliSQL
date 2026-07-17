# DuckDB in AliSQL
![MySQL with DuckDB](./pic/mysql_with_duckdb.png)

[ [AliSQL DuckDB 引擎](./duckdb-zh.md) | [DuckDB in AliSQL](./duckdb-en.md) ]

## DuckDB Overview

[DuckDB](https://github.com/duckdb/duckdb) is an embedded analytical database with columnar storage and execution. The following properties are relevant to its integration with AliSQL:

- **Columnar execution and storage** for scans, joins, and aggregations
- **Automatic compression selection** based on column type and data distribution
- **Embedded library** that runs in the MySQL server process
- **Extension interfaces** used by the AliSQL integration
- **Permissive license** that allows commercial use


## DuckDB's Role in AliSQL

InnoDB remains AliSQL's transactional storage engine. DuckDB provides a columnar path for scans and aggregations, either in the same server or on a replica used for analytical traffic. The integration supports:

- **Hybrid Workloads**: Run both OLTP (MySQL/InnoDB) and OLAP (DuckDB) queries in a single database system
- **Analytical Queries**: In the TPC-H SF100 reference results below, several queries are more than **200x faster** than InnoDB in that test environment
- **Storage Cost Reduction**: Columnar compression can reduce the storage footprint of analytical replicas; measure the ratio with production-shaped data
- **MySQL-Compatible SQL Interface**: DuckDB is integrated as a storage engine, so applications continue to use the MySQL protocol and familiar SQL. Compatibility is high but not identical; see the compatibility notes below
- **Replica Topology**: A DuckDB node can consume row-based binlogs from an InnoDB primary
- **Startup Conversion**: Existing InnoDB user tables can be converted to DuckDB during server startup

DuckDB runs behind the AliSQL server layer, so clients continue to connect through the MySQL protocol. The integration covers common MySQL SQL patterns but is not fully compatible.

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
DuckDB is connected through MySQL's pluggable storage-engine interface:

![MySQL Architecture](./pic/mysql_arch.png)

The diagram highlights four layers involved in the integration:
- **Runtime Layer**: Handles MySQL runtime tasks like communication, access control, system configuration, and monitoring
- **Binlog Layer**: Manages binlog generation, replication, and application
- **SQL Layer**: Handles SQL parsing, optimization, and execution
- **Storage Engine Layer**: Manages data storage and access

### DuckDB Read-Only Instance Architecture

![DuckDB Architecture](./pic/duckdb_arch.png)

In the read-only topology shown above:

- Analytical queries are routed to the DuckDB node instead of the transactional primary
- Row-based binlogs replicate data from the primary
- InnoDB stores system tables and data-dictionary metadata on the DuckDB node
- User tables are stored in DuckDB

### Query Path

![Query Path](./pic/query_path.png)

1. Users connect via MySQL client
2. MySQL parses the query and performs necessary processing
3. SQL is sent to DuckDB engine for execution
4. DuckDB returns results to server layer
5. Server layer converts results to MySQL format and returns to client

**Compatibility**:

- AliSQL normalizes supported MySQL syntax before sending a query to DuckDB
- The integration maps selected MySQL functions and data types to DuckDB
- Compatibility is not complete. Validate application SQL, data types, collations, and DDL against the target build; RDS product metrics do not automatically apply to a self-managed server

### Binlog Replication Path

![Binlog Replication](./pic/binlog_replication.png)


AliSQL can run a DuckDB node as a binlog replica. DuckDB does not expose MySQL's two-phase commit interface, so AliSQL uses a dedicated commit, replay, and recovery protocol for this topology.

**Idempotent Replay**:

- Replay state is recorded so recovery can repeat an interrupted apply operation without duplicating committed changes

**DML Replay Optimization**:
- DuckDB favors large transactions; frequent small transactions can increase replication lag
- Batch replay groups small transactions to improve replication throughput and reduce commit overhead
- Multi-Trx Batch requires `replica_parallel_workers=0`, and `duckdb_multi_trx_in_batch` can be changed only while replication is stopped
- The resulting replay rate and lag depend on row shape, transaction size, hardware, and source concurrency
- Batch writes are also used for `INSERT` and, when the data-import restrictions permit, `DELETE` on a DuckDB primary
![Batch commit](./pic/batch_commit.png)

### DDL Compatibility & Optimizations

![DDL Compatibility](./pic/ddl_support.png)

- Natively supported DDL uses Inplace/Instant execution
- AliSQL uses Copy DDL for operations that DuckDB does not support in place, such as column reordering
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

## Alibaba Cloud RDS MySQL

RDS MySQL offers DuckDB analytical primary and read-only instances. The managed read-only topology includes binlog synchronization, DDL handling, and resource isolation from the transactional primary. The analytical primary is a separate product topology.

Supported versions, SQL compatibility, performance references, and trial instructions are listed on the RDS product page. Those results apply to the documented RDS configurations, not to an arbitrary source build.

- DuckDB analytical instances: [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) / [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance)


## See also

- [DuckDB Variables Reference](./duckdb_variables-en.md)
- [How to Setup DuckDB Node](./how-to-setup-duckdb-node-en.md)
- [DuckDB GitHub Repository](https://github.com/duckdb/duckdb)
- [Detailed Article (Chinese)](https://mp.weixin.qq.com/s/_YmlV3vPc9CksumXvXWBEw)
- [AliSQL](https://github.com/alibaba/AliSQL.git)
