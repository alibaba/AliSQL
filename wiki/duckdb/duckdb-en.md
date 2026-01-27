# DuckDB in AliSQL
![MySQL with DuckDB](./pic/mysql_with_duckdb.png)

[ [AliSQL DuckDB 引擎](./duckdb-zh.md) | [DuckDB in AliSQL](./duckdb-en.md) ]

## What is DuckDB?

[DuckDB](https://github.com/duckdb/duckdb) is an open-source embedded analytical database system (OLAP) designed for data analysis workloads. DuckDB is rapidly becoming a popular choice in data science, BI tools, and embedded analytics scenarios due to its key characteristics:

- **Exceptional Query Performance**: Single-node DuckDB performance not only far exceeds InnoDB, but even surpasses ClickHouse and SelectDB
- **Excellent Compression**: DuckDB uses columnar storage and automatically selects appropriate compression algorithms based on data types, achieving very high compression ratios
- **Embedded Design**: DuckDB is an embedded database system, naturally suitable for integration into MySQL
- **Plugin Architecture**: DuckDB uses a plugin-based design, making it very convenient for third-party development and feature extensions
- **Friendly License**: DuckDB's license allows any form of use, including commercial purposes


## Why Integrate DuckDB with AliSQL?

MySQL has long lacked an analytical query engine. While InnoDB is naturally designed for OLTP and excels in TP scenarios, its query efficiency is very low for analytical workloads. This integration enables:

- **Hybrid Workloads**: Run both OLTP (MySQL/InnoDB) and OLAP (DuckDB) queries in a single database system
- **High-Performance Analytics**: Analytical query performance improves up to **200x** compared to InnoDB
- **Storage Cost Reduction**: DuckDB read replicas typically use only **20%** of the main instance's storage space due to high compression
- **100% MySQL Syntax Compatibility**: No learning curve - DuckDB is integrated as a storage engine, so users continue using MySQL syntax
- **Zero Additional Management Cost**: DuckDB instances are managed, operated, and monitored exactly like regular RDS MySQL instances
- **One-Click Deployment**: Create DuckDB read-only instances with automatic data conversion from InnoDB to DuckDB

**AliSQL** integrates **DuckDB** as a native AP engine, empowering users with high-performance, lightweight analytical capabilities while maintaining a seamless, MySQL-compatible experience.


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
- Automated compatibility testing platform with ~170,000 SQL tests shows **[99% compatibility rate](https://www.alibabacloud.com/help/en/rds/apsaradb-rds-for-mysql/compatibility-of-duckdb-based-analytical-instances?spm=a2c63.p38356.help-menu-26090.d_3_4_2.6a97448exEuaFG)**

### Binlog Replication Path

![Binlog Replication](./pic/binlog_replication.png)


AliSQL allows DuckDB nodes to serve as replicas via Binlog synchronization. By re-engineering the transaction commit and replay processes, AliSQL overcomes the lack of 2PC support in DuckDB, ensuring full data and metadata consistency even after abnormal crashes.

**Idempotent Replay**:
- Since DuckDB doesn't support two-phase commit, custom transaction commit and binlog replay processes ensure data consistency after instance crashes

**DML Replay Optimization**:
- DuckDB favors large transactions; frequent small transactions cause severe replication lag
- Implemented batch replay mechanism achieving **300K rows/s** replay capability
- In Sysbench testing, achieves zero replication lag, even higher than InnoDB replay performance
- Batch-write optimization also applies to the primary node: with our DML optimizations, INSERT and DELETE may achieve excellent performance on the primary.
![Batch commit](./pic/batch_commit.png)

### DDL Compatibility & Optimizations

![DDL Compatibility](./pic/ddl_support.png)

- Natively supported DDL uses Inplace/Instant execution
- For DDL operations DuckDB doesn't natively support (e.g., column reordering), implemented Copy DDL mechanism
- Convert from InnoDB to DuckDB using multi-threaded parallel execution. Execution time reduced by **7x**
![Copy DDL from InnoDB](./pic/parallel_copy_from_innodb.png)


## Performance Benchmarks
**Test Environment**:
- ECS Instance: 32 CPU, 128GB Memory, ESSD PL1 Cloud Disk 500GB
- Benchmark: TPC-H SF100

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
|total|15.31|25234.31|80.01

DuckDB demonstrates significant performance advantages over InnoDB in analytical query scenarios, with up to **200x improvement**.

## Try It on Alibaba Cloud
You can experience RDS MySQL with DuckDB engine on Alibaba Cloud:

https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-based-analytical-instance/


## See also

- [DuckDB Variables Reference](./duckdb_variables-en.md)
- [How to Setup DuckDB Node](./how-to-setup-duckdb-node-en.md)
- [DuckDB GitHub Repository](https://github.com/duckdb/duckdb)
- [Detailed Article (Chinese)](https://mp.weixin.qq.com/s/_YmlV3vPc9CksumXvXWBEw)
- [AliSQL](https://github.com/alibaba/AliSQL.git)
