# AliSQL 中的 DuckDB
![MySQL with DuckDB](./pic/mysql_with_duckdb.png)

[ [DuckDB in AliSQL](./duckdb-en.md) | [AliSQL DuckDB 引擎](./duckdb-zh.md) ]

## DuckDB 简介

[DuckDB](https://github.com/duckdb/duckdb) 是采用列式存储和执行的嵌入式分析数据库。与 AliSQL 集成相关的特性包括：

- **列式执行与存储**：用于扫描、连接和聚合
- **自动选择压缩算法**：根据列类型和数据分布选择编码
- **嵌入式库**：与 MySQL 服务端运行在同一进程
- **扩展接口**：供 AliSQL 接入和扩展 DuckDB
- **宽松的许可证**：允许商业使用

## DuckDB 在 AliSQL 中的用途

InnoDB 仍是 AliSQL 的事务型存储引擎，DuckDB 为扫描和聚合提供列式执行路径，可以与 InnoDB 表部署在同一服务，也可以作为承接分析流量的从节点。当前集成支持：

- **混合负载**：在同一个数据库系统中同时运行 OLTP（MySQL/InnoDB）与 OLAP（DuckDB）查询
- **分析查询**：在下方 TPC-H SF100 参考结果中，多项查询在该测试环境下相比 InnoDB 加速超过 **200 倍**
- **降低存储成本**：列式压缩可以降低分析从节点的存储占用，实际压缩率应使用业务数据测量
- **兼容 MySQL 的 SQL 接口**：DuckDB 以存储引擎方式集成，应用继续使用 MySQL 协议和熟悉的 SQL；兼容度较高但并非完全一致，详见下文兼容性说明
- **从节点拓扑**：DuckDB 节点可从 InnoDB 主库消费 Row 格式 Binlog
- **启动转换**：可以在服务启动阶段将现有 InnoDB 用户表转换为 DuckDB

DuckDB 位于 AliSQL 服务端层之后，客户端仍通过 MySQL 协议连接。集成层覆盖常用 MySQL SQL，但并非完全兼容。

## 快速开始

在启动配置中启用 DuckDB；`duckdb_mode` 在服务启动后为只读：

```ini
[mysqld]
duckdb_mode=ON
duckdb_memory_limit=2147483648
duckdb_threads=0
duckdb_temp_directory=/path/to/duckdb-tmp
```

之后即可通过 MySQL SQL 创建或转换分析表：

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

生产拓扑与初始化步骤请参见[如何搭建 DuckDB 节点](./how-to-setup-duckdb-node-zh.md)。

## AliSQL 8.0.44 增强

- DuckDB 内核升级到 v1.4.4。
- 可选 SQL Normalization 可转写更多 MySQL 语法和函数，包括跨库查询及 Prepared Statement 自动 reprepare。
- 增加 Generated Column 转换、Latin1、BIT/默认值兼容以及 BLOB/VARCHAR 比较支持。
- 增加用户查询与复制查询的单查询线程上限，以及可选的高精度计算模式。
- DuckDB 表间 Copy DDL 可选择使用 `INSERT ... SELECT` 加速。
- 完善批量复制、GTID 管理、幂等回放、崩溃回滚和孤表清理。

完整变更与控制参数参见 [AliSQL 8.0.44 发布说明](../changes-in-alisql-8.0.44.md)和 [DuckDB 参数参考](./duckdb_variables-zh.md)。

## 架构

### MySQL 可插拔存储引擎架构
DuckDB 通过 MySQL 可插拔存储引擎接口接入：

![MySQL Architecture](./pic/mysql_arch.png)

图中列出了与本次集成相关的四层：
- **运行时层（Runtime Layer）**：处理通信、访问控制、系统配置、监控等 MySQL 运行时任务
- **Binlog 层（Binlog Layer）**：负责 Binlog 生成、复制与应用
- **SQL 层（SQL Layer）**：负责 SQL 解析、优化与执行
- **存储引擎层（Storage Engine Layer）**：负责数据存储与访问

### DuckDB 只读实例架构

![DuckDB Architecture](./pic/duckdb_arch.png)

在图示的只读拓扑中：

- 分析查询发送到 DuckDB 节点，不占用事务主库的查询执行资源
- 通过 Row 格式 Binlog 从主库复制数据
- DuckDB 节点上的系统表和数据字典元数据由 InnoDB 保存
- 用户表存放在 DuckDB 中

### 查询路径（Query Path）

![Query Path](./pic/query_path.png)

1. 用户通过 MySQL 客户端连接
2. MySQL 解析查询并进行必要处理
3. 将 SQL 发送到 DuckDB 引擎执行
4. DuckDB 将结果返回给服务端层
5. 服务端层将结果转换为 MySQL 格式并返回给客户端

**兼容性**：

- AliSQL 在查询进入 DuckDB 前对支持的 MySQL 语法进行转写
- 集成层将部分 MySQL 函数和数据类型映射到 DuckDB
- 兼容范围并非完整覆盖；迁移前应在目标构建上验证业务 SQL、数据类型、Collation 和 DDL，RDS 产品指标不能直接套用于自建版本

### Binlog 复制路径（Replication Path）

![Binlog Replication](./pic/binlog_replication.png)

AliSQL 支持 DuckDB 节点通过 Binlog 作为从库。由于 DuckDB 没有提供 MySQL 两阶段提交接口，AliSQL 为该拓扑实现了专用的提交、回放和恢复协议。

**幂等回放（Idempotent Replay）**：

- 回放状态会被记录，恢复时可以重新执行中断的 Apply 操作，而不会重复已经提交的变更

**DML 回放优化（DML Replay Optimization）**：
- DuckDB 更偏好大事务；频繁的小事务可能增加复制延迟
- 通过批量回放合并小事务，提高复制吞吐并减少提交开销
- Multi-Trx Batch 要求 `replica_parallel_workers=0`，并且只能在复制停止时修改 `duckdb_multi_trx_in_batch`
- 实际回放速率与延迟取决于行大小、事务规模、硬件和主库并发
- DuckDB 主节点上的 `INSERT`，以及满足数据导入限制时的 `DELETE`，也会使用批量写入
![Batch commit](./pic/batch_commit.png)

### DDL 兼容性与优化

![DDL Compatibility](./pic/ddl_support.png)

- 原生支持的 DDL 使用 Inplace/Instant 执行
- 对于 DuckDB 原生不支持的 DDL（例如列重排），实现了 Copy DDL 机制
- InnoDB 转 DuckDB 支持多线程并行转换，以缩短迁移时间
![Copy DDL from InnoDB](./pic/parallel_copy_from_innodb.png)

## 适用范围与限制

- DuckDB 面向分析型负载。对延迟敏感的事务写入应保留在 InnoDB；需要负载隔离时建议使用 DuckDB 从节点。
- DuckDB 复制表应开启 `duckdb_require_primary_key`。DuckDB 不会实际创建 MySQL PRIMARY/UNIQUE 索引，源端数据需要自行保证唯一性。
- MySQL SQL 兼容范围较广，但并非完整覆盖；应针对目标版本验证语法、函数、排序规则和 DDL 行为。
- DuckDB 原生不支持 MySQL 两阶段提交。AliSQL 提供了专用提交与幂等回放协议，不应绕过受支持的 Binlog 复制路径。
- `duckdb_use_direct_io` 仍为实验能力，不建议在生产环境启用。

## 性能基准测试（Performance Benchmarks）
**测试环境**：
- ECS 规格：32 CPU，128GB 内存，ESSD PL1 云盘 500GB
- Benchmark：TPC-H SF100
- 下表数值为查询耗时（秒）；`1800` 表示测试超时，`OOM` 表示内存不足。

以下数据是原始集成材料提供的方向性参考结果，不构成性能承诺。本文未记录精确的引擎版本、缓存状态、Schema 选项和重复运行方法，请使用自身数据与配置重新验证。`total` 行保留原始汇总方式；由于超时和 OOM 属于截断结果，不能将其作为严格的端到端加速比。

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

在这组参考结果中，DuckDB 的多项已完成查询相比 InnoDB 加速超过 **200 倍**。

## 阿里云 RDS MySQL

RDS MySQL 提供 DuckDB 分析主实例和分析只读实例。托管只读拓扑包含 Binlog 同步、DDL 处理以及与事务主实例的资源隔离；分析主实例是另一种独立产品拓扑。

RDS 产品页列出了支持版本、SQL 兼容范围、性能参考和试用方式。这些结果对应文档中的 RDS 配置，不代表任意自建版本的表现。

- DuckDB 分析实例：[中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance)

## 相关链接（See also）

- [DuckDB 参数参考](./duckdb_variables-zh.md)
- [如何搭建 DuckDB 节点](./how-to-setup-duckdb-node-zh.md)
- [DuckDB GitHub 仓库](https://github.com/duckdb/duckdb)
- [详细文章（中文）](https://mp.weixin.qq.com/s/_YmlV3vPc9CksumXvXWBEw)
- [AliSQL](https://github.com/alibaba/AliSQL.git)
