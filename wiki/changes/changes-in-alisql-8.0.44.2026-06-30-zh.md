# AliSQL 8.0.44 功能发布说明（2026-06-30）

[ [Release Notes](../changes-in-alisql-8.0.44.md) | [English](./changes-in-alisql-8.0.44.2026-06-30-en.md) | [中文](./changes-in-alisql-8.0.44.2026-06-30-zh.md) ]

AliSQL 8.0.44 基于 MySQL 8.0.44。本次发布更新了 DuckDB 集成，并增加原生向量索引、Binlog Cache Free Flush、Persist Binlog Into Redo V2 和 Native Flashback。大多数新执行路径默认关闭；Flashback 查询入口默认开启，但快照生成和 Undo Retention 默认关闭。

## 功能概览

| 领域 | 功能 | 默认状态 |
|------|------|----------|
| 分析引擎 | DuckDB 存储引擎及 8.0.44 增强 | `duckdb_mode=NONE` |
| 向量检索 | 原生 `VECTOR` 类型与 HNSW 向量索引 | `vidx_disabled=ON` |
| 大事务优化 | Binlog Cache Free Flush | `OFF` |
| Binlog 持久化 | Persist Binlog Into Redo V2 | `persist_binlog_to_redo=OFF` |
| 历史数据查询 | Native Flashback `AS OF TIMESTAMP` | 快照任务默认 `OFF` |

## 1. DuckDB 存储引擎增强

AliSQL 将 DuckDB 作为原生分析型存储引擎集成，应用继续通过 MySQL 协议和 SQL 入口访问分析数据。本次更新将 DuckDB 内核升级到 v1.4.4，并补充兼容性、DDL、复制和资源治理能力。

### 新功能

- SQL Normalization：将受支持的 MySQL SQL 转写为 DuckDB 兼容形式，覆盖跨库查询、时间表达式、更多内置函数和 Prepared Statement 自动 reprepare。
- Generated Column：支持将包含生成列的表转换为 DuckDB。
- 类型与字符集：增加 Latin1、BLOB/VARCHAR 比较和 BIT 类型支持，修复 BINARY、VARBINARY、BIT 默认值处理。
- 查询线程控制：分别限制用户查询和复制查询可使用的 DuckDB Worker 线程数。
- Copy DDL 优化：DuckDB 表间复制可使用 `INSERT ... SELECT`，减少服务层逐行搬运。
- 高精度计算：可按会话优先选择高精度计算路径。
- 复制 CPU 治理：支持限制 DuckDB 复制任务的 CPU 使用。

### 稳定性修复

- 修复 JSON_UNQUOTE 处理 non-flat vector、`JSON_CONTAINS` 错误路径、`CONCAT`、带子查询 UPDATE/DELETE 等场景的崩溃。
- 修复 Prepared Statement 使用 `CURSOR_TYPE_READ_ONLY` 时的崩溃。
- 修复分区表 Copy DDL 崩溃、失败后 DuckDB 孤表清理和启动转换期间 Shutdown 的处理。
- 修复 BINARY、VARBINARY、BIT 默认值及多项 MySQL SQL 兼容问题。

### 复制修复

- 修复 Multi-Trx Batch 提交后 `Exec_Master_Log_Pos` 未更新。
- Multi-Trx Batch 要求 `replica_parallel_workers=0`；只能在复制停止时修改 `duckdb_multi_trx_in_batch`。
- 修复 Multi-Channel Replication 将 GTID `gno=0` 写入 `gtid_executed`。
- 修复 `commit_partial_batch` 失败后的回滚和 DuckDB Commit 失败后的 GTID 释放。
- 支持 `log_replica_updates=OFF` 场景下的 Multi-Trx Batch，避免空事务触发批量提交。
- 完善 Batch DML 临时表拆分、跨库事件、幂等回放和 2PC 判定。

### 新增参数

| 参数 | 类型 | 作用域 | 默认值 | 范围/说明 |
|------|------|--------|--------|-----------|
| `duckdb_sql_normalization` | BOOL | GLOBAL, SESSION | `OFF` | 开启 MySQL 到 DuckDB 的 SQL 转写 |
| `duckdb_max_threads_per_query` | ULONGLONG | GLOBAL, SESSION | `1000000` | 1 到 4611686018427387904，单个用户查询线程上限 |
| `duckdb_max_threads_per_query_rpl` | ULONGLONG | GLOBAL | `1000000` | 同上，单个复制查询线程上限 |
| `duckdb_psmt_cursor_send_extra_eof` | BOOL | GLOBAL, SESSION | `ON` | 空 Cursor 结果额外发送 EOF；Connector/J 9.5.0 之前建议保持开启 |
| `duckdb_prefer_high_precision` | BOOL | GLOBAL, SESSION | `OFF` | 优先使用高精度计算路径 |
| `duckdb_convert_tables_with_generated_columns` | BOOL | GLOBAL | `ON` | 允许转换包含生成列的表 |
| `duckdb_copy_data_between_tables_use_ins_sel` | BOOL | GLOBAL, SESSION | `OFF` | DuckDB 表间 Copy DDL 使用 `INSERT ... SELECT` |
| `ignore_index_hint_error` | BOOL | GLOBAL | `OFF` | Index Hint 引用不存在的索引时输出 Warning 而非报错 |

以上参数均可动态修改。`ignore_index_hint_error` 是全局 SQL 兼容参数，不只影响 DuckDB 表。

## 2. 原生向量索引 VIDX

VIDX 提供原生 `VECTOR(N)` 列、向量计算函数和基于 HNSW 的近似最近邻检索，支持欧氏距离与余弦距离，向量最大维度为 16,383。

### 主要能力

- 原生浮点向量存储与 MySQL SQL 接口。
- HNSW 图索引和 SIMD 距离计算加速。
- 支持向量与普通标量条件组合查询。
- 支持可空向量列；`NULL` 向量不会写入 HNSW 索引。
- 提供向量转换、维度检查、欧氏距离和余弦距离函数。

### 使用示例

```sql
SET GLOBAL vidx_disabled = OFF;
SET SESSION transaction_isolation = 'READ-COMMITTED';

CREATE TABLE embeddings (
    id BIGINT PRIMARY KEY,
    content VARCHAR(255),
    embedding VECTOR(3)
) ENGINE=InnoDB;

INSERT INTO embeddings VALUES
    (1, 'first',  VEC_FROMTEXT('[0.1,0.2,0.3]')),
    (2, 'second', VEC_FROMTEXT('[0.2,0.1,0.4]')),
    (3, 'missing', NULL);

CREATE VECTOR INDEX embedding_hnsw
    ON embeddings(embedding) M=6 DISTANCE=COSINE;

SELECT id, content,
       VEC_DISTANCE_COSINE(
           embedding, VEC_FROMTEXT('[0.1,0.2,0.3]')
       ) AS distance
FROM embeddings
ORDER BY distance
LIMIT 10;
```

### 函数

- 字符串转向量：`VEC_FROMTEXT`、`TO_VECTOR`、`STRING_TO_VECTOR`。
- 向量转字符串：`VEC_TOTEXT`、`FROM_VECTOR`、`VECTOR_TO_STRING`。
- 维度：`VECTOR_DIM`。
- 距离：`VEC_DISTANCE`、`VEC_DISTANCE_EUCLIDEAN`、`VEC_DISTANCE_COSINE`。

### 参数

| 参数 | 作用域 | 默认值 | 范围 | 说明 |
|------|--------|--------|------|------|
| `vidx_disabled` | GLOBAL | `ON` | `ON`, `OFF` | 禁止/允许创建向量列和向量索引 |
| `vidx_default_distance` | GLOBAL, SESSION | `EUCLIDEAN` | `EUCLIDEAN`, `COSINE` | 默认距离类型 |
| `vidx_hnsw_default_m` | GLOBAL, SESSION | `6` | 3 到 200 | HNSW 节点连接数 |
| `vidx_hnsw_ef_search` | GLOBAL, SESSION | `20` | 1 到 10000 | HNSW 查询候选集大小 |
| `vidx_hnsw_cache_size` | GLOBAL | 16 MiB | 1 MiB 到 `ULLONG_MAX` 字节 | HNSW 节点缓存上限 |

### 使用限制

- 向量索引只支持 InnoDB，并要求 `READ COMMITTED` 隔离级别。
- 向量索引 DDL 不支持 `ALGORITHM=INPLACE`，索引不能设置为 `INVISIBLE`。
- 查询向量必须与索引列维度一致。
- 标量距离函数对 `NULL` 返回 `NULL`；按距离升序排序时这些行位于末尾。
- HNSW 的层级和邻居选择包含随机与启发式步骤，副本之间不保证图拓扑逐字节一致。

## 3. 大事务优化：Binlog Cache Free Flush

普通大事务提交时，事务 Binlog Cache 会先落到临时文件，再复制到正式 Binlog，产生第二次大文件写入。Free Flush 在临时文件中预留 Binlog 头部空间，提交时完成文件并将其 Rename 为新的 Binlog 文件，从而减少提交阶段的 I/O 放大和延迟。

Free Flush 已支持 InnoDB 大事务，要求服务端注册的 2PC 参与者为 Binlog 和 InnoDB。当前 DuckDB 集成会额外注册一个 2PC 参与者，因此包含 DuckDB 的 AliSQL 使用普通 Binlog Group Commit；下一版本将支持 DuckDB 场景的大事务优化。

```sql
SET GLOBAL binlog_cache_free_flush_limit_size = 268435456;
SET GLOBAL binlog_cache_free_flush = ON;
```

### 参数

| 参数 | 作用域 | 默认值 | 范围 | 说明 |
|------|--------|--------|------|------|
| `binlog_cache_free_flush` | GLOBAL，动态 | `OFF` | `ON`, `OFF` | 开启 Free Flush |
| `binlog_cache_free_flush_limit_size` | GLOBAL，动态 | 256 MiB | 10 MiB 到 `ULLONG_MAX` 字节 | 进入 Free Flush 的事务 Cache 大小阈值 |

### 生效条件与自动回退

事务 Cache 必须超过阈值、已经写入磁盘临时文件并完成 Finalize；Statement Cache 必须为空；事务不能包含 Incident，也不能修改 `mysql.gtid_executed`；Binlog 必须处于打开状态。Binlog 加密场景下，临时文件与正式 Binlog 使用不同密钥，因此自动回退到普通提交路径。

InnoDB 必须是唯一注册的非 Binlog 2PC 引擎。存在其他 2PC 引擎或任一安全检查不满足时，事务使用普通 Binlog Group Commit，提交本身不受影响。

## 4. Persist Binlog Into Redo V2

该功能将符合条件的事务 Binlog Event 作为事务提交的一部分持久化到 InnoDB Redo，由独立后台线程异步写入并同步 Binlog 文件。若崩溃时 Binlog 文件尾部落后于已提交 Redo，恢复流程会先从 Redo 重建缺失的 Binlog Tail，再继续标准 Binlog 恢复。

### 推荐配置

```ini
[mysqld]
sync_binlog=1
innodb_flush_log_at_trx_commit=1
binlog_order_commits=OFF
replica_preserve_commit_order=OFF
persist_binlog_to_redo=ON
```

`innodb_flush_log_at_trx_commit=1` 用于维持完整的事务持久化基线，让承载 Binlog Event 的 Redo 在提交时同步落盘；当前实现不会将它作为功能开关的强制检查项。

### 参数

| 参数 | 作用域 | 默认值 | 范围 | 说明 |
|------|--------|--------|------|------|
| `persist_binlog_to_redo` | GLOBAL，动态 | `OFF` | `ON`, `OFF` | 开启 Binlog in Redo V2 |
| `persist_binlog_to_redo_size_limit` | GLOBAL，动态 | 1 MiB | 0 到 10 MiB | 可写入 Redo 的事务 Binlog Cache 上限 |
| `sync_binlog_interval` | GLOBAL，动态 | 50000 微秒 | 1 到 100000000 微秒 | 后台同步 Binlog 的时间间隔 |
| `binlog_buffer_size` | GLOBAL，只读 | 20 MiB | 20 MiB 到 1 GiB | 异步 Binlog 环形缓冲区大小 |
| `wait_binlog_flush` | GLOBAL，动态 | `ON` | `ON`, `OFF` | Commit 返回前等待 Binlog 写入文件 |
| `binlog_group_delay` | GLOBAL，动态 | 100 纳秒 | 0 到 1000000000 纳秒 | Group Leader 等待更多事务的时间 |
| `binlog_group_delay_running_threads` | GLOBAL，动态 | `100` | 0 到 100000 | 启用 Group Delay 的运行线程阈值 |

### 生效条件与自动回退

- 要求 `sync_binlog=1`、`binlog_order_commits=OFF`、`replica_preserve_commit_order=OFF`。
- 只支持一个活动的非 Binlog 2PC 引擎。DuckDB Mode 开启时不能启用；DuckDB Mode 关闭时会忽略空闲的 DuckDB 注册。
- 修改非事务表、使用 Statement Binlog Cache 或 Cache 超过 Size Limit 的事务走普通 Binlog Group Commit。
- Clone 等要求 Commit Order 的场景自动回退。

## 5. Native Flashback

Native Flashback 基于保留的 InnoDB Undo 重建历史一致性 Read View，通过 `AS OF TIMESTAMP` 直接查询过去时刻的数据，不需要恢复备份或在另一实例回放 Binlog。

### 启动配置

Flashback 快照后台线程只会在服务启动时检查并创建。启动时只要 `innodb_rds_flashback_task_enabled=ON` 或 `innodb_undo_retention>0` 满足一个条件就会创建线程，但实际生成快照必须同时开启任务并设置非零 Undo Retention。如果两者在启动时都关闭，之后仅通过动态 SET 打开并不会补建后台线程。

```ini
[mysqld]
innodb_rds_flashback_task_enabled=ON
innodb_undo_retention=3600
innodb_rds_flashback_interval=1
```

`innodb_rds_flashback_enabled=ON` 默认开启的是查询入口，本身不会生成快照或保留 Undo。

### 查询示例

```sql
SELECT id, status
FROM orders AS OF TIMESTAMP DATE_SUB(NOW(), INTERVAL 5 MINUTE)
WHERE customer_id = 1001;

CREATE TABLE recovered_orders LIKE orders;
INSERT INTO recovered_orders
SELECT *
FROM orders AS OF TIMESTAMP '2026-07-16 10:00:00';
```

时间表达式必须在单次执行中为常量，并可转换为 TIMESTAMP。

返回的 Read View 来自已保留的快照历史，不一定与请求的时间点完全一致。`innodb_rds_flashback_allow_gap` 限制允许的时间差，`innodb_rds_flashback_print_warning` 控制非精确匹配时是否输出告警。

使用 Flashback 恢复数据时，应先写入独立表并核对结果。若需通过 `RENAME TABLE` 替换原表，应先停止业务读写，并保留原表备份名直到恢复结果验证完成。

### 管理过程

| 调用 | 作用 |
|------|------|
| `CALL dbms_admin.analyze_flashback_snapshots()` | 返回最小时间、最大时间和 InnoDB 报告的 Undo 空间总占用（MiB） |
| `CALL dbms_admin.show_flashback_snapshots(ts, order, limit)` | 查看快照 TRX_ID、时间和 Read View；`order` 为 `ASC` 或 `DESC`；需要 `SUPER` |
| `CALL dbms_admin.del_flashback_snapshots(ts, order)` | 删除匹配的快照历史；`order` 为 `ASC` 或 `DESC`；需要 `SUPER` |

### 参数

以下参数均为 GLOBAL、动态 InnoDB Plugin 参数。

| 参数 | 默认值 | 范围/单位 | 说明 |
|------|--------|-----------|------|
| `innodb_rds_flashback_task_enabled` | `OFF` | BOOL | 参与生成 Flashback 快照；同时要求 Undo Retention 非零 |
| `innodb_rds_flashback_task_stop_all` | `OFF` | BOOL | 暂停全部快照任务 |
| `innodb_rds_flashback_interval` | `1` | 1 到 86400 秒 | 快照生成间隔 |
| `innodb_rds_flashback_enabled` | `ON` | BOOL | 允许 `AS OF TIMESTAMP` 查询 |
| `innodb_rds_flashback_allow_gap` | `30` | 0 到 `UINT32_MAX` 分钟 | 请求时间与匹配快照的最大间隔 |
| `innodb_rds_flashback_print_warning` | `ON` | BOOL | 使用非精确快照时输出 Warning |
| `innodb_undo_retention` | `0` | 0 到 `UINT32_MAX` 秒 | Undo 保留目标时间；生成快照也要求该值非零 |
| `innodb_undo_space_supremum_size` | `10240` | 0 到 `UINT32_MAX` MiB | 触发强制清理前的 Undo 空间上限 |
| `innodb_undo_space_reserved_size` | `0` | 0 到 `UINT32_MAX` MiB | Undo 保留空间阈值 |

### 使用限制

- 仅支持 InnoDB Base Table 的 SELECT Table Reference。
- 不支持临时表、View、`FOR UPDATE` 和 `LOCK IN SHARE MODE`。
- 查询时间超出已保留历史时返回 `ER_SNAPSHOT_OUT_OF_RANGE`。
- 改变主键记录可见性的 DDL，包括部分 Instant/Inplace 列变更，可能使旧快照不兼容并返回 `ER_FLASHBACK_PK_INVISIBLE`。
- 实际保留窗口同时受时间和 Undo 空间阈值约束，空间压力可能缩短可查询历史。

## 升级与使用建议

- 除 Flashback 查询入口外，新执行路径均默认关闭；Flashback 快照生成和 Undo Retention 同样默认关闭。
- DuckDB 兼容率较高但并非完全等同 MySQL，应使用真实业务 SQL 验证语法、函数、Collation 和 DDL。
- VIDX 的 HNSW 参数需要结合数据规模、召回率和延迟测试调整。
- Binlog in Redo V2 会在事务不满足检查条件时使用普通提交路径。Free Flush 当前要求 InnoDB 是唯一的非 Binlog 2PC 引擎，下一版本将支持 DuckDB 场景的大事务优化。
- 按峰值并发而不是平均负载规划 Binlog Buffer、Undo Retention 和 HNSW Cache。

## 阿里云 RDS MySQL

RDS MySQL 提供这些功能的托管版本，并负责产品拓扑、数据同步、备份、监控与支持。RDS 支持版本、参数名、默认值和范围可能与本源码不同，使用 RDS 实例时应以产品文档为准。

| 功能 | 官方文档 |
|------|----------|
| DuckDB 分析实例 | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) |
| 向量存储 | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/vector-storage-1) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/vector-storage-1) |
| Native Flashback | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/native-flashback) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/native-flashback) |
| Binlog Cache Free Flush | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush) |
| Binlog in Redo | [中文](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-in-redo) / [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-in-redo) |
