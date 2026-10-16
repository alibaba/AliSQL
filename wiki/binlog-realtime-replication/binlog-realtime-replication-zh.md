# Binlog 实时复制（BRR）

[ [AliSQL](../../README_zh.md) | [English](./binlog-realtime-replication-en.md) | [中文](./binlog-realtime-replication-zh.md) ]

BRR 将复制工作与主库执行过程重叠：符合条件的大事务可以在提交前传输缓存中的 Binlog Event；专用从库 Worker 还可以在主库提交前开始执行受支持的 DDL 和事务。这样可以减少主库提交后剩余的复制工作量，实际收益取决于事务大小、DDL 耗时、网络和从库资源。

BRR 默认关闭。本文参数以当前 AliSQL 源码为准，不能直接套用 RDS 产品默认值或名字相近的旧参数。

## 提前传输与实时回放

| 模式 | 主库 | 从库 |
|------|------|------|
| 大事务提前传输 | 事务执行期间读取符合条件的 Transaction Cache 文件 | 接收 BRR Cache 文件，完整事务仍可由普通复制线程回放 |
| DDL 实时回放 | 支持的 ALTER/重建操作超过耗时阈值后提前发送 | BRR Worker 提前执行，最后等待主库的提交或回滚结果 |
| 大事务实时回放 | `binlog_realtime_apply_long_trx_enabled=ON` 时发送所需依赖信息 | BRR Worker 提前回放符合条件的 Event，最后等待主库事务结果 |

提前传输、提前执行不等于提前提交。BRR 根据 GTID、依赖关系和主库结果完成或回滚提前执行的工作；它不会把异步复制变成同步复制，也不保证零延迟。未分配给 BRR Worker 的事务由普通复制路径处理。

## 前置条件

下文示例假设已有运行正常、数据一致的 GTID 主从复制：各节点 `server_id` 唯一，开启 Binlog，并使用 `gtid_mode=ON`、`enforce_gtid_consistency=ON` 和 `binlog_format=ROW`。在启用 BRR 前完成从库数据初始化和复制账号配置。

- 两端必须支持相同的 BRR 协议。使用 `SHOW GLOBAL STATUS LIKE 'brr_version'` 检查；当前实现返回 `3`。
- BRR 协商只支持名称为空的默认复制通道，不支持命名通道和 Group Replication 通道。
- 大事务提前传输需要具有预留头部空间的具名 Transaction Cache 文件，因此需要在主库开启 `binlog_cache_free_flush`；即使最终提交回退到普通 Group Commit，这一文件基础设施仍可供 BRR 使用。
- 事务 Cache 必须超过阈值并溢出到磁盘。开启 Binlog 事务压缩，或使用非事务性 Statement Cache 的事务，不走这一大事务路径。

## 开启大事务提前传输

在主库执行：

```sql
SET GLOBAL binlog_cache_free_flush = ON;
SET GLOBAL binlog_realtime_transmit_source_enabled = ON;

-- 示例阈值为 256 MiB，实际默认值为 512 GiB。
SET GLOBAL binlog_realtime_replica_long_transaction_limit_size = 268435456;
```

虽然名字包含 `replica`，`binlog_realtime_replica_long_transaction_limit_size` 实际控制的是**主库**的事务准入阈值。旧参数 `binlog_realtime_transmit_long_transaction_limit_size` 不会更新实际生效的阈值。使用未压缩事务（`binlog_transaction_compression=OFF`），并在修改主库配置后使用新事务验证。

在从库重连复制接收线程，使其协商 BRR：

```sql
STOP REPLICA;
SET GLOBAL binlog_realtime_transmit_replica_enabled = ON;
START REPLICA;
```

`binlog_realtime_apply_workers=0` 时，只开启传输而不创建专用实时回放 Worker。确认普通复制和 BRR 接收正常后，再启用实时回放。`SET GLOBAL` 不会自动跨服务重启保留配置；选定参数后应同步写入各节点配置文件或另行持久化。

## 开启实时回放

实时回放要求 GTID 和并行复制。默认复制通道还需要满足：`PRIVILEGE_CHECKS_USER=NULL`、`ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS=OFF`、`REQUIRE_TABLE_PRIMARY_KEY_CHECK=STREAM`，且没有复制过滤规则。BRR Worker 与 `replica_parallel_workers` 对应的普通复制 Worker 是两个独立线程池。

停止复制后配置从库：

```sql
STOP REPLICA;
SET GLOBAL replica_parallel_type = 'LOGICAL_CLOCK';
SET GLOBAL replica_parallel_workers = 4;
SET GLOBAL replica_preserve_commit_order = OFF;
SET GLOBAL binlog_realtime_apply_workers = 4;
CHANGE REPLICATION SOURCE TO REQUIRE_ROW_FORMAT = 1;
START REPLICA;
```

Worker 数量在 SQL 回放线程启动时读取，仅修改全局参数不会调整已经运行的 BRR Worker。如果此前已启用 BRR Worker，停止复制后应等 `replica_brr_worker_size` 归零再启动。线程未按预期创建时，应检查错误日志。

之后在主库开启所需的实时回放模式：

```sql
SET GLOBAL binlog_realtime_apply_ddl_source_enabled = ON;
SET GLOBAL binlog_realtime_apply_long_trx_enabled = ON;
```

DDL 传输使用 `binlog_realtime_ddl_time_limit`，默认 1000 毫秒，设为零表示不等待。范围是受支持的事务性 ALTER TABLE/重建路径，包括适用的 OPTIMIZE TABLE 操作。Instant DDL、非事务性 DDL、复制回放线程执行的 DDL，以及不写 Binlog 的 DDL，不进入该路径。外部 XA 事务不进行大事务实时回放。

`optimize_table_write_binlog_advance` 是独立选项，默认 `OFF`。开启后，OPTIMIZE 在表重建成功后、后续 ANALYZE 之前写入 Binlog，因此会改变重建完成后发生错误时的行为；启用 BRR 本身不要求开启它。

## 参数

下表参数均为全局、可动态赋值。接收端协商与 Worker 数量变更，需要按上文重启对应复制线程才会生效。字节范围包含上下限。

| 参数 | 设置位置 | 默认值 | 范围/含义 |
|------|----------|--------|-----------|
| `binlog_realtime_transmit_source_enabled` | 主库 | `OFF` | 开启 BRR 发送 |
| `binlog_realtime_transmit_replica_enabled` | 从库 | `OFF` | 接收线程连接时协商 BRR |
| `binlog_realtime_replica_long_transaction_limit_size` | 主库 | 512 GiB（`549755813888`） | 10 MiB 到 `ULLONG_MAX` 字节；事务 Cache 必须超过此值 |
| `binlog_realtime_transmit_read_block_size` | 主库 | 2 MiB（`2097152`） | 4096 到 `ULLONG_MAX` 字节；累计新增 Cache 数据达到阈值后 Flush 并更新可读位置 |
| `binlog_realtime_transmit_max_send_size` | 主库 | 2 MiB（`2097152`） | 1024 到 `ULLONG_MAX` 字节；每批发送预算，超过此值的单个 Event 不提前发送 |
| `binlog_realtime_apply_ddl_source_enabled` | 主库 | `OFF` | 开启符合条件的 DDL 发送 |
| `binlog_realtime_ddl_time_limit` | 主库 | 1000 毫秒 | 0 到 `UINT_MAX` 毫秒，零表示不等待 |
| `binlog_realtime_apply_long_trx_enabled` | 主库 | `OFF` | 发送大事务实时回放需要的元数据 |
| `binlog_realtime_apply_workers` | 从库 | `0` | 0 到 1024；SQL 回放线程启动时创建的 BRR Worker 数量 |
| `binlog_realtime_apply_worker_heartbeat_period` | 从库 | 200 毫秒 | 1 到 `UINT_MAX` 毫秒；Worker 轮询间隔 |
| `binlog_realtime_enable_log_message` | 两端 | `OFF` | 输出详细 BRR 错误日志信息 |
| `optimize_table_write_binlog_advance` | 执行 OPTIMIZE 的主库 | `OFF` | 在表重建成功后、ANALYZE 之前写 Binlog |

为兼容旧配置而保留的 `binlog_realtime_transmit_long_transaction_limit_size`（默认 512 GiB）和 `binlog_realtime_transmit_enable_log_message`（默认 `ON`）不是实际控制项，应使用表中生效的阈值和日志参数。

## 监控与排查

主库：

```sql
SHOW GLOBAL STATUS LIKE 'brr_version';
SHOW GLOBAL STATUS LIKE 'source_brr%';
SHOW GLOBAL STATUS LIKE 'source_sent_brr_events_count';
SELECT * FROM information_schema.BRR_SOURCE_TRX_INFO;
SELECT * FROM information_schema.BRR_SENDER_TRX_INFO;
```

从库：

```sql
SHOW REPLICA STATUS\G
SHOW GLOBAL STATUS LIKE 'replica_brr%';
SHOW GLOBAL STATUS LIKE 'replica_received_brr_events_count';
SELECT * FROM information_schema.BRR_REPLICA_INFO;
```

- `replica_brr_io_thread_count` 表示完成协商的接收线程数，`replica_brr_worker_size` 表示实际 BRR Worker 数量。
- 发送、接收 Event 计数反映传输活动；已分配、已提交和已回滚事务计数可用于区分实时回放和普通回退。
- `source_brr_exceed_max_send_size_count` 用于观察发送大小限制排除的 Event。
- Information Schema 表描述当前任务，空闲时可以没有记录。Cache/Worker 数量是当前值，Event/事务累计计数持续到服务进程结束；应比较观察窗口内的增量。
- 两端协议版本不一致时不会协商 BRR，应检查错误日志和普通复制状态。
- 大事务没有提前传输时，检查主库阈值、Cache 落盘、Free Flush Cache 设置、压缩及复制回放线程限制；已收到但未提前执行时，检查 Worker 数量和全部实时回放前置条件。

排查时可临时开启 `binlog_realtime_enable_log_message`，逐事务详细日志可能产生较大日志量。

## 使用边界与相关功能

- 本文配置面向 InnoDB 主从组合。其他引擎组合需要结合各自复制限制验证。
- DuckDB Multi-Trx Batch 要求 `replica_parallel_workers=0`，BRR 实时回放要求并行 Worker；不能原样混用 [DuckDB 批量复制部署配置](../duckdb/how-to-setup-duckdb-node-zh.md)和上文实时回放配置。
- 主库的复制回放线程不会发起这条提前传输路径，不能假设级联复制会在上游事务提交前继续向下游发送 BRR。
- BRR 复用 [Binlog Cache Free Flush](../binlog-cache-free-flush/binlog-cache-free-flush-zh.md) 的 Cache 文件基础设施。BRR 传输成功不代表提交时使用了 Free Flush 优化，事务准入检查仍然生效。本分支已移除此前针对额外 2PC 引擎的自动回退检查，当前 DuckDB Mode 行为及恢复注意事项见该指南。
- BRR Cache 文件占用磁盘，提前执行的 Worker 在主库提交前就可能消耗 CPU、I/O 和锁资源，应按并发源端工作量和普通复制负载规划从库容量。
- 停止接收线程或连接断开可能导致尚未完成的提前执行任务回滚，后续由普通复制和重连流程决定回放。不要手工删除 BRR Cache 文件。

关闭 BRR 时，先让主库停止产生新的提前执行任务，再停止从库复制并等待 BRR Worker 退出：

```sql
-- 主库
SET GLOBAL binlog_realtime_apply_ddl_source_enabled = OFF;
SET GLOBAL binlog_realtime_apply_long_trx_enabled = OFF;
SET GLOBAL binlog_realtime_transmit_source_enabled = OFF;

-- 从库
STOP REPLICA;
SET GLOBAL binlog_realtime_transmit_replica_enabled = OFF;
SET GLOBAL binlog_realtime_apply_workers = 0;
-- 确认 replica_brr_worker_size 归零后再启动。
START REPLICA;
```

按拓扑需求保留或恢复普通复制配置，核对 `SHOW REPLICA STATUS`，并同步修改持久化/启动配置。

实现参考：[参数定义](../../sql/sys_vars_ext.cc)、[主库默认值和 DDL 阈值](../../sql/rpl_binlog_sender_ext.h)、[接收端协商和 Worker 准入](../../sql/rpl_replica.cc)、[事务 Cache 准入](../../sql/binlog_ostream_ext.cc)。
