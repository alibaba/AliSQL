# AliSQL 8.0.44 功能发布说明（2026-10-16）

[ [发布说明](../changes-in-alisql-8.0.44.md) | [English](./changes-in-alisql-8.0.44.2026-10-16-en.md) | [中文](./changes-in-alisql-8.0.44.2026-10-16-zh.md) ]

本次更新新增 Binlog 实时复制（BRR），升级内嵌 DuckDB 源码，并修复两项功能集成验证中发现的问题。MySQL 基础版本仍为 8.0.44。

## 更新概览

| 领域 | 更新 | 默认值 |
|------|------|--------|
| 复制 | BRR 提前传输，以及受支持 DDL 和大事务的可选实时回放 | 主从传输开关 `OFF`，BRR Worker 数量 `0` |
| 分析 | DuckDB 源码快照 `v1.5.2-dev736`，提交 `8a66223119b16000f3814fa7150cfa7370b63209` | `duckdb_mode=NONE` |
| DuckDB DDL | AUTO_INCREMENT 自增列支持 | `duckdb_support_auto_increment=OFF` |
| DuckDB Checkpoint | 后台自动 Checkpoint 与 Full Checkpoint 间隔 | 后台模式 `OFF`，间隔 `0` |

## Binlog 实时复制

BRR 在主库提交前传输符合条件的事务 Cache Event，并可在专用从库 Worker 中提前执行受支持的 DDL 和大事务。最终完成仍依赖主库事务结果和 GTID 依赖关系。它将主库执行、网络传输和从库回放重叠起来，但不提供同步复制或零延迟保证。

- 主从通过默认复制通道协商相同的 BRR 协议版本。
- 大事务提前传输要求主库 `binlog_cache_free_flush=ON`，存在符合条件的磁盘 Cache，且没有开启 Binlog 事务压缩。
- 实际生效的阈值参数是设置在主库的 `binlog_realtime_replica_long_transaction_limit_size`，默认 **512 GiB**，下限 10 MiB。
- 实时回放要求 GTID、并行复制、非零 `binlog_realtime_apply_workers`、`replica_preserve_commit_order=OFF`，以及文档列出的通道限制。
- DDL 提前传输的默认耗时阈值为 1000 毫秒，覆盖受支持的事务性 ALTER/重建路径；不包括 Instant DDL 和复制回放线程执行的 DDL。
- 新增 Information Schema 表 `BRR_SOURCE_TRX_INFO`、`BRR_SENDER_TRX_INFO`、`BRR_REPLICA_INFO`，并提供 BRR 状态计数器观察运行情况。
- `optimize_table_write_binlog_advance` 是独立选项，默认 `OFF`，用于将 OPTIMIZE 写 Binlog 的时机移到表重建成功之后、ANALYZE 之前。

启停流程、完整默认值、兼容旧参数、监控与回退行为参见 [BRR 使用指南](../binlog-realtime-replication/binlog-realtime-replication-zh.md)。

## DuckDB 更新

本次内嵌的是固定开发快照 `v1.5.2-dev736`，并非声称使用了上游稳定版 v1.5.2。升级包括集成 API 适配和优化器名称扩展，同时保留 AliSQL 的 `duckdb_disabled_optimizers` 空集合默认值。

### 新增功能

- 可选的 DuckDB AUTO_INCREMENT 自增列处理，覆盖字典、DDL 和复制路径。
- 可选的后台自动 Checkpoint，以及 Full Checkpoint 之间的间隔配置。
- `JSON_OBJECTAGG` 与更多时间、字符串 SQL 兼容处理。
- DuckDB 表的跨库 RENAME TABLE。

### 兼容性与可靠性

- 正确引用包含引号、反引号、反斜杠和嵌入 NUL 的列默认值、标识符及字面量。
- 修复已有数据时 ADD/MODIFY/CHANGE NOT NULL，以及 ADD COLUMN DEFAULT 后 SET NOT NULL 的处理。
- 修复生成列依赖、分区显式引擎转换，并对非分区表上的非法分区操作增加保护。
- 启动转换遇到 VIEW 元数据锁死锁时重试，并保留退出和错误处理。
- 处理 Appender 回滚异常，并在连续查询之间重置解析器错误位置。
- 修复原子跨引擎 DDL 失败时所需的 InnoDB FIL Cache 字符集处理。

### 新增参数

以下三个参数均为全局、动态参数：

| 参数 | 默认值 | 范围 | 作用 |
|------|--------|------|------|
| `duckdb_support_auto_increment` | `OFF` | `ON`, `OFF` | 开启 DuckDB 表的 AUTO_INCREMENT 支持 |
| `duckdb_enable_background_checkpoint` | `OFF` | `ON`, `OFF` | 在后台线程执行自动 Checkpoint |
| `duckdb_background_checkpoint_full_interval` | `0` | 0 到 `ULLONG_MAX` | 后台模式中两次 Full Checkpoint 之间的 Concurrent Checkpoint 数量；零表示始终 Full |

详细说明参见 [DuckDB 集成指南](../duckdb/duckdb-zh.md)和[参数参考](../duckdb/duckdb_variables-zh.md)。

## 集成修复

- 释放 Relay Log Reader 跳过的 BRR 元数据事件，修复内存泄漏。
- XA 启动恢复没有当前 THD 时，正确处理元数据锁获取。
- 未开启提前写 Binlog 时，多表 OPTIMIZE 部分成功仍保留正常 Binlog 和 GTID 生成。
- 完善 BRR 回归测试的同步、清理等待、事件计数预期和用例隔离。

## 升级与配置说明

- BRR、DuckDB 自增列和后台 Checkpoint 均需要显式启用，已有 AliSQL 默认值保留。
- DuckDB Multi-Trx Batch 要求 `replica_parallel_workers=0`，不能原样混用需要并行 Worker 的 BRR 实时回放配置。
- BRR 使用 Free Flush Cache 文件；本次集成移除了此前针对额外 2PC 引擎的自动回退检查，开启 DuckDB Mode 时仍选择专用提交路径。当前行为及恢复注意事项见 [Free Flush 指南](../binlog-cache-free-flush/binlog-cache-free-flush-zh.md)。
- 本次不包含 RDS 专用打包、发布元数据、私有子模块构建、回收站集成，以及源端已回退的精度超过 38 的 DECIMAL 功能。
- 之前的功能与发布历史参见 [2026-06-30 发布说明](./changes-in-alisql-8.0.44.2026-06-30-zh.md)。
