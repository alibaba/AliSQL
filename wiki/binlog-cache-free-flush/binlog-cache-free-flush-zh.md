# Binlog Cache Free Flush

[ [AliSQL](../../README_zh.md) | [English](./binlog-cache-free-flush-en.md) | [中文](./binlog-cache-free-flush-zh.md) ]

Binlog Cache Free Flush 是大事务提交优化。事务 Binlog Cache 溢出到临时文件后，普通路径会在提交阶段把该文件再次复制到 Binlog。Free Flush 在临时文件中预留 Binlog 头部空间，提交时完成文件并把它重命名为下一个 Binlog 文件，从而避免第二次大文件复制。

下文配置适用于自建 AliSQL 8.0.44。RDS MySQL 的参数名和版本要求不同，参见[阿里云 RDS MySQL](#阿里云-rds-mysql)。

## 本版本支持范围

Free Flush 面向 DuckDB Mode 未开启时符合条件的 InnoDB 大事务。BRR 集成移除了此前仅因注册了额外非 Binlog 2PC 引擎就拒绝 Free Flush 的检查。因此，包含 DuckDB 的构建在 `duckdb_mode=NONE` 时仍可能进入 Free Flush，不能再依据引擎注册情况判断一定回退。

开启 DuckDB Mode 后，Binlog 提交入口会先选择专用 DuckDB 提交路径，不再进入后面的 Free Flush 准入检查；开启 `binlog_cache_free_flush` 不会将该路径变成 Free Flush 提交。

Free Flush 会先关闭旧 Binlog，再把 Cache 文件重命名为新 Binlog。崩溃重启验证需要覆盖这两步之间的窗口及实际注册的引擎组合，不能将已移除的引擎数量检查当作仍然存在的恢复保护。

## 配置

配置 Cache 文件基础设施及 InnoDB 事务大小阈值：

```sql
SET GLOBAL binlog_cache_free_flush_limit_size = 268435456;
SET GLOBAL binlog_cache_free_flush = ON;
```

两个参数均可动态修改。参数设置成功只代表完成配置，不代表某个事务实际进入了优化路径。

## 事务准入条件

在符合要求的构建中，单个事务只有通过所有检查才进入 Free Flush：

- `binlog_cache_free_flush=ON`。
- Transaction Binlog Cache 大于 `binlog_cache_free_flush_limit_size`。
- Transaction Cache 已写入磁盘，并具有非零的预留头部空间。
- Transaction Cache 已完成 Finalize，且不包含 Incident。
- Statement Binlog Cache 为空。
- Binlog 加密和 Cache 文件加密均未开启。
- Binlog 处于打开状态。
- 事务未修改 `mysql.gtid_executed`。
- 提交入口没有选择专用的 DuckDB Mode 路径。

未进入 Free Flush 的事务继续使用配置决定的提交路径：已选中的专用 DuckDB 路径，或者普通 Group Commit/Binlog in Redo 路径。

## 参数

| 参数 | 作用域 | 默认值 | 范围 | 作用 |
|------|--------|--------|------|------|
| `binlog_cache_free_flush` | 全局，动态 | `OFF` | `ON`, `OFF` | 开启 Free Flush 准入检查 |
| `binlog_cache_free_flush_limit_size` | 全局，动态 | 256 MiB | 10 MiB 到 `ULLONG_MAX` 字节 | 进入 Free Flush 的最小事务 Binlog Cache 大小 |

开源版本的下限是 10 MiB。自建实例不能照搬 RDS 产品中的参数范围或 `loose_` 参数名。

## 运维建议

### 与 Binlog 实时复制配合

[BRR 大事务提前传输](../binlog-realtime-replication/binlog-realtime-replication-zh.md)也需要 `binlog_cache_free_flush=ON` 提供的具名 Cache 文件和预留头部空间。因此，即使事务不满足 Free Flush 提交检查，BRR 仍需要在主库开启这一设置。为 BRR 启用 Cache 文件基础设施，不代表提交时满足重命名优化路径的准入条件。

### 提交路径检查

- 保留普通 Group Commit 作为回退路径。
- 检查当前 DuckDB Mode；本版本不能只依据引擎注册情况排除 Free Flush。
- 使用超过阈值的事务压测并观察实际 I/O；仅检查参数状态不能证明 Free Flush 已执行。
- Binlog 加密一定会触发回退，因为 Cache 临时文件与最终 Binlog 使用不同的文件密钥。
- 任何启用优化路径的构建，都应在准入测试中保留崩溃注入和重启恢复测试。

## 阿里云 RDS MySQL

RDS MySQL 也提供 Binlog Cache Free Flush。支持版本、参数名和参数范围参见官方[中文文档](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush)和[英文文档](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush)。

RDS 使用 `loose_binlog_cache_free_flush*`，并有单独的阈值范围；自建 AliSQL 不应直接照搬这些参数名和取值范围。
