# Binlog Cache Free Flush

[ [AliSQL](../../README_zh.md) | [English](./binlog-cache-free-flush-en.md) | [中文](./binlog-cache-free-flush-zh.md) ]

Binlog Cache Free Flush 是大事务提交优化。事务 Binlog Cache 溢出到临时文件后，普通路径会在提交阶段把该文件再次复制到 Binlog。Free Flush 在临时文件中预留 Binlog 头部空间，提交时完成文件并把它重命名为下一个 Binlog 文件，从而避免第二次大文件复制。

下文配置适用于自建 AliSQL 8.0.44。RDS MySQL 的参数名和版本要求不同，参见[阿里云 RDS MySQL](#阿里云-rds-mysql)。

## 本版本支持范围

本版本已经支持 InnoDB 大事务的 Free Flush，适用于 Binlog 加 InnoDB 的 2PC 组合。当前 DuckDB 集成会注册额外的 2PC 参与者，因此包含 DuckDB 的 AliSQL 使用普通 Binlog Group Commit，即使 `duckdb_mode=NONE` 也是如此。下一版本将支持 DuckDB 场景的大事务优化。

这个限制来自崩溃恢复。Free Flush 当前支持 Binlog 加一个非 Binlog 2PC 存储引擎。构建中包含 DuckDB 时，只要开启 Binlog，DuckDB 就会额外注册一个 2PC `prepare` 回调，与 `duckdb_mode` 的取值无关。

Free Flush 会先关闭旧 Binlog，再把临时文件重命名为新 Binlog。如果在两步之间发生崩溃，并且注册了多个非 Binlog 2PC 引擎，当前恢复代码无法安全处理遗留的 Prepared Transaction。在这种配置下使用普通 Group Commit 可以避开该恢复状态，事务正确性不受影响。

## 配置

只有服务端注册的 2PC 参与者为 Binlog 和 InnoDB 时，才开启 Free Flush：

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
- 除 InnoDB 外，没有注册其他 2PC 存储引擎。

任一检查不满足时，该事务会使用普通 Binlog Group Commit，并且仍可正常提交。

## 参数

| 参数 | 作用域 | 默认值 | 范围 | 作用 |
|------|--------|--------|------|------|
| `binlog_cache_free_flush` | 全局，动态 | `OFF` | `ON`, `OFF` | 开启 Free Flush 准入检查 |
| `binlog_cache_free_flush_limit_size` | 全局，动态 | 256 MiB | 10 MiB 到 `ULLONG_MAX` 字节 | 进入 Free Flush 的最小事务 Binlog Cache 大小 |

开源版本的下限是 10 MiB。自建实例不能照搬 RDS 产品中的参数范围或 `loose_` 参数名。

## 运维建议

- 保留普通 Group Commit 作为回退路径。
- 确认 InnoDB 是唯一注册的非 Binlog 2PC 引擎。
- 使用超过阈值的事务压测并观察实际 I/O；仅检查参数状态不能证明 Free Flush 已执行。
- Binlog 加密一定会触发回退，因为 Cache 临时文件与最终 Binlog 使用不同的文件密钥。
- 任何启用优化路径的构建，都应在准入测试中保留崩溃注入和重启恢复测试。

## 阿里云 RDS MySQL

RDS MySQL 也提供 Binlog Cache Free Flush。支持版本、参数名和参数范围参见官方[中文文档](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush)和[英文文档](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush)。

RDS 使用 `loose_binlog_cache_free_flush*`，并有单独的阈值范围；自建 AliSQL 不应直接照搬这些参数名和取值范围。
