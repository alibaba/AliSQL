# Binlog Cache Free Flush

[ [AliSQL](../../README_zh.md) | [English](./binlog-cache-free-flush-en.md) | [中文](./binlog-cache-free-flush-zh.md) ]

Binlog Cache Free Flush 是大事务提交优化。事务 Binlog Cache 溢出到临时文件后，普通路径会在提交阶段把该文件再次复制到 Binlog。Free Flush 在临时文件中预留 Binlog 头部空间，提交时完成文件并把它重命名为下一个 Binlog 文件，从而避免第二次大文件复制。

本文描述 AliSQL 8.0.44 开源分支。阿里云 RDS MySQL 托管产品的信息参见[阿里云 RDS MySQL](#阿里云-rds-mysql)。

## 本分支的可用状态

> **本分支已经包含实现和参数，但在标准 DuckDB 构建中优化路径不生效，所有事务都会回退到普通 Binlog Group Commit。**

Free Flush 的崩溃恢复当前只支持 Binlog 加一个非 Binlog 2PC 存储引擎。标准构建开启 Binlog 后，即使 `duckdb_mode=NONE`，DuckDB 也会注册 2PC `prepare` 回调，因此 2PC 引擎数量无法通过 Free Flush 的安全检查。

这就是不再使用“受限”表述的原因：修改 `duckdb_mode` 或开启 Free Flush 参数，都不能让标准构建进入优化路径。只有没有额外注册 2PC 引擎的构建才可能生效，除非后续重新设计崩溃恢复协议以支持这类引擎组合。

回退是有意的安全行为。Free Flush 会先正常关闭旧 Binlog，再把临时文件变成新 Binlog。如果在这个间隙崩溃，多个非 Binlog 2PC 引擎并存时，现有恢复路径无法安全处理遗留的 Prepared Transaction。

## 在兼容构建中开启

只有确认构建中没有额外注册的 2PC 引擎后，才使用以下配置：

```sql
SET GLOBAL binlog_cache_free_flush_limit_size = 268435456;
SET GLOBAL binlog_cache_free_flush = ON;
```

两个参数均可动态修改。参数设置成功只代表完成配置，不代表某个事务实际进入了优化路径。

## 事务准入条件

在兼容构建中，单个事务只有通过所有检查才进入 Free Flush：

- `binlog_cache_free_flush=ON`。
- Transaction Binlog Cache 大于 `binlog_cache_free_flush_limit_size`。
- Transaction Cache 已写入磁盘，并具有非零的预留头部空间。
- Transaction Cache 已完成 Finalize，且不包含 Incident。
- Statement Binlog Cache 为空。
- Binlog 加密和 Cache 文件加密均未开启。
- Binlog 处于打开状态。
- 事务未修改 `mysql.gtid_executed`。
- 2PC 配置中只有 Binlog 和一个非 Binlog 存储引擎。

任一检查不满足时，该事务会使用普通 Binlog Group Commit，并且仍可正常提交。

## 参数

| 参数 | 作用域 | 默认值 | 范围 | 作用 |
|------|--------|--------|------|------|
| `binlog_cache_free_flush` | 全局，动态 | `OFF` | `ON`, `OFF` | 开启 Free Flush 准入检查 |
| `binlog_cache_free_flush_limit_size` | 全局，动态 | 256 MiB | 10 MiB 到 `ULLONG_MAX` 字节 | 进入 Free Flush 的最小事务 Binlog Cache 大小 |

开源版本的下限是 10 MiB。自建实例不能照搬 RDS 产品中的参数范围或 `loose_` 参数名。

## 运维建议

- 把普通 Group Commit 作为必须保留的回退路径。
- 自定义构建开启功能前，先确认实际注册的 2PC 引擎集合。
- 使用超过阈值的事务压测并观察实际 I/O；仅检查参数状态不能证明 Free Flush 已执行。
- Binlog 加密一定会触发回退，因为 Cache 临时文件与最终 Binlog 使用不同的文件密钥。
- 任何启用优化路径的构建，都应在准入测试中保留崩溃注入和重启恢复测试。

## 阿里云 RDS MySQL

阿里云 RDS MySQL 提供商业支持的 Free Flush 能力，并管理适用内核版本和产品参数。请参见官方[中文文档](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush)或[英文文档](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-cache-free-flush)。

RDS 页面使用 `loose_binlog_cache_free_flush*`，具有产品专用的阈值范围和支持版本。这些内容适用于 RDS 实例，不适用于本源码分支。RDS 已提供该优化，也不会改变前文所述标准开源构建中的 DuckDB 2PC 交互。
