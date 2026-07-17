# Persist Binlog Into Redo V2

[ [AliSQL](../../README_zh.md) | [English](./binlog-in-redo-en.md) | [中文](./binlog-in-redo-zh.md) ]

Persist Binlog Into Redo V2 也称 Binlog in Redo。它在事务提交时把符合条件的 Binlog Event 写入 InnoDB Redo，再由后台写线程和同步线程持久化 Binlog 文件。如果崩溃时 Binlog 尾部落后于已经提交的 Redo，恢复阶段会先从 Redo 重建缺失的 Binlog 尾部，再继续正常的 Binlog 恢复。

下文配置适用于自建 AliSQL 8.0.44。RDS MySQL 的支持版本和参数参见[阿里云 RDS MySQL](#阿里云-rds-mysql)。

## 优化目标

在普通持久化配置下，前台提交可能需要同时同步 InnoDB Redo 和 Binlog。Binlog in Redo 把符合条件的 Binlog 数据纳入 Redo 的持久化边界，并把 Binlog 写入和同步工作移出前台提交路径。不符合条件的事务继续使用普通 Binlog Group Commit。

## 配置

在服务启动前配置：

```ini
[mysqld]
sync_binlog=1
innodb_flush_log_at_trx_commit=1
binlog_order_commits=OFF
replica_preserve_commit_order=OFF
persist_binlog_to_redo=ON
```

当前实现只有在 `sync_binlog=1`、`binlog_order_commits=OFF` 和 `replica_preserve_commit_order=OFF` 时才会选择 Binlog in Redo。Clone 操作临时要求提交顺序时也会回退。

`innodb_flush_log_at_trx_commit=1` 不是代码中的启用检查，但它是推荐的持久化基线，因为符合条件的 Binlog Event 依赖同步落盘的 Redo 完成崩溃恢复。

启动后检查实际配置：

```sql
SHOW GLOBAL VARIABLES WHERE Variable_name IN (
  'persist_binlog_to_redo',
  'persist_binlog_to_redo_size_limit',
  'sync_binlog',
  'binlog_order_commits',
  'replica_preserve_commit_order'
);
```

## 事务准入和回退

单个事务只有满足所有相关检查时才使用 Binlog in Redo：

- Redo 可写，且 Binlog 配置为 `sync_binlog=1`。
- 当前不要求提交顺序。
- 事务只修改事务型表。
- Statement Binlog Cache 为空。
- Transaction Binlog Cache 不超过 `persist_binlog_to_redo_size_limit`。
- 当前 2PC 配置中只有一个非 Binlog 存储引擎。

标准 AliSQL 构建在 `duckdb_mode=NONE` 时可以使用 Binlog in Redo：启用检查会扣除未激活的 DuckDB 2PC 注册。DuckDB 模式处于激活状态时，开启 Binlog in Redo 会被拒绝。

某个事务不满足准入条件时，会使用普通 Binlog Group Commit；其他符合条件的事务仍可使用 Binlog in Redo。

## 参数

| 参数 | 作用域 | 默认值 | 范围 | 作用 |
|------|--------|--------|------|------|
| `persist_binlog_to_redo` | 全局，动态 | `OFF` | `ON`, `OFF` | 开启 Binlog in Redo V2 |
| `persist_binlog_to_redo_size_limit` | 全局，动态 | 1 MiB | 0 到 10 MiB | 可进入优化路径的事务 Binlog Cache 上限 |
| `sync_binlog_interval` | 全局，动态 | 50000 us | 1 到 100000000 us | 后台同步 Binlog 的时间间隔 |
| `binlog_buffer_size` | 全局，只读 | 20 MiB | 20 MiB 到 1 GiB | 异步 Binlog 环形缓冲区大小；需在启动时配置 |
| `wait_binlog_flush` | 全局，动态 | `ON` | `ON`, `OFF` | 提交返回前等待对应 Binlog 文件写入 |
| `binlog_group_delay` | 全局，动态 | 100 ns | 0 到 1000000000 ns | Leader 收集更大提交组时的等待时间 |
| `binlog_group_delay_running_threads` | 全局，动态 | `100` | 0 到 100000 | 启用 Group Delay 的最小运行线程数 |

`wait_binlog_flush=ON` 等待对应数据写入 Binlog 文件，但不等同于等待 Binlog 文件完成同步。符合条件的提交依赖已同步的 Redo 提供崩溃恢复。调整该参数会改变提交可见性和延迟行为，应结合目标持久化策略进行压测。

`binlog_group_delay` 只有在线程数达到配置阈值时才生效。增大延迟可能扩大提交组，但也会增加提交时延。

## 容量规划和运维

- `persist_binlog_to_redo_size_limit` 应覆盖常见事务，同时避免单个事务占用过多 Redo 和异步缓冲区。
- `binlog_buffer_size` 是只读参数，必须在启动前按峰值准入流量而非平均吞吐量配置。
- 同时监控 Redo 压力和 Binlog 写延迟；把工作移出前台路径并不会消除底层 I/O。
- 保留普通 Group Commit 路径，并测试包含非事务语句和超过大小上限事务的混合负载。
- 使用与生产一致的持久化配置验证崩溃恢复和复制。

## 阿里云 RDS MySQL

RDS MySQL 也支持 Binlog in Redo。支持版本、复制要求、备份行为、参数和性能数据参见官方[中文文档](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/binlog-in-redo)和[英文文档](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/binlog-in-redo)。

这些内容属于 RDS 产品配置。自建版本应使用本页的前置条件和参数，不要直接照搬 RDS 配置。
