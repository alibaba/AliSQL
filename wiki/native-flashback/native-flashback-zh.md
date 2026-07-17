# Native Flashback

[ [AliSQL](../../README_zh.md) | [English](./native-flashback-en.md) | [中文](./native-flashback-zh.md) ]

Native Flashback 允许 InnoDB 查询通过 `AS OF TIMESTAMP` 读取保留的历史数据。它基于快照记录和 Undo 构造一致性历史读视图，适合在误更新、误删除后快速核查或恢复数据，无需先在其他实例上恢复备份并重放 Binlog。

本文描述 AliSQL 8.0.44 开源分支。阿里云 RDS MySQL 托管产品的信息参见[阿里云 RDS MySQL](#阿里云-rds-mysql)。

## 工作原理

AliSQL 周期性记录 InnoDB 快照，并保留重建历史行版本所需的 Undo。`AS OF TIMESTAMP` 查询从快照历史中选择一个读视图，然后以该视图执行 InnoDB 一致性读。

请求时间与实际选中的快照时间不一定完全相同。`innodb_rds_flashback_allow_gap` 限制两者允许的最大差值，`innodb_rds_flashback_print_warning` 控制使用非精确快照时是否输出告警。

## 开启 Native Flashback

生成快照必须**同时**满足以下两个条件：

- `innodb_rds_flashback_task_enabled=ON`
- `innodb_undo_retention>0`

服务启动时，只要上述任一参数开启，就会创建后台线程。如果启动时任务关闭且保留时间为零，之后再动态开启两个参数也不会补建缺失的线程。因此应在启动前同时配置：

```ini
[mysqld]
innodb_rds_flashback_task_enabled=ON
innodb_undo_retention=3600
innodb_rds_flashback_interval=1
```

`innodb_rds_flashback_enabled=ON` 是查询入口，默认已经开启；它本身不会生成快照，也不会保留 Undo。即使其他参数已开启，`innodb_rds_flashback_task_stop_all=ON` 也会暂停快照创建和历史清理。

启动后检查实际参数：

```sql
SHOW GLOBAL VARIABLES LIKE 'innodb_rds_flashback%';
SHOW GLOBAL VARIABLES LIKE 'innodb_undo_retention';
```

执行历史查询前，应等待快照历史覆盖目标时间。

## 查询和恢复历史数据

```sql
SELECT id, status
FROM orders AS OF TIMESTAMP DATE_SUB(NOW(), INTERVAL 5 MINUTE)
WHERE customer_id = 1001;
```

时间表达式在一次语句执行中必须是常量，并且结果必须是时间值。

恢复数据时，先把历史结果写入独立表：

```sql
CREATE TABLE recovered_orders LIKE orders;

INSERT INTO recovered_orders
SELECT *
FROM orders AS OF TIMESTAMP '2026-07-16 10:00:00';
```

替换线上数据前，应核对行数和业务约束。执行 `RENAME TABLE` 前停止应用读写，并保留原表作为备份，直到恢复结果验证完成。

## 检查快照历史

| 存储过程 | 返回结果或操作 |
|----------|----------------|
| `CALL dbms_admin.analyze_flashback_snapshots()` | 返回最早时间、最晚时间，以及 InnoDB 报告的 Undo 总空间用量（MiB） |
| `CALL dbms_admin.show_flashback_snapshots(ts, order, limit)` | 列出快照事务 ID、时间和读视图；`order` 为 `ASC` 或 `DESC`；需要 `SUPER` 权限 |
| `CALL dbms_admin.del_flashback_snapshots(ts, order)` | 删除匹配的快照历史；`order` 为 `ASC` 或 `DESC`；需要 `SUPER` 权限 |

执行恢复前，可通过分析过程确认目标时间是否位于当前保留窗口内。

## 参数

以下参数均为 InnoDB 全局动态参数。为了确保后台线程被创建，仍需按前文要求在启动前配置。

| 参数 | 默认值 | 范围或单位 | 作用 |
|------|--------|------------|------|
| `innodb_rds_flashback_task_enabled` | `OFF` | 布尔值 | 参与快照生成；同时要求 Undo 保留时间非零 |
| `innodb_rds_flashback_task_stop_all` | `OFF` | 布尔值 | 暂停所有快照历史任务 |
| `innodb_rds_flashback_interval` | `1` | 1 到 86400 秒 | 快照生成间隔 |
| `innodb_rds_flashback_enabled` | `ON` | 布尔值 | 允许 `AS OF TIMESTAMP` 查询 |
| `innodb_rds_flashback_allow_gap` | `30` | 0 到 `UINT32_MAX` 分钟 | 请求时间与选中快照之间允许的最大差值 |
| `innodb_rds_flashback_print_warning` | `ON` | 布尔值 | 选中非精确快照时输出告警 |
| `innodb_undo_retention` | `0` | 0 到 `UINT32_MAX` 秒 | Undo 保留目标；生成快照也要求该值非零 |
| `innodb_undo_space_supremum_size` | `10240` | 0 到 `UINT32_MAX` MiB | 保留清理使用的 Undo 空间上限 |
| `innodb_undo_space_reserved_size` | `0` | 0 到 `UINT32_MAX` MiB | Undo 预留空间阈值 |

## 使用边界和运维建议

- `AS OF TIMESTAMP` 仅支持 InnoDB 基表的 `SELECT` 表引用。
- 不支持临时表、视图、`FOR UPDATE` 和 `LOCK IN SHARE MODE`。
- 请求时间超出保留历史时返回 `ER_SNAPSHOT_OUT_OF_RANGE`。
- 改变可见主键记录的 DDL 可能使旧快照不兼容，并返回 `ER_FLASHBACK_PK_INVISIBLE`；不保证可跨任意 DDL 读取历史数据。
- Undo 保留时间是目标值，不是无条件保证。空间阈值和工作负载压力可能缩短实际可查询窗口。
- 延长 Undo 保留会增加空间占用和 Purge 压力，应使用接近生产的负载观察 Undo 增长和查询延迟。
- Flashback 用于定向恢复，不能替代备份、Binlog 和经过演练的容灾方案。

## 阿里云 RDS MySQL

阿里云 RDS MySQL 将 Native Flashback 产品化，并提供托管内核发布、产品适用条件和保留策略管理。请参见官方[中文文档](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/native-flashback)或[英文文档](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/native-flashback)。

RDS 的支持版本、控制台操作、服务生命周期和参数策略以官方产品文档为准。自建本仓库版本时，应以本指南中的启动条件、默认值、范围和 SQL 边界为准；RDS 中的同名或相近配置不会改变开源代码的实际行为。
