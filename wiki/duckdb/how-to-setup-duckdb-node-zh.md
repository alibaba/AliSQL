# 快速部署 AliSQL DuckDB 节点
[ [How to setup DuckDB node in AliSQL](./how-to-setup-duckdb-node-en.md) | [快速部署 AliSQL DuckDB 节点](./how-to-setup-duckdb-node-zh.md) ]

## 概述

AliSQL 集成 DuckDB 作为分析型存储引擎。典型用法是：

- **用户数据**：存入 **DuckDB 引擎**
- **系统表与元数据**：仍由 **InnoDB** 保存（例如 `mysql.*`、数据字典等）

你可以：

1. 从零初始化一个将用户 InnoDB 表 DDL 自动转为 DuckDB 的新实例。
2. 在启动阶段将现有 **InnoDB 实例转换** 为 DuckDB。
3. 构建 **DuckDB 从节点（HTAP/读写分离）**，从主库复制数据用于分析。

---

## 1. 从零开始构建 DuckDB 实例（推荐新建实例）

适用于：初始化一个全新实例，后续用户表 DDL 自动转为 DuckDB。

### 1.1 安装 AliSQL 8.0.44

#### 方式 A：从源码编译安装
```bash
git clone https://github.com/alibaba/AliSQL.git
cd AliSQL

# Build (release)
sh build.sh -t release -d /opt/alisql

# Install
make install
```

#### 方式 B：使用 RPM 安装包
```bash
# 示例安装 x86 架构下 centos 8 的 AliSQL 8.0.44
wget https://github.com/alibaba/AliSQL/releases/download/AliSQL-8.0.44-1/alisql-8.0.44-1.el8.x86_64.rpm
rpm -ivh alisql-8.0.44-1.el8.x86_64.rpm
```

---

### 1.2 初始化目录与生成配置文件脚本

> 说明：脚本要求传入**绝对路径**，并生成目录结构与 `alisql.cnf`。

```bash
cat > init_alisql_dir.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./init_alisql_dir.sh <absolute_path>
# Example:
#   ./init_alisql_dir.sh /root/alisql_8044

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <absolute_path>"
  exit 1
fi

DIR="$1"
if [[ "$DIR" != /* ]]; then
  echo "Error: Argument must be an ABSOLUTE PATH (starting with /)."
  echo "Current input: $DIR"
  exit 1
fi

DIR="${DIR%/}"
echo "Initializing AliSQL directories at: $DIR"

mkdir -p \
  "$DIR/data/dbs" \
  "$DIR/data/mysql" \
  "$DIR/run" \
  "$DIR/log/mysql" \
  "$DIR/tmp"

cat > "$DIR/alisql.cnf" <<EOF2
[mysqld]
# --- Paths ---
datadir = ${DIR}/data/dbs
socket  = ${DIR}/run/mysql.sock
pid-file = ${DIR}/run/mysqld.pid
tmpdir  = ${DIR}/tmp

# --- Basic ---
character_set_server = utf8mb4
skip_ssl
lower_case_table_names = 1
core-file

# --- Logs ---
log-error = ${DIR}/log/mysql/error.log

# Binary logging (optional; enable if you need replication/point-in-time recovery)
log-bin = ${DIR}/log/mysql/mysql-bin
binlog_format = ROW

# --- InnoDB (system tables / metadata) ---
innodb_data_home_dir = ${DIR}/data/mysql
innodb_log_group_home_dir = ${DIR}/data/mysql

# --- DuckDB Storage Engine ---
duckdb_mode=ON
force_innodb_to_duckdb=ON

# DuckDB resources (0 = auto)
duckdb_memory_limit=2147483648
duckdb_threads=0
duckdb_temp_directory=${DIR}/tmp
EOF2

echo "OK: Directory structure created."
echo "Config file: $DIR/alisql.cnf"
EOF

chmod +x init_alisql_dir.sh
```

> 更多配置参数可参考：
> - MySQL 8.0 官方文档：https://dev.mysql.com/doc/refman/8.0/en/
> - DuckDB 参数参考： [AliSQL DuckDB 参数](./duckdb_variables-zh.md)

---

### 1.3 初始化并启动实例

```bash
# Example: store all data under $HOME/alisql_8044
./init_alisql_dir.sh $HOME/alisql_8044

# Initialize
/opt/alisql/bin/mysqld --defaults-file=$HOME/alisql_8044/alisql.cnf --initialize-insecure

# Start, the port is 3306 by default
/opt/alisql/bin/mysqld_safe --defaults-file=$HOME/alisql_8044/alisql.cnf &
```

---

### 1.4 验证与使用

> 即使不显式写 `ENGINE=DuckDB`，当 `force_innodb_to_duckdb=ON` 时，用户侧创建/变更 InnoDB 表将被自动转换为 DuckDB 引擎。

```sql
CREATE DATABASE test;
USE test;

-- 创建 DuckDB 表
CREATE TABLE t (
  id INT PRIMARY KEY,
  name VARCHAR(255)
) ENGINE = DuckDB;

-- 先临时关闭强制转换，确保 DuckDB 到 InnoDB 的转换真实发生。
SET GLOBAL force_innodb_to_duckdb = OFF;
ALTER TABLE t ENGINE = InnoDB;
ALTER TABLE t ENGINE = DuckDB;
SET GLOBAL force_innodb_to_duckdb = ON;

-- DML
INSERT INTO t VALUES (1, 'John');
SELECT * FROM t;
UPDATE t SET name = 'Jane' WHERE id = 1;
DELETE FROM t WHERE id = 1;

-- DDL
ALTER TABLE t ADD COLUMN age INT;
ALTER TABLE t RENAME TO t_new;
```

---

## 2. 在启动阶段将现有 InnoDB 实例转换为 DuckDB

适用于：希望将历史数据整体迁移到 DuckDB 以提升分析性能。

### 步骤

1. **停止现有实例**
确保 MySQL 实例已 clean shutdown。

2. **在 `my.cnf` 增加参数**
```ini
[mysqld]
duckdb_mode=ON
force_innodb_to_duckdb=ON

duckdb_memory_limit=2147483648
duckdb_threads=0
duckdb_temp_directory=/path/to/duckdb_temp_dir

# Convert all InnoDB tables at startup
duckdb_convert_all_at_startup=ON
duckdb_convert_all_at_startup_threads=32
duckdb_convert_all_at_startup_ignore_error=OFF
```

3. **启动实例**

4. **验证转换状态与结果**
```sql
-- 检查转换阶段
SHOW GLOBAL STATUS LIKE 'DuckDB_convert_stage_at_startup';

-- 查询 DuckDB 表
SELECT table_schema, table_name, engine
FROM information_schema.tables
WHERE engine = 'DuckDB';
```

### 注意事项

1. 启动自动转换会大量读写数据并占用 CPU/IO；建议通过 error log 观察进度与报错。
2. 首次迁移应保持 `duckdb_convert_all_at_startup_ignore_error=OFF`，让单表失败终止转换。如果确需开启，实例可能在仅转换部分用户表后启动，投入使用前必须逐表核对。
3. 转换过程中可能需要额外磁盘空间（建议预留 ≥ 原始数据大小）。
4. 转换前务必备份（推荐使用备份集拉起 DuckDB 节点）。
5. MySQL 5.7 及以下请先升级到 8.0。
6. 非 8.0.44 的实例启动后可能触发升级流程，建议先确保 clean shutdown。

---

## 3. 构建 DuckDB 从节点（HTAP / 读写分离）

适用于：主库（InnoDB/OLTP）负责事务，从库（DuckDB/AP）负责复杂分析；通过 binlog 复制同步数据。

### 步骤

1. **准备主库**

- 开启 binlog
- `binlog_format=ROW`
- 配置非零且唯一的 `server_id`（例如 `server_id=1`）
- 使用 `gtid_mode=ON` 和 `enforce_gtid_consistency=ON` 开启 GTID

2. **配置从节点 `my.cnf`**

```ini
[mysqld]
server_id=2
gtid_mode=ON
enforce_gtid_consistency=ON
duckdb_mode=ON
duckdb_require_primary_key=ON
force_innodb_to_duckdb=ON

# Multi-Trx Batch 与并行复制 Worker 不兼容。
replica_parallel_workers=0

duckdb_dml_in_batch=ON
duckdb_update_modified_column_only=ON
duckdb_multi_trx_in_batch=ON
duckdb_multi_trx_timeout=5000
duckdb_multi_trx_max_batch_length=268435456
```

从节点的 `server_id` 必须与主库及复制拓扑中的其他服务不同。

3. **准备存量数据与 GTID 状态**

主库已有数据时，必须先基于同一时点的一致性物理备份或逻辑备份准备从节点，恢复后的数据与 `gtid_executed`/`gtid_purged` 必须对应同一个主库快照。使用物理备份时，先恢复 InnoDB 数据，再按第 2 节执行启动转换；使用逻辑备份时，将备份导入 DuckDB 节点，并恢复备份工具记录的 GTID 元数据。继续操作前应核对行数与 GTID 状态。

只有主库为空，或者仍保留从起始位置开始的全部所需 Binlog 时，才能直接使用第 1 节的空实例。否则 Auto Position 会因 `ER_SOURCE_HAS_PURGED_REQUIRED_GTIDS` 停止，应重新使用备份构建从节点。

4. **建立主从复制**
```sql
CHANGE MASTER TO
  MASTER_HOST='your-master-ip',
  MASTER_USER='repl',
  MASTER_PASSWORD='your-password',
  MASTER_AUTO_POSITION=1;

START REPLICA;
```

确认 `SHOW REPLICA STATUS\G` 中 I/O 和 SQL 线程均无报错后，再将查询流量路由到该节点。

5. **完成**
复制建立后，将分析/报表查询路由到 DuckDB 从节点。

## RDS 托管方案

阿里云 RDS MySQL 提供托管的 DuckDB 分析主实例和分析只读实例产品，其开通流程、产品拓扑、数据同步和适用条件属于 RDS 专属能力。使用托管方案时，请以官方[中文文档](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance)或[英文文档](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance)为准，不应沿用本文的自建初始化流程。
