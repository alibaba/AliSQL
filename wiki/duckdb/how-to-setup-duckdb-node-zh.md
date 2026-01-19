# 如何构建和使用 AliSQL DuckDB 节点

## 引言

[DuckDB](https://github.com/duckdb/duckdb) 是一款为在线分析处理（OLAP）设计的嵌入式数据库，以其卓越的性能、高压缩率和易于集成的特性而著称。AliSQL 将 DuckDB 作为全新的分析存储引擎集成进来，让用户可以在熟悉的 MySQL 环境中，获得顶级的分析查询能力。

本章将介绍如何部署一个用于分析处理的 DuckDB 节点，主要包括以下两种场景：

1.  **构建一个独立的 DuckDB 节点**
2.  **构建一个 DuckDB 从节点**
> 本文提供的参数均为示例参数，请根据实际情况进行修改。参数的含义和默认值，请参考 [DuckDB in AliSQL 参数介绍](./duckdb_variables-zh.md)。

---

## 1. 构建一个独立的 DuckDB 节点

独立的 DuckDB 节点是您的主要分析数据存储。所有用户数据都将存入 DuckDB 引擎，InnoDB 仅用于保存系统表和元数据。您可以从一个全新的实例开始，也可以将现有的 InnoDB 实例快速转换为 DuckDB。

### 场景一: 从零开始构建一个全新的 DuckDB 实例

此场景适用于初始化一个全新的数据库实例，后续所有新建的表都将默认使用 DuckDB 引擎。

#### 步骤:

1.  **配置 `my.cnf`**
    ```ini
    [mysqld]
    # 1. 启用 DuckDB 引擎。
    # 必选项，用以启用 DuckDB 存储引擎。
    duckdb_mode=ON

    # 2. 强制将 InnoDB 引擎重定向到 DuckDB。
    # 推荐项，开启后用户创建 InnoDB 表或对 InnoDB 表执行 DDL 操作时会自动触发
    # 到 DuckDB引擎的转换，这可以简化操作，确保了所有用户数据都落入 DuckDB。
    force_innodb_to_duckdb=ON

    # 3. 配置资源
    # 根据您的服务器硬件，合理分配内存、线程和临时目录。0 表示自动配置。
    duckdb_memory_limit=0
    duckdb_threads=0
    duckdb_temp_directory=/path/to/duckdb_temp_dir
    ```

2.  **启动实例**
    使用上述的配置初始化并启动 AliSQL 实例。
    ```shell
    # 初始化数据目录
    mysqld --defaults-file=/etc/my.cnf  --initialize-insecure

    # 启动实例
    mysqld_safe --defaults-file=/etc/my.cnf &
    ```

3.  **验证**
    实例启动后，您可以正常创建表和数据库。虽然您可能没有显式指定引擎，但所有新表都将由 DuckDB 负责存储和管理。您可以开始导入数据并执行分析查询。
    ```sql
    -- 示例
    -- 1. 创建 DuckDB 表
    CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255)) ENGINE = DuckDB;

    -- 2. 将 DuckDB 表转换成 InnoDB 表
    ALTER TABLE t ENGINE = InnoDB;

    -- 3. 将 InnoDB 表转换成 DuckDB 表
    ALTER TABLE t ENGINE = DuckDB;

    -- 4. DML
    INSERT INTO t VALUES (1, 'John');
    SELECT * FROM t;
    UPDATE t SET name = 'Jane' WHERE id = 1;
    DELETE FROM t WHERE id = 1;

    -- 5.DDL
    ALTER TABLE t ADD COLUMN age INT;
    ALTER TABLE t RENAME TO t_new;
    ```

### 场景二: 从现有 InnoDB 实例一键转换

此场景适用于将已有的 InnoDB 实例整体迁移到 DuckDB 以获得分析性能的提升。

#### 步骤:

1.  **停止现有实例**
    确保您的MySQL 数据库实例已安全关闭。

2.  **配置 `my.cnf`**
    在配置文件中添加以下参数，以触发启动时自动转换。
    ```ini
    [mysqld]
    # 1. 启用 DuckDB 引擎。
    # 必选项，用以启用 DuckDB 存储引擎。
    duckdb_mode=ON

    # 2. 强制将 InnoDB 引擎重定向到 DuckDB。
    # 推荐项，开启后用户创建 InnoDB 表或对 InnoDB 表执行 DDL 操作时会自动触发
    # 到 DuckDB引擎的转换，这可以简化操作，确保了所有用户数据都落入 DuckDB。
    force_innodb_to_duckdb=ON

    # 3. 配置资源
    # 根据您的服务器硬件，合理分配内存、线程和临时目录。0 表示自动配置。
    duckdb_memory_limit=0
    duckdb_threads=0
    duckdb_temp_directory=/path/to/duckdb_temp_dir
    
    # 4. 开启启动时自动转换功能。
    # 推荐项，开启后，AliSQL 将在启动过程中，自动将所有 InnoDB 表转换成 DuckDB 表。
    # 若参数未开启，则需要在启动之后执行 ALTER TABLE ... ENGINE = DuckDB，手动将
    # InnoDB 表转换成 DuckDB 表。
    duckdb_convert_all_at_startup=ON

    # 5. 配置启动自动转换过程并行转表的数量。
    # 可选项，增加线程数可以显著加快大批量表的转换速度，可根据服务器硬件来配置。
    duckdb_convert_all_at_startup_threads=32

    # 6. 配置启动自动转换过程忽略错误
    # 可选项，如果您担心某些表可能因兼容性问题转换失败而导致启动中断，可以开启此选项。
    duckdb_convert_all_at_startup_ignore_error=ON
    ```

3.  **启动实例**
    使用 AliSQL 启动数据库实例。

4.  **验证**
    当实例成功启动并可以接受连接时，您可以查询实例转换引擎的状态。当所有目标数据均已迁移至 DuckDB 时，即可体验 DuckDB 的高性能分析查询。
    ```sql
    -- 查询数据库实例转换引擎的状态。
    -- "EMPTY" - 未开始
    -- "INIT" - 初始化
    -- "CHECKING" - 检查元数据
    -- "CHECK_FAILED" - 检查元数据失败
    -- "CONVERTING" - 正在转换
    -- "CONVERT_FAILED" - 转换失败
    -- "FINISHED" - 转换完成
    SHOW GLOABL STATUS LIKE 'DuckDB_convert_stage_at_startup';

    -- 查看数据库中的 DuckDB 表
    SELECT * FROM information_schema.tables WHERE ENGINE = 'DuckDB';
    ```

> **注意事项**
> 1. 当开启自动转换功能时，由于实例在后台执行全量数据的读取、转换和写入，启动过程会耗费硬件资源。您可以通过观察错误日志（error log）来监控转换进度和可能出现的错误。
> 2. 数据转换过程会占用额外的磁盘空间，请确保磁盘空间 ≥ 原始数据大小。
> 3. 转换前请对原 InnoDB 数据库进行备份，建议在备份集数据上进行 DuckDB 引擎的转换。
> 4. 5.7及更低版本的 mysql 请先升级到 8.0。
> 5. 非 8.0.44 的 MySQL 在启动后将自动升级至 AliSQL 8.0.44，建议在使用 AliSQL 8.0.44启动前确保数据库完成 clean shutdown。
---

## 2. 构建一个 DuckDB 从节点（HTAP）

将 DuckDB 节点作为主库（Source）的从库（Replica）是一个经典的读写分离和 AP/TP 混合负载场景。主库处理在线事务（OLTP），并将数据通过 Binlog 同步到 DuckDB 从节点，由从节点专门负责复杂的分析查询（OLAP）。

得益于专门的 DML 优化，DuckDB 从节点的回放性能极高，甚至可以超越 InnoDB，轻松实现无延迟复制。

#### 步骤:

1.  **准备主库**
    确保您的主库已开启 Binlog（格式为 `ROW`）和 GTID。这是所有主从复制的基础。

2.  **配置从节点的 `my.cnf`**
    在从节点的配置文件中，除了启用 DuckDB，还需要开启一系列为复制和写入性能优化的参数。
    ```ini
    [mysqld]
    # 1. 启用 DuckDB 引擎。
    # 必选项，用以启用 DuckDB 存储引擎。
    duckdb_mode=ON

    # 2. 开启强制主键要求
    # 作为从库，为确保数据能正确按行应用，必须要求表有主键。
    duckdb_require_primary_key=ON

    # 3. 强制将 InnoDB 引擎重定向到 DuckDB。
    # 推荐项，开启后用户创建 InnoDB 表或对 InnoDB 表执行 DDL 操作时会自动触发
    # 到 DuckDB引擎的转换，这可以简化操作，确保了所有用户数据都落入 DuckDB。
    force_innodb_to_duckdb=ON

    # 4. 开启 DML 攒批处理
    # 推荐项，将多个 DML 操作合并提交，提升写入性能。
    dml_in_batch=ON
    update_modified_column_only=ON

    # 5. 开启跨事务批量提交
    # 推荐项，将多个事务聚合成一个大批次提交，极大提升吞吐量。
    duckdb_multi_trx_in_batch=ON
    
    # 6. 调整批量提交的策略。
    # 可选项，用于控制攒批的大小。
    duckdb_multi_trx_timeout=5000
    duckdb_multi_trx_max_batch_length=268435456
    ```

3.  **初始化 DuckDB 节点**
    在从节点上启动 AliSQL 实例。此操作可参考：从现有 InnoDB 实例一键转换

4.  **建立主从关系**
    在从节点上，执行以下 SQL 命令：
    ```sql
    -- 指向主库的正确位置和 Binlog 信息
    CHANGE MASTER TO
      MASTER_HOST='your-master-ip',
      MASTER_USER='repl',
      MASTER_PASSWORD='your-password',
      MASTER_AUTO_POSITION=1;
    
    -- 启动复制
    START SLAVE;
    ```

5.  **完成**
    复制链路建立后，主库的变更会以极高的性能实时同步到 DuckDB 从节点。您现在可以将所有分析和报表类的复杂查询都路由到这个从节点上，而不会影响主库的事务处理性能。
