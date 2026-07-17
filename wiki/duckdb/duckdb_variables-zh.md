# AliSQL 中 DuckDB 引擎相关参数

[ [DuckDB Engine Variables in AliSQL](./duckdb_variables-en.md) | [AliSQL DuckDB 引擎参数](./duckdb_variables-zh.md) ]

> 本文记录自建 AliSQL 源码分支的参数。阿里云 RDS MySQL DuckDB 分析实例具有产品专属的适用条件和托管配置，RDS 实例请参见官方[中文文档](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance)或[英文文档](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance)。

## AliSQL 8.0.44 DuckDB 增强新增参数

| 参数 | 作用域 | 默认值 | 作用 |
|------|--------|--------|------|
| `duckdb_sql_normalization` | GLOBAL, SESSION | `OFF` | 将更多 MySQL SQL 转写为 DuckDB 兼容形式 |
| `duckdb_max_threads_per_query` | GLOBAL, SESSION | `1000000` | 限制单个用户查询的线程数 |
| `duckdb_max_threads_per_query_rpl` | GLOBAL | `1000000` | 限制单个复制查询的线程数 |
| `duckdb_psmt_cursor_send_extra_eof` | GLOBAL, SESSION | `ON` | 兼容旧版 JDBC Cursor 的 EOF 行为 |
| `duckdb_prefer_high_precision` | GLOBAL, SESSION | `OFF` | 优先使用 DuckDB 高精度计算路径 |
| `duckdb_convert_tables_with_generated_columns` | GLOBAL | `ON` | 允许转换包含生成列的表 |
| `duckdb_copy_data_between_tables_use_ins_sel` | GLOBAL, SESSION | `OFF` | DuckDB 表间 Copy DDL 使用 `INSERT ... SELECT` |
| `ignore_index_hint_error` | GLOBAL | `OFF` | 将不存在的索引 Hint 从错误降级为警告 |

本节参数均支持动态修改。标为 `GLOBAL, SESSION` 的参数同时具有全局默认值和连接级取值：`SET GLOBAL` 影响后续新建连接，`SET SESSION` 只影响当前连接。

### `duckdb_sql_normalization`

- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: `OFF`
- **取值范围**: `ON` \| `OFF`
- **含义**: 在查询发送给 DuckDB 前，转写受支持的 MySQL 语法和函数，包括跨库名称处理及 Prepared Statement reprepare。建议针对实际工作负载完成兼容性验证后按会话开启。

### `duckdb_max_threads_per_query`

- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 无符号整型
- **默认值**: `1000000`
- **取值范围**: `1` ~ `4611686018427387904`
- **含义**: 单个用户查询可使用的 DuckDB Worker 线程上限。可将其设置为低于 `duckdb_threads` 的合理值，避免单个查询占满线程池。

### `duckdb_max_threads_per_query_rpl`

- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 无符号整型
- **默认值**: `1000000`
- **取值范围**: `1` ~ `4611686018427387904`
- **含义**: 单个复制查询可使用的 DuckDB Worker 线程上限。

### `duckdb_psmt_cursor_send_extra_eof`

- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: `ON`
- **取值范围**: `ON` \| `OFF`
- **含义**: Prepared Statement 以 Cursor 方式执行并返回空结果集时，额外发送 EOF 包。MySQL Connector/J 9.5.0 之前的版本应保持开启。

### `duckdb_prefer_high_precision`

- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: `OFF`
- **取值范围**: `ON` \| `OFF`
- **含义**: 在 DuckDB 支持的场景中优先使用高精度计算路径，可能以部分性能换取更高数值精度。

### `duckdb_convert_tables_with_generated_columns`

- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: `ON`
- **取值范围**: `ON` \| `OFF`
- **含义**: 是否允许将包含生成列的表转换为 DuckDB 存储引擎。关闭后，此类转换会在复制数据前失败。

### `duckdb_copy_data_between_tables_use_ins_sel`

- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: `OFF`
- **取值范围**: `ON` \| `OFF`
- **含义**: Copy DDL 在两个 DuckDB 表之间复制数据时，使用 DuckDB `INSERT ... SELECT` 路径替代服务层逐行传输。开启前应验证工作负载使用的表定义和数据类型。

### `ignore_index_hint_error`

- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: `OFF`
- **取值范围**: `ON` \| `OFF`
- **含义**: 允许 Index Hint 引用不存在的索引，服务端输出警告而不是拒绝语句。该参数影响整个服务，不仅限于 DuckDB 表。

### `duckdb_mode`
- **参数范围**: 全局参数
- **修改形式**: 静态修改（需重启生效）
- **参数类型**: 枚举类型
- **默认值**: NONE
- **取值范围**: NONE \| ON
- **含义**: 控制是否启用 DuckDB 存储引擎。设置为 `ON` 时启用，`NONE` 表示不启用。该参数为只读参数，只能在启动时指定。

### `duckdb_require_primary_key`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: ON
- **取值范围**: ON \| OFF
- **含义**: 是否要求所有 DuckDB 表必须定义主键。若开启，则建表时未指定主键将报错。
> 说明：DuckDB 表中实际不会创建任何索引，主键/唯一键的唯一性需要用户来保证。当构建 DuckDB 节点作为从节点时，请务必开启此参数以确保数据复制的正确性。

### `duckdb_memory_limit`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 整型（单位字节）
- **默认值**: 0
- **取值范围**: 0 ~ ULLONG_MAX
- **修改步长**: 1024 字节
- **含义**: 设置 DuckDB 可使用的最大内存上限。0 表示由系统自动决定，通常为物理内存的80%。
> 说明：当启用 DuckDB 引擎时，建议降低 innodb_buffer_pool_size 的配置，以释放更多内存给 DuckDB 存储引擎使用。

### `duckdb_temp_directory`
- **参数范围**: 全局参数
- **修改形式**: 静态修改
- **参数类型**: 字符串
- **默认值**:
- **含义**: 指定 DuckDB 写入临时文件的目录路径。该参数为只读参数，只能在实例启动前配置。

### `duckdb_max_temp_directory_size`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 整型（单位字节）
- **默认值**: 0（表示使用磁盘可用空间的 90%）
- **取值范围**: 0 ~ ULLONG_MAX
- **修改步长**: 1024 字节
- **含义**: 限制 `duckdb_temp_directory` 所在目录中 DuckDB 可使用的最大磁盘空间。0 表示由系统自动决定，通常为磁盘空间的 90%。

### `duckdb_threads`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 整型
- **默认值**: 0（表示自动选择线程数）
- **取值范围**: 0 ~ 1048576
- **含义**: 设置 DuckDB 使用的总线程数量。0 表示由系统根据 CPU 核心数自动调整。

### `duckdb_use_direct_io`
- **参数范围**: 全局参数
- **修改形式**: 静态修改
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 是否使用 Direct I/O 模式进行数据读写，以绕过操作系统缓存，提升大文件 I/O 性能。该参数为只读参数，只能在实例启动前配置。
> 说明：当前 DuckDB Direct I/O 模式并不稳定，不建议使用。

### `duckdb_scheduler_process_partial`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: ON
- **取值范围**: ON \| OFF
- **含义**: 任务调度器是否在重新调度前部分处理任务，有助于提高多个并发查询之间的公平性。

### `duckdb_merge_join_threshold`
- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 整型（行数）
- **默认值**: 4611686018427387904
- **取值范围**: 0 ~ 4611686018427387904
- **含义**: 当任一表的行数超过此阈值时，DuckDB 将选择使用归并连接（Merge Join）而非哈希连接。

### `duckdb_convert_all_at_startup`
- **参数范围**: 全局参数
- **修改形式**: 静态修改（只读）
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 是否在实例启动时，自动将所有 InnoDB 引擎的表转换为 DuckDB 表。使用此参数可以简单地完成数据从 InnoDB 到 DuckDB 的全量迁移。该参数为只读参数，只能在实例启动前配置。

### `duckdb_convert_all_at_startup_ignore_error`
- **参数范围**: 全局参数
- **修改形式**: 静态修改（只读）
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 在启动阶段将 InnoDB 表转换到 DuckDB 引擎的过程中，是否忽略转换错误并继续执行。开启后实例可能在仅转换部分用户表的情况下完成启动，必须检查每个失败并核对最终引擎分布。该参数为只读参数，只能在实例启动前配置。

### `duckdb_convert_all_at_startup_threads`
- **参数范围**: 全局参数
- **修改形式**: 动态修改（供下一次启动转换使用）
- **参数类型**: 整型
- **默认值**: 4
- **取值范围**: 1 ~ 64
- **含义**: 指定启动转换使用的线程数量，用于加速批量迁移。启动后修改不会影响已经执行的转换；如需在下次启动转换时生效，应写入 `my.cnf` 或持久化该值。

### `duckdb_convert_all_skip_mtr_db`
- **参数范围**: 全局参数
- **修改形式**: 静态修改
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 在启动时转换表的过程中，是否跳过名为 `mtr` 的数据库。此参数一般情况下只用于辅助通过测试用例。该参数为只读参数，只能在实例启动前配置。

### `duckdb_force_no_collation`
- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 是否禁用排序规则的下推优化，强制使用二进制比较。当 DuckDB 的查询并不关心数据的排序顺序和大小写时，可以设置此参数为 `ON`，以提升性能。

### `duckdb_source_set_insert_only_to_binlog`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 当事务仅包含插入操作时，是否向 Binlog 中设置 `insert_only` 标志，以优化复制性能。

### `duckdb_explain_output`
- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 枚举类型
- **默认值**: PHYSICAL_ONLY
- **取值范围**: ALL \| OPTIMIZED_ONLY \| PHYSICAL_ONLY
- **含义**: 设置DuckDB 查询 `EXPLAIN` 命令输出的默认格式。可选值包括全部计划、优化后计划或物理执行计划。

### `duckdb_multi_trx_in_batch`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 是否允许将 relay log 中的多个事务合并为一个批次提交，以提高吞吐量。该参数仅在备节点生效；修改时复制必须处于停止状态，并且启动复制前必须设置 `replica_parallel_workers=0`。

### `duckdb_multi_trx_timeout`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 整型
- **默认值**: 5000ms
- **取值范围**: 0 ~ 100000
- **含义**: 延迟事务提交的超时时间（毫秒），用于等待更多事务进入同一批次。该参数仅在备节点生效。

### `duckdb_multi_trx_max_batch_length`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 整型（字节）
- **默认值**: 256MB
- **取值范围**: 0 ~ ULLONG_MAX
- **含义**: 延迟提交批次的最大长度（以字节为单位），达到后立即触发提交。该参数仅在备节点生效。

### `duckdb_commit_multi_trx_due_to_reader`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: ON
- **取值范围**: ON \| OFF
- **含义**: 当 relay log 为空时，是否触发多事务批量提交。该参数仅在备节点生效。

### `duckdb_commit_multi_trx_due_to_rotate`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: ON
- **取值范围**: ON \| OFF
- **含义**: **已弃用**。是否在接收到主库的 Rotate Event 时提交多个事务。该参数仅在备节点生效。

### `duckdb_commit_multi_trx_due_to_rotate_frequency`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 整型
- **默认值**: 1
- **取值范围**: 0 ~ 1048576
- **含义**: 当 `duckdb_commit_multi_trx_due_to_rotate` 启用时，每接收多少个 binlog rotate 事件后提交一次。0 表示从不，1 表示每次都提交。该参数仅在备节点生效。

### `duckdb_copy_ddl_threads`
- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 整型
- **默认值**: 4
- **取值范围**: 0 ~ 64
- **含义**: 在执行 DDL 将 InnoDB 表转换为 DuckDB 引擎时，用于执行 DDL 操作的线程数量。并行转换过程使用了 InnoDB 的 parallel read 的框架，但是这一过程中并行线程数不受参数 `innodb_parallel_read_threads`的控制。

### `duckdb_checkpoint_threshold`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 整型（字节）
- **默认值**: 268435456（256MB）
- **取值范围**: 0 ~ ULLONG_MAX
- **修改步长**: 1024 字节
- **含义**: DuckDB 引擎中的 WAL 日志大小达到此阈值时，自动触发 checkpoint 操作。

### `duckdb_use_double_for_decimal`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: ON
- **取值范围**: ON \| OFF
- **含义**: DuckDB 引擎不支持精度高于 38 的 DECIMAL 类型。该参数用于控制对于精度高于 38 的 DECIMAL 类型，是否使用 DOUBLE 替代。参数支持动态修改，但实例创建表后再改变取值，可能导致不同表使用不一致的物理列类型。
> 说明：该参数影响列的实际类型，实例创建后不应更改。

### `duckdb_disabled_optimizers`
- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 枚举类型
- **默认值**: 0（空集合）
- **取值范围**:  
  EXPRESSION_REWRITER, FILTER_PULLUP, FILTER_PUSHDOWN, EMPTY_RESULT_PULLUP,  
  CTE_FILTER_PUSHER, REGEX_RANGE, IN_CLAUSE, JOIN_ORDER, DELIMINATOR,  
  UNNEST_REWRITER, UNUSED_COLUMNS, STATISTICS_PROPAGATION, COMMON_SUBEXPRESSIONS,  
  COMMON_AGGREGATE, COLUMN_LIFETIME, BUILD_SIDE_PROBE_SIDE, LIMIT_PUSHDOWN,  
  TOP_N, COMPRESSED_MATERIALIZATION, DUPLICATE_GROUPS, REORDER_FILTER,  
  SAMPLING_PUSHDOWN, JOIN_FILTER_PUSHDOWN, EXTENSION, MATERIALIZED_CTE,  
  SUM_REWRITER, LATE_MATERIALIZATION
- **含义**: 禁用 DuckDB 引擎中指定的优化器规则。

### `duckdb_data_import_mode`
- **参数范围**: 全局、会话参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 是否启用数据导入模式。在此模式下，仅支持基于主键的常量等值条件的删除和插入操作。
> 说明
> 1. 适用于批量导入场景，将多次的插入/删除操作合并为一次批量操作，提高导入性能。
> 2. 此参数在事务中无法修改。
> 3. 此参数为 ON 时，修改的表要求必须具备主键。
> 4. 此参数为 ON 时，将无法执行 update 操作，请将 update 操作转换为 delete + insert 操作。
> 5. 此参数为 ON 时，不满足条件的 DML 操作将会报错。
> 6. 只有参数 `duckdb_dml_in_batch` 开启时，此参数才会生效。

### `duckdb_idempotent_data_import_enabled`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**: 在参数`duckdb_data_import_mode`为 ON 时，是否启用幂等数据导入。启用后，重复导入相同数据（如重启恢复）不会导致数据重复。
> 说明：开启幂等数据导入功能后，数据导入性能会有一定程度的下降。

### `duckdb_appender_allocator_flush_threshold`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 整型（字节）
- **默认值**: 64MB
- **取值范围**: 0 ~ ULLONG_MAX
- **修改步长**: 1024 字节
- **含义**: DuckDB 以攒批形式写入数据时，攒批耗费的内存达到此阈值时，主动执行 flush 释放内存以避免 OOM。

### `duckdb_log_options`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 枚举类型
- **默认值**: 0（无日志）
- **取值范围**:
  DUCKDB_MULTI_TRX_BATCH_COMMIT, DUCKDB_MULTI_TRX_BATCH_DETAIL, DUCKDB_QUERY, DUCKDB_QUERY_RESULT
- **含义**: 指定需要记录日志的 DuckDB 操作类型，用于调试和审计。

### `force_innodb_to_duckdb`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: OFF
- **取值范围**: ON \| OFF
- **含义**:  在创建表或执行 DDL 时是否强制将 InnoDB 存储引擎替换为 DuckDB，用于测试或迁移场景。

### `duckdb_copy_ddl_in_batch`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: ON
- **取值范围**: ON \| OFF
- **含义**: 是否使用批量插入方式加速将 InnoDB 表转为 DuckDB 引擎的 DDL 过程。开启后可显著提升从 InnoDB 向 DuckDB 数据转换的性能。

### `duckdb_dml_in_batch`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: ON
- **取值范围**: ON \| OFF
- **含义**: 是否启用批处理模式来加速 INSERT/UPDATE/DELETE 等 DML 操作。开启后，多个变更操作会被合并为批次提交，提高吞吐量并减少事务开销。
> 说明：
> 1. 开启此参数后，DuckDB 节点作为从节点且当主节点的 binlog 格式为 row 时，DuckDB 引擎会自动完成所有 DML 操作的攒批。
> 2. 开启此参数后，DuckDB 主节点上的插入操作可以攒批，删除操作能否攒批依赖参数 `duckdb_data_import_mode` 的开关及其限制，更新操作无法攒批。

### `duckdb_batch_max_row_count`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 无符号整型（行数）
- **默认值**: 0（不限制批次行数）
- **取值范围**: 0 ~ ULLONG_MAX
- **含义**: 单个 DuckDB DML 批次允许包含的最大行数，用于拆分超大批次并降低峰值内存占用。0 表示不按行数限制。

### `duckdb_update_modified_column_only`
- **参数范围**: 全局参数
- **修改形式**: 动态修改
- **参数类型**: 布尔类型
- **默认值**: ON
- **取值范围**: ON \| OFF
- **含义**: 在回放 Binlog 时，是否仅更新实际被修改的列。开启后可减少不必要的列写入，提升复制效率，降低 I/O 和内存压力。
