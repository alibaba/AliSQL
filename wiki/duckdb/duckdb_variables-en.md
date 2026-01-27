# DuckDB Engine Variables in AliSQL
[ [AliSQL DuckDB 引擎参数](./duckdb_variables-zh.md) | [DuckDB Engine Variables in AliSQL](./duckdb_variables-en.md) ]

## `duckdb_mode`
- **Scope**: Global  
- **Change type**: Static (restart required)  
- **Data type**: Enum  
- **Default**: `NONE`  
- **Valid values**: `NONE` \| `ON`  
- **Description**: Controls whether the DuckDB storage engine is enabled. `ON` enables DuckDB; `NONE` disables it. This variable is read-only and can only be set at startup.

---

## `duckdb_require_primary_key`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `ON`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Whether all DuckDB tables must define a primary key. If enabled, creating a table without a primary key will fail.

> Notes:
> - DuckDB tables do not actually create indexes. Uniqueness for PRIMARY KEY / UNIQUE KEY must be guaranteed by the user.
> - When using a DuckDB node as a replica, you must enable this variable to ensure replication correctness.

---

## `duckdb_memory_limit`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Integer (bytes)  
- **Default**: `0`  
- **Valid range**: `0` ~ `ULLONG_MAX`  
- **Step**: 1024 bytes  
- **Description**: Sets the maximum memory DuckDB is allowed to use. `0` means automatic (typically ~80% of physical memory).

> Note: When DuckDB is enabled, it is recommended to reduce `innodb_buffer_pool_size` to free more memory for DuckDB.

---

## `duckdb_temp_directory`
- **Scope**: Global  
- **Change type**: Static  
- **Data type**: String  
- **Default**: (empty)  
- **Description**: Directory path where DuckDB writes temporary files. This variable is read-only and can only be configured before startup.

---

## `duckdb_max_temp_directory_size`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Integer (bytes)  
- **Default**: `0` (uses 90% of available disk space)  
- **Valid range**: `0` ~ `ULLONG_MAX`  
- **Step**: 1024 bytes  
- **Description**: Limits the maximum disk space DuckDB can use under `duckdb_temp_directory`. `0` means automatic (typically ~90% of free disk space).

---

## `duckdb_threads`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Integer  
- **Default**: `0` (auto)  
- **Valid range**: `0` ~ `1048576`  
- **Description**: Sets the total number of threads used by DuckDB. `0` lets the system choose based on CPU cores.

---

## `duckdb_use_direct_io`
- **Scope**: Global  
- **Change type**: Static  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Whether to use Direct I/O to bypass the OS page cache for data reads/writes and improve large-file I/O performance. This variable is read-only and can only be set before startup.

> Note: DuckDB Direct I/O is currently unstable and is not recommended.

---

## `duckdb_scheduler_process_partial`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `ON`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Whether the scheduler partially processes tasks before rescheduling, which can improve fairness across concurrent queries.

---

## `duckdb_merge_join_threshold`
- **Scope**: Session  
- **Change type**: Dynamic  
- **Data type**: Integer (rows)  
- **Default**: `4611686018427387904`  
- **Valid range**: `0` ~ `4611686018427387904`  
- **Description**: If the row count of either table exceeds this threshold, DuckDB prefers Merge Join over Hash Join.

---

## `duckdb_convert_all_at_startup`
- **Scope**: Global  
- **Change type**: Static (read-only)  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Whether to automatically convert all InnoDB tables to DuckDB tables during server startup. This variable is read-only and can only be configured before startup.

---

## `duckdb_convert_all_at_startup_ignore_error`
- **Scope**: Global  
- **Change type**: Static (read-only)  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: During startup conversion from InnoDB to DuckDB, whether to ignore conversion errors and continue. This variable is read-only and can only be configured before startup.

---

## `duckdb_convert_all_at_startup_threads`
- **Scope**: Global  
- **Change type**: Static  
- **Data type**: Integer  
- **Default**: `4`  
- **Valid range**: `1` ~ `64`  
- **Description**: Number of threads used to convert tables at startup to accelerate bulk migration. This variable is read-only and can only be configured before startup.

---

## `duckdb_convert_all_skip_mtr_db`
- **Scope**: Global  
- **Change type**: Static  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Whether to skip a database named `mtr` during startup conversion. Typically used only for test purposes. This variable is read-only and can only be configured before startup.

---

## `duckdb_force_no_collation`
- **Scope**: Session  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Disables collation pushdown optimization and forces binary comparison. If your queries do not care about collation/case order, setting this to `ON` may improve performance.

---

## `duckdb_source_set_insert_only_to_binlog`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: When a transaction contains only INSERT operations, whether to set an `insert_only` flag in the binlog to optimize replication performance.

---

## `duckdb_explain_output`
- **Scope**: Session  
- **Change type**: Dynamic  
- **Data type**: Enum  
- **Default**: `PHYSICAL_ONLY`  
- **Valid values**: `ALL` \| `OPTIMIZED_ONLY` \| `PHYSICAL_ONLY`  
- **Description**: Controls the default output format of DuckDB `EXPLAIN`: all plans, optimized plan only, or physical plan only.

---

## `duckdb_multi_trx_in_batch`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Whether to merge multiple transactions from the relay log into a single batch commit to improve throughput. Effective only on replicas.

---

## `duckdb_multi_trx_timeout`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Integer (ms)  
- **Default**: `5000` ms  
- **Valid range**: `0` ~ `100000`  
- **Description**: Commit delay timeout (milliseconds) used to wait for more transactions to join the same batch. Effective only on replicas.

---

## `duckdb_multi_trx_max_batch_length`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Integer (bytes)  
- **Default**: `256MB`  
- **Valid range**: `0` ~ `ULLONG_MAX`  
- **Description**: Maximum batch size in bytes. Once reached, the batch is committed immediately. Effective only on replicas.

---

## `duckdb_commit_multi_trx_due_to_reader`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `ON`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: When the relay log is empty, whether to trigger a multi-transaction batch commit. Effective only on replicas.

---

## `duckdb_commit_multi_trx_due_to_rotate`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `ON`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: **Deprecated.** Whether to commit multiple transactions when a Rotate Event is received from the primary. Effective only on replicas.

---

## `duckdb_commit_multi_trx_due_to_rotate_frequency`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Integer  
- **Default**: `1`  
- **Valid range**: `0` ~ `1048576`  
- **Description**: When `duckdb_commit_multi_trx_due_to_rotate` is enabled, commit once per N binlog rotate events. `0` means never; `1` means every time. Effective only on replicas.

---

## `duckdb_copy_ddl_threads`
- **Scope**: Session  
- **Change type**: Dynamic  
- **Data type**: Integer  
- **Default**: `4`  
- **Valid range**: `0` ~ `64`  
- **Description**: Number of threads used during DDL conversion from InnoDB to DuckDB. The parallel conversion uses InnoDB parallel read infrastructure, but this thread count is not controlled by `innodb_parallel_read_threads`.

---

## `duckdb_checkpoint_threshold`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Integer (bytes)  
- **Default**: `268435456` (256MB)  
- **Valid range**: `0` ~ `ULLONG_MAX`  
- **Step**: 1024 bytes  
- **Description**: Automatically triggers a checkpoint when DuckDB WAL reaches this size.

---

## `duckdb_use_double_for_decimal`
- **Scope**: Global  
- **Change type**: Static  
- **Data type**: Boolean  
- **Default**: `ON`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: DuckDB does not support DECIMAL precision > 38. This variable controls whether to use DOUBLE instead for DECIMAL with precision > 38. This variable is read-only and can only be set before startup.

> Note: This affects the actual column type and should not be changed after the instance is created.

---

## `duckdb_disabled_optimizers`
- **Scope**: Session  
- **Change type**: Dynamic  
- **Data type**: Enum set  
- **Default**: `0` (empty set)  
- **Valid values**:  
  `EXPRESSION_REWRITER`, `FILTER_PULLUP`, `FILTER_PUSHDOWN`, `EMPTY_RESULT_PULLUP`,  
  `CTE_FILTER_PUSHER`, `REGEX_RANGE`, `IN_CLAUSE`, `JOIN_ORDER`, `DELIMINATOR`,  
  `UNNEST_REWRITER`, `UNUSED_COLUMNS`, `STATISTICS_PROPAGATION`, `COMMON_SUBEXPRESSIONS`,  
  `COMMON_AGGREGATE`, `COLUMN_LIFETIME`, `BUILD_SIDE_PROBE_SIDE`, `LIMIT_PUSHDOWN`,  
  `TOP_N`, `COMPRESSED_MATERIALIZATION`, `DUPLICATE_GROUPS`, `REORDER_FILTER`,  
  `SAMPLING_PUSHDOWN`, `JOIN_FILTER_PUSHDOWN`, `EXTENSION`, `MATERIALIZED_CTE`,  
  `SUM_REWRITER`, `LATE_MATERIALIZATION`
- **Description**: Disables the specified optimizer rules in DuckDB.

---

## `duckdb_data_import_mode`
- **Scope**: Session  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Enables data import mode. In this mode, only DELETE and INSERT operations with constant equality predicates on the primary key are supported.

> Notes:
> 1. Intended for bulk import: merges multiple INSERT/DELETE operations into a single batch to improve performance.
> 2. This variable cannot be changed inside a transaction.
> 3. When `ON`, the modified table must have a primary key.
> 4. When `ON`, UPDATE is not supported; rewrite UPDATE as DELETE + INSERT.
> 5. When `ON`, unsupported DML will raise an error.
> 6. This variable takes effect only when `duckdb_dml_in_batch` is enabled.

---

## `duckdb_idempotent_data_import_enabled`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: When `duckdb_data_import_mode=ON`, enables idempotent data import. If enabled, re-importing the same data (e.g., after restart/recovery) will not create duplicates.

> Note: Enabling idempotent import may reduce import performance.

---

## `duckdb_appender_allocator_flush_threshold`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Integer (bytes)  
- **Default**: `64MB`  
- **Valid range**: `0` ~ `ULLONG_MAX`  
- **Step**: 1024 bytes  
- **Description**: When DuckDB writes data in batches, if batch memory usage reaches this threshold, DuckDB proactively flushes to release memory and avoid OOM.

---

## `duckdb_log_options`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Enum set  
- **Default**: `0` (no logging)  
- **Valid values**:  
  `DUCKDB_MULTI_TRX_BATCH_COMMIT`, `DUCKDB_MULTI_TRX_BATCH_DETAIL`, `DUCKDB_QUERY`, `DUCKDB_QUERY_RESULT`
- **Description**: Selects which DuckDB operations are logged for debugging and auditing.

---

## `force_innodb_to_duckdb`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `OFF`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: When creating tables or running DDL, whether to force-replace the InnoDB engine with DuckDB. Useful for testing or migration scenarios.

---

## `duckdb_copy_ddl_in_batch`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `ON`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Whether to use batch inserts to accelerate DDL conversion from InnoDB to DuckDB. Enabling this can significantly improve conversion performance.

---

## `duckdb_dml_in_batch`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `ON`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: Enables batch mode to accelerate DML (INSERT/UPDATE/DELETE). When enabled, multiple changes can be merged into batches to improve throughput and reduce transaction overhead.

> Notes:
> 1. When enabled on a DuckDB replica and the primary uses row-based binlog, DuckDB automatically batches DML during replay.
> 2. When enabled on a DuckDB primary, INSERT can be batched; whether DELETE can be batched depends on `duckdb_data_import_mode` and its constraints; UPDATE cannot be batched.

---

## `update_modified_column_only`
- **Scope**: Global  
- **Change type**: Dynamic  
- **Data type**: Boolean  
- **Default**: `ON`  
- **Valid values**: `ON` \| `OFF`  
- **Description**: During binlog replay, whether to update only the columns that actually changed. Enabling this reduces unnecessary writes and improves replication efficiency while lowering I/O and memory pressure.