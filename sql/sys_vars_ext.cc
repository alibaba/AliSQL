/*****************************************************************************

Copyright (c) 2025, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#include "sql/sys_vars.h"
#include "sql/duckdb/duckdb_config.h"
#include "sql/rpl_applier_reader.h"
#include "sql/rpl_rli.h"
#include "sql/duckdb/log.h"
#include "sql/sql_table_ext.h"

/** DuckDB related variables begin. */
static Sys_var_bool Sys_duckdb_require_primary_key(
    "duckdb_require_primary_key",
    "Whether to require a primary key for Duckdb tables",
    GLOBAL_VAR(duckdb_require_primary_key), CMD_LINE(OPT_ARG), DEFAULT(true),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_enum Sys_duckdb_mode(
    "duckdb_mode",
    "Whether to enable duckdb storage engine, legal values are NONE and ON.",
    READ_ONLY GLOBAL_VAR(myduck::global_mode), CMD_LINE(REQUIRED_ARG),
    myduck::mode_names, DEFAULT(myduck::DUCKDB_NONE), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(nullptr), ON_UPDATE(nullptr), nullptr, sys_var::PARSE_EARLY);

static Sys_var_ulonglong Sys_duckdb_memory_limit(
    "duckdb_memory_limit", "The maximum memory duckdb can use, 0 means auto.",
    GLOBAL_VAR(myduck::global_memory_limit), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, ULLONG_MAX), DEFAULT(0), BLOCK_SIZE(1024), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(nullptr), ON_UPDATE(myduck::update_memory_limit));

static Sys_var_charptr Sys_duckdb_temp_directory(
    "duckdb_temp_directory",
    "Set the directory to which duckdb write temp files",
    READ_ONLY GLOBAL_VAR(myduck::global_duckdb_temp_directory),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(nullptr));

static Sys_var_ulonglong Sys_duckdb_max_temp_directory_size(
    "duckdb_max_temp_directory_size",
    "The maximum amount of duckdb data stored "
    "inside the 'duckdb_temp_directory', 0 means '90% of available disk space'",
    GLOBAL_VAR(myduck::global_max_temp_directory_size), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, ULLONG_MAX), DEFAULT(0), BLOCK_SIZE(1024), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(myduck::update_max_temp_directory_size));

static Sys_var_ulonglong Sys_duckdb_threads(
    "duckdb_threads",
    "The number of total threads used by duckdb, 0 means 'auto'",
    GLOBAL_VAR(myduck::global_max_threads), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, 1024 * 1024), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(nullptr), ON_UPDATE(myduck::update_threads));

static Sys_var_bool Sys_duckdb_use_direct_io(
    "duckdb_use_direct_io",
    "Whether duckdb uses direct io to read and write data.",
    READ_ONLY GLOBAL_VAR(myduck::global_use_dio), CMD_LINE(OPT_ARG),
    DEFAULT(false));

static Sys_var_bool Sys_duckdb_scheduler_process_partial(
    "duckdb_scheduler_process_partial",
    "Partially process tasks before rescheduling - allows for more scheduler "
    "fairness between separate queries.",
    GLOBAL_VAR(myduck::global_scheduler_process_partial), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(myduck::update_scheduler_process_partial));

static Sys_var_ulonglong Sys_duckdb_merge_join_threshold(
    "duckdb_merge_join_threshold",
    "The number of rows we need on either table to choose a merge join",
    SESSION_VAR(duckdb_merge_join_threshold), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, 4611686018427387904), DEFAULT(4611686018427387904),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr));

static Sys_var_bool Sys_duckdb_convert_all_at_startup(
    "duckdb_convert_all_at_startup",
    "Whether convert all non-DuckDB engine tables to DuckDB at startup.",
    READ_ONLY GLOBAL_VAR(duckdb_convert_all_at_startup), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_duckdb_convert_all_at_startup_ignore_error(
    "duckdb_convert_all_at_startup_ignore_error",
    "Whether ignore DDL error when converting table to DuckDB at startup.",
    READ_ONLY GLOBAL_VAR(duckdb_convert_all_at_startup_ignore_error),
    CMD_LINE(OPT_ARG), DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(0), ON_UPDATE(0));

constexpr uint DUCKDB_CONVERT_MAX_THREADS = 64;
static Sys_var_uint Sys_duckdb_convert_all_at_startup_threads(
    "duckdb_convert_all_at_startup_threads",
    "The number of threads to convert the table to DuckDB at startup.",
    GLOBAL_VAR(duckdb_convert_all_at_startup_threads), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, DUCKDB_CONVERT_MAX_THREADS), DEFAULT(4), BLOCK_SIZE(1));

static Sys_var_bool Sys_duckdb_convert_all_skip_mtr_db(
    "duckdb_convert_all_skip_mtr_db",
    "Whether convert database 'mtr' to DuckDB at startup.",
    READ_ONLY GLOBAL_VAR(duckdb_convert_all_skip_mtr_db), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_duckdb_force_no_collation(
    "duckdb_force_no_collation", "Disable push collation in DuckDB.",
    SESSION_VAR(duckdb_force_no_collation), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

extern bool duckdb_source_set_insert_only_to_binlog;
static Sys_var_bool Sys_duckdb_set_insert_only_to_binlog(
    "duckdb_source_set_insert_only_to_binlog",
    "Whether set insert_only flag to Binlog when a transaction only contains "
    "Insert.",
    GLOBAL_VAR(duckdb_source_set_insert_only_to_binlog), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static const char *duckdb_explain_type_names[] = {"ALL", "OPTIMIZED_ONLY",
                                                  "PHYSICAL_ONLY", NullS};
static Sys_var_enum Sys_explain_format(
    "duckdb_explain_output",
    "The default type in which the EXPLAIN statement used in duckdb egnine"
    "Valid values are LOGICAL (default), PHYSICAL.",
    SESSION_VAR(duckdb_explain_output_type), CMD_LINE(OPT_ARG),
    duckdb_explain_type_names,
    DEFAULT(static_cast<ulong>(Duckdb_explain_output_type::PHYSICAL_ONLY_)),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr), ON_UPDATE(nullptr));

static Sys_var_bool Sys_duckdb_multi_trx_in_batch(
    "duckdb_multi_trx_in_batch",
    "Whether commit multiple transactions in a single batch.",
    GLOBAL_VAR(duckdb_multi_trx_in_batch), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_ulonglong Sys_duckdb_multi_trx_timeout(
    "duckdb_multi_trx_timeout",
    "DuckDB delays transaction commit timeout (in ms)",
    GLOBAL_VAR(duckdb_multi_trx_timeout), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 100000), DEFAULT(5000), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_ulonglong Sys_duckdb_multi_trx_max_batch_length(
    "duckdb_multi_trx_max_batch_length",
    "DuckDB delays transaction commit batch length limit (in Byte)",
    GLOBAL_VAR(duckdb_multi_trx_max_batch_length), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, ULLONG_MAX), DEFAULT(256 * 1024 * 1024), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_duckdb_commit_multi_trx_due_to_reader(
    "duckdb_commit_multi_trx_due_to_reader",
    "Whether commit multiple transactions when relay log is empty.",
    GLOBAL_VAR(duckdb_commit_multi_trx_due_to_reader), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_duckdb_commit_multi_trx_due_to_rotate(
    "duckdb_commit_multi_trx_due_to_rotate",
    "This variables is deprecated. Whether commit multiple transactions when"
    "apply a Rotate Event from Master.",
    GLOBAL_VAR(duckdb_commit_multi_trx_due_to_rotate), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_uint Sys_duckdb_commit_multi_trx_due_to_rotate_frequency(
    "duckdb_commit_multi_trx_due_to_rotate_frequency",
    "When duckdb_commit_multi_trx_due_to_rotate is enabled, commit "
    "multiple transactions every so many binlogs. 0 represents never, "
    "1 represents that commit is required for each rotate.",
    GLOBAL_VAR(duckdb_commit_multi_trx_due_to_rotate_frequency),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(0, 1024 * 1024), DEFAULT(1),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(nullptr));

static Sys_var_uint Sys_duckdb_copy_ddl_threads(
    "duckdb_copy_ddl_threads",
    "The number of threads to do COPY DDL from InnoDB to DuckDB.",
    SESSION_VAR(duckdb_copy_ddl_threads), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, 64), DEFAULT(4), BLOCK_SIZE(1), NO_MUTEX_GUARD, IN_BINLOG,
    ON_CHECK(nullptr));

static Sys_var_ulonglong Sys_duckdb_checkpoint_threshold(
    "duckdb_checkpoint_threshold",
    "The WAL size threshold at which to automatically trigger a "
    "checkpoint (e.g. 1GB)",
    GLOBAL_VAR(myduck::checkpoint_threshold), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, ULLONG_MAX), DEFAULT(268435456), BLOCK_SIZE(1024),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(myduck::update_checkpoint_threshold));

static Sys_var_bool Sys_duckdb_use_double_for_decimal(
    "duckdb_use_double_for_decimal",
    "Whether to use double for decimal type with precision higher than 38. "
    "Note that this is a global variable and will affect the actual column type "
    "of duckdb table, so it should not be changed after instance is created.",
    GLOBAL_VAR(myduck::use_double_for_decimal), CMD_LINE(OPT_ARG), DEFAULT(true),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr), ON_UPDATE(nullptr));

static const char *duckdb_disabled_optimizers_names[] = {
    "EXPRESSION_REWRITER",
    "FILTER_PULLUP",
    "FILTER_PUSHDOWN",
    "EMPTY_RESULT_PULLUP",
    "CTE_FILTER_PUSHER",
    "REGEX_RANGE",
    "IN_CLAUSE",
    "JOIN_ORDER",
    "DELIMINATOR",
    "UNNEST_REWRITER",
    "UNUSED_COLUMNS",
    "STATISTICS_PROPAGATION",
    "COMMON_SUBEXPRESSIONS",
    "COMMON_AGGREGATE",
    "COLUMN_LIFETIME",
    "BUILD_SIDE_PROBE_SIDE",
    "LIMIT_PUSHDOWN",
    "TOP_N",
    "COMPRESSED_MATERIALIZATION",
    "DUPLICATE_GROUPS",
    "REORDER_FILTER",
    "SAMPLING_PUSHDOWN",
    "JOIN_FILTER_PUSHDOWN",
    "EXTENSION",
    "MATERIALIZED_CTE",
    "SUM_REWRITER",
    "LATE_MATERIALIZATION",
    nullptr};

static Sys_var_set Sys_duckdb_disabled_optimizers(
    "duckdb_disabled_optimizers",
    "Disable a specific set of optimizers in DuckDB",
    HINT_UPDATEABLE SESSION_VAR(duckdb_disabled_optimizers),
    CMD_LINE(REQUIRED_ARG), duckdb_disabled_optimizers_names, DEFAULT(0),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr));

static Sys_var_bool Sys_duckdb_data_import_mode(
    "duckdb_data_import_mode",
    "Whether to enable data import mode. "
    "Currently only supports delete using equal primary key conditions "
    "and insert.",
    SESSION_VAR(duckdb_data_import_mode), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(check_outside_trx), ON_UPDATE(0));

extern bool duckdb_idempotent_data_import_enabled;
static Sys_var_bool Sys_duckdb_idempotent_data_import_enabled(
    "duckdb_idempotent_data_import_enabled",
    "Whether enable idempotent data import for DuckDB. When enabled, repeated "
    "imports of the same data (e.g., during restart) will not cause "
    "duplication.",
    GLOBAL_VAR(duckdb_idempotent_data_import_enabled), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

bool duckdb_disabled_optimizers_string_representation(
    THD *thd, ulonglong duckdb_disabled_optimizers, LEX_STRING *ls) {
  set_to_string(thd, ls, duckdb_disabled_optimizers,
                duckdb_disabled_optimizers_names);
  return ls->str == nullptr;
}

static Sys_var_ulonglong Sys_duckdb_appender_allocator_flush_threshold(
    "duckdb_appender_allocator_flush_threshold",
    "Peak allocation threshold at "
    "which to flush the allocator when DuckDB appender flushs chunk.",
    GLOBAL_VAR(myduck::appender_allocator_flush_threshold),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(0, ULLONG_MAX),
    DEFAULT(64 * 1024 * 1024), BLOCK_SIZE(1024), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(nullptr),
    ON_UPDATE(myduck::update_appender_allocator_flush_threshold));

static Sys_var_set Sys_duckdb_log_options(
    "duckdb_log_options", "Specify DuckDB operation types that need to be recorded",
    GLOBAL_VAR(myduck::duckdb_log_options), CMD_LINE(OPT_ARG),
    myduck::duckdb_log_types, DEFAULT(0));

static Sys_var_bool Sys_force_innodb_to_duckdb(
    "force_innodb_to_duckdb", "innodb storage converted to duckdb.",
    GLOBAL_VAR(force_innodb_to_duckdb), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));
/* DuckDB related variables end. */