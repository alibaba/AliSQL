#pragma once

#include "handler.h"
#include "sql_alter.h"
namespace myduck {

/** Prepares the creation of a DuckDB table by modifying the create_info and
  alter_info objects to align with DuckDB's constraints and limitations.
  Called by mysql_prepare_create_table.
  @param thd pointer to THD
  @param create_info pointer to HA_CREATE_INFO containing table creation
  parameters
  @param alter_info pointer to Alter_info containing additional table alteration
  information

  @return Return false if success.
*/
bool prepare_create_duckdb_table(THD *thd, HA_CREATE_INFO *create_info,
                                 Alter_info *alter_info);

/** Flags which are not supported by DuckDB. */
const ulonglong UNSUPPORT_ALTER_FLAGS =
    /* PARTITION OPERATIONS. */
    Alter_info::ALTER_ADD_PARTITION | Alter_info::ALTER_DROP_PARTITION |
    Alter_info::ALTER_COALESCE_PARTITION |
    Alter_info::ALTER_REORGANIZE_PARTITION | Alter_info::ALTER_PARTITION |
    Alter_info::ALTER_ADMIN_PARTITION | Alter_info::ALTER_TABLE_REORG |
    Alter_info::ALTER_REBUILD_PARTITION | Alter_info::ALTER_ALL_PARTITION |
    Alter_info::ALTER_REMOVE_PARTITIONING |
    Alter_info::ALTER_TRUNCATE_PARTITION |
    Alter_info::ALTER_EXCHANGE_PARTITION |
    /* COLUMN OPERATIONS. */
    Alter_info::ALTER_COLUMN_ORDER;

/** Flags which are ignored by DuckDB. */
const ulonglong IGNORED_ALTER_FLAGS =
    /* INDEX OPERATIONS. */
    Alter_info::ALTER_RENAME_INDEX |
    /* FOREIGN KEY OPERATIONS. */
    Alter_info::ADD_FOREIGN_KEY | Alter_info::DROP_FOREIGN_KEY |
    /* ALTER ORDER */
    Alter_info::ALTER_ORDER;

/** Flags which may be ignored by DuckDB. */
const ulonglong MAY_IGNORED_ALTER_FLAGS =
    Alter_info::ALTER_ADD_INDEX | Alter_info::ALTER_DROP_INDEX;

/** Prepare alter_info for ALTER DuckDB table.
  For alter_info->flags, the information contained in IGNORED_ALTER_FLAGS will
  be ignored.
  @param[in]      thd          thread handle.
  @param[in]      create_info  A structure describing the table to be created
  @param[in, out] alter_info   Alter_info describing which columns, defaults or
                               indexes are dropped or modified. */
void prepare_alter_duckdb_table(THD *thd, HA_CREATE_INFO *create_info,
                                Alter_info *alter_info);

/** Checks whether the given table is a DuckDB table.
  @param table pointer to TABLE object
  @return true if the table is a DuckDB table, false otherwise
*/
bool is_duckdb_table(const TABLE *table);

/** Checks whether the given ALTER TABLE operation is supported by DuckDB.
  @param alter_info  pointer to Alter_info object containing the ALTER TABLE
  operation
  @return true if the operation is supported, false otherwise
*/
bool is_supported_ddl(Alter_info *alter_info);

// /** Report error message of DuckDB table struct to user.
//   @param[in]  err_msg   error message
//   @return true always */
// bool report_duckdb_table_struct_error(std::string const &err_msg);

/** Precheck if the table can be converted to DuckDB table.
  @param[in]  table  table to be converted
  @return true if the table can be converted to DuckDB table, false otherwise
*/
bool precheck_convert_to_duckdb(const TABLE_SHARE *table);

bool report_duckdb_table_struct_error(std::string const &err_msg);
}  // namespace myduck
