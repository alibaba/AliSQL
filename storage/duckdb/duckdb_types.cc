#include "duckdb_types.h"
#include <stddef.h>
#include <cassert>
#include "m_string.h"  // strend()
#include "../../sql/sql_table.h"

// refers create_table_info_t::normalize_table_name
DatabaseTableNames::DatabaseTableNames(const char *name) {
  const char *name_ptr;
  size_t name_len;
  const char *db_ptr;
  size_t db_len;
  const char *ptr;

  /* Scan name from the end */

  ptr = strend(name) - 1;

  /* seek to the last path separator */
  while (ptr >= name && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }

  name_ptr = ptr + 1;
  name_len = strlen(name_ptr);

  /* skip any number of path separators */
  while (ptr >= name && (*ptr == '\\' || *ptr == '/')) {
    ptr--;
  }

  assert(ptr >= name);

  /* seek to the last but one path separator or one char before
  the beginning of name */
  db_len = 0;
  while (ptr >= name && *ptr != '\\' && *ptr != '/') {
    ptr--;
    db_len++;
  }

  db_ptr = ptr + 1;

  std::string raw_table_name = std::string(name_ptr, name_len);
  std::string raw_db_name = std::string(db_ptr, db_len);

  /*
    When there are escape characters in the table name or database name
    (such as '-' is converted to '@002d'), we need to restore it to the
    original characters to avoid being unable to find the DuckDB table.
  */
  char ori_db_name[NAME_LEN + 1];
  char ori_table_name[NAME_LEN + 1];
  size_t tbl_name_length = filename_to_tablename(
      raw_table_name.c_str(), ori_table_name, sizeof(ori_table_name));
  size_t db_name_length = filename_to_tablename(
      raw_db_name.c_str(), ori_db_name, sizeof(ori_db_name));
  table_name = std::string(ori_table_name, tbl_name_length);
  db_name = std::string(ori_db_name, db_name_length);
}

Databasename::Databasename(const char *path_name) {
  char dbname[FN_REFLEN];
  const char *end, *ptr;
  char tmp_buff[FN_REFLEN + 1];

  char *tmp_name = tmp_buff;
  /* Scan name from the end */
  ptr = strend(path_name) - 1;
  while (ptr >= path_name && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  ptr--;
  end = ptr;
  while (ptr >= path_name && *ptr != '\\' && *ptr != '/') {
    ptr--;
  }
  uint name_len = (uint)(end - ptr);
  memcpy(tmp_name, ptr + 1, name_len);
  tmp_name[name_len] = '\0';

  filename_to_tablename(tmp_name, dbname, sizeof(tmp_buff) - 1);
  name = std::string(dbname);
}
