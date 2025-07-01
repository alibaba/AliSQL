#ifndef VIDX_INDEX_INCLUDED
#define VIDX_INDEX_INCLUDED

/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_inttypes.h"         // uintxx_t
#include "sql/dd/properties.h"   // dd::Properties
#include "sql/dd/types/table.h"  // dd::Table
#include "sql/key.h"             // KEY

#include "vidx/vidx_common.h"

class THD;
struct TABLE;
struct handlerton;
struct TABLE_SHARE;
struct st_plugin_int;
class JOIN_TAB;
class ORDER;
class Alter_info;

namespace dd {
class Schema;
}  // namespace dd

namespace vidx {
constexpr uint32_t DATA_ROW_ID_LEN = 6;

class Item_func_vec_distance;

extern st_plugin_int *vidx_plugin;
extern bool feature_disabled;

bool check_vector_ddl_and_rewrite_sql(THD *thd, Alter_info *alter_info,
                                      KEY *key_info, const uint key_count,
                                      TABLE *table);

namespace hnsw {
uint index_options_print(const uint distance, const uint m, char *buf,
                         uint buf_len);

bool copy_index_option_m(THD *thd, uint *to, const uint from);
}  // namespace hnsw

bool copy_index_option_distance(THD *thd, uint *to, const uint from);

/* Create the auxiliary table for the vector index.
@param[in] thd                 Thread context.
@param[in] key                 The vector key.
@param[in] dd_table            The dd table object describing the base table.
@param[in] table               The base table
@param[in] db_name             The DB name.
@param[in] old_table_id        The old table id before truncate, or
                               dd::INVALID_OBJECT_ID if new create.
@return true if failed. */
bool create_table(THD *thd, KEY *key, dd::Table *dd_table, TABLE *table,
                  const char *db_name, const uint64_t old_table_id);

/* Drop the auxiliary table for the vector index.
@param[in] thd                 Thread context.
@param[in] dd_table            The dd table object describing the base table.
@param[in] db_name             The DB name.
@return true if failed. */
bool delete_table(THD *thd, const dd::Table *dd_table, const char *db_name);

/* Rename the auxiliary table for the vector index.
@param[in] thd                 Thread context.
@param[in] dd_table            The dd table object describing the base table.
@param[in] base                The base handlerton.
@param[in] new_schema          The dd schema object.
@param[in] old_db              The old DB name before rename.
@param[in] new_db              The new DB name after rename.
@param[in] flags               flags. See also mysql_rename_table().
@return true if failed. */
bool rename_table(THD *thd, dd::Table *dd_table, handlerton *base,
                  const dd::Schema &new_schema, const char *old_db,
                  const char *new_db, uint flags);

/* Build the info of vector key.
@param[in] thd                 Thread context.
@param[in] share               The table share.
@param[in] dd_table            The dd table object describing the base table.
@param[in] nr                  The number of the vector key.
@return true if failed. */
bool build_hlindex_key(THD *thd, TABLE_SHARE *share, const dd::Table *dd_table,
                       const uint nr);

/* Test if ORDER BY is a single distance function(ORDER BY VEC_DISTANCE),
sort order is descending, and vector index is more efficient than original
access path.
@param[in]  tab           JOIN_TAB to check
@param[in]  order         pointer to ORDER struct.
@param[in]  limit         maximum number of rows to select.
@param[out] order_idx     idx choosen.
@return True if vector index is enabled and efficient, otherwise False. */
bool test_if_cheaper_vector_ordering(JOIN_TAB *tab, ORDER *order, ha_rows limit,
                                     int *order_idx);

/* Check if the key is a vector key.
@param[in] key                 The vector key.
@return true if the key is a vector key. */
static inline bool key_is_vector(KEY *key) {
  return key != nullptr && (key->flags & HA_VECTOR);
}

/* Check the option "__hlindexes__" of the dd table exists and is not empty.
@param[in]  dd_table   The dd table.
@return true if "__hlindexes__" exists and is not empty, otherwise false. */
static inline bool dd_table_has_hlindexes(const dd::Table *dd_table) {
  return dd_table->options().exists("__hlindexes__");
}

/* Check if the dd table is a vector table.
@param[in]  dd_table   The dd table.
@return true if the dd table is a vector table, otherwise false. */
static inline bool dd_table_is_hlindex(const dd::Table *dd_table) {
  assert(dd_table->options().exists("__vector_column__") ==
         dd_table->options().exists("__vector_m__"));
  assert(dd_table->options().exists("__vector_column__") ==
         dd_table->options().exists("__vector_distance__"));
  return dd_table->options().exists("__vector_column__");
}
}  // namespace vidx

#endif /* VIDX_INDEX_INCLUDED */
