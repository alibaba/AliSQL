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

#include <regex>
#include "mysql/plugin.h"                    // MYSQL_DAEMON_PLUGIN, ...
#include "scope_guard.h"                     // Scope_guard
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd_table.h"                 // dd::table_exists, ...
#include "sql/dd/dictionary.h"               // dd::release_mdl
#include "sql/dd/string_type.h"              // dd::String_type
#include "sql/dd/types/index.h"              // dd::Index
#include "sql/dd/types/index_element.h"      // dd::Index_element
#include "sql/dd_table_share.h"              // open_table_def
#include "sql/field.h"                       // Field
#include "sql/handler.h"                     // setup_transaction_participant
#include "sql/item_strfunc.h"                // vidx::Item_func_vec_distance
#include "sql/join_optimizer/access_path.h"  // AccessPath
#include "sql/mysqld.h"                      // reg_ext_length
#include "sql/sql_base.h"                    // EXTRA_RECORD
#include "sql/sql_class.h"                   // THD
#include "sql/sql_lex.h"                     // LEX
#include "sql/sql_plugin_ref.h"              // st_plugin_int
#include "sql/sql_select.h"                  // JOIN_TAB
#include "sql/sql_table.h"                   // mysql_rename_table

#include "vidx/vidx_hnsw.h"
#include "vidx/vidx_index.h"

namespace vidx {
/* -------------------- Macros -------------------- */
#define VIDX_NAME "vidx_%016lx_%02x"
static constexpr uint VIDX_NAME_LEN = 4 + 1 + 16 + 1 + 2 + 1;
static constexpr uint vidx_num = 0;

#define TL_FIRST_WRITE TL_WRITE_ALLOW_WRITE

static constexpr uint32_t SCAN_COST = 4;

/* -------------------- External Vars Definition -------------------- */
st_plugin_int *vidx_plugin;
bool feature_disabled = false;

/* -------------------- Static Vars Definition -------------------- */
static TYPELIB distances = {array_elements(distance_names) - 1, "",
                            distance_names, nullptr};

static MYSQL_SYSVAR_BOOL(disabled, feature_disabled, PLUGIN_VAR_RQCMDARG,
                         "Whether to enable vector index feature", nullptr,
                         nullptr, true);

static MYSQL_THDVAR_ENUM(default_distance, PLUGIN_VAR_RQCMDARG,
                         "Distance function to build the vector index for",
                         nullptr, nullptr, EUCLIDEAN, &distances);

static MYSQL_THDVAR_UINT(
    hnsw_default_m, PLUGIN_VAR_RQCMDARG,
    "Larger values mean slower SELECTs and INSERTs, larger index size "
    "and higher memory consumption but more accurate results",
    nullptr, nullptr, hnsw::M_DEF, hnsw::M_MIN, hnsw::M_MAX, 1);

static MYSQL_THDVAR_UINT(
    hnsw_ef_search, PLUGIN_VAR_RQCMDARG,
    "Larger values mean slower SELECTs but more accurate results. "
    "Defines the minimal number of result candidates to look for in the "
    "vector index for ORDER BY ... LIMIT N queries. The search will never "
    "search for less rows than that, even if LIMIT is smaller",
    nullptr, nullptr, 20, 1, hnsw::max_ef, 1);

static MYSQL_SYSVAR_ULONGLONG(hnsw_cache_size, hnsw::max_cache_size,
                              PLUGIN_VAR_RQCMDARG,
                              "Upper limit for one HNSW vector index cache",
                              nullptr, nullptr, hnsw::DEF_CACHE_SIZE,
                              1024 * 1024, ULONG_LONG_MAX, 1);

static SYS_VAR *sys_vars[] = {
    MYSQL_SYSVAR(disabled),        MYSQL_SYSVAR(default_distance),
    MYSQL_SYSVAR(hnsw_default_m),  MYSQL_SYSVAR(hnsw_ef_search),
    MYSQL_SYSVAR(hnsw_cache_size), nullptr};

static struct st_mysql_storage_engine daemon = {MYSQL_DAEMON_INTERFACE_VERSION};

/* -------------------- Static Functions Definition -------------------- */
static int plugin_init(void *p) {
  vidx_plugin = (st_plugin_int *)p;
  vidx_plugin->data = hnsw::trx_handler;

  if (setup_transaction_participant(vidx_plugin)) return 1;

  return 0;
}

static int plugin_deinit(void *) { return 0; }

/* Get the string value in the dd table's option "__hlindexes__".
@param[in]  dd_table   the dd table
@param[out] hlindexes      the value of the option "__hlindexes__" */
static inline void dd_table_get_hlindexes(const dd::Table *dd_table,
                                          dd::String_type *hlindexes) {
  assert(hlindexes != nullptr);
  assert(dd_table_has_hlindexes(dd_table));

  dd_table->options().get("__hlindexes__", hlindexes);
  assert(!hlindexes->empty());
}

/* Set the string value in the dd table's option "__hlindexes__".
@param[in]  dd_table   the dd table
@param[in]  hlindexes      the value of the option "__hlindexes__" */
static inline void dd_table_set_hlindexes(dd::Table *dd_table,
                                          dd::String_type hlindexes) {
  dd_table->options().set("__hlindexes__", hlindexes);
}

static inline uint get_tref_len(TABLE *table) {
  assert(table->s->keys == 0 ||
         strcmp(table->key_info[0].name, primary_key_name) != 0 ||
         table->key_info[0].flags & HA_NOSAME);

  return (table->s->keys == 0 ||
          strcmp(table->key_info[0].name, primary_key_name) != 0)
             ? DATA_ROW_ID_LEN
             : table->key_info[0].key_length;
}

static const char *build_name(THD *thd, const uint64_t base, const uint num,
                              std::string &error_message) {
  /* The length of vector index table name should be shorter than 64 because
  the `name` of `tables` is varchar(64). See also Tables::Tables() in
  sql/dd/impl/tables/tables.cc */
  static_assert(VIDX_NAME_LEN <= 64);

  char *name = reinterpret_cast<char *>(thd->mem_root->Alloc(VIDX_NAME_LEN));

  if (name == nullptr) {
    error_message = "Failed to allocate memory for table name.";
  } else {
    snprintf(name, VIDX_NAME_LEN, VIDX_NAME, base, num);
  }

  return reinterpret_cast<const char *>(name);
}

static bool request_mdl_lock(THD *thd, const char *db_name,
                             const char *table_name, enum_mdl_type mdl_type,
                             enum_mdl_duration lock_duration,
                             std::string &error_message,
                             MDL_request *mdl_request = nullptr) {
  if (mdl_request == nullptr) {
    mdl_request = new (thd->mem_root) MDL_request;

    if (mdl_request == nullptr) {
      error_message = "Failed to allocate memory for mdl_request.";
      return true;
    }
  }

  MDL_REQUEST_INIT(mdl_request, MDL_key::TABLE, db_name, table_name, mdl_type,
                   lock_duration);
  if (thd->mdl_context.acquire_lock(mdl_request,
                                    thd->variables.lock_wait_timeout)) {
    error_message = "Failed to acquire DML lock.";
    return true;
  }

  return false;
}

static const dd::Table *open_hlindex_dd(THD *thd, const char *hlindex_name,
                                        const char *db_name,
                                        std::string &error_message) {
  /* Acquire the dd table */
  const dd::Table *hlindex_dd = nullptr;
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  if (thd->dd_client()->acquire(db_name, hlindex_name, &hlindex_dd)) {
    error_message = "Failed to acquire vector dd table.";
    return nullptr;
  }

  if (hlindex_dd == nullptr || !dd_table_is_hlindex(hlindex_dd)) {
    assert(false);

    error_message = "Can't find vector table in dd.";
    return nullptr;
  }

  assert(hlindex_dd->hidden() == dd::Abstract_table::HT_HIDDEN_HLINDEX ||
         hlindex_dd->hidden() == dd::Abstract_table::HT_HIDDEN_DDL);
  assert(dd_table_is_hlindex(hlindex_dd));

  return hlindex_dd;
}

static std::string sql_regex_replacement(
    const std::string &sql, const std::regex &pattern,
    std::string (*replacement)(const std::string &)) {
  std::string result;
  size_t last_pos = 0;
  std::sregex_iterator end;

  for (std::sregex_iterator it(sql.begin(), sql.end(), pattern); it != end;
       ++it) {
    std::smatch match = *it;
    if (!match[1].matched) break;

    result += sql.substr(last_pos, match.position() - last_pos);
    result += replacement(match[1].str());
    last_pos = match.position() + match.length();
  }

  if (last_pos == 0) {
    /* No match */
    return sql;
  }

  return result + sql.substr(last_pos);
}

static void rewrite_sql(THD *thd, const std::string &result) {
  /* Reset the thd's query string */
  char *new_query =
      strmake_root(thd->mem_root, result.c_str(), result.length());

  if (new_query == nullptr) {
    my_error(ER_DA_OOM, MYF(0));
    assert(false);

    return;
  }

  thd->set_query(new_query, strlen(new_query));
}

static std::string replacement_vector(const std::string &catching) {
  return std::string(RDS_COMMENT_VIDX_START "vector(") + catching +
         std::string(")" RDS_COMMENT_VIDX_END " varbinary(") +
         std::to_string(4 * stoi(catching)) + std::string(")");
}

// Rewrite the sql string. Replace vector(X) to
// /*!99999 vector(X) */ varbinary(4 * X)
// But avoid double replacement like:
// /*!99999 vector(X) */ varbinary(4 * X) =>
// /*!99999 /*!99999 vector(X) */ varbinary(4 * X) */ varbinary(4 * X)
static void rewrite_sql_of_vector_column(THD *thd) {
  /* Don't use '\b' after '\)' because it will not match space. */
  /* First check if the query already contains the processed format */
  std::string query_str = to_string(thd->query());

  /* Pattern to match already processed vector declarations */
  std::regex processed_pattern(
      R"(/\*!99999 vector\(\d+\) \*/ varbinary\(\d+\))");

  /* If not contain processed format, process the query */
  if (!std::regex_search(query_str, processed_pattern)) {
    rewrite_sql(
        thd, sql_regex_replacement(query_str,
                                   std::regex{R"(\bvector\s*\(\s*(\d+)\s*\))",
                                              std::regex_constants::icase},
                                   replacement_vector));
  }

  thd->m_query_has_vector_column = false;
  /* TODO: There is little possible that user use vector(x) as the name of
  table or other objects. */
}

/* Check if in one ddl query, other operations is performed while alter a
vector index. there are 3 results:
0: alter vector index,  and there are other operations performed in one query.
   --> set my_error and return true.
1: alter vector index, and there are not other operations performed.
   --> Replace the whole query string to be inside comment with version 99999.
2: not alter vector index, thus the keywords of vector column must be parsed.
   --> rewrite the keywords of vector column in the query.
*/
static uint check_alter_vector_ddl(THD *thd, Alter_info *alter_info,
                                   const uint key_count,
                                   const uint old_key_count, KEY *old_vidx,
                                   KEY *new_vidx) {
  assert(old_vidx == nullptr || old_vidx->flags & HA_VECTOR);
  assert(new_vidx == nullptr || new_vidx->flags & HA_VECTOR);

  if (old_vidx == nullptr) {
    assert(new_vidx != nullptr);

    if (alter_info->flags == Alter_info::ALTER_ADD_INDEX) {
      /* ADD a vector index. */
      assert(!thd->m_query_has_vector_column);
      assert(key_count > old_key_count);

      if ((key_count - old_key_count) == 1) {
        /* There are not other operations performed. */
        return 1;
      }
    }
  } else if (new_vidx == nullptr) {
    assert(old_vidx != nullptr);

    if (alter_info->flags == Alter_info::ALTER_DROP_INDEX) {
      /* DROP a vector index. */
      assert(!thd->m_query_has_vector_column);
      assert(old_key_count > key_count);

      if ((old_key_count - key_count) == 1) {
        /* There are not other operations performed. */
        return 1;
      }
    }
  } else if (my_strcasecmp(system_charset_info, new_vidx->name,
                           old_vidx->name) != 0) {
    /* RENAME a vector index. */
    if (alter_info->flags == Alter_info::ALTER_RENAME_INDEX &&
        alter_info->alter_rename_key_list.size() == 1) {
      /* There are not other operations performed. */
      assert(!thd->m_query_has_vector_column);
      assert(my_strcasecmp(system_charset_info,
                           alter_info->alter_rename_key_list[0]->old_name,
                           old_vidx->name) == 0);
      assert(my_strcasecmp(system_charset_info,
                           alter_info->alter_rename_key_list[0]->new_name,
                           new_vidx->name) == 0);

      return 1;
    }
  } else {
    /* The vector index is not modified. */
    /* Attention, ALTER_INDEX_VISIBILITY is not supported for the vector index.
     */
    return 2;
  }

  /* There are other operations performed with the vector index in the same
  ddl query. */
  return 0;
}

/* Return true if the vector field in the rec is NULL.
Otherwise, return false. */
static bool check_vector_is_null(TABLE *table, const uchar *rec, KEY *vec_key) {
  const ptrdiff_t offset = rec - table->record[0];
  Field *field = vec_key->key_part->field;

  assert(field->is_vector());

  return field->is_real_null(offset);
}

/* -------------------- External Functions Definition -------------------- */
bool check_vector_ddl_and_rewrite_sql(THD *thd, Alter_info *alter_info,
                                      KEY *key_info, const uint key_count,
                                      TABLE *table) {
  KEY *old_vidx = table->s->get_vec_key();
  KEY *new_vidx = nullptr;

  if (key_count > 0 && vidx::key_is_vector(key_info + key_count - 1)) {
    /* key_info is already sorted in mysql_prepare_create_table() */
    new_vidx = key_info + key_count - 1;
  }

  if (old_vidx == nullptr && new_vidx == nullptr) {
  rewrite_vector_column:
    if (thd->m_query_has_vector_column) {
      rewrite_sql_of_vector_column(thd);
    }

    return false;
  }

  switch (check_alter_vector_ddl(thd, alter_info, key_count,
                                 table->s->total_keys, old_vidx, new_vidx)) {
    case 1:
      /* The DDL query only alter the vector index. */
      rewrite_sql(thd, RDS_COMMENT_VIDX_START + to_string(thd->query()) +
                           RDS_COMMENT_VIDX_END);
      return false;

    case 2:
      /* The DDL query does not alter the vector index. */
      goto rewrite_vector_column;

    default:
      assert(0);
      [[fallthrough]];

    case 0:
      /* The DDL query not only alter the vector index, which is not
      supported yet. */
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "perform other operations while alter a vector index");
      return true;
  }
}

namespace hnsw {
uint get_ef_search(THD *thd) { return THDVAR(thd, hnsw_ef_search); }

uint index_options_print(const uint distance, const uint m, char *buf,
                         uint buf_len) {
  assert(validate_index_option_distance(distance));
  assert(validate_index_option_m(m));

  uint len = snprintf(buf, buf_len, " M=%d DISTANCE=%s" RDS_COMMENT_VIDX_END, m,
                      distance_names[distance]);

  if (len >= buf_len) {
    return buf_len - 1;
  }

  return len;
}

bool copy_index_option_m(THD *thd, uint *to, const uint from) {
  if (from == UINT_MAX) {
    /* distance is not set. */
    *to = THDVAR(thd, hnsw_default_m);
    return false;
  }

  if (!validate_index_option_m(from)) {
    return true;
  }

  *to = from;
  return false;
}
}  // namespace hnsw

bool copy_index_option_distance(THD *thd, uint *to, const uint from) {
  if (from == UINT_MAX) {
    /* distance is not set. */
    *to = THDVAR(thd, default_distance);
    return false;
  }

  assert(validate_index_option_distance(from));

  *to = from;
  return false;
}

bool create_table(THD *thd, KEY *key, dd::Table *dd_table, TABLE *table,
                  const char *db_name, const uint64_t old_table_id) {
  assert(key_is_vector(key));
  assert(dd_table->engine() == "InnoDB");

  std::string error_message;

  /* 1. Build table name and path */
  const char *hlindex_name =
      build_name(thd, dd_table->se_private_id(), vidx_num, error_message);
  if (hlindex_name == nullptr) {
  error_end:
    my_error(ER_VECTOR_INDEX_FAILED, MYF(0),
             thd_sql_command(thd) == SQLCOM_TRUNCATE ? "Truncate" : "Create",
             key->name, db_name, dd_table->name().c_str(),
             (hlindex_name == nullptr ? "?" : hlindex_name),
             error_message.c_str());
    return true;
  }

  char path[FN_REFLEN + 1];
  bool was_truncated;
  build_table_filename(path, sizeof(path) - 1 - reg_ext_length, db_name,
                       hlindex_name, "", 0, &was_truncated);
  // Check truncation, will lead to overflow when adding extension
  if (was_truncated) {
    my_error(ER_IDENT_CAUSES_TOO_LONG_PATH, MYF(0), sizeof(path) - 1, path);
    return true;
  }

  DBUG_EXECUTE_IF("crash_before_vidx_ddl", DBUG_SUICIDE(););
  DBUG_EXECUTE_IF("failed_before_vidx_ddl", {
    error_message = "debug failed before vidx ddl.";
    goto error_end;
  });

  /* 2. Request MDL X lock */
  if (request_mdl_lock(thd, db_name, hlindex_name, MDL_EXCLUSIVE,
                       MDL_TRANSACTION, error_message)) {
    goto error_end;
  }

  /* 3. Check if the hlindex name exists. */
  bool exists;
  if (dd::table_exists(thd->dd_client(), db_name, hlindex_name, &exists)) {
    return true;  // Error is already reported.
  }

  if (!exists &&
      ha_check_if_table_exists(thd, db_name, hlindex_name, &exists)) {
    /* Table doesn't exist. Check if some engine can provide it. */
    my_printf_error(ER_OUT_OF_RESOURCES,
                    "Failed to open '%-.64s', error while "
                    "unpacking from engine",
                    MYF(0), hlindex_name);
    return true;
  }

  if (exists) {
    error_message = "Vector table name exists.";
    goto error_end;
  }

  if (old_table_id == dd::INVALID_OBJECT_ID) {
    /* CREATE TABLE */
    /* 4. Create dd table and store it */
    std::unique_ptr<dd::Table> hlindex_dd_ptr = hnsw::create_dd_table(
        thd, hlindex_name, key, dd_table, table, db_name, get_tref_len(table));

    if (!hlindex_dd_ptr) {
      return true;  // Error is already reported.
    }

    if (thd->dd_client()->store(hlindex_dd_ptr.get())) {
      error_message = "Failed to store vector dd table.";
      goto error_end;
    }
  } else {
    /* TRUNCATE TABLE */
    assert(old_table_id != dd_table->se_private_id());

    /* 4. Rename old table. */
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    const dd::Schema *schema = nullptr;
    if (thd->dd_client()->acquire(db_name, &schema) || schema == nullptr) {
      error_message = "Failed to acquire schema.";
      goto error_end;
    }

    const char *old_name =
        build_name(thd, old_table_id, vidx_num, error_message);
    if (old_name == nullptr ||
        request_mdl_lock(thd, db_name, old_name, MDL_EXCLUSIVE, MDL_TRANSACTION,
                         error_message)) {
      goto error_end;
    }

    if (mysql_rename_table(thd, table->file->ht, db_name, old_name, db_name,
                           old_name, *schema, db_name, hlindex_name,
                           NO_DD_COMMIT | VIDX_RENAME)) {
      return true;
    }
  }

  /* 5. Get dd table. */
  dd::Table *hlindex_dd = nullptr;

  if (thd->dd_client()->acquire_for_modification(db_name, hlindex_name,
                                                 &hlindex_dd) ||
      hlindex_dd == nullptr) {
    error_message = "Failed to acquire_for_modification vector dd table.";
    goto error_end;
  }

  if (!dd_table_is_hlindex(hlindex_dd)) {
    error_message = "hlindex's name may be used by other tables.";
    goto error_end;
  }

  /* 6. Create table */
  HA_CREATE_INFO unused;
  if (ha_create_table(thd, path, db_name, hlindex_name, &unused, true, false,
                      hlindex_dd) != 0) {
    return true;  // Error is already reported.
  }

  /* 7. Update dd table */
  if (thd->dd_client()->update<dd::Table>(hlindex_dd)) {
    error_message = "Failed to update vector dd table.";
    goto error_end;
  }

  /* 8. Set hlindexes name of base dd table */
  vidx::dd_table_set_hlindexes(dd_table, key->name);

  return false;
}

bool delete_table(THD *thd, const dd::Table *dd_table, const char *db_name) {
  assert(dd_table_has_hlindexes(dd_table));
  assert(dd_table->engine() == "InnoDB");

  std::string error_message;

  /* 1. Build name and path. */
  const char *hlindex_name =
      build_name(thd, dd_table->se_private_id(), vidx_num, error_message);
  if (hlindex_name == nullptr) {
  error_end:
    dd::String_type key_name;
    dd_table_get_hlindexes(dd_table, &key_name);

    my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Drop", key_name.c_str(), db_name,
             dd_table->name().c_str(),
             (hlindex_name == nullptr ? "?" : hlindex_name),
             error_message.c_str());
    return true;
  }

  char path[FN_REFLEN + 1];
  bool was_truncated;
  build_table_filename(path, sizeof(path) - 1 - reg_ext_length, db_name,
                       hlindex_name, "", 0, &was_truncated);
  // Check truncation, will lead to overflow when adding extension
  if (was_truncated) {
    my_error(ER_IDENT_CAUSES_TOO_LONG_PATH, MYF(0), sizeof(path) - 1, path);
    return true;
  }

  DBUG_EXECUTE_IF("crash_before_vidx_ddl", DBUG_SUICIDE(););
  DBUG_EXECUTE_IF("failed_before_vidx_ddl", {
    error_message = "debug failed before vidx ddl.";
    goto error_end;
  });

  /* 2. Acquire the dd table with X mdl. */
  if (request_mdl_lock(thd, db_name, hlindex_name, MDL_EXCLUSIVE,
                       MDL_TRANSACTION, error_message)) {
    goto error_end;
  }

  const dd::Table *hlindex_dd =
      open_hlindex_dd(thd, hlindex_name, db_name, error_message);

  if (hlindex_dd == nullptr) {
    goto error_end;
  }

  /* 3. Drop table */
  handlerton *hton{nullptr};

  if (dd::table_storage_engine(thd, hlindex_dd, &hton)) {
    return true;
  }

  if (ha_delete_table(thd, hton, path, db_name, hlindex_name, hlindex_dd,
                      false)) {
    return true;
  }

  /* 4. remove the "__hlindexes__" option in base dd table. The base dd
  table may be used later in acquire_uncached_table() to build the base
  table share */
  ((dd::Table *)dd_table)->options().remove("__hlindexes__");

  /* 5. Drop dd table */
  return dd::drop_table(thd, db_name, hlindex_name, *hlindex_dd);
}

bool rename_table(THD *thd, dd::Table *dd_table, handlerton *base,
                  const dd::Schema &new_schema, const char *old_db,
                  const char *new_db, uint flags) {
  assert(dd_table_has_hlindexes(dd_table));
  assert(dd_table->engine() == "InnoDB");

  std::string error_message;

  /* 1. Build table name */
  const char *hlindex_name =
      build_name(thd, dd_table->se_private_id(), vidx_num, error_message);

  if (hlindex_name == nullptr) {
  error_end:
    dd::String_type key_name;
    dd_table_get_hlindexes(dd_table, &key_name);

    my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Rename", key_name.c_str(), new_db,
             dd_table->name().c_str(),
             (hlindex_name == nullptr ? "?" : hlindex_name),
             error_message.c_str());
    return true;
  }

  DBUG_EXECUTE_IF("crash_before_vidx_ddl", DBUG_SUICIDE(););
  DBUG_EXECUTE_IF("failed_before_vidx_ddl", {
    error_message = "debug failed before vidx ddl.";
    goto error_end;
  });

  /* 2. Request MDL X lock */
  if (request_mdl_lock(thd, new_db, hlindex_name, MDL_EXCLUSIVE,
                       MDL_TRANSACTION, error_message) ||
      request_mdl_lock(thd, old_db, hlindex_name, MDL_EXCLUSIVE,
                       MDL_TRANSACTION, error_message)) {
    goto error_end;
  }

  /* 3. Rename table */
  return mysql_rename_table(thd, base, old_db, hlindex_name, old_db,
                            hlindex_name, new_schema, new_db, hlindex_name,
                            flags | VIDX_RENAME);
}

bool build_hlindex_key(THD *thd, TABLE_SHARE *table_share,
                       const dd::Table *dd_table, const uint nr) {
  assert(dd_table_has_hlindexes(dd_table));
  assert(table_share->hlindex == nullptr);
  assert(table_share->hlindex_data == nullptr);
  assert(table_share->hlindexes() == 1);
  assert(nr == table_share->keys);

  std::string error_message;

  /* 1. Build name. */
  dd::String_type key_name;
  dd_table_get_hlindexes(dd_table, &key_name);

  const char *hlindex_name =
      build_name(thd, dd_table->se_private_id(), vidx_num, error_message);
  if (hlindex_name == nullptr) {
  error_end:
    my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Show", key_name.c_str(),
             table_share->db.str, table_share->table_name.str,
             (hlindex_name == nullptr ? "?" : hlindex_name),
             error_message.c_str());
    return true;
  }

  /* 2. Acquire the dd table with S mdl. */
  MDL_request mdl_request;
  if (request_mdl_lock(thd, table_share->db.str, hlindex_name, MDL_SHARED,
                       MDL_EXPLICIT, error_message, &mdl_request)) {
    goto error_end;
  }

  Scope_guard guard{[thd, &mdl_request]() {
    if (mdl_request.ticket != nullptr) dd::release_mdl(thd, mdl_request.ticket);
  }};

  const dd::Table *hlindex_dd =
      open_hlindex_dd(thd, hlindex_name, table_share->db.str, error_message);

  if (hlindex_dd == nullptr) {
    goto error_end;
  }

  /* 3. Build the key info. Do fill_index_from_dd() and
  fill_index_elements_from_dd(). */
  KEY *vec_key = &(table_share->key_info[nr]);
  KEY_PART_INFO *key_part = vec_key->key_part;
  MEM_ROOT *mem_root = &(table_share->mem_root);

  /* Don't assert table_share is not temp table, because the vector index may be
  in a temp table during the copy ddl. */

  vec_key->flags = HA_VECTOR;
  vec_key->name =
      strmake_root(mem_root, key_name.c_str(), key_name.length() + 1);
  vec_key->algorithm = HA_KEY_ALG_BTREE;
  vec_key->is_algorithm_explicit = false;
  vec_key->is_visible = true;
  vec_key->user_defined_key_parts = 1;
  vec_key->parser = nullptr;
  vec_key->engine_attribute.length = 0;
  vec_key->engine_attribute.str = nullptr;
  vec_key->secondary_engine_attribute.length = 0;
  vec_key->secondary_engine_attribute.str = nullptr;

  dd::String_type comment = hlindex_dd->comment();
  if (comment.length() > 0) {
    vec_key->comment.length = comment.length();
    vec_key->comment.str =
        strmake_root(mem_root, comment.c_str(), comment.length() + 1);
    vec_key->flags |= HA_USES_COMMENT;
  } else {
    vec_key->comment.length = 0;
  }

  hlindex_dd->options().get("__vector_m__", &(vec_key->vector_m));
  hlindex_dd->options().get("__vector_distance__", &(vec_key->vector_distance));
  hlindex_dd->options().get("__vector_column__", &(key_part->fieldnr));
  Field *field = key_part->field = table_share->field[key_part->fieldnr - 1];
  key_part->key_part_flag = 0;
  key_part->length = field->key_length();
  key_part->offset = field->offset(table_share->default_values);
  key_part->type = field->key_type();
  key_part->bin_cmp = ((field->real_type() != MYSQL_TYPE_VARCHAR &&
                        field->real_type() != MYSQL_TYPE_STRING) ||
                       (field->charset()->state & MY_CS_BINSORT));

  vec_key->key_length = key_part->length;
  table_share->keynames.type_names[nr] = vec_key->name;
  table_share->keys_in_use.set_bit(nr);
  table_share->visible_indexes.set_bit(nr);

  return false;
}

bool test_if_cheaper_vector_ordering(JOIN_TAB *tab, ORDER *order, ha_rows limit,
                                     int *order_idx) {
  if (order == nullptr || order->next != nullptr ||
      order->direction != ORDER_ASC ||
      !is_function_of_type(*order->item, Item_func::VECTOR_DISTANCE_FUNC)) {
    return false;
  }

  Item_func_vec_distance *item =
      down_cast<Item_func_vec_distance *>(*order->item);
  int item_idx = item->get_key();
  ha_rows rows;

  if (item_idx == -1) {
    /* args in function are not one vector column and one const value. */
    return false;
  }

  assert(item_idx >= 0);
  assert((uint)item_idx >= tab->table()->s->keys);
  assert((uint)item_idx < tab->table()->s->total_keys);

  if (tab->table()->force_index_order) {
    /* Handle the hint about force index. */
    if (tab->table()->keys_in_use_for_order_by.is_set(item_idx)) {
      if (limit == HA_POS_ERROR || limit > tab->table()->file->stats.records) {
        limit = tab->table()->file->stats.records;
      }

      goto use_vector_index;
    } else {
      return false;
    }
  }

  if (limit == HA_POS_ERROR && limit >= tab->table()->file->stats.records) {
    return false;
  }

  switch (tab->type()) {
    case JT_RANGE:
      rows = tab->range_scan()->num_output_rows();
      break;

    case JT_ALL:
    case JT_INDEX_SCAN:
      rows = tab->table()->file->stats.records;
      break;

    default:
      return false;
  }

  static_assert(SCAN_COST > 1);

  if (tab->index() == 0) {
    /* PRIMARY index scanning vs vector index scanning */
    if (limit > rows / SCAN_COST) {
      return false;
    }
  } else if (limit >= rows) {
    /* Secondary index scanning vs vector index scanning */
    return false;
  }

use_vector_index:
  assert(limit <= tab->table()->file->stats.records);
  assert(limit != HA_POS_ERROR);

  *order_idx = item_idx;
  item->set_limit(limit);

  tab->set_type(JT_INDEX_SCAN);
  tab->ref().key = item_idx;
  tab->ref().key_parts = 0;
  tab->set_index(item_idx);
  tab->set_vec_func(item);

  return true;
}
}  // namespace vidx

using namespace vidx;
using namespace vidx::hnsw;

int TABLE::hlindex_open(uint nr) {
  assert(s->hlindexes() == 1);
  assert(nr == s->keys);

  if (in_use->tx_isolation != ISO_READ_COMMITTED) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "other transaction isolation levels except READ COMMITTED for the "
             "vector index");
    return 1;
  }

  if (hlindex == nullptr) {
    std::string error_message;
    KEY *vec_key = s->key_info + nr;
    char path[FN_REFLEN + 1];

    /* 1.Build name. */
    const char *hlindex_name =
        build_name(in_use, s->m_se_private_id, vidx_num, error_message);

    if (hlindex_name == nullptr) {
    error_end:
      assert(0);
      my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Open", vec_key->name, s->db.str,
               s->table_name.str,
               (hlindex_name == nullptr ? "?" : hlindex_name),
               error_message.c_str());
      return 1;
    }

    /* 2. Acquire the dd table with S mdl. */
    if (request_mdl_lock(in_use, s->db.str, hlindex_name, MDL_SHARED_READ,
                         MDL_TRANSACTION, error_message)) {
      goto error_end;
    }

    const dd::Table *hlindex_dd =
        open_hlindex_dd(in_use, hlindex_name, s->db.str, error_message);

    if (hlindex_dd == nullptr) {
      goto error_end;
    }

    /* 3. Open the shared hlindex */
    s->lock_share();
    Scope_guard guard{[this]() { s->unlock_share(); }};

    if (s->hlindex == nullptr) {
      /* Build the table key. */
      MDL_key dml_key{MDL_key::TABLE, s->db.str, hlindex_name};
      size_t key_length = dml_key.length() - 1;
      const char *key = (const char *)dml_key.ptr() + 1;

      /* Open the table hlindex */
      if (s->tmp_table != NO_TMP_TABLE) {
        /* Base table is temp. */
        bool was_truncated;
        build_table_filename(path, sizeof(path) - 1 - reg_ext_length, s->db.str,
                             hlindex_name, "", 0, &was_truncated);

        s->hlindex = reinterpret_cast<TABLE_SHARE *>(
            in_use->mem_root->Alloc(sizeof(TABLE_SHARE)));

        init_tmp_table_share(in_use, s->hlindex, key, key_length,
                             strend(key) + 1, path, nullptr);
      } else if ((s->hlindex = alloc_table_share(s->db.str, hlindex_name, key,
                                                 key_length, false)) ==
                 nullptr) {
        /* Base table is normal. */
        error_message = "Failed to alloc_table_share.";
        goto error_end;
      }

      if (open_table_def(in_use, s->hlindex, *hlindex_dd)) {
        error_message = "Failed to open_table_def.";
        goto error_end;
      }

      s->hlindex->is_hlindex = true;

      assert(s->hlindex->hlindex_data == nullptr);
      assert(s->hlindex->hlindex == nullptr);
    }

    /* 4. Open a new hlindex */
    hlindex =
        (TABLE *)my_malloc(key_memory_TABLE, sizeof(*hlindex), MYF(MY_WME));

    if (hlindex == nullptr) {
      error_message = "Failed to my_malloc hlindex table.";
      goto error_end;
    }

    if (s->hlindex->tmp_table == NO_TMP_TABLE) {
      mysql_mutex_lock(&LOCK_open);
      s->hlindex->increment_ref_count();
      mysql_mutex_unlock(&LOCK_open);
    }

    int error = open_table_from_share(in_use, s->hlindex, hlindex_name,
                                      (uint)(HA_OPEN_KEYFILE | HA_OPEN_RNDFILE |
                                             HA_GET_INDEX | HA_TRY_READ_ONLY),
                                      EXTRA_RECORD, in_use->open_options,
                                      hlindex, false, hlindex_dd);

    if (error != 0 || hlindex == nullptr) {
      error_message = "Failed to open_table_from_share.";
      goto error_end;
    }

    hlindex->in_use = nullptr;
  }

  return 0;
}

int TABLE::hlindex_lock(uint nr [[maybe_unused]]) {
  assert(s->hlindexes() == 1);
  assert(nr == s->keys);
  assert(hlindex);

  if (hlindex->in_use != in_use) {
    hlindex->file->rebind_psi();
    hlindex->file->ha_extra(HA_EXTRA_RESET_STATE);

    hlindex->reset();
    hlindex->set_created();
    hlindex->use_all_columns();

    /* mark in use for this query */
    hlindex->in_use = in_use;
    /* use the main table's lock_descriptor. */
    hlindex->pos_in_table_list = pos_in_table_list;

    assert(hlindex->file->lock_count() <= 1);

    return hlindex->file->ha_external_lock(
        in_use, reginfo.lock_type < TL_WRITE_ALLOW_WRITE ? F_RDLCK : F_WRLCK);
  }

  return 0;
}

int TABLE::reset_hlindexes() {
  if (hlindex && hlindex->in_use) {
    hlindex->in_use = nullptr;
    hlindex->pos_in_table_list = nullptr;
  }

  return 0;
}

int TABLE::hlindexes_on_insert() {
  assert(s->hlindexes() == 1 || s->hlindexes() == 0);

  for (uint key = s->keys; key < s->total_keys; key++) {
    if (vidx::check_vector_is_null(this, record[0], key_info + key)) {
      continue;
    }

    int err;

    if ((err = hlindex_open(key)) || (err = hlindex_lock(key)) ||
        (err = mhnsw_insert(this, key_info + key))) {
      return err;
    }
  }

  return 0;
}

int TABLE::hlindexes_on_update() {
  assert(s->hlindexes() == 1 || s->hlindexes() == 0);

  for (uint key = s->keys; key < s->total_keys; key++) {
    const bool old_is_null =
        vidx::check_vector_is_null(this, record[1], key_info + key);
    const bool new_is_null =
        vidx::check_vector_is_null(this, record[0], key_info + key);

    if (old_is_null && new_is_null) {
      continue;
    }

    // TODO: if tref and vector are not changed, update should be all skipped.

    int err;

    if ((err = hlindex_open(key)) || (err = hlindex_lock(key)) ||
        (err = old_is_null
                   ? 0
                   : mhnsw_invalidate(this, record[1], key_info + key)) ||
        (err = new_is_null ? 0 : mhnsw_insert(this, key_info + key))) {
      return err;
    }
  }

  return 0;
}

int TABLE::hlindexes_on_delete(const uchar *buf) {
  assert(s->hlindexes() == 1 || s->hlindexes() == 0);
  assert(buf == record[0] || buf == record[1]);  // note: REPLACE

  for (uint key = s->keys; key < s->total_keys; key++) {
    if (vidx::check_vector_is_null(this, buf, key_info + key)) {
      continue;
    }

    int err;

    if ((err = hlindex_open(key)) || (err = hlindex_lock(key)) ||
        (err = mhnsw_invalidate(this, buf, key_info + key))) {
      return err;
    }
  }

  return 0;
}

int TABLE::hlindexes_on_delete_all() {
  assert(s->hlindexes() == 1 || s->hlindexes() == 0);

  for (uint key = s->keys; key < s->total_keys; key++) {
    int err;

    if ((err = hlindex_open(key)) || (err = hlindex_lock(key)) ||
        (err = mhnsw_delete_all(this, key_info + key))) {
      return err;
    }
  }

  return 0;
}

int TABLE::hlindex_read_first(uint key, void *item) {
  assert(s->hlindexes() == 1);
  assert(key == s->keys);

  int err;

  if ((err = hlindex_open(key)) || (err = hlindex_lock(key)) ||
      (err = mhnsw_read_first(this, key_info + key, (Item *)item))) {
    return err;
  }

  return 0;
}

int TABLE::hlindex_read_next() { return mhnsw_read_next(this); }

int TABLE::hlindex_read_end() { return mhnsw_read_end(this); }

mysql_declare_plugin(vidx){
    MYSQL_DAEMON_PLUGIN,
    &vidx::daemon,
    "vidx",
    "AliCloud",
    "A plugin for vector index algorithm", /* Plugin name */
    PLUGIN_LICENSE_GPL,
    vidx::plugin_init, /* Plugin Init */
    nullptr,
    vidx::plugin_deinit, /* Plugin Deinit */
    0x0100,              /* Plugin Version: major.minor */
    nullptr,             /* status variables */
    vidx::sys_vars,      /* system variables */
    nullptr,             /* config options */
    0,                   /* flags */
} mysql_declare_plugin_end;
