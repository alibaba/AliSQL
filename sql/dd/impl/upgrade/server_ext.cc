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

#include "server_ext.h"
#include "scope_guard.h"
#include "sql/duckdb/duckdb_table.h"
#include "sql/sql_thd_internal_api.h"

namespace dd {
namespace upgrade {

namespace myduck {

using Schemas = std::map<String_type, std::vector<String_type>>;
using Foreign_keys = std::map<String_type, std::vector<String_type>>;

/**
  Check if schema is system schema.
  @param[in]  schema  Schema name.
  @return true if schema is system schema, otherwise false.
*/
static bool is_system_schema(const dd::String_type &schema) {
  return (schema.compare(INFORMATION_SCHEMA_NAME.str) == 0 ||
          schema.compare(PERFORMANCE_SCHEMA_DB_NAME.str) == 0 ||
          schema.compare(MYSQL_SCHEMA_NAME.str) == 0 ||
          schema.compare("sys") == 0 ||
          (duckdb_convert_all_skip_mtr_db && (schema.compare("mtr") == 0)));
}

/** Convert tables to DuckDB in parallel. */
class Convert_all_to_duckdb {
 public:
  Convert_all_to_duckdb(std::vector<String_type> *schemas, Schemas *schema_to_tables, Foreign_keys *fks, uint threads,
                        bool ignore_error)
      : m_schemas(schemas),
        m_schema_to_tables(schema_to_tables),
        m_foreign_keys(fks),
        m_n_threads(threads),
        m_error(false),
        m_ignore_error(ignore_error) {}

  /**
    Execute convert all to duckdb.
    @return false ON SUCCESS, otherwise ON FAILURE
  */
  bool execute(bool convert_all_tables) {
    /* Create schemas. */
    std::thread create_schemas_thread(
        &Convert_all_to_duckdb::create_duckdb_schemas_thread, this);
    create_schemas_thread.join();

    if (convert_all_tables) {
      /* Drop foreign key. */
      std::thread drop_fks_thread(
          &Convert_all_to_duckdb::drop_foreign_keys_thread, this);
      drop_fks_thread.join();

      /* Alter tables. */
      std::vector<std::thread> threads;
      for (uint i = 0; i < m_n_threads; i++) {
        threads.emplace_back(
            std::thread(&Convert_all_to_duckdb::alter_table_thread, this));
      }

      for (auto &t : threads) {
        t.join();
      }
    }

    return is_error();
  }

 private:
  /** Schemas to convert. */
  std::vector<String_type> *m_schemas;

  /** Tables to convert. */
  Schemas *m_schema_to_tables;

  /** Foreign keys. */
  Foreign_keys *m_foreign_keys;

  /** Number of threads. */
  uint m_n_threads;

  /** Error flag. */
  std::atomic<bool> m_error;

  /** Ignore error flag. */
  bool m_ignore_error;

  /** Mutex to protect m_schema_to_tables. */
  std::mutex m_mutex;

  /** RAII to lock and unlock mutex. */
  class Mutex_guard {
   public:
    explicit Mutex_guard(std::mutex *mutex) : m_mutex(mutex) {
      assert(m_mutex != nullptr);
      m_mutex->lock();
    }

    ~Mutex_guard() { m_mutex->unlock(); }

   private:
    std::mutex *m_mutex;
  };

  /**
    Fetch table name from m_schema_to_tables.
    NOTE: table_name is full name(`schema`.`table`)

    @param[in, out]  table_name  table name
    @return true if table name is fetched, otherwise false.
  */
  bool fetch_table_name(String_type &table_name) {
    bool found = false;
    Mutex_guard guard(&m_mutex);

    for (auto it_s = m_schema_to_tables->begin(); it_s != m_schema_to_tables->end();
         /* no op */) {
      std::vector<String_type> &table_names = it_s->second;
      if (table_names.empty()) {
        it_s = m_schema_to_tables->erase(it_s);
        continue;
      }

      String_type schema_name = it_s->first;
      auto it_t = table_names.begin();
      table_name = "`" + schema_name + "`.`" + (*it_t) + "`";
      table_names.erase(it_t);
      found = true;
      break;
    }

    return found;
  }

  /**
    Check if error happens.
    @return true if error happens, otherwise false.
  */
  bool is_error() {
    if (m_ignore_error) {
      return false;
    }

    return m_error.load();
  }

  /**
    Worker thread of creating schemas in DuckDB.
  */
  void create_duckdb_schemas_thread() {
    my_thread_init();
    assert(current_thd == nullptr);
    THD *thd = create_internal_thd();
    assert(current_thd == thd);

    for (auto &schema : *m_schemas) {
      if (is_system_schema(schema)) continue;
      char *db_str = strdup_root(thd->mem_root, schema.c_str());
      /* Only DuckDB schema will be created currently. */
      ha_create_database(db_str);
    }
    /* Commit transaction to ensure the schemas are created. */
    dd::end_transaction(thd, false);

    destroy_internal_thd(thd);
    assert(current_thd == nullptr);
    my_thread_end();
  }

  /**
    Worker thread of dropping foreign keys in InnoDB.
  */
  void drop_foreign_keys_thread() {
    my_thread_init();
    assert(current_thd == nullptr);
    THD *thd = create_internal_thd();
    assert(current_thd == thd);

    {
      Disable_autocommit_guard autocommit_guard(thd);
      Disable_binlog_guard disable_binlog(thd);
      Disable_sql_log_bin_guard disable_sql_log_bin(thd);

      for (auto &pair : *m_foreign_keys) {
        for (auto &fk : pair.second) {
          Ed_connection con(thd);
          LEX_STRING str;

          thd->set_query_id(next_query_id());
          String_type query =
              "ALTER TABLE " + pair.first + " DROP FOREIGN KEY " + fk;
          lex_string_strmake(thd->mem_root, &str, query.c_str(), query.size());
          bool ret [[maybe_unused]] = con.execute_direct(str);
          assert(!ret);
        }
      }
    }

    destroy_internal_thd(thd);
    assert(current_thd == nullptr);
    my_thread_end();
  }

  /**
    Alter table to convert to DuckDB.
    @param[in]  table_name  table name.
  */
  void alter_table(const String_type &table_name) {
    THD *thd = current_thd;
    Ed_connection con(thd);
    LEX_STRING str;

    thd->set_query_id(next_query_id());
    LogErr(INFORMATION_LEVEL, ER_SERVER_CONVERT_DUCKDB_TABLE,
           table_name.c_str());
    String_type query = "ALTER TABLE " + table_name + " ENGINE = DuckDB";
    lex_string_strmake(thd->mem_root, &str, query.c_str(), query.size());
    bool ret = con.execute_direct(str);

    bool expected = m_error.load();
    bool desired;
    do {
      desired = expected | ret;
    } while (!m_error.compare_exchange_weak(expected, desired));
  }

  /**
    Worker thread of converting table to DuckDB engine.
  */
  void alter_table_thread() {
    my_thread_init();
    assert(current_thd == nullptr);
    THD *thd = create_internal_thd();
    assert(current_thd == thd);

    /*
      Specify thread_id explicitly to avoid creating a temporary table with
      the same name during DDL.
    */
    {
      thd->set_new_thread_id();
      plugin_thdvar_init(thd, true);

      Disable_autocommit_guard autocommit_guard(thd);
      Disable_binlog_guard disable_binlog(thd);
      Disable_sql_log_bin_guard disable_sql_log_bin(thd);

      String_type full_name;
      while (fetch_table_name(full_name) && !is_error()) {
        assert(!full_name.empty());
        alter_table(full_name);
      }

      plugin_thdvar_cleanup(thd, true);
    }
    destroy_internal_thd(thd);
    assert(current_thd == nullptr);
    my_thread_end();
  }
};

/**
  Get all table names under the schema.
  Refers MySQL_check::get_schema_tables.
  @param[in]  thd       Thread handle.
  @param[in]  schema    Schema name.
  @param[out] tables    Table names.
  @param[out] fks       Foreign keys.
  @retval false  ON SUCCESS
  @retval true   ON FAILURE
*/
static bool get_schema_tables(THD *thd, const char *schema,
                              std::vector<String_type> *tables,
                              Foreign_keys *fk_tables) {
  Schema_MDL_locker mdl_handler(thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Schema *sch = nullptr;
  dd::Stringstream_type t_list;
  bool ret = false;

  if (mdl_handler.ensure_locked(schema) ||
      thd->dd_client()->acquire(schema, &sch) ||
      thd->dd_client()->fetch_schema_component_names<Abstract_table>(sch,
                                                                     tables)) {
    LogErr(ERROR_LEVEL, ER_DD_UPGRADE_FAILED_TO_FETCH_TABLES);
    return (true);
  }

  const char *schema_name = sch->name().c_str();
  /* For RDS, lower_case_table_names will not be 2. */
  for (auto it = tables->begin(); it != tables->end(); /* no op */) {
    const char *table_name = it->c_str();
    MDL_request table_request;
    MDL_REQUEST_INIT(&table_request, MDL_key::TABLE, schema_name, table_name,
                     MDL_SHARED, MDL_EXPLICIT);
    if (thd->mdl_context.acquire_lock(&table_request,
                                      thd->variables.lock_wait_timeout)) {
      return true;
    }

    dd::cache::Dictionary_client::Auto_releaser table_releaser(
        thd->dd_client());
    const dd::Abstract_table *table_obj = nullptr;
    if (thd->dd_client()->acquire(schema_name, table_name, &table_obj))
      return true;

    if (table_obj == nullptr) {
      /* table has been dropped or renamed. */
      continue;
    }

    const dd::Table *table = dynamic_cast<const dd::Table *>(table_obj);
    if (table_obj->type() != dd::enum_table_type::BASE_TABLE ||
        table == nullptr ||
        my_strcasecmp(system_charset_info, table->engine().c_str(), "InnoDB") !=
            0) {
      thd->mdl_context.release_lock(table_request.ticket);
      it = tables->erase(it);
      continue;
    }

    if (table->foreign_keys().size() > 0) {
      String_type full_name =
          "`" + String_type(schema) + "`.`" + table->name() + "`";
      for (const dd::Foreign_key *fk : table->foreign_keys()) {
        String_type fk_name = "`" + fk->name() + "`";
        ((*fk_tables)[full_name]).push_back(fk_name);
      }
    }

    if (!duckdb_convert_all_at_startup_ignore_error) {
      LogErr(INFORMATION_LEVEL, ER_CHECKING_TABLE_BEFORE_CONVERT_DUCKDB,
             table->name().c_str());
      ret |= ::myduck::precheck_convert_to_duckdb(table);
    }

    thd->mdl_context.release_lock(table_request.ticket);
    it++;
  }

  return ret;
}

enum enum_convert_stage {
  CONVERT_EMPTY = 0,
  CONVERT_INIT,
  CONVERT_CHECKING,
  CONVERT_CHECK_FAILED,
  CONVERT_CONVERTING,
  CONVERT_CONVERT_FAILED,
  CONVERT_FINISHED,
  CONVERT_END
};

const char *const convert_stage_string[CONVERT_END] = {
    "EMPTY",      "INIT",           "CHECKING", "CHECK_FAILED",
    "CONVERTING", "CONVERT_FAILED", "FINISHED"};

enum_convert_stage convert_stage = enum_convert_stage::CONVERT_EMPTY;

/**
  Convert all non-DuckDB tables (except system schemas) to DuckDB engine.
  Refers MySQL_check::check_all_schemas.

  If failed, convert_stage will be set to CONVERT_CHECK_FAILED or
  CONVERT_FAILED.
*/
static void alter_all_schemas() {
  THD *thd = create_internal_thd();
  thd->set_new_thread_id();
  current_thd = thd;
  auto destroy_thd = create_scope_guard([&]() { destroy_internal_thd(thd); });

  convert_stage = enum_convert_stage::CONVERT_INIT;
  DBUG_EXECUTE_IF("sleep_before_alter_all_schemas", my_sleep(6000000););

  ErrorHandlerFunctionPointer existing_hook = error_handler_hook;
  auto grd = create_scope_guard([&]() { error_handler_hook = existing_hook; });
  error_handler_hook = my_message_stderr;
  Disable_autocommit_guard autocommit_guard(thd);
  Bootstrap_error_handler bootstrap_error_handler;
  bootstrap_error_handler.set_log_error(true);
  Server_option_guard<bool> acl_guard(&opt_noacl, true);
  Server_option_guard<bool> general_log_guard(&opt_general_log, false);
  Server_option_guard<bool> slow_log_guard(&opt_slow_log, false);
  Disable_binlog_guard disable_binlog(thd);
  Disable_sql_log_bin_guard disable_sql_log_bin(thd);

  bool err = false;
  bool ignore_error = duckdb_convert_all_at_startup_ignore_error;
  uint n_threads = duckdb_convert_all_at_startup_threads;
  std::vector<String_type> schemas;
  Schemas schema_to_tables;
  Foreign_keys table_to_fks;

  convert_stage = enum_convert_stage::CONVERT_CHECKING;
  if (thd->dd_client()->fetch_global_component_names<dd::Schema>(&schemas)) {
    convert_stage = enum_convert_stage::CONVERT_CHECK_FAILED;
    goto end;
  }

  for (String_type &schema : schemas) {
    if (is_system_schema(schema)) {
      continue;
    }

    if (duckdb_convert_all_at_startup) {
      /* If error happens in get_schema_tables, we should not ignore the error. */
      LogErr(INFORMATION_LEVEL, ER_CHECKING_DB_BEFORE_CONVERT_DUCKDB,
            schema.c_str());
      if (get_schema_tables(thd, schema.c_str(), &schema_to_tables[schema],
                            &table_to_fks)) {
        convert_stage = enum_convert_stage::CONVERT_CHECK_FAILED;
        goto end;
      }
    }
  }

  {
    convert_stage = enum_convert_stage::CONVERT_CONVERTING;
    Convert_all_to_duckdb executor(&schemas, &schema_to_tables, &table_to_fks, n_threads,
                                   ignore_error);
    if (executor.execute(duckdb_convert_all_at_startup)) {
      convert_stage = enum_convert_stage::CONVERT_CONVERT_FAILED;
    } else {
      convert_stage = enum_convert_stage::CONVERT_FINISHED;
    }
  }

end:
  err = (convert_stage != enum_convert_stage::CONVERT_FINISHED);
  if (err) {
    LogErr(ERROR_LEVEL, ER_SERVER_CONVERT_DUCKDB_FAILED);
  }

  close_thread_tables(thd);
  close_cached_tables(nullptr, nullptr, false, LONG_TIMEOUT);
  dd::end_transaction(thd, err);
}

static PSI_thread_key key_thread_duckdb_convertor;
static my_thread_handle duckdb_convertor_thread_id;

static PSI_thread_info all_threads[] = {
    {&key_thread_duckdb_convertor, "duckdb_convert", "duckdb_cvt",
     PSI_FLAG_THREAD_SYSTEM, 0, PSI_DOCUMENT_ME}};

static void *convert_thread(void *MY_ATTRIBUTE((unused))) {
  my_thread_init();
  on_duckdb_convert_progress.store(true);
  alter_all_schemas();
  on_duckdb_convert_progress.store(false);
  my_thread_end();
  my_thread_exit(0);
  return 0;
}

void create_duckdb_convertor_thread() {
  my_thread_attr_t duckdb_attr;
  my_thread_attr_init(&duckdb_attr);
  /* Set detach. */
  my_thread_attr_setdetachstate(&duckdb_attr, MY_THREAD_CREATE_DETACHED);
  pthread_attr_setscope(&duckdb_attr, PTHREAD_SCOPE_SYSTEM);

#ifdef HAVE_PSI_INTERFACE
  const char *category = "sql";
  int count;
  count = static_cast<int>(array_elements(all_threads));
  mysql_thread_register(category, all_threads, count);
#endif  // HAVE_PSI_INTERFACE

  /* THD will be released in alter_all_schema if create. */
  if (mysql_thread_create(key_thread_duckdb_convertor,
                          &duckdb_convertor_thread_id, &duckdb_attr,
                          convert_thread, nullptr)) {
    LogErr(ERROR_LEVEL, ER_SERVER_CONVERT_DUCKDB_FAILED);
  }

  my_thread_attr_destroy(&duckdb_attr);
}

int show_convert_stage(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_CHAR;
  strncpy(buff, convert_stage_string[convert_stage], SHOW_VAR_FUNC_BUFF_SIZE);
  buff[SHOW_VAR_FUNC_BUFF_SIZE - 1] = 0;
  var->value = buff;
  return 0;
}
}  // namespace myduck

}  // namespace upgrade
}  // namespace dd
