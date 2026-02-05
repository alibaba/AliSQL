/* Copyright (c) 2023 Alibaba and/or its affiliates. All rights reserved.

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

/**
  @file

  Extends the binlog code for AliSQL
*/

#include "binlog_ext.h"

#include "handler.h"
#include "log.h"
#include "mutex_lock.h"
#include "my_dbug.h"
#include "my_thread.h"
#include "mysqld.h"
#include "opt_costconstantcache.h"
#include "rpl_gtid.h"
#include "rpl_info_factory.h"
#include "transaction.h"
#include "tztime.h"
#include <sstream>
#include <fcntl.h>
#include <unistd.h>

#include "duckdb/duckdb_config.h"
#include "duckdb/duckdb_context.h"
#include "duckdb/duckdb_query.h"

Binlog_ext mysql_bin_log_ext;

Binlog_ext::Binlog_ext() {}

bool Binlog_ext::duckdb_commit(THD *thd) {
  DBUG_ENTER("Binlog_ext::duckdb_commit");
  my_off_t total_bytes = 0;
  bool do_rotate = false;
  bool error = false;

  thd->get_transaction()->m_flags.run_hooks = false;

  DBUG_EXECUTE_IF("crash_commit_before_log", DBUG_SUICIDE(););

  mysql_mutex_lock(&mysql_bin_log.LOCK_log);
  // used for truncate binlog if error happens
  mysql_bin_log.lock_binlog_end_pos();
  my_off_t before_pos = mysql_bin_log.binlog_end_pos;
  mysql_bin_log.unlock_binlog_end_pos();

  mysql_bin_log.assign_automatic_gtids_to_flush_group(thd);
  {
    std::pair<int, my_off_t> result = mysql_bin_log.flush_thread_caches(thd);
    error = result.first;
    total_bytes = result.second;
  }

  if (error == 0 && total_bytes > 0) {
    my_off_t flush_end_pos;
    error = mysql_bin_log.flush_cache_to_file(&flush_end_pos);
  }

  if (error == 0 && total_bytes > 0) {
    std::pair<bool, bool> result =
      mysql_bin_log.sync_binlog_file(false);
    error = result.first;
  }

  if (error) {
    mysql_bin_log.handle_binlog_flush_or_sync_error(
        thd, false /* need_lock_log */,
        (thd->commit_error == THD::CE_FLUSH_GNO_EXHAUSTED_ERROR)
            ? ER_THD(thd, ER_GNO_EXHAUSTED)
            : nullptr);
  }
  DBUG_EXECUTE_IF("crash_after_binlog_sync", DBUG_SUICIDE(););
  DEBUG_SYNC(thd, "after_binlog_sync");


  /*
    For the case DuckDB engine does not envovled into the transaction,
    A duckdb transaction should be started and the handlerton is registered
    to Server layer here for updating duckdb_binlog_position table.
  */

  auto duckdb_ctx = thd->get_duckdb_context();
  if (!duckdb_ctx->has_transaction() && duckdb_ctx->duckdb_trans_begin())
    assert(1);
  if (thd->get_transaction()->is_active(Transaction_ctx::SESSION)) {
    trans_register_ha(thd, true, myduck_hton, nullptr);
  } else if (thd->get_transaction()->is_active(Transaction_ctx::STMT)) {
    /*
      If a SQL does nothing(e.g ALTER TABLE t1), it even doesn't open the
      table. It will be the status that both transaction and stmt is not
      active. In this case, ha_commit_trans will not be called.
      tc_log->commit is called directly. It should not be registered
      in this case.
    */
    trans_register_ha(thd, false, myduck_hton, nullptr);
  }

  std::ostringstream stmt;
  /* Different from 8.0, 8.0 uses get_real_file_size(). */
  stmt << "INSERT INTO mysql.duckdb_binlog_position VALUES('" << 
    mysql_bin_log.get_log_fname()
    << "', " << mysql_bin_log.log_file.pos_in_file << ")";

  auto query_res = myduck::duckdb_query(thd, stmt.str(), false);

  // For the case SQL does nothing, commit DuckDB Engine here.
  if (!query_res->HasError() &&
    !thd->get_transaction()->is_active(Transaction_ctx::SESSION) &&
    !thd->get_transaction()->is_active(Transaction_ctx::STMT)) {
      query_res = myduck::duckdb_query(thd, "COMMIT", false);
      DBUG_EXECUTE_IF("crash_after_duckdb_commit", DBUG_SUICIDE(););
  }

  if (query_res->HasError()) {
    thd->commit_error = THD::CE_COMMIT_ERROR;
    my_error(ER_DUCKDB_COMMIT_ERROR, MYF(0), query_res->GetError().c_str());
  }

  if (thd->commit_error == THD::CE_NONE) {
    ::finish_transaction_in_engines(thd,
        thd->get_transaction()->m_flags.real_commit, false);
  }

  // finish_transaction_in_engines may return CE_COMMIT_ERROR
  if (thd->commit_error == THD::CE_COMMIT_ERROR) {
    /* Different from 8.0, here we need truncate binlog file and IO cache both.*/
    mysql_bin_log_ext.truncate(mysql_bin_log.get_log_fname(), before_pos);
    reinit_io_cache(&mysql_bin_log.log_file, WRITE_CACHE, before_pos, false,
                      before_pos < mysql_bin_log.log_file.pos_in_file /*clear_cache*/);
    (void) mysql_bin_log.finish_commit(thd);
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

    DBUG_RETURN(true);
  }

  mysql_bin_log.update_binlog_end_pos();

  /* Different from 8.0, 8.0 uses get_real_file_size(). */
  mysql_bin_log.lock_binlog_end_pos();
  do_rotate = (mysql_bin_log.binlog_end_pos >=
               (my_off_t)mysql_bin_log.max_size);
  mysql_bin_log.unlock_binlog_end_pos();
  (void) mysql_bin_log.finish_commit(thd);
  mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

   /*
    If we need to rotate, we do it without commit error.
    Otherwise the thd->commit_error will be possibly reset.
   */
  if (DBUG_EVALUATE_IF("force_rotate", 1, 0) ||
      (do_rotate && thd->commit_error == THD::CE_NONE)) {
    bool check_purge = false;
    mysql_mutex_lock(&mysql_bin_log.LOCK_log);
    /*
      The transaction has committed, thus we ignore rotation error
      here. The server will crash if any severe error happens during
      rotation.
    */
    (void)mysql_bin_log.rotate(false, &check_purge);
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

    if (check_purge)
      mysql_bin_log.purge();
  }
  DBUG_RETURN(false);
}

bool Binlog_ext::duckdb_binlog_init() {
  using namespace myduck;
  std::string query =
    "SELECT 1 FROM information_schema.tables "
    "WHERE table_schema = 'mysql' AND table_name = 'duckdb_binlog_position'";
  auto res = duckdb_query(query);
  if (res->HasError()) {
    sql_print_error(ER(ER_DUCKDB), "Failed to SELECT information_schema.tables");
    return true;
  }

  auto mres= res->Cast<duckdb::StreamQueryResult>().Materialize();
  if (mres->RowCount() > 0) return false;

  sql_print_information("Create duckdb_binlog_position table");

  query = "CREATE SCHEMA IF NOT EXISTS mysql";
  res = duckdb_query(query);
  if (res->HasError()) goto err;

  query = "CREATE TABLE mysql.duckdb_binlog_position("
          "  file VARCHAR(128) NOT NULL,"
          "  position BIGINT NOT NULL)";
  res = duckdb_query(query);
  if (res->HasError()) goto err;

  return false;
err:
  sql_print_error(ER(ER_DUCKDB), "Failed to initialize duckdb_binlog_position");
  return true;
}

bool Binlog_ext::duckdb_binlog_rotate() {
  if (!duckdb_global_mode_on()) return false;

  // Ignore truncate error here. It should not crash the server.
  auto query_res =
    myduck::duckdb_query("TRUNCATE TABLE mysql.duckdb_binlog_position");

  DBUG_EXECUTE_IF("crash_during_duckdb_binlog_rotate", DBUG_SUICIDE(););

  std::ostringstream stmt;
  /* Different from 8.0, 8.0 uses get_real_file_size(). */
  mysql_bin_log.lock_binlog_end_pos();
  stmt << "INSERT INTO mysql.duckdb_binlog_position VALUES('" << 
    mysql_bin_log.get_log_fname() << "', " << 
    mysql_bin_log.binlog_end_pos << ")";
  mysql_bin_log.unlock_binlog_end_pos();
  query_res = myduck::duckdb_query(stmt.str());

  return DBUG_EVALUATE_IF("simulate_duckdb_binlog_roate_error", true,
                          query_res->HasError());
}

bool Binlog_ext::duckdb_recover(const char* log_name) {
  IO_CACHE log;
  const char *err;
  Log_event *ev;
  Format_description_log_event fdle(BINLOG_VERSION);
  if (open_binlog_file(&log, log_name, &err) < 0)
  {
    sql_print_error("%s", err);
    return true;
  }

  if (!(ev= Log_event::read_log_event(&log, 0, &fdle,
                                       opt_master_verify_checksum)) &&
        ev->get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT &&
        (ev->common_header->flags & LOG_EVENT_BINLOG_IN_USE_F ||
         DBUG_EVALUATE_IF("eval_force_bin_log_recovery", true, false)))
    return false;

  /* The log_name must be same with 'mysql_bin_log.get_log_fname()'. */
  std::string stmt;
  stmt = "SELECT max(position) FROM mysql.duckdb_binlog_position WHERE file = '";
  stmt += log_name;
  stmt += "'";
  auto res = myduck::duckdb_query(stmt);

  if (res->HasError()) {
    sql_print_error(ER(ER_DUCKDB), "Failed to read positon from mysql.duckdb_binlog_position");
    return true;
  }

  auto mres= res->Cast<duckdb::StreamQueryResult>().Materialize();
  sql_print_information(mres->ToString().c_str());

  if (mres->RowCount() == 0) return false;
  if (mres->GetValue(0,0).IsNull()) return false;

  auto pos = mres->GetValue<int64_t>(0, 0);
  if (pos == 0) return false;

  /* Different from 8.0, here we need truncate binlog file and IO cache both.*/
  mysql_bin_log_ext.truncate(log_name, pos);

  std::ostringstream errmsg;
  errmsg << "Truncate last binlog file to position " << pos;
  sql_print_information(errmsg.str().c_str());
  return false;
}

void Binlog_ext::truncate(const char *p, my_off_t pos)
{
  off_t l = static_cast<off_t>(pos);

  int fd = open(p, O_WRONLY);
  if (fd == -1){
    errno = EACCES;
    return;
  }
  if (ftruncate(fd, l) == -1){
    close(fd);
    errno = EACCES;
    return;
  }
  close(fd);
}