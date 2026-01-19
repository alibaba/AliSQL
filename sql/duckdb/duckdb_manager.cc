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

/** @file sql/duckdb/duckdb_manager.cc
 */

#include "duckdb_manager.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/duckdb/duckdb_config.h"
#include "sql/duckdb/duckdb_mysql_udf.h"
#include "sql/duckdb/duckdb_timezone.h"
#include "sql/mysqld.h"

namespace myduck {

DuckdbManager *DuckdbManager::m_instance = nullptr;

DuckdbManager::DuckdbManager() : m_database(nullptr) {}

bool DuckdbManager::Initialize() {
  if (m_database != nullptr) {
    return false;
  }

  std::lock_guard<std::mutex> lock(m_mutex);

  /** TODO: reinitialize m_database when it is invalidated. There is a problem:
  reinitialization need to close all connections, in order to destroy the duckdb
  instance saved in thd. */
  if (m_database != nullptr) {
    return false;
  }

  duckdb::DBConfig config;

  /** TODO: Use direct io to read/write data files. */
  /* Unfortunally, pread failed when MainHeader::CheckMagicBytes. */
  config.options.use_direct_io = global_use_dio;

  config.options.scheduler_process_partial = global_scheduler_process_partial;

  if (global_max_threads != 0) {
    config.options.maximum_threads = global_max_threads;
  }

  if (global_memory_limit != 0) {
    config.options.maximum_memory = global_memory_limit;
  }

  if (global_duckdb_temp_directory != nullptr)
    config.options.temporary_directory = global_duckdb_temp_directory;
  else {
    /* Default same as data directory. */
    char path[FN_REFLEN];
    fn_format(path, DUCKDB_DEFAULT_TMP_NAME, mysql_real_data_home, "", MYF(0));
    config.options.temporary_directory = path;
  }

  if (global_max_temp_directory_size != 0)
    config.options.maximum_swap_space = global_max_temp_directory_size;

  if (appender_allocator_flush_threshold != 0) {
    config.options.appender_allocator_flush_threshold =
        appender_allocator_flush_threshold;
  }

  config.options.checkpoint_wal_size = checkpoint_threshold;

  // For now, we store all tables in one file
  char path[FN_REFLEN];
  fn_format(path, DUCKDB_FILE_NAME, mysql_real_data_home, "", MYF(0));
  m_database = new duckdb::DuckDB(path, &config);

  if (m_database == nullptr) return true;

  TimeZoneOffsetHelper::init_timezone();

  duckdb::Connection con(*m_database);
  register_mysql_udf(&con);

  LogErr(INFORMATION_LEVEL, ER_DUCKDB, "DuckdbManager::Initialize succeed.");

  return false;
}

bool DuckdbManager::CreateInstance() {
  assert(m_instance == nullptr);
  m_instance = new DuckdbManager();
  if (m_instance == nullptr) {
    LogErr(ERROR_LEVEL, ER_DUCKDB, "DuckdbManager::CreateInstance failed.");
    return true;
  }
  return false;
}

DuckdbManager::~DuckdbManager() {
  if (m_database != nullptr) {
    delete m_database;
    m_database = nullptr;
  }
}

void DuckdbManager::Cleanup() {
  if (m_instance == nullptr) return;

  delete m_instance;
  m_instance = nullptr;
}

DuckdbManager &DuckdbManager::Get() {
  assert(m_instance != nullptr);

  bool ret = m_instance->Initialize();
  assert(!ret);

  return *m_instance;
}

std::shared_ptr<duckdb::Connection> DuckdbManager::CreateConnection() {
  auto &instance = Get();
  auto connection = std::make_shared<duckdb::Connection>(*instance.m_database);
  return connection;
}

}  // namespace myduck
