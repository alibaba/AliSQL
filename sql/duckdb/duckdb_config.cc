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

#include "duckdb_config.h"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/config.hpp"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/duckdb/duckdb_query.h"
#include "sql/handler.h"

#include <cctype>
#include <cstring>
#include <string>

#include <ostream>

namespace myduck {
const char *mode_names[] = {"NONE", "ON", NullS};

ulonglong global_memory_limit = 0;

const char *global_duckdb_temp_directory = nullptr;
const char *global_config_options = nullptr;

ulonglong global_max_temp_directory_size = 0;

ulonglong global_max_threads = 0;

ulonglong max_threads_per_query = 1000000;

ulonglong global_mode = 0;

bool global_use_dio = false;

bool global_scheduler_process_partial = true;

ulonglong appender_allocator_flush_threshold = 0;

ulonglong checkpoint_threshold = 268435456;

bool use_double_for_decimal;

ulonglong duckdb_max_threads_per_query_rpl = 1000000;

bool is_disabled(const handlerton &ht) {
  return (ht.db_type == DB_TYPE_DUCKDB && global_mode != DUCKDB_ON);
}

bool update_memory_limit(sys_var *sys_var [[maybe_unused]], THD *thd,
                         enum_var_type type [[maybe_unused]]) {
  std::ostringstream oss;
  if (global_memory_limit == 0) {
    oss << "RESET memory_limit";
  } else {
    oss << "SET memory_limit = '";
    oss << BytesToHumanReadableString(global_memory_limit);
    oss << "'";
  }

  return duckdb_query_and_send(thd, oss.str(), false, true);
}

bool update_max_temp_directory_size(sys_var *sys_var [[maybe_unused]], THD *thd,
                                    enum_var_type type [[maybe_unused]]) {
  std::ostringstream oss;

  if (global_max_temp_directory_size == 0) {
    oss << "RESET GLOBAL max_temp_directory_size";
  } else {
    oss << "SET GLOBAL max_temp_directory_size = '";
    oss << BytesToHumanReadableString(global_max_temp_directory_size);
    oss << "'";
  }

  return duckdb_query_and_send(thd, oss.str(), false, true);
}

bool update_threads(sys_var *sys_var [[maybe_unused]], THD *thd,
                    enum_var_type type [[maybe_unused]]) {
  std::ostringstream oss;
  if (global_max_threads == 0) {
    oss << "RESET GLOBAL threads";
  } else {
    oss << "SET GLOBAL threads = " << global_max_threads;
  }

  return duckdb_query_and_send(thd, oss.str(), false, true);
}

bool update_scheduler_process_partial(sys_var *sys_var [[maybe_unused]],
                                      THD *thd,
                                      enum_var_type type [[maybe_unused]]) {
  std::ostringstream oss;
  oss << "SET scheduler_process_partial = ";
  if (global_scheduler_process_partial) {
    oss << "true";
  } else {
    oss << "false";
  }

  return duckdb_query_and_send(thd, oss.str(), false, true);
}

bool update_appender_allocator_flush_threshold(sys_var *sys_var
                                               [[maybe_unused]],
                                               THD *thd,
                                               enum_var_type type
                                               [[maybe_unused]]) {
  std::ostringstream oss;
  if (appender_allocator_flush_threshold == 0) {
    oss << "RESET GLOBAL appender_allocator_flush_threshold";
  } else {
    oss << "SET GLOBAL appender_allocator_flush_threshold = '";
    oss << BytesToHumanReadableString(appender_allocator_flush_threshold);
    oss << "'";
  }

  return duckdb_query_and_send(thd, oss.str(), false, true);
}

bool update_checkpoint_threshold(sys_var *sys_var [[maybe_unused]], THD *thd,
                                 enum_var_type type [[maybe_unused]]) {
  std::ostringstream oss;
  oss << "SET GLOBAL checkpoint_threshold = '";
  oss << BytesToHumanReadableString(checkpoint_threshold);
  oss << "'";

  return duckdb_query_and_send(thd, oss.str(), false, true);
}

/** DuckDB settings that already have their own duckdb_* variable. Setting
them through duckdb_config_options would make SHOW VARIABLES report a value
DuckDB is not using, so they are refused. */
static const struct {
  const char *setting;
  const char *variable;
} mapped_settings[] = {
    {"max_memory", "duckdb_memory_limit"},
    {"threads", "duckdb_threads"},
    {"temp_directory", "duckdb_temp_directory"},
    {"max_temp_directory_size", "duckdb_max_temp_directory_size"},
    {"checkpoint_threshold", "duckdb_checkpoint_threshold"},
    {"scheduler_process_partial", "duckdb_scheduler_process_partial"},
    {"appender_allocator_flush_threshold",
     "duckdb_appender_allocator_flush_threshold"},
};

static std::string trim(const std::string &str) {
  const char *space = " \t\r\n";
  auto begin = str.find_first_not_of(space);
  if (begin == std::string::npos) return "";
  auto end = str.find_last_not_of(space);
  return str.substr(begin, end - begin + 1);
}

/** AliSQL's DuckDB casts strings to BOOLEAN the MySQL way, so 'true' would
become false. Boolean settings are parsed here instead, accepting what MySQL
accepts for boolean options. */
static bool parse_bool(const std::string &value, bool *result) {
  std::string lower;
  for (char c : value) lower += static_cast<char>(tolower(c));
  if (lower == "on" || lower == "true" || lower == "1") {
    *result = true;
    return true;
  }
  if (lower == "off" || lower == "false" || lower == "0") {
    *result = false;
    return true;
  }
  return false;
}

static bool config_option_error(const std::string &message) {
  std::string text = "duckdb_config_options: " + message;
  LogErr(ERROR_LEVEL, ER_DUCKDB, text.c_str());
  return true;
}

bool apply_config_options(duckdb::DBConfig &config) {
  if (global_config_options == nullptr) return false;

  std::string list(global_config_options);
  size_t pos = 0;
  while (pos <= list.size()) {
    size_t next = list.find(';', pos);
    if (next == std::string::npos) next = list.size();
    std::string item = trim(list.substr(pos, next - pos));
    pos = next + 1;
    if (item.empty()) continue;

    size_t eq = item.find('=');
    if (eq == std::string::npos) {
      return config_option_error("expected name=value, got '" + item + "'");
    }
    std::string name = trim(item.substr(0, eq));
    std::string value = trim(item.substr(eq + 1));

    auto option = duckdb::DBConfig::GetOptionByName(name);
    if (!option) {
      return config_option_error("unknown DuckDB setting '" + name + "'");
    }
    for (const auto &mapped : mapped_settings) {
      if (strcmp(option->name, mapped.setting) == 0) {
        return config_option_error("'" + name + "' is controlled by " +
                                   mapped.variable +
                                   ", set that variable instead");
      }
    }

    duckdb::Value input(value);
    if (strcmp(option->parameter_type, "BOOLEAN") == 0) {
      bool flag;
      if (!parse_bool(value, &flag)) {
        return config_option_error("'" + name +
                                   "' expects ON/OFF, TRUE/FALSE or 1/0, got '" +
                                   value + "'");
      }
      input = duckdb::Value::BOOLEAN(flag);
    }

    try {
      config.SetOption(*option, input);
    } catch (std::exception &ex) {
      duckdb::ErrorData error(ex);
      return config_option_error("cannot set '" + name +
                                 "': " + error.RawMessage());
    }

    std::string applied = "duckdb_config_options: " +
                          std::string(option->name) + " = " + value;
    LogErr(INFORMATION_LEVEL, ER_DUCKDB, applied.c_str());
  }
  return false;
}
}  // namespace myduck
