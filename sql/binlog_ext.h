/* Copyright (c) 2023, Alibaba and/or its affiliates. All rights reserved.

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
#ifndef BINLOG_EXT_INCLUDED
#define BINLOG_EXT_INCLUDED

#include "binlog.h"
#include "log_event.h"
#include "sql_class.h"
#include "xa.h"

/** Extension of MYSQL_BIN_LOG. */
class Binlog_ext {
 public:
  Binlog_ext();

  /**
    Create mysql.duckdb_binlog_position table and initialize if it is not
    created and duckdb mode is on.
  */
  bool duckdb_binlog_init();

 /**
   Commit a transaction on duckdb server.

   @param[in]  thd  THD object of the session.
  */
  bool duckdb_commit(THD *thd);

  /**
    Truncate binlog file to the position stored in mysql.duckdb_binlog_position
    if it is necessary.
  */
  bool duckdb_recover(const char* log_name);

  /**
    When a new binlog is created, duckdb_binlog_position's position should
    be updated to the position of the new binlog file.
  */
  bool duckdb_binlog_rotate();

  void truncate(const char *p, my_off_t pos);

 private:
  Binlog_ext &operator=(const Binlog_ext &) = delete;
  Binlog_ext(const Binlog_ext &) = delete;
  Binlog_ext(Binlog_ext &&) = delete;
};
extern Binlog_ext mysql_bin_log_ext;
#endif