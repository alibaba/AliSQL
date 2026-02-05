/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

#pragma once

#include "my_base.h"
#include "sql_alter.h"
#include "table.h"

class THD;
class Log_event;
class Gtid_set;

bool duckdb_manager_create_ins();
void duckdb_manager_cleanup();
void duckdb_create_test_schema(THD *thd);
bool myduck_is_duckdb_table(const TABLE *table);

bool myduck_duckdb_query_and_send(THD *thd, const std::string &query, bool send_result, bool push_error);

/*
  DuckDB Update Query Wrapper

  SYNOPSIS
    [Input]
    thd			thread handler
    query		queries send to duckdb

    [Output]
    duckdb_update_rows		updated rows.

  RETURN
    false - OK
    true  - error
*/
bool myduck_duckdb_query_update(THD *thd, std::string &query, ha_rows &duckdb_update_rows);

bool duckdb_global_mode_on();

void thd_duckdb_context_update_on_commit(THD *thd);

int thd_duckdb_context_save_batch_gtid_set(THD *thd);

bool thd_duckdb_context_need_implicit_commit_batch(THD *thd, Log_event *ev);

int thd_duckdb_context_implicit_commit_batch(THD *thd);

bool thd_duckdb_context_duckdb_delay_commit(THD *thd);

int thd_duckdb_context_commit_partial_batch(THD *thd);

int thd_duckdb_context_flush_appenders(THD *thd, std::string &error_msg);

void rli_info_thd_duckdb_context_add_gtid_set(THD *thd, Gtid_set * s);

void duckdb_context_prepare_gtids_for_binlog_commit(THD *thd);