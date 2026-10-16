/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates.

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

#include "sql/field.h"  // store
#include "sql/rpl_binlog_sender_ext.h"
#include "sql/rpl_binlog_sender_i_s_ext.h"
#include "sql/rpl_replica_ext.h"
#include "sql/sql_show.h"
#include "sql/table.h"

struct ST_FIELD_INFO;
struct TABLE_LIST;
class Item;

/* Fields of the dynamic table BRR_REPLICA_INFO. */
ST_FIELD_INFO i_s_brr_replica_info[] = {
    {"BRR_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"BRR_TYPE", BRR_TYPE_LEN, MYSQL_TYPE_STRING, 0, 0, "", 0},
    {"TMP_FILE_NAME", BINLOG_TMP_FILE_LEN, MYSQL_TYPE_STRING, 0, 0, "", 0},
    {"GNO", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, "", 0},
    {"RECEIVED_POS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"BRR_EVENTS_RECEIVED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"TRX_CONTENT_EVENTS_RECEIVED", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, "", 0},
    {"TRX_COMPLETE", MY_INT32_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"ASSIGNED_TO_BRR_WORKER", MY_INT32_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"APPLY_POS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

static int i_s_brr_replica_info_fill(
    THD *thd,                       /*!< in: thread */
    Table_ref *tables,              /*!< in/out: tables to fill */
    const Brr_replica_info_t *info) /*!< in: brr source information */
{
  TABLE *table;
  Field **fields;
  DBUG_TRACE;
  table = tables->table;
  fields = table->field;
  fields[0]->store(info->index, true);
  fields[1]->store(info->type, strlen(info->type), system_charset_info);
  fields[2]->store(info->file_name, strlen(info->file_name),
                   system_charset_info);
  fields[3]->store(info->gno, true);
  fields[4]->store(info->received_pos, true);
  fields[5]->store(info->brr_events_received, true);
  fields[6]->store(info->trx_content_events_received, true);
  fields[7]->store(info->trx_complete, true);
  fields[8]->store(info->assigned_to_brr_worker, true);
  fields[9]->store(info->apply_pos, true);
  return schema_table_store_record(thd, table);
}

int i_s_brr_replica_info_fill_table(
    THD *thd,          /*!< in: thread */
    Table_ref *tables, /*!< in/out: tables to fill */
    Item *)            /*!< in: condition (ignored) */
{
  int status = 0;
  DBUG_TRACE;

  /* Only allow the PROCESS privilege holder to access the stats. */
  if (check_global_access(thd, PROCESS_ACL)) {
    return 0;
  }

  std::list<Brr_replica_info_t> brr_info_list;

  /* Walk through each Brr_cache. */
  brr_rpl_info.get_brr_replica_info_for_i_s(brr_info_list);

  for (auto brr_info : brr_info_list) {
    status = i_s_brr_replica_info_fill(thd, tables, &brr_info);
    /* If something goes wrong, break and return */
    if (status) {
      break;
    }
  }

  return status;
}
