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

#include "sql/rpl_binlog_sender_i_s_ext.h"

#include "sql/field.h"  // store
#include "sql/rpl_binlog_sender_ext.h"
#include "sql/rpl_replica_ext.h"
#include "sql/sql_show.h"
#include "sql/table.h"

struct ST_FIELD_INFO;
struct TABLE_LIST;
class Item;

/* Fields of the dynamic table BRR_SENDER_TRX_INFO. */
ST_FIELD_INFO i_s_brr_sender_field_info[] = {
    {"DUMP_THREAD_ID", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"BRR_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"REPLICA_SERVER_ID", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"SEND_POS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"EVENTS_SEND", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"TRX_CONTENT_EVENTS_SEND", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, "", 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

static int i_s_brr_sender_trx_info_fill(
    THD *thd,                          /*!< in: thread */
    Table_ref *tables,                 /*!< in/out: tables to fill */
    const Brr_sender_trx_info_t *info) /*!< in: brr source information */
{
  TABLE *table;
  Field **fields;
  DBUG_TRACE;
  table = tables->table;
  fields = table->field;
  fields[0]->store(info->dump_thread_id, true);
  fields[1]->store(info->brr_idx, true);
  fields[2]->store(info->replica_server_id, true);
  fields[3]->store(info->send_pos, true);
  fields[4]->store(info->events_send, true);
  fields[5]->store(info->trx_content_events_send, true);
  return schema_table_store_record(thd, table);
}

/* Get the brr_info information of each Brr_reader. */
extern void get_brr_sender_trx_info_for_i_s(
    Brr_binlog_sender::Brr_reader *brr_reader, Brr_sender_trx_info_t *brr_info);

int i_s_brr_sender_trx_info_fill_table(
    THD *thd,          /*!< in: thread */
    Table_ref *tables, /*!< in/out: tables to fill */
    Item *)            /*!< in: condition (ignored) */
{
  int status = 0;
  DBUG_TRACE;

  /* Check whether the real-time transmit is on. */
  if (!brr_trx_mngr.get_enabled()) {
    return 0;
  }

  /* Only allow the PROCESS privilege holder to access the stats. */
  if (check_global_access(thd, PROCESS_ACL)) {
    return 0;
  }

  std::list<Brr_sender_trx_info_t> brr_info_list;

  /* Walk through each Binlog_sender. */
  mysql_mutex_lock(&LOCK_replica_list);
  for (auto sender_it = brr_binlog_sender_set.begin();
       sender_it != brr_binlog_sender_set.end(); sender_it++) {
    Brr_binlog_sender *brr_binlog_sender = *sender_it;
    if (!brr_binlog_sender) continue;

    /* Fetch each Brr_reader info */
    mysql_bin_log.lock_binlog_end_pos();

    std::list<Brr_binlog_sender::Brr_reader *> *brr_reader_list =
        brr_binlog_sender->get_brr_reader_list();

    for (auto reader_it = brr_reader_list->begin();
         reader_it != brr_reader_list->end(); reader_it++) {
      Brr_binlog_sender::Brr_reader *brr_reader = *reader_it;
      Brr_sender_trx_info_t brr_info;
      get_brr_sender_trx_info_for_i_s(brr_reader, &brr_info);
      brr_info_list.push_back(brr_info);
    }

    mysql_bin_log.unlock_binlog_end_pos();
  }
  mysql_mutex_unlock(&LOCK_replica_list);

  for (auto brr_info : brr_info_list) {
    status = i_s_brr_sender_trx_info_fill(thd, tables, &brr_info);
    /* If something goes wrong, break and return */
    if (status) {
      break;
    }
  }

  return status;
}

/* Fields of the dynamic table BRR_SOURCE_TRX_INFO. */
ST_FIELD_INFO i_s_brr_source_trx_field_info[] = {
    {"TRX_THREAD_ID", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"BRR_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"BRR_TYPE", BRR_TYPE_LEN, MYSQL_TYPE_STRING, 0, 0, "", 0},
    {"STATUS", MY_INT32_NUM_DECIMAL_DIGITS, MYSQL_TYPE_TINY, 0, MY_I_S_UNSIGNED,
     "", 0},
    {"START_POS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"READABLE_POS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     MY_I_S_UNSIGNED, "", 0},
    {"TMP_FILE_NAME", BINLOG_TMP_FILE_LEN, MYSQL_TYPE_STRING, 0, 0, "", 0},
    {"GNO", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, "", 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

static int i_s_brr_source_trx_info_fill(
    THD *thd,                              /*!< in: thread */
    Table_ref *tables,                     /*!< in/out: tables to fill */
    const Brr_source_trx_info_t *brr_info) /*!< in: buffer pool
    information */
{
  TABLE *table;
  Field **fields;
  DBUG_TRACE;
  table = tables->table;
  fields = table->field;
  fields[0]->store(brr_info->brr_thread_id, true);
  fields[1]->store(brr_info->index, true);
  fields[2]->store(brr_info->type, strlen(brr_info->type), system_charset_info);
  fields[3]->store(brr_info->status, true);
  fields[4]->store(brr_info->start_read_pos, true);
  fields[5]->store(brr_info->end_position, true);
  fields[6]->store(brr_info->file_name, strlen(brr_info->file_name),
                   system_charset_info);
  fields[7]->store(brr_info->gno, true);
  return schema_table_store_record(thd, table);
}

/* Get the brr_info information of each brr_trx. */
extern void get_brr_source_trx_info_for_i_s(const Brr_trx *brr_trx,
                                            Brr_source_trx_info_t *brr_info);

int i_s_brr_source_trx_info_fill_table(
    THD *thd,          /*!< in: thread */
    Table_ref *tables, /*!< in/out: tables to fill */
    Item *)            /*!< in: condition (ignored) */
{
  int status = 0;
  DBUG_TRACE;

  /* Check whether the real-time transmit is on. */
  if (!brr_trx_mngr.get_enabled()) {
    return 0;
  }

  /* Only allow the PROCESS privilege holder to access the stats. */
  if (check_global_access(thd, PROCESS_ACL)) {
    return 0;
  }

  std::list<Brr_source_trx_info_t> brr_info_list;

  /* Walk through each Brr_trx. */
  mysql_bin_log.lock_binlog_end_pos();

  auto brr_trx_list = brr_trx_mngr.get_brr_trx_list();
  for (auto it = brr_trx_list->begin(); it != brr_trx_list->end(); it++) {
    Brr_source_trx_info_t brr_info;
    get_brr_source_trx_info_for_i_s(it->second, &brr_info);
    brr_info_list.push_back(brr_info);
  }

  mysql_bin_log.unlock_binlog_end_pos();

  for (auto brr_info : brr_info_list) {
    status = i_s_brr_source_trx_info_fill(thd, tables, &brr_info);
    /* If something goes wrong, break and return */
    if (status) {
      break;
    }
  }
  return status;
}