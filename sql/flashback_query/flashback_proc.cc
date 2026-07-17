/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

#include "flashback_proc.h"
#include "flashback.h"

#include <assert.h>

#include "my_time.h"
#include "sql/item_timefunc.h"
#include "sql/protocol.h"
#include "sql/sql_time.h"
#include "sql/tztime.h"

namespace im {

namespace flashback {

/**
  Show the records in innodb_flashback_snapshot

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_flashback_proc_show::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_flashback_proc_show::pc_execute");
  DBUG_RETURN(false);
}

/**
  Query innodb_flashback_snapshot.
*/
void Sql_cmd_flashback_proc_show::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  snapshot_transform_result_container_t container;
  DBUG_ENTER("Sql_cmd_flashback_proc_show::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  /* Parse the input parameters */
  Item *ts = (*m_list)[0];
  if (try_cast_to_datetime(thd, &ts)) {
    my_error(ER_NATIVE_PROC_PARAMETER_MISMATCH, MYF(0), 1,
             m_proc->qname().c_str());
    DBUG_VOID_RETURN;
  }
  int unused;
  my_timeval tm;
  if (ts->get_timeval(&tm, &unused)) {
    my_error(ER_NATIVE_PROC_PARAMETER_MISMATCH, MYF(0), 1,
             m_proc->qname().c_str());
    DBUG_VOID_RETURN;
  }
  ulonglong utc = (ulonglong)tm.m_tv_sec;

  char buff[128];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  res = (*m_list)[1]->val_str(&str);
  const char *type = strmake_root(thd->mem_root, res->ptr(), res->length());
  bool is_asc = my_strcasecmp(system_charset_info, "ASC", type) == 0;
  bool is_desc = my_strcasecmp(system_charset_info, "DESC", type) == 0;
  if (!(is_asc || is_desc)) {
    my_error(ER_NATIVE_PROC_PARAMETER_MISMATCH, MYF(0), 2,
             m_proc->qname().c_str());
    DBUG_VOID_RETURN;
  }

  int limit = (*m_list)[2]->val_int();

  /* Get flashback snapshots */
  container.m_size = limit;
  FLASHBACK_CALL(get_snapshots)(&container, utc, is_desc);

  /* Send result */
  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;
  for (auto it = container.m_list.cbegin(); it != container.m_list.cend();
       it++) {
    MYSQL_TIME timestamp;
    snapshot_transform_result_t result = *it;

    protocol->start_row();
    protocol->store(result.trx_id);

    my_time_t nr = (my_time_t)result.utc;
    thd->time_zone()->gmt_sec_to_TIME(&timestamp, nr);
    protocol->store_datetime(timestamp, 0);

    protocol->store_string(result.memo.c_str(), result.memo.length(),
                           system_charset_info);

    if (protocol->end_row()) DBUG_VOID_RETURN;
  }
  my_eof(thd);

  DBUG_VOID_RETURN;
}

/* Singleton instance for show_flashback_snapshots */
Proc *Flashback_proc_show::instance() {
  static Proc *proc = new Flashback_proc_show();

  return proc;
}

/**
  Evoke the sql_cmd object for show_flashback_snapshots() proc.
*/
Sql_cmd *Flashback_proc_show::evoke_cmd(THD *thd,
                                        mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Delete the records in innodb_flashback_snapshot

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_flashback_proc_del::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_flashback_proc_del::pc_execute");

  /* Parse the input parameters */
  Item *ts = (*m_list)[0];
  if (try_cast_to_datetime(thd, &ts)) {
    my_error(ER_NATIVE_PROC_PARAMETER_MISMATCH, MYF(0), 1,
             m_proc->qname().c_str());
    DBUG_RETURN(true);
  }
  int unused;
  my_timeval tm;
  if (ts->get_timeval(&tm, &unused)) {
    my_error(ER_NATIVE_PROC_PARAMETER_MISMATCH, MYF(0), 1,
             m_proc->qname().c_str());
    DBUG_RETURN(true);
  }
  ulonglong utc = (ulonglong)tm.m_tv_sec;

  char buff[128];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  res = (*m_list)[1]->val_str(&str);
  const char *type = strmake_root(thd->mem_root, res->ptr(), res->length());
  bool is_asc = my_strcasecmp(system_charset_info, "ASC", type) == 0;
  bool is_desc = my_strcasecmp(system_charset_info, "DESC", type) == 0;
  if (!(is_asc || is_desc)) {
    my_error(ER_NATIVE_PROC_PARAMETER_MISMATCH, MYF(0), 2,
             m_proc->qname().c_str());
    DBUG_RETURN(true);
  }

  /* Delete flashback snapshots */
  FLASHBACK_CALL(delete_snapshots)(utc, is_desc);

  DBUG_RETURN(false);
}

/* Singleton instance for del_flashback_snapshots */
Proc *Flashback_proc_del::instance() {
  static Proc *proc = new Flashback_proc_del();

  return proc;
}

/**
  Evoke the sql_cmd object for del_flashback_snapshots() proc.
*/
Sql_cmd *Flashback_proc_del::evoke_cmd(THD *thd,
                                       mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Analyze the records in innodb_flashback_snapshot

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_flashback_proc_analyze::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_flashback_proc_analyze::pc_execute");
  DBUG_RETURN(false);
}

/**
  Analyze innodb_flashback_snapshot.
*/
void Sql_cmd_flashback_proc_analyze::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  snapshot_analyze_result_t result;
  DBUG_ENTER("Sql_cmd_flashback_proc_show::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  /* Analyze flashback snapshots */
  FLASHBACK_CALL(analyze_snapshots)(&result);

  /* Send result */
  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;
  if (result.valid()) {
    protocol->start_row();

    MYSQL_TIME timestamp;
    my_time_t nr = (my_time_t)result.min_utc;
    thd->time_zone()->gmt_sec_to_TIME(&timestamp, nr);
    protocol->store_datetime(timestamp, 0);

    nr = (my_time_t)result.max_utc;
    thd->time_zone()->gmt_sec_to_TIME(&timestamp, nr);
    protocol->store_datetime(timestamp, 0);

    protocol->store(result.undo_size);

    if (protocol->end_row()) DBUG_VOID_RETURN;
  }
  my_eof(thd);
  DBUG_VOID_RETURN;
}

/* Singleton instance for analyze_flashback_snapshots */
Proc *Flashback_proc_analyze::instance() {
  static Proc *proc = new Flashback_proc_analyze();

  return proc;
}

/**
  Evoke the sql_cmd object for analyze_flashback_snapshots() proc.
*/
Sql_cmd *Flashback_proc_analyze::evoke_cmd(THD *thd,
                                           mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

}  // namespace flashback
}  // namespace im