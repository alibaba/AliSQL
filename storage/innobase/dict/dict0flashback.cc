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

#include "dict0flashback.h"

#include "my_time.h"
#include "mysql/thread_type.h"
#include "sql/dd/types/object_table.h"
#include "sql/dd/types/object_table_definition.h"
#include "sql/flashback_query/flashback.h"
#include "sql_class.h"
#include "tztime.h"

#include "dict0dd.h"
#include "pars0pars.h"
#include "que0que.h"
#include "row0sel.h"
#include "srv0srv.h"
#include "trx0purge.h"
#include "trx0sys.h"
#include "trx0trx.h"

namespace im {

static bool scn_history_start_shutdown = false;

os_event_t scn_history_event = nullptr;

/** SCN rolling forward interval */
ulint srv_scn_history_interval = 1;

/** SCN rolling forward task */
bool srv_scn_history_task_enabled = false;
bool srv_scn_history_task_stop_all = false;

/** The max time of scn history record keeped */
ulint srv_scn_history_keep_seconds = 0;

bool srv_scn_valid_enabled = true;

ulint srv_scn_valid_volume = 30;

bool srv_scn_print_warning = true;

ulint undo_space_supremum_size = 102400;
ulint undo_space_reserve_size = 0;

/** Global instance of Flashback_manager_v2 */
Flashback_manager_v2 *flashback_sys = nullptr;

/** Current undo used size (Pages) */
std::atomic<ulint> undo_total_used_size;

/* The oldest readview record */
static std::atomic<bool> is_oldest_result_valid{false};
static scn_transform_result_t oldest_result;

static ulint mb_to_pages(ulint size) {
  return (ulint)(1024.0 * 1024.0 / univ_page_size.physical() * size);
}

static ulint pages_to_mb(ulint pages) {
  return (ulint)(univ_page_size.physical() * pages / 1024.0 / 1024.0);
}

static void get_used_rseg_size() {
  ulint value = 0;

  /** Fetch data with undo::spaces->s_lock().
  undo::spaces->x_lock() is used in following scenarios:
  1. Truncate undo tablespaces.
  2. Check undo tablespace location.
  3. Drop an undo tablespace.
  4. Replay the delete log.
  5. Fix up an undo tablespace if it was in the process of being truncated
  when the server crashed.
  6. Open existing undo tablespaces, when server starts.
  7. Create the implicit undo tablespaces.
  The above scenarios are not serious if blocked by undo::spaces->s_lock() */
  undo::spaces->s_lock();
  for (auto undo_space : undo::spaces->m_spaces) {
    for (auto rseg : *undo_space->rsegs()) {
      value += rseg->get_curr_size();
    }
  }
  undo::spaces->s_unlock();

  undo_total_used_size.store(value);
}

unsigned long long get_undo_total_used_size() {
  get_used_rseg_size();
  return undo_total_used_size.load();
}

/* ------------------------------------------------------------------------- */
/* ------------------------ Begin Flashback Servive ------------------------ */

static bool srv_fetch_scn_history_step(void *node_void, void *result_void);

static dberr_t roll_forward_scn_delete(ulint condition, SCN_FETCH_ORDER order);

static bool srv_fetch_scn_histories_step(void *node_void,
                                          void *result_list_void) {
  scn_transform_result_t result;
  srv_fetch_scn_history_step(node_void, &result);

  if (result.err == DB_SUCCESS && result.utc > 0) {
    snapshot_transform_result_container_t *result_list =
        (snapshot_transform_result_container_t *)(result_list_void);
    snapshot_transform_result_t resnapshot_transform_result;
    resnapshot_transform_result.trx_id = result.scn;
    resnapshot_transform_result.utc = result.utc;
    resnapshot_transform_result.memo = result.memo;

    result_list->m_list.push_back(resnapshot_transform_result);
    if (result_list->m_list.size() >= result_list->m_size) return false;
  }

  return true;
}

static void get_snapshot(snapshot_transform_result_container_t *result_list,
                         ulonglong utc, bool is_desc) {
  dberr_t err;
  trx_t *trx;
  pars_info_t *pinfo;

  if (dict_sys->scn_hist == nullptr) {
    return;
  }

  trx = trx_allocate_for_background();
  trx->isolation_level = TRX_ISO_READ_UNCOMMITTED;
  trx_start_internal_read_only(trx, UT_LOCATION_HERE);

  std::string sql_str =
      "PROCEDURE FETCH_SCN_HISTORIES () IS\n"
      "found INT;\n"
      "DECLARE FUNCTION fetch_scn_histories_step;\n"
      "DECLARE CURSOR scn_histories_cur IS\n"
      "  SELECT\n"
      "  scn,\n"
      "  utc, \n"
      "  limit_no, \n"
      "  memo \n"
      "  FROM \"" SCN_HISTORY_TABLE_FULL_NAME
      "\"\n"
      "  WHERE\n";
  /* Fill search condition */
  if (is_desc) {
    sql_str +=
        "  utc < :condition\n"
        "  ORDER BY utc DESC;\n";
  } else {
    sql_str +=
        "  utc >= :condition\n"
        "  ORDER BY utc ASC;\n";
  }
  sql_str +=
      "BEGIN\n"

      "OPEN scn_histories_cur;\n"
      "found := 1;\n"
      "WHILE found = 1 LOOP\n"
      "  FETCH scn_histories_cur INTO\n"
      "    fetch_scn_histories_step();\n"
      "  IF (SQL % NOTFOUND) THEN\n"
      "    found := 0;\n"
      "  END IF;\n"
      "END LOOP;\n"
      "CLOSE scn_histories_cur;\n"

      "END;\n";

  pinfo = pars_info_create();
  pars_info_add_ull_literal(pinfo, "condition", utc);

  pars_info_bind_function(pinfo, "fetch_scn_histories_step",
                          srv_fetch_scn_histories_step, result_list);

  err = que_eval_sql(pinfo, sql_str.c_str(), trx);
  if (err != DB_SUCCESS) {
    ib::warn(ER_RDS_FLASHBACK)
        << "Cannot select from" << SCN_HISTORY_TABLE_FULL_NAME << ": "
        << ut_strerr(err);
  }

  /* pinfo is freed by que_eval_sql() */

  trx_commit_for_mysql(trx);

  trx_free_for_background(trx);

  return;
}

static void delete_snapshot(ulonglong utc, bool is_desc) {
  dberr_t err;
  if (is_desc) {
    /* delete from innodb_flashback_snapshot where utc < utc */
    err = roll_forward_scn_delete(utc, SCN_FETCH_ORDER::UTC_DESC);
  } else {
    /* delete from innodb_flashback_snapshot where utc >= utc */
    err = roll_forward_scn_delete(utc, SCN_FETCH_ORDER::UTC_ASC);
  }

  if (err != DB_SUCCESS) {
    ib::warn(ER_RDS_FLASHBACK)
        << "Cannot delete from" << SCN_HISTORY_TABLE_FULL_NAME << ": "
        << ut_strerr(err);
  }
  return;
}

static void analyze_snapshot(snapshot_analyze_result_t *result) {
  scn_transform_result_t tmp_result;

  result->min_utc = 0;
  result->max_utc = 0;
  result->undo_size = pages_to_mb(undo_total_used_size.load());

  /* select * from innodb_flashback_snapshot where utc >= 0
     order by utc asc */
  try_scn_transform(0, SCN_FETCH_ORDER::UTC_ASC, &tmp_result);
  if (tmp_result.err == DB_SUCCESS && tmp_result.utc > 0) {
    result->min_utc = tmp_result.utc;
    /* select * from innodb_flashback_snapshot where utc < IB_UINT64_MAX
       order by utc desc */
    try_scn_transform(std::numeric_limits<uint64_t>::max(),
                      SCN_FETCH_ORDER::UTC_DESC, &tmp_result);
    if (tmp_result.err == DB_SUCCESS && tmp_result.utc > 0) {
      result->max_utc = tmp_result.utc;
    }
  }

  return;
}

static flashback_service_t global_flashback_service = {
    get_snapshot, delete_snapshot, analyze_snapshot};

void register_innodb_flashback_service() {
  register_flashback_service(&global_flashback_service);
}

/* ------------------------- End Flashback Servive ------------------------- */
/* ------------------------------------------------------------------------- */

/** Create INNODB_FLASHBACK_SNAPSHOT table */
dd::Object_table *create_innodb_scn_hist_table() {
  dd::Object_table *tbl = nullptr;
  dd::Object_table_definition *def = nullptr;

  tbl = dd::Object_table::create_object_table();
  ut_ad(tbl);

  tbl->set_hidden(true);
  def = tbl->target_table_definition();
  def->set_table_name(SCN_HISTORY_TABLE_NAME);
  def->add_field(0, "scn", "scn BIGINT UNSIGNED NOT NULL");
  def->add_field(1, "utc", "utc BIGINT UNSIGNED NOT NULL");
  def->add_field(2, "limit_no", "limit_no BIGINT UNSIGNED NOT NULL");
  def->add_field(3, "memo", "memo TEXT");
  /** SCN as primary key */
  def->add_index(0, "index_pk", "PRIMARY KEY(scn)");
  /** UTC index to speed lookup*/
  def->add_index(1, "index_utc", "index(utc)");
  def->add_index(2, "index_limit_no", "index(limit_no)");

  return tbl;
}

/**
  Interpret the SCN and UTC from select node every record.

  @param[in]      node      SEL_NODE_T
  @param[in/out]  result    SCN_TRANFORM_RESULT_T

  @retval         true      Unused
*/
static bool srv_fetch_scn_history_step(void *node_void, void *result_void) {
  sel_node_t *node = (sel_node_t *)node_void;
  scn_transform_result_t *result = (scn_transform_result_t *)(result_void);
  que_common_t *cnode;
  int i;
  ut_ad(result->state == SCN_TRANSFORM_STATE::NOT_FOUND);

  result->state = SCN_TRANSFORM_STATE::SUCCESS;

  /* this should loop exactly 4 times for scn and utc  */
  for (cnode = static_cast<que_common_t *>(node->select_list), i = 0;
       cnode != nullptr;
       cnode = static_cast<que_common_t *>(que_node_get_next(cnode)), i++) {
    const byte *data;
    dfield_t *dfield = que_node_get_val(cnode);
    dtype_t *type = dfield_get_type(dfield);
    ulint len = dfield_get_len(dfield);

    data = static_cast<const byte *>(dfield_get_data(dfield));

    switch (i) {
      case 0:
        ut_a(dtype_get_mtype(type) == DATA_INT);
        ut_a(len == 8);

        result->scn = mach_read_from_8(data);
        break;
      case 1:
        ut_a(dtype_get_mtype(type) == DATA_INT);
        ut_a(len == 8);

        result->utc = mach_read_from_8(data);
        break;
      case 2:
        ut_a(dtype_get_mtype(type) == DATA_INT);
        ut_a(len == 8);

        result->limit_no = mach_read_from_8(data);
        break;
      case 3:
        ut_a(dtype_get_mtype(type) == DATA_BLOB);

        result->memo = std::string(reinterpret_cast<const char *>(data), len);
        break;
      default:
        ut_error;
    }
  }
  /* The result must have 4 columns */
  ut_ad(i == 4);

  return true;
}

/**
  Try the best to transform UTC to read view,
  it will search the first record which is more than the utc.
  Pls confirm the result state to judge whether the tranformation is success,
  and also it can report the error to client user if required through
  result.err.

  @param[in]      condition       The filter condition.
  @param[in]      order           The sort order.
  @param[in/out]  result
*/
void try_scn_transform(const ulint condition, SCN_FETCH_ORDER order,
                       scn_transform_result_t *result) {
  trx_t *trx;
  pars_info_t *pinfo;

  result->reset();
  if (dict_sys->scn_hist == nullptr) {
    result->err = DB_ERROR;
    return;
  }

  trx = trx_allocate_for_background();
  trx->isolation_level = TRX_ISO_READ_UNCOMMITTED;
  trx_start_internal_read_only(trx, UT_LOCATION_HERE);

  std::string sql_str =
      "PROCEDURE FETCH_SCN_HISTORY () IS\n"
      "DECLARE FUNCTION fetch_scn_history_step;\n"
      "DECLARE CURSOR scn_history_cur IS\n"
      "  SELECT\n"
      "  scn,\n"
      "  utc, \n"
      "  limit_no, \n"
      "  memo \n"
      "  FROM \"" SCN_HISTORY_TABLE_FULL_NAME
      "\"\n"
      "  WHERE\n";
  /* Fill search condition */
  if (order == SCN_FETCH_ORDER::UTC_ASC) {
    sql_str +=
        "  utc >= :condition\n"
        "  ORDER BY utc ASC;\n";
  } else if (order == SCN_FETCH_ORDER::UTC_DESC) {
    sql_str +=
        "  utc < :condition\n"
        "  ORDER BY utc DESC;\n";
  } else if (order == SCN_FETCH_ORDER::LIMIT_NO_ASC) {
    sql_str +=
        "  limit_no >= :condition\n"
        "  ORDER BY limit_no ASC;\n";
  } else if (order == SCN_FETCH_ORDER::LIMIT_NO_DESC) {
    sql_str +=
        "  limit_no < :condition\n"
        "  ORDER BY limit_no DESC;\n";
  }
  sql_str +=
      "BEGIN\n"

      "OPEN scn_history_cur;\n"
      "FETCH scn_history_cur INTO\n"
      "  fetch_scn_history_step();\n"
      "IF (SQL % NOTFOUND) THEN\n"
      "  CLOSE scn_history_cur;\n"
      "  RETURN;\n"
      "END IF;\n"
      "CLOSE scn_history_cur;\n"

      "END;\n";

  pinfo = pars_info_create();
  pars_info_add_ull_literal(pinfo, "condition", condition);

  pars_info_bind_function(pinfo, "fetch_scn_history_step",
                          srv_fetch_scn_history_step, result);

  result->err = que_eval_sql(pinfo, sql_str.c_str(), trx);

  /* pinfo is freed by que_eval_sql() */

  trx_commit_for_mysql(trx);

  trx_free_for_background(trx);
}

/**
  Delete the old record in innodb_flashback_snapshot.

  Note: the delete process called every SRV_SCN_HISTORY_INTERVAL
        or the undo used size reached the supremum size and the
        records in innodb_flashback_snapshot have blocked purge.
*/
static dberr_t roll_forward_scn_delete(ulint condition, SCN_FETCH_ORDER order) {
  pars_info_t *pinfo;
  dberr_t ret;
  trx_t *trx;

  if (srv_read_only_mode) {
    return DB_READ_ONLY;
  }
  trx = trx_allocate_for_background();
  trx_start_internal(trx, UT_LOCATION_HERE);

  std::string sql_str =
      "PROCEDURE ROLL_FORWARD_SCN() IS\n"
      "BEGIN\n"

      "DELETE FROM \"" SCN_HISTORY_TABLE_FULL_NAME
      "\"\n"
      "WHERE\n";
  /* Fill search condition */
  if (order == SCN_FETCH_ORDER::UTC_ASC) {
    sql_str += "utc >= :condition; \n";
  } else if (order == SCN_FETCH_ORDER::UTC_DESC) {
    sql_str += "utc < :condition; \n";
  } else if (order == SCN_FETCH_ORDER::LIMIT_NO_DESC) {
    sql_str += "limit_no <= :condition; \n";
  }
  sql_str += "END;";

  pinfo = pars_info_create();
  pars_info_add_ull_literal(pinfo, "condition", condition);

  ret = que_eval_sql(pinfo, sql_str.c_str(), trx);

  if (ret == DB_SUCCESS) {
    trx_commit_for_mysql(trx);
  } else {
    trx->op_info = "rollback of internal trx on rolling forward SCN delete";
    trx_rollback_to_savepoint(trx, nullptr);
    trx->op_info = "";
    ut_a(trx->error_state == DB_SUCCESS);
  }

  trx_free_for_background(trx);

  return ret;
}

/**
  Rolling forward the SCN every SRV_SCN_HISTORY_INTERVAL.
*/
static dberr_t roll_forward_scn(bool need_delete) {
  pars_info_t *pinfo;
  dberr_t ret;
  ulint keep;
  ulint utc;
  ulint limit_no;
  char *memo;
  trx_id_t scn;
  trx_t *trx;
  ReadView *readview = nullptr;

  utc = time(nullptr);

  keep = utc - srv_scn_history_keep_seconds;

#ifdef UNIV_DEBUG
  /* select * from innodb_flashback_snapshot where utc >= utc - 60
     order by utc asc */
  scn_transform_result_t result;
  try_scn_transform(utc - 60, SCN_FETCH_ORDER::UTC_ASC, &result);
  DBUG_EXECUTE_IF("scn_debug_info", {
    fprintf(stderr,
            "Scn convert result: scn: %lu, utc: %lu, state: %s, err: %s\n",
            result.scn, result.utc,
            result.state == SCN_TRANSFORM_STATE::NOT_FOUND ? "NOT_FOUND"
                                                           : "SUCCESS",
            result.err == DB_SUCCESS ? "SUCCESS" : "ERROR");
  });
#endif

  trx = trx_allocate_for_background();
  if (srv_read_only_mode) {
    trx_start_internal_read_only(trx, UT_LOCATION_HERE);
  } else {
    trx_start_internal(trx, UT_LOCATION_HERE);
  }

  scn = trx->id;
  trx_sys->mvcc->view_open(trx->read_view, trx);
  limit_no = trx->read_view->low_limit_no();
  dd::String_type ss = trx->read_view->encode_to_dd_properties();
  memo = (char *)ss.c_str();

  // Copy the trx->read_view and add to list before close
  readview = ut::new_withkey<ReadView>(UT_NEW_THIS_FILE_PSI_KEY);
  ut_ad(readview);
  readview->copy_complete(*trx->read_view);
  readview->m_flashback_flag = true;
  flashback_sys->add_readview(readview);

  mutex_enter(&trx_sys->mutex);
  trx_sys->mvcc->view_close(trx->read_view, true);
  mutex_exit(&trx_sys->mutex);

#ifdef UNIV_DEBUG
  DBUG_EXECUTE_IF("scn_debug_info",
                  { fprintf(stderr, "Memo info_2: %s\n", memo); });
#endif

#ifndef NDEBUG
  while (DBUG_EVALUATE_IF("simulate_block_insert_snapshot", 1, 0)) {
    my_sleep(50 * 1000);
  }
#endif

  std::string sql_str =
      "PROCEDURE ROLL_FORWARD_SCN() IS\n"
      "BEGIN\n";
  if (need_delete) {
    sql_str += "DELETE FROM \"" SCN_HISTORY_TABLE_FULL_NAME
               "\"\n"
               "WHERE\n"
               "utc < :keep; \n";
  }
  sql_str += "INSERT INTO \"" SCN_HISTORY_TABLE_FULL_NAME
             "\"\n"
             "VALUES\n"
             "(\n"
             ":scn,\n"
             ":utc,\n"
             ":limit_no,\n"
             ":memo\n"
             ");\n"
             "END;";

  pinfo = pars_info_create();
  pars_info_add_ull_literal(pinfo, "keep", keep);
  pars_info_add_ull_literal(pinfo, "scn", scn);
  pars_info_add_ull_literal(pinfo, "utc", utc);
  pars_info_add_ull_literal(pinfo, "limit_no", limit_no);
  pars_info_add_str_literal(pinfo, "memo", memo);

  ret = que_eval_sql(pinfo, sql_str.c_str(), trx);

  if (ret == DB_SUCCESS) {
    trx_commit_for_mysql(trx);
  } else {
    trx->op_info = "rollback of internal trx on rolling forward SCN";
    trx_rollback_to_savepoint(trx, nullptr);
    trx->op_info = "";
    ut_a(trx->error_state == DB_SUCCESS);
  }

  trx_free_for_background(trx);

  flashback_sys->remove_readview(readview);

  return ret;
}

/**
  The only entrance of delete records of table innodb_flashback_snapshot.

  There are 3 different cases:

  1. set innodb_undo_retention = 0, which means delete all records
  2. the used undo size > innodb_undo_space_supremum_size, and the purge
     progress is blocked by innodb_flashback_snapshot
  3. the used undo size > innodb_undo_space_reserved_size

  For case 1, just delete all records;

  For case 2, delete some records to make the purge progress keep going;

  For case 3, satisfy the innodb_undo_retention, and delete extra records;
*/
static void delete_snapshot_records() {
  if (!srv_scn_history_task_enabled && srv_scn_history_keep_seconds == 0) return;

  dberr_t err = DB_SUCCESS;
  scn_transform_result_t result;
  ulint purge_limit_no = 0;

  /* 1. Get current undo used size */
  ulint used_size = (ulint)get_undo_total_used_size();
  ulint limit_size = mb_to_pages(undo_space_supremum_size);
  ulint reserve_size = mb_to_pages(undo_space_reserve_size);
  bool delete_all = srv_scn_history_keep_seconds == 0;
  bool force_delete = used_size > limit_size;
  bool need_delete =
      !srv_scn_history_task_enabled || (used_size > reserve_size);

  /* 2. Calculate the limit_no to delete records */
  if (delete_all) {
    purge_limit_no = std::numeric_limits<uint64_t>::max();
  } else if (force_delete && flashback_sys->get_blocking_purge()) {
    /* select * from innodb_flashback_snapshot where limit_no >= 0
       order by limit_no asc */
    try_scn_transform(0, SCN_FETCH_ORDER::LIMIT_NO_ASC, &result);
    if (result.err == DB_SUCCESS && result.utc > 0) {
      ulint low_limit_no = result.limit_no;
      /* select * from innodb_flashback_snapshot where limit_no < IB_UINT64_MAX
         order by limit_no desc */
      try_scn_transform(std::numeric_limits<uint64_t>::max(),
                        SCN_FETCH_ORDER::LIMIT_NO_DESC, &result);
      if (result.err == DB_SUCCESS && result.utc > 0) {
        ulint up_limit_no = result.limit_no;
        /* Note: it's just an empirical value (10%), which can be replaced by
        (1 - limit_size / used_size), but not necessary */
        purge_limit_no = low_limit_no + (up_limit_no - low_limit_no) * 0.1;
      }
    }
  } else if (need_delete) {
    ulint utc = time(nullptr) - srv_scn_history_keep_seconds;
    /* select * from innodb_flashback_snapshot where utc < utc
       order by utc desc */
    try_scn_transform(utc, SCN_FETCH_ORDER::UTC_DESC, &result);
    if (result.err == DB_SUCCESS && result.utc > 0) {
      purge_limit_no = result.limit_no;
    }
  }

  /* 3. Delete records */
  if (purge_limit_no == 0) return;
  /* delete from innodb_flashback_snapshot where limit_no <= purge_limit_no */
  err = roll_forward_scn_delete(purge_limit_no, SCN_FETCH_ORDER::LIMIT_NO_DESC);
  if (err != DB_SUCCESS) {
    ib::error(ER_RDS_FLASHBACK)
        << "Cannot delete from " << SCN_HISTORY_TABLE_FULL_NAME
        << " where limit_no < " << purge_limit_no << " : " << ut_strerr(err);
  }
}

/** Start the background thread of scn rolling forward */
void srv_scn_history_thread(void) {
  scn_transform_result_t result;
  THD *thd = create_internal_thd();
  dberr_t err = DB_SUCCESS;

  while (dict_sys->scn_hist == nullptr && !scn_history_start_shutdown) {
    thd->system_thread = SYSTEM_THREAD_DD_RESTART;
    dict_sys->scn_hist = dd_table_open_on_name(
        thd, nullptr, SCN_HISTORY_TABLE_FULL_NAME, false, DICT_ERR_IGNORE_NONE);
    thd->system_thread = SYSTEM_THREAD_BACKGROUND;

    if (dict_sys->scn_hist == nullptr) {
      os_event_wait_time(scn_history_event, std::chrono::seconds{1});
      os_event_reset(scn_history_event);
    }
  }

  if (dict_sys->scn_hist == nullptr) {
    destroy_internal_thd(thd);
    return;
  }

  /* Fetch the latest utc in innodb_flashback_snapshot:
     select * from innodb_flashback_snapshot where utc < IB_UINT64_MAX
     order by utc desc */
  try_scn_transform(std::numeric_limits<uint64_t>::max(),
                    SCN_FETCH_ORDER::UTC_DESC, &result);
  if (result.err == DB_SUCCESS && result.utc > 0) {
    if (result.utc > (ulint)time(nullptr)) {
      ib::error(ER_RDS_FLASHBACK)
          << "Cannot rolling forward scn for time in table is newer than now";
    }
  }

  /* Loop here, maintain the records in innodb_flashback_snapshot */
  while (!scn_history_start_shutdown) {
    os_event_wait_time(scn_history_event,
                       std::chrono::seconds{srv_scn_history_interval});

    if (!srv_scn_history_task_stop_all) {
      if (srv_scn_history_task_enabled && srv_scn_history_keep_seconds) {
        err = roll_forward_scn(false);
        if (err != DB_SUCCESS) {
          ib::error(ER_RDS_FLASHBACK)
              << "Cannot rolling forward scn and save into"
              << SCN_HISTORY_TABLE_FULL_NAME << ": " << ut_strerr(err);
        }
      }

      delete_snapshot_records();

      /* Set is_oldest_result_valid to false, which means the oldest readview
      can has been changed. */
      is_oldest_result_valid = false;
    }

    if (scn_history_start_shutdown) break;

    os_event_reset(scn_history_event);
  }

  destroy_internal_thd(thd);
}

/** Init the background thread attributes */
void srv_scn_history_thread_init() { scn_history_event = os_event_create(); }

/** Deinit the background thread attributes */
void srv_scn_history_thread_deinit() {
  ut_a(!srv_read_only_mode);
  ut_ad(!srv_thread_is_active(srv_threads.m_scn_hist));

  os_event_destroy(scn_history_event);
  scn_history_event = nullptr;
  scn_history_start_shutdown = false;
}

/** Shutdown the background thread of scn rolling forward */
void srv_scn_history_shutdown() {
  scn_history_start_shutdown = true;
  os_event_set(scn_history_event);
  srv_threads.m_scn_hist.join();
}

/**
  Fill m_ptr with ReadView according to the utc,
  this operation will reuse a read view or open a new read view.

  If the statement execute complete, release_asof_readview should
  be called to close the read view.

  @param[in/out]      snapshot      Snapshot on TABLE
*/
dberr_t fill_snapshot_readview(Snapshot_info_t *snapshot) {
  dberr_t err = DB_SUCCESS;
  ReadView *view = nullptr;

  ut_ad(snapshot->valid());
  if (snapshot->valid()) {
    ulint utc = snapshot->get_timestamp();
    err = flashback_sys->get_readview(view, utc);

    if (err == DB_SUCCESS) {
      ut_a(view != nullptr);
      snapshot->set_ptr((void *)view);
    }
  }

  return err;
}

/**
  Close the read  view in snapshot.

  @param[in/out]      snapshot      Snapshot on TABLE
*/
void release_snapshot_readview(Snapshot_info_t *snapshot) {
  if (snapshot->valid() && snapshot->get_ptr() != nullptr) {
    ReadView *view = (ReadView *)snapshot->get_ptr();
    flashback_sys->remove_readview(view);
    snapshot->reset();
  }
}

ReadView_guard::ReadView_guard(trx_t *trx)
    : m_trx(trx), m_backup_view(nullptr), m_valid(false), m_first_bind(true) {}

ReadView_guard::~ReadView_guard() { restore_readview(); }

/**
  Get read view for snapshot, and bind the read view to trx.

  @param[in,out]      prebuilt      prebuilt struct for the table handler
  @param[in]          origin_view   the original read view of trx
*/
dberr_t ReadView_guard::bind_snapshot(row_prebuilt_t *prebuilt,
                                      ReadView *origin_view) {
  dberr_t err = DB_SUCCESS;
  dict_index_t *index = prebuilt->index;
  TABLE *mysql_table = prebuilt->m_mysql_table;
  Snapshot_info_t *snapshot;
  ReadView *view = nullptr;

  if (srv_scn_valid_enabled && mysql_table != nullptr &&
      mysql_table->snapshot.valid()) {
    /* Can not be temporary table */
    if (index && index->table && index->table->is_temporary()) {
      ut_a(false);
    }

    /* Fill snapshot read view */
    snapshot = &mysql_table->snapshot;
    ut_a(snapshot->valid());
    if (snapshot->get_ptr() == nullptr) {
      err = fill_snapshot_readview(snapshot);

      /* Check primary key visibility */
      if (err == DB_SUCCESS) {
        ut_a(snapshot->get_ptr() != nullptr);
        view = (ReadView *)snapshot->get_ptr();
        ut_ad(index->table->first_index());
        bool is_visible = view->changes_visible(
            index->table->first_index()->trx_id, index->table->name);
        if (!is_visible) {
          err = DB_FLASHBACK_PK_INVISIBLE;
          view = nullptr;
        }
      }

      /* Check the utc of read view */
      if (srv_scn_print_warning && view != nullptr &&
          view->m_utc != snapshot->get_timestamp()) {
        THD *thd = current_thd;
        char buff[MAX_DATE_STRING_REP_LENGTH];
        MYSQL_TIME t_mysql_time;
        my_time_t nr = (my_time_t)view->m_utc;
        thd->time_zone()->gmt_sec_to_TIME(&t_mysql_time, nr);
        my_TIME_to_str(t_mysql_time, buff, 0);

        push_warning_printf(thd, Sql_condition::SL_WARNING,
                            ER_SNAPSHOT_MISMATCH, "The actual snapshot is: %s",
                            buff);
      }
    } else {
      view = (ReadView *)snapshot->get_ptr();
    }
  }

  /* Bind the history read view to trx */
  if (view != nullptr) {
    ut_ad(err == DB_SUCCESS);

    /* Store the original read view of trx */
    if (m_first_bind) {
      m_backup_view = origin_view;
      m_first_bind = false;
    }

    m_trx->read_view = view;
    m_valid = true;
  }

  return err;
}

/**
  Restore the original read view of trx.
*/
void ReadView_guard::restore_readview() {
  if (m_valid) {
    m_trx->read_view = m_backup_view;
    m_first_bind = true;
    m_valid = false;
  }
}

Flashback_manager_v2::Flashback_manager_v2()
    : m_blocking_purge(false),
      m_purge_block_status(SCN_PURGE_BLOCK_STATUS::NOT_BLOCK) {
  mutex_create(LATCH_ID_FLASHBACK_LIST, &m_mutex);
  UT_LIST_INIT(m_list);
}

Flashback_manager_v2::~Flashback_manager_v2() {
  for (ReadView *view = UT_LIST_GET_FIRST(m_list); view != nullptr;
       view = UT_LIST_GET_FIRST(m_list)) {
    UT_LIST_REMOVE(m_list, view);
    ut::delete_(view);
  }

  ut_a(UT_LIST_GET_LEN(m_list) == 0);

  mutex_free(&m_mutex);
}

/**
  Reuse or open a read view accordind to the utc. If there is a same read view
  in list, the read view will be reused and increase m_ref_count of read view.
  Unless open a new read view, add to list and m_ref_count is set to 1.

  @param[in/out]      view      ReadView
  @param[in]          utc       Measure by second.
  @retval             result
*/
dberr_t Flashback_manager_v2::get_readview(ReadView *&view, ulint utc) {
  dberr_t err = DB_SUCCESS;
  scn_transform_result_t result;

  /* 1. Search m_list first */
  mutex_enter(&m_mutex);
  for (const ReadView *readview = UT_LIST_GET_FIRST(m_list);
       readview != nullptr;
       readview = UT_LIST_GET_NEXT(m_flashback_list, readview)) {
    if (readview->m_utc == utc) {
      view = (ReadView *)readview;
      view->m_ref_count++;
      break;
    }
  }
  mutex_exit(&m_mutex);

  /* 2. Search innodb_flashback_snapshot and recovery a view */
  if (view == nullptr) {
    /* select * from innodb_flashback_snapshot where utc >= utc
       order by utc asc */
    try_scn_transform(utc, SCN_FETCH_ORDER::UTC_ASC, &result);

    // Handle result
    if (result.err == DB_SUCCESS) {
      // case 1: no records found
      if (result.state == SCN_TRANSFORM_STATE::NOT_FOUND) {
        err = DB_SNAPSHOT_OUT_OF_RANGE;
      }
      // case 2: the record is very new
      else if (result.utc == 0 ||
               result.utc - utc > srv_scn_valid_volume * 60) {
        err = DB_SNAPSHOT_OUT_OF_RANGE;
      }
      // case 3: maybe purged
      else if (result.limit_no < purge_sys->view.low_limit_no()) {
        err = DB_SNAPSHOT_OUT_OF_RANGE;
      }
    } else {
      // case 4: execute query error
      err = DB_FLASHBACK_INTERNAL_ERROR;
    }

    if (err == DB_SUCCESS) {
      ut_ad(result.utc > 0);

      /* Since there is a gap between search innodb_flashback_snapshot and
      add the history read view to m_list, it can be unsafe to use this
      history read view in row_search_mvcc. For example:

      1. search innodb_flashback_snapshot and get a record
      2. the scn thread delete the record
      3. purge_sys delete the undo (not blocked)
      4. genearate a history read view with the record and add to m_list
      5. row_search_mvcc use the history read view (unsafe)

      To solve this problem, step 4 should be under the protection of
      purge_sys->latch, and the limit_no of record can not be less than
      purge_sys->view.low_limit_no(). */

      DEBUG_SYNC_C("before_lock_purge_sys_latch");
      rw_lock_s_lock(&purge_sys->latch, UT_LOCATION_HERE);
      DEBUG_SYNC_C("after_lock_purge_sys_latch");

      if (result.limit_no < purge_sys->view.low_limit_no()) {
        err = DB_SNAPSHOT_OUT_OF_RANGE;
      } else {
        ReadView *readview = nullptr;
        readview = ut::new_withkey<ReadView>(UT_NEW_THIS_FILE_PSI_KEY);
        ut_ad(readview);
        readview->m_utc = result.utc;
        readview->m_flashback_flag = true;
        readview->decode_from_dd_properties(result.memo.c_str());

        add_readview(readview);
        view = readview;
      }

      rw_lock_s_unlock(&purge_sys->latch);
    }
    DEBUG_SYNC_C("after_decode_snapshot_readview");
  }

  return err;
}

/**
  Add a read view to list. The m_ref_count is set to 1.

  @param[in]      view      ReadView
*/
void Flashback_manager_v2::add_readview(ReadView *view) {
  mutex_enter(&m_mutex);
  ut_ad(view->m_ref_count == 0);
  view->m_ref_count = 1;
  UT_LIST_ADD_LAST(m_list, view);
  mutex_exit(&m_mutex);
}

/**
  Remove a read view from list. Close the read view if m_ref_count reach 0.

  @param[in]      view      ReadView
*/
void Flashback_manager_v2::remove_readview(ReadView *view) {
  mutex_enter(&m_mutex);
  ut_ad(view->m_ref_count > 0);
  view->m_ref_count--;
  if (view->m_ref_count == 0) {
    UT_LIST_REMOVE(m_list, view);
    ut::delete_(view);
  }
  mutex_exit(&m_mutex);
}

/**
  Get an oldest record from innodb_flashback_snapshot.

  TODO: what if the result is invalid? For example: the limit_no
  is too old and the undo has been purged.

  Note: this method can not be called under purge_sys->latch

  @param[in/out]  result    scn_transform_result_t
*/
void Flashback_manager_v2::get_oldest_readview_record(
    scn_transform_result_t *result) {
  /* The trx_purge can be called frequently, which may cause try_scn_transform
  become the hotspot. Since the expired readview records are deleted
  periodically, here no need to select from table every time. Only fetch the
  oldest readview if is_oldest_result_valid is false. */
  if (!is_oldest_result_valid.load() || oldest_result.err != DB_SUCCESS) {
    oldest_result.reset();
    /* select * from innodb_flashback_snapshot where limit_no >= 0
       order by limit_no asc */
    try_scn_transform(0, SCN_FETCH_ORDER::LIMIT_NO_ASC, &oldest_result);

    is_oldest_result_valid = true;
  }

  result->copy(&oldest_result);
}

/**
  Generate an oldest read view for purge_sys. The read view can be decoded
  from innodb_flashback_snapshot record or the history read view in use.

  Note: this method should be called under purge_sys->latch

  @retval             ReadView
*/
ReadView *Flashback_manager_v2::get_oldest_readview(
    const scn_transform_result_t *result) {
  ReadView *view = nullptr;
  /* Mark the oldest read view */
  ulint target_limit_no = std::numeric_limits<uint64_t>::max();

  /* 1. Reset status */
  m_blocking_purge = false;
  m_purge_block_status = SCN_PURGE_BLOCK_STATUS::NOT_BLOCK;

  /* 2. Check innodb_flashback_snapshot record first */
  if (result->err == DB_SUCCESS && result->utc > 0) {
    target_limit_no = result->limit_no;
  }

  /* 3. Search m_list */
  mutex_enter(&m_mutex);
  for (const ReadView *readview = UT_LIST_GET_FIRST(m_list);
       readview != nullptr;
       readview = UT_LIST_GET_NEXT(m_flashback_list, readview)) {
    if (readview->low_limit_no() <= target_limit_no) {
      view = (ReadView *)readview;
      target_limit_no = view->low_limit_no();
    }
  }
  if (view != nullptr) {
    view->m_ref_count++;
    m_purge_block_status = SCN_PURGE_BLOCK_STATUS::BLOCK_BY_VIEW;
  }
  mutex_exit(&m_mutex);

  /* 4. Decode read view from innodb_flashback_snapshot record */
  if (view == nullptr && target_limit_no != std::numeric_limits<uint64_t>::max()) {
    ut_a(result->err == DB_SUCCESS && result->utc > 0);
    ReadView *readview = nullptr;
    readview = ut::new_withkey<ReadView>(UT_NEW_THIS_FILE_PSI_KEY);
    ut_ad(readview);
    readview->m_utc = result->utc;
    readview->m_flashback_flag = true;
    readview->decode_from_dd_properties(result->memo.c_str());

    add_readview(readview);
    view = readview;
    m_purge_block_status = SCN_PURGE_BLOCK_STATUS::BLOCK_BY_RECORD;
  }

  return view;
}

}  // namespace im
