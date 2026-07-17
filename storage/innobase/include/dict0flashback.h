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

#ifndef dict0flashback_h
#define dict0flashback_h

#include "os0event.h"
#include "row0mysql.h"
#include "trx0types.h"
#include "univ.i"

#include "sql/table_ext.h"  // Snapshot_info_t

#ifdef UNIV_PFS_THREAD
extern mysql_pfs_key_t scn_history_thread_key;
#endif

#ifdef UNIV_PFS_MUTEX
extern mysql_pfs_key_t flashback_list_mutex_key;
#endif

namespace dd {
class Object_table;
}

class ReadView;

/** SCN history table named "INNODB_FLASHBACK_SNAPSHOT" */
#define SCN_HISTORY_TABLE_NAME "innodb_flashback_snapshot"
#define SCN_HISTORY_TABLE_FULL_NAME "mysql/innodb_flashback_snapshot"

/** Transformation state between scn and utc */
enum class SCN_TRANSFORM_STATE { NOT_FOUND, SUCCESS };

enum class SCN_FETCH_ORDER { UTC_ASC, UTC_DESC, LIMIT_NO_ASC, LIMIT_NO_DESC };

enum class SCN_PURGE_BLOCK_STATUS { NOT_BLOCK, BLOCK_BY_RECORD, BLOCK_BY_VIEW };

/** Transformation result between scn and utc */
struct scn_transform_result_t {
  trx_id_t scn;
  ulint utc;
  ulint limit_no;
  std::string memo;
  SCN_TRANSFORM_STATE state;
  dberr_t err;

  scn_transform_result_t() {
    scn = 0;
    utc = 0;
    limit_no = 0;
    state = SCN_TRANSFORM_STATE::NOT_FOUND;
    err = DB_SUCCESS;
  }

  void reset() {
    scn = 0;
    utc = 0;
    limit_no = 0;
    state = SCN_TRANSFORM_STATE::NOT_FOUND;
    err = DB_SUCCESS;
  }

  void copy(scn_transform_result_t *rh) {
    scn = rh->scn;
    utc = rh->utc;
    limit_no = rh->limit_no;
    memo = rh->memo;
    state = rh->state;
    err = rh->err;
  }
};

namespace im {

extern os_event_t scn_history_event;

/** SCN rolling forward interval */
extern ulint srv_scn_history_interval;

/** SCN rolling forward task */
extern bool srv_scn_history_task_enabled;
extern bool srv_scn_history_task_stop_all;

/** The max time of scn history record keeped */
extern ulint srv_scn_history_keep_seconds;

/** Whether to enable flashback query */
extern bool srv_scn_valid_enabled;

/**
  The max gap between the oldest read view in INNODB_FLASHBACK_SNAPSHOT
  and the time user specify, if the gap is larger than this value,
  the as of select will return error.
*/
extern ulint srv_scn_valid_volume;

/** Whether to print warnings if the snapshot is not complete matching */
extern bool srv_scn_print_warning;

/** The max undo size can flaskback query used (MB) */
extern ulint undo_space_supremum_size;

/** The reserved size can flaskback query used (MB) */
extern ulint undo_space_reserve_size;

/** Create INNODB_FLASHBACK_SNAPSHOT table */
dd::Object_table *create_innodb_scn_hist_table();

/** Register InnoDB flashback backend service. */
void register_innodb_flashback_service();

/** Start the background thread of scn rolling forward */
extern void srv_scn_history_thread(void);

/** Shutdown the background thread of scn rolling forward */
extern void srv_scn_history_shutdown();

/** Init the background thread attributes */
extern void srv_scn_history_thread_init();

/** Deinit the background thread attributes */
extern void srv_scn_history_thread_deinit();

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
                       scn_transform_result_t *result);

/**
  Fill m_ptr with ReadView according to the utc,
  this operation will reuse a read view or open a new read view.

  If the statement execute complete, release_asof_readview should
  be called to close the read view.

  @param[in/out]      snapshot      Snapshot on TABLE
*/
dberr_t fill_snapshot_readview(Snapshot_info_t *snapshot);

/**
  Close the read  view in snapshot.

  @param[in/out]      snapshot      Snapshot on TABLE
*/
extern void release_snapshot_readview(Snapshot_info_t *snapshot);

/**
  Guard class of ReadView.

  This is used in row_search_mvcc, which will replace the read view
  of trx by snapshot, and restore to the original read view when leave
  row_search_mvcc.
*/
class ReadView_guard {
 public:
  explicit ReadView_guard(trx_t *trx);
  virtual ~ReadView_guard();

  /**
    Get read view for snapshot, and bind the read view to trx.

    @param[in,out]      prebuilt      prebuilt struct for the table handler
    @param[in]          origin_view   the original read view of trx
  */
  dberr_t bind_snapshot(row_prebuilt_t *prebuilt, ReadView *origin_view);

  /**
    Restore the original read view of trx.
  */
  void restore_readview();

 private:
  /** The trx to protect */
  trx_t *m_trx;
  /** The original read view of trx */
  ReadView *m_backup_view;
  /** Whether thr read view of trx need to restore */
  bool m_valid;
  /** Whether the first time to bind snapshot. If true, should save the
  original read view of trx to m_backup_view */
  bool m_first_bind;
};

/**
  A class to manage the read view recovery from innodb_flashback_snapshot.
  All opened read views will be added into a list.
*/
class Flashback_manager_v2 {
 public:
  explicit Flashback_manager_v2();
  virtual ~Flashback_manager_v2();

  /**
    Reuse or open a read view accordind to the utc. If there is a same read view
    in list, the read view will be reused and increase m_ref_count of read view.
    Unless open a new read view, add to list and m_ref_count is set to 1.

    @param[in/out]      view      ReadView
    @param[in]          utc       Measure by second.
    @retval             result
  */
  dberr_t get_readview(ReadView *&view, ulint utc);

  /**
    Add a read view to list. The m_ref_count is set to 1.

    @param[in]      view      ReadView
  */
  void add_readview(ReadView *view);

  /**
    Remove a read view from list. Close the read view if m_ref_count reach 0.

    @param[in]      view      ReadView
  */
  void remove_readview(ReadView *view);

  void get_oldest_readview_record(scn_transform_result_t *result);

  ReadView *get_oldest_readview(const scn_transform_result_t *);

  /** Get the list size without mutex protect */
  ulint get_list_size() { return UT_LIST_GET_LEN(m_list); }

  void blocking_purge() { m_blocking_purge = true; }

  bool get_blocking_purge() { return m_blocking_purge; }

  ulint get_purge_block_status() { return (ulint)m_purge_block_status; }

  void reset_purge_block_status() {
    m_purge_block_status = SCN_PURGE_BLOCK_STATUS::NOT_BLOCK;
  }

 private:
  typedef UT_LIST_BASE_NODE_T(ReadView, m_flashback_list) view_list_t;

  /** Protect list and m_ref_count */
  ib_mutex_t m_mutex;
  /** The list store opened read views */
  view_list_t m_list;
  /** Whether the purge process is blocked by innodb_flashback_snapshot */
  bool m_blocking_purge;
  /** Mark source of oldest view, from record or history view in use */
  SCN_PURGE_BLOCK_STATUS m_purge_block_status;
};

/** Global instance of Flashback_manager_v2 */
extern Flashback_manager_v2 *flashback_sys;
/** Get undo total used size */
unsigned long long get_undo_total_used_size();

}  // namespace im

#endif