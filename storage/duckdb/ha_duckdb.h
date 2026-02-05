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

#include <sys/types.h>

#include "delta_appender.h"
#include "duckdb_types.h"
#include "my_base.h" /* ha_rows */
#include "my_compiler.h"
#include "mysql/plugin.h"
#include "duckdb/duckdb_query.h"
#include <handler.h> /* handler */
#include <sql_class.h>
#include "sql_show.h"
#include "sql_table.h"
#include "thr_lock.h" /* THR_LOCK, THR_LOCK_DATA */

extern handlerton *duckdb_hton;

/** @brief
  Duckdb_share is a class that will be shared among all open handlers.
*/
class Duckdb_share : public Handler_share {
 public:
  THR_LOCK lock;
  Duckdb_share();
  ~Duckdb_share() { thr_lock_delete(&lock); }
};

/** @brief
  Class definition for the storage engine
*/
class ha_duckdb : public handler {
  THR_LOCK_DATA lock;         ///< MySQL lock
  Duckdb_share *share;        ///< Shared lock info
  Duckdb_share *get_share();  ///< Get the share

 public:
  ha_duckdb(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_duckdb();

  enum row_type get_row_type() const;

  const char *table_type() const { return "DUCKDB"; }

  const char **bas_ext() const;

  /**
    Replace key algorithm with one supported by SE, return the default key
    algorithm for SE if explicit key algorithm was not provided.

    @sa handler::adjust_index_algorithm().
  */
  enum ha_key_alg get_default_index_algorithm()  {
    return HA_KEY_ALG_HASH;
  }

  bool is_index_algorithm_supported(enum ha_key_alg key_alg)  {
    return key_alg == HA_KEY_ALG_HASH;
  }

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const {
    /*
      We are saying that this engine is just statement capable to have
      an engine that can only handle statement-based logging. This is
      used in testing.
    */
    // TODO: supprt HA_NO_AUTO_INCREMENT through duckdb sequence.
    return (HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE |
            HA_NO_AUTO_INCREMENT | HA_NULL_IN_KEY | HA_CAN_INDEX_BLOBS);
  }

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx, uint part, bool all_parts) const {
    return 0;
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const {
    return HA_MAX_REC_LENGTH;
  }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_keys() const { return MAX_KEY; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_parts() const { return MAX_REF_PARTS; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const { return 10240; }

  uint max_supported_key_part_length(HA_CREATE_INFO *) const {
    return 10240;
  }

  /** @brief
    Called in test_quick_select to determine if indexes should be used.
  */
  double scan_time() {
    return (double)(stats.records + stats.deleted) / 20.0 + 10;
  }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  double read_time(uint, uint, ha_rows rows) {
    return (double)rows / 20.0 + 1;
  }

  /*
    Everything below are methods that we implement in ha_duckdb.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_duckdb.cc; it's a required method.
  */
  int open(const char *name, int mode, uint test_if_locked) ;  // required

  /** @brief
    We implement this in ha_duckdb.cc; it's a required method.
  */
  int close(void) ;  // required

  /** @brief
    We implement this in ha_duckdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(uchar *buf) ;

  /** @brief
    We implement this in ha_duckdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, uchar *new_data) ;

  /** @brief
    We implement this in ha_duckdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf) ;

  /** @brief
    We implement this in ha_duckdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) ;

  /** @brief
    We implement this in ha_duckdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar *buf) ;

  /** @brief
    We implement this in ha_duckdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar *buf) ;

  /** @brief
    We implement this in ha_duckdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf) ;

  /** @brief
    We implement this in ha_duckdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar *buf) ;

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan) ;  // required
  int rnd_end() ;
  int rnd_next(uchar *buf) ;             ///< required
  int rnd_pos(uchar *buf, uchar *pos) ;  ///< required
  void position(const uchar *record) ;   ///< required
  int info(uint) ;                       ///< required
  int extra(enum ha_extra_function operation) ;
  int external_lock(THD *thd, int lock_type) ;  ///< required
  int delete_all_rows(void) ;
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key) ;
  int delete_table(const char *table) ;
  int rename_table(const char *from, const char *to) ;
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info) ;  ///< required

  uint lock_count(void) const;

  THR_LOCK_DATA **store_lock(
      THD *thd, THR_LOCK_DATA **to,
      enum thr_lock_type lock_type) ;  ///< required

  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table, Alter_inplace_info *ha_alter_info) ;

  bool commit_inplace_alter_table(TABLE *altered_table,
                                  Alter_inplace_info *ha_alter_info,
                                  bool commit) ;

  int truncate() ;

  bool commit_and_begin();

 private:
  std::unique_ptr<duckdb::QueryResult> query_result;

  std::unique_ptr<duckdb::DataChunk> current_chunk;

  size_t current_row_index;

  MY_BITMAP m_blob_map;

  bool m_first_write;
};
