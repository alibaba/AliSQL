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

#ifndef BINLOG_EXT_INCLUDED
#define BINLOG_EXT_INCLUDED

#include <my_systime.h>
#include <atomic>
#include <map>
#include <memory>
#include <vector>
#include "libbinlogevents/include/control_events.h"
#include "map_helpers.h"
#include "my_inttypes.h"
#include "mysql/components/services/bits/mysql_mutex_bits.h"
#include "rpl_commit_stage_manager.h"
#include "spin_lock.h"
#include "sql/basic_ostream.h"
#include "sql/binlog.h"
#include "sql/log_event.h"
#include "sql/server_redo_log.h"
#include "sql/set_var.h"
#include "sql/sql_class.h"
#include "sql/xa.h"

const char BINLOG_CACHE_DIR[FN_REFLEN] = "#alisql_binlog_cache_temp_file";
const char BINLOG_CACHE_NAME[FN_REFLEN] = "rds_binlog_cache";

/**
   The format of binlog redo log looks like:
   +----------+--------------+-------------------+-----------+
   | Type(1B) | File Num(4B) | File Position(8B) | File Data |
   +----------+--------------+-------------------+-----------+
   - Type is server_redo_log::LOG_TYPE_BINLOG
   - File Num is the suffix number of the binlog file
   - File Pos is the offset of the binlog events.
   - File Data is the binlog events.
*/
struct Binlog_redo_log_format {
  static const uint FILE_NUM_OFFSET = 1;
  static const uint FILE_POS_OFFSET = 5;
  static const uint FILE_DATA_OFFSET = 13;
};

/** Read a binlog redo log from given buffer. */
class Binlog_redo_log_reader : private Binlog_redo_log_format {
 public:
  Binlog_redo_log_reader(const uchar *buffer, uint length)
      : m_buffer(buffer), m_length(length) {}

  /* return the value of File Num field. */
  uint log_num() {
    return *reinterpret_cast<const uint *>(m_buffer + FILE_NUM_OFFSET);
  }
  /* return the value of File Pos field */
  my_off_t log_pos() { return uint8korr(m_buffer + FILE_POS_OFFSET); }

  /* return the pointer of File Data field */
  const uchar *data() { return m_buffer + FILE_DATA_OFFSET; }
  /* return the length of File Data field */
  uint data_len() { return m_length - FILE_DATA_OFFSET; }

 private:
  const uchar *m_buffer;
  uint m_length;
};

/** Extension of MYSQL_BIN_LOG. */
class Binlog_ext {
 public:
  class Async_writer;

  Binlog_ext();

  /* Lock for sync binlog file in redo flusher thread */
  Spin_lock *get_file_sync_lock() { return &m_file_sync_lock; }
  bool file_sync_lock_no_wait() { return m_file_sync_lock.lock_no_wait(); }
  void file_sync_lock() { m_file_sync_lock.lock(); }
  void file_sync_unlock() { m_file_sync_lock.unlock(); }

  /**
    Whether persisting engine log is required for the flush group. Right now it
    just returns false because the recovery-apply-binlog machinery which used
    it was reverted.

    @retval   true   Persisting engine log is required.
    @retval   false  Persisting engine log is not required.
  */
  bool force_ha_flush_log(THD *thd);

  /*
    Initialize the mutex and conditions which sould be initialized
    with Binlog::init_pthread_objects() together.
   */
  void init_pthread_objects();
  /**
    Initialize at server startup.

    @param[in]  is_normal_startup  True if it is a server startup but not
                                   server initialization, help
                                   or option validation.
  */
  bool init(bool is_normal_startup = true);
  void cleanup();

  /**
    Binlog_ext has something to do when opening or rotating binlog file. It is
    called just after MYSQL_BIN_LOG::m_binlog_file is opened.

    @retval  false  Succeeds.
    @retval  true   Error happens.
  */
  bool open_binlog_file();

  /**
    Whether the transaction group should be persisted to redo log.
    @param[in]  thd  THD of the session.

    @retval  true   It should be persisted to redo log.
    @retval  false  It should NOT be persisted to redo log.
  */
  bool should_persist_to_redo(THD *thd);

  /**
    Commits a transaction.

    @param[in]  thd  thd object of the session.

    @retval  false  Succeeds
    @retval  true   Error
  */
  bool commit(THD *thd);

  /**
    Wait until all data in the asynchronous write buffer is written into binlog
    file.

    @param[in]  thd  thd object of the session.
  */
  void switch_to_sync_mode(THD *thd);

  /**
    For internal XA transactions, commit process looks like:
    - MYSQL_BIN_LOG::ordered_commit()
      - flush binlog(m_atomic_prep_xids++)
      - engine commit
      - finish_commit()(m_atomic_prep_xids--)
      - rotate() (wait until m_atomic_prep_xids is 0)

    m_atomic_prep_xids is used to block binlog rotations between write binlog
    and engine commit.
  */
  class XA_rotate_guard {
   public:
    XA_rotate_guard(THD *thd, const bool *has_error)
        : m_thd(thd), m_has_error(has_error) {}
    ~XA_rotate_guard();

   private:
    THD *m_thd;
    const bool *m_has_error;
  };

  /**
    XA transaction increase m_atomic_prep_xids for blocking binlog rotation.

    @param[in]  thd  THD object of the xa transaction.
  */
  void xa_inc_prep_xids(THD *thd);
  /**
    XA transaction checks whether rotate is needed. For XA transactions,
    Rotation cannot be done in ordered_commit. It is delayed until finishing
    engine prepare/commit/rollback.

    @param[in]  thd   THD object of the xa transaction.
    @param[in]  do_rotate  true means rotate is needed

    @retval true if it is a rotation happens on XA transaction
  */
  bool xa_delay_rotate(THD *thd, bool do_rotate);

  mysql_mutex_t *get_writeset_history_lock() {
    return &m_writeset_history_mutex;
  }

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
  bool duckdb_recover(const char *log_name);

  /**
    When a new binlog is created, duckdb_binlog_position's position should
    be updated to the position of the new binlog file.
  */
  bool duckdb_binlog_rotate();

 private:
  Binlog_ext &operator=(const Binlog_ext &) = delete;
  Binlog_ext(const Binlog_ext &) = delete;
  Binlog_ext(Binlog_ext &&) = delete;

  void update_gtid_after_commit(THD *first_seen);

  /**
    Whether init() has completed. The server may abort before init() runs (for
    instance on an invalid option); cleanup() must not tear down objects which
    were never set up.
  */
  bool m_inited = false;

  /**
    Writeset history is accessed in write_transaction which is protected by
    LOCK_log in BGC. This mutex is added to protect writeset history in
    optimized BGC in which writeset history will be accessed in parallel.
  */
  mysql_mutex_t m_writeset_history_mutex;
  /**
     Spin lock for protect the binlog sync operation and binlog file open, close
     operations. Sync is skipped if it is opening or closing the binlog file.
  */
  Spin_lock m_file_sync_lock;

  class Redo_observer : public server_redo_log::Observer {
   public:
    bool before_checkpoint(uint64) override;
  };
  Redo_observer m_redo_observer;

  /** The suffix number of current binlog file. It is stored in big-endian. */
  uint m_log_num;
  /** Points to the log name without path */
  char *m_log_name;

  rpl_sidno m_current_server_sidno;

  void rotate(THD *thd);
};
extern Binlog_ext mysql_bin_log_ext;

/* Option globals of the persist-binlog-into-redo feature. */
extern bool opt_persist_binlog_to_redo;
extern uint opt_persist_binlog_to_redo_size_limit;
extern uint opt_sync_binlog_interval;
extern uint opt_binlog_buffer_size;
extern bool opt_wait_binlog_flush;
extern uint opt_binlog_group_delay;
extern uint opt_binlog_group_delay_running_threads;

/* Sys var update callback for opt_sync_binlog_interval. */
bool update_sync_binlog_interval(sys_var *, THD *, enum_var_type);

void trx_cache_write_event(THD *thd, Log_event *event);

/**
  This class provide functions of Binlog cache free flush, the main process is
  in ::commit().
*/
class Binlog_cache_free_flush {
 public:
  Binlog_cache_free_flush();

  /**
    Create or clear the binlog cache dir to store the binlog cache temp file, at
    the same dir with binlog file.

    Binlog cache free flush change the temp file type from UNLINK_FILE to
    KEEP_FILE, because UNLINK_FILE cannot be persisted to file system before
    Linux 3.11.

    Use the dir created by this function to contain binlog cache temp file.
    Binlog cache will clear its temp file during destruction. If server not
    shutdown normally, this dir will be cleared during next time startup.
  */
  void init_binlog_cache_dir();

  /**
    Save binlog new file name generated during rotate. The binlog cache tmp file
    will be rename to this name during persist.

    @param[in] new_name, new binlog file name, should not exceed 'FN_REFLEN'
    bytes including '\0'.
  */
  void save_new_file_name(const char *new_name);

  /**
    This function convert the long transaction's binlog cache tmp file to binlog
    file, and finish the flush, sync and commit stage in ordered_commit.

    After executed this function, binlog files will be:

    mysql-binlog.000001      (Active binlog file before free flush)
    mysql-binlog.000002      (Binlog file of the long transaction)
    mysql-binlog.000003      (Active binlog file after free flush)

    @param[in] thd, thd of current session.

    @return If true, skip flush and sync stage, error store in
    thd->commit_error. If false, go back to original logic.
  */
  bool commit(THD *thd);

  /**
    Check whether free flush should be executed on the transaction.

    @param[in] thd thd of current session.
 */
  inline bool check(THD *thd);

  /**
    Check whether reserved space is enough for front events of binlog file.

    @param[in] thd thd of current session.
  */
  inline bool check_reserved_space_enough(my_off_t reserved_size);

  /**
    Add keep tmp file count and return old values.

    @param[out] old value of keep tmp file count.
  */
  ulonglong inc_tmp_file_count() { return m_tmp_file_count.fetch_add(1); }

  const char *get_tmp_file_name() const { return m_tmp_file_name; }

  bool is_free_flushing() const { return m_is_free_flushing; }

  bool is_enabled() const { return m_is_enabled; }

  bool is_dir_initialized() const { return m_is_dir_initialized; }

  ulong get_reserved_size() const { return m_reserved_size.load(); }

  void update_reserved_size(ulong position) {
    m_reserved_size.store(position + binary_log::Gtid_event::MAX_EVENT_LENGTH +
                          512);
  }

  const ulonglong &get_limit_size_var() { return m_limit_size; }

  const bool &get_enabled_var() { return m_is_enabled; }

 private:
  Binlog_cache_free_flush &operator=(const Binlog_cache_free_flush &) = delete;
  Binlog_cache_free_flush(const Binlog_cache_free_flush &) = delete;
  Binlog_cache_free_flush(Binlog_cache_free_flush &&) = delete;

  /**
    Persist binlog cache tmp file to binlog file.

    This function persist binlog cache tmp file with file name stored in
    m_current_new_name, and write the new file name to binlog index file.

    @return false on success, true on error.
  */
  bool rename_temp_to_binlog();

  /**
    Calculate the gtid event size. Use the logic in write_transaction to make it
    real.

    @param[out] gtid event size.
  */
  my_off_t get_gtid_event_length(THD *thd);

  /**
    Calculate the size of header and front events for binlog cache free flush.

    Should wait for prepare xid equal to zero before calculate previous gtid
    event length if accurate_calculation is true.

    @param[in] accurate_calculation Whether calculate accurate front event
    length.
    @param[out] header and front events size.
  */
  ulong calculate_front_event_length();

  /** New name generated by binlog rotate. */
  char m_current_new_name[FN_REFLEN];

  /** Tmp file name of binlog cache. */
  char m_tmp_file_name[FN_REFLEN];

  /** Var of binlog cache free flush limit size. */
  ulonglong m_limit_size;

  /** Var of binlog cache free flush. */
  bool m_is_enabled;

  /** Whether binlog cache dir initialized success. */
  bool m_is_dir_initialized;

  /** Whether in binlog cache free flush process. */
  bool m_is_free_flushing;

  /** Size of binlog cache reserve for front events. */
  std::atomic<ulong> m_reserved_size;

  /** Record the number of keep tmp files created. */
  std::atomic<ulonglong> m_tmp_file_count;
};
extern Binlog_cache_free_flush binlog_cache_free_flush;

/*
  At server startup, Binlog_redo_recovery registers a redo log applier. Redo log
  recovery process will feed the binlog redo log to it. It will write the
  binlog events back to binlog file if binlog file lost the events.
*/
class Binlog_redo_recovery : server_redo_log::Applier {
 public:
  Binlog_redo_recovery();
  ~Binlog_redo_recovery();

  bool recovery_begin() override;
  bool recovery_end() override;
  bool apply(const unsigned char *ptr, uint len) override;

 private:
  /** name of the last binlog file */
  char m_log_name[FN_REFLEN];
  /** the suffix number of the last binlog file. It is in big-endian */
  uint m_log_num = 0;
  /** length of the last binlog file */
  my_off_t m_log_length = 0;
  /** The last binlog file has checksum or not */
  bool m_have_checksum = false;

  /** It is for writing binlog events back to the last binlog file */
  std::unique_ptr<MYSQL_BIN_LOG::Binlog_ofile> m_ofile;

  uchar *m_buffer = {nullptr};
  size_t m_buffer_size = 0;
  /**
    The binlog blocks has been copy into m_buffer.
    Key is binlog offset and value is length of the binlog block.
  */
  std::map<uint64_t, uint64_t> m_binlog_blocks;

  /**
    Increase recovery buffer for storing more binlog events.

    @param[in]  size  new size of the buffer.
  */
  bool increase_buffer(size_t size);
  /**
    Copy binlog events into recovery buffer.

    @param[in]  src  the buffer of the binlog events
    @param[in]  len  length of the binlog events.
    @param[in]  log_pos  binlog position of the binlog events.
  */
  void copy_to_buffer(const uchar *src, size_t len, my_off_t log_pos);
  /**
    Write the binlog events from recovery buffer to binlog file.

    @param[in]  log_pos  start position of the binlog event will be copy
    @param[in]  len      length of the binlog events will be copy
    @param[out] ostream  the output stream where binlog events will be written.
  */
  bool copy_to_file(my_off_t log_pos, size_t len, Basic_ostream *ostream);
};

#endif  // BINLOG_EXT_INCLUDED
