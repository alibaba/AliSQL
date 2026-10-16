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

#ifndef RDS_MYSQL_8_0_RPL_REPLICA_EXT_H
#define RDS_MYSQL_8_0_RPL_REPLICA_EXT_H

#include "binlog_ostream.h"     // RDS_binlog_cache_storage
#include "sql/binlog_reader.h"  // Relaylog_file_reader
#include "sql/log_event.h"      // Format_description_log_event
#include "sql/mysqld.h"
#include "sql/rpl_info.h"

#define BRR_HEADER_MAGIC_SIZE 2
#define BRR_HEADER_INDEX_SIZE 8

#define DEFAULT_REALTIME_APPLY_WORKER_HEARTBEAT_PERIOD 200
#define DEFAULT_REALTIME_APPLY_WORKERS 0
#define BRR_NONBLOCK_DDL_LOCK_WAIT_TIMEOUT 1
#define BRR_NONBLOCK_DDL_RETRY_INTERVAL 1

extern bool binlog_realtime_transmit_enable_log_message;
extern uint binlog_realtime_apply_worker_heartbeat_period;
extern ulong binlog_realtime_apply_workers;

#define LOG_BRR_ERROR(fmt, args...)                              \
  {                                                              \
    char msg[LOG_BUFF_MAX];                                      \
    snprintf(msg, LOG_BUFF_MAX, fmt, ##args);                    \
    LogErr(ERROR_LEVEL, ER_BINLOG_REALTIME_REPLICA_FAILED, msg); \
  }

#define LOG_BRR_WARNING(fmt, args...)                              \
  {                                                                \
    char msg[LOG_BUFF_MAX];                                        \
    snprintf(msg, LOG_BUFF_MAX, fmt, ##args);                      \
    LogErr(WARNING_LEVEL, ER_BINLOG_REALTIME_REPLICA_FAILED, msg); \
  }

#define LOG_BRR_MESSAGE(fmt, args...)                                     \
  {                                                                       \
    if (binlog_realtime_transmit_enable_log_message) {                    \
      char msg[LOG_BUFF_MAX];                                             \
      snprintf(msg, LOG_BUFF_MAX, fmt, ##args);                           \
      LogErr(INFORMATION_LEVEL, ER_BINLOG_REALTIME_REPLICA_MESSAGE, msg); \
    }                                                                     \
  }

class Master_info;
class Brr_rpl_info;
struct Brr_replica_info_t;

const unsigned char BRR_MAGIC_NUM = 0x0f;
extern const char *RDS_BRR_VERSION;

/** The type code of brr magic type. */
enum Brr_magic_type : int {
  NORMAL_EVENT,
  BRR_LONG_TRX_EVENT,
  BRR_ROLLBACK_EVENT,
  BRR_DDL_BEGIN_EVENT,
  BRR_DDL_EVENT,
  BRR_LONG_TRX_BEGIN_EVENT
};

/** The type code of brr trx. */
enum Brr_trx_type : int { BRR_UNKNOWN, BRR_LONG_TRX, BRR_DDL };

/** Initialize the psi keys for binlog realtime replica replica-side.*/
void init_brr_replica_psi_keys();

/**
  This cache contain the brr events sent from master, each transaction will
  have a brr_cache.
*/
class Brr_cache {
 public:
  Brr_cache() {}
  Brr_cache &operator=(const Brr_cache &) = delete;
  Brr_cache(const Brr_cache &) = delete;
  ~Brr_cache() { close(); }

  bool file_seek(my_off_t offset) { return m_file.truncate(offset); }

  bool write(const unsigned char *buffer, my_off_t length) {
    return m_file.write(buffer, length);
  }

  bool flush_and_update_pos(bool need_lock) {
    if (my_b_flush_io_cache(m_file.get_io_cache(), 1)) return true;
    update_end_pos(need_lock);
    return false;
  }

  my_off_t get_file_reserved_size() { return m_file_reserved_size; }

  ulonglong get_brr_index() { return m_brr_index; }

  const char *tmp_file_name() const { return m_file.tmp_file_name(); }

  void update_end_pos(bool need_lock);

  /** Return the end pos of binlog cache, include reserved space if exist. */
  my_off_t get_file_end_pos() const { return m_file.length(); }

  /** Get the readable pos of the brr cache. */
  my_off_t get_readable_pos() const { return m_brr_cache_end_pos; }

  bool flush_and_sync();

  /** Close and detach the cache tmp file. */
  void detach_temp_file();

  bool open(my_off_t cache_size, ulonglong brr_index, ulonglong reserved_size,
            ulonglong brr_io_version);

  void close();

  void set_brr_type(Brr_magic_type brr_event_type) {
    assert(m_brr_type == Brr_trx_type::BRR_UNKNOWN);

    if (brr_event_type == Brr_magic_type::BRR_DDL_BEGIN_EVENT) {
      m_brr_type = Brr_trx_type::BRR_DDL;
      replica_brr_ddl_received++;
    } else {
      assert(brr_event_type == Brr_magic_type::BRR_LONG_TRX_BEGIN_EVENT);
      m_brr_type = Brr_trx_type::BRR_LONG_TRX;
      replica_brr_long_trx_received++;
    }
  }

  Brr_trx_type get_brr_type() { return m_brr_type; }

  void set_brr_worker(Relay_log_info *worker) { m_worker = worker; }

  Relay_log_info *get_brr_worker() { return m_worker; }

  void set_brr_gtid_spec(const Gtid_specification &spec) {
    m_brr_gtid_spec = spec;
  }

  Gtid_specification get_brr_gtid_spec() { return m_brr_gtid_spec; }

  void set_file_reader(Relaylog_file_reader *file_reader) {
    m_file_reader = file_reader;
  }

  Relaylog_file_reader *get_file_reader() { return m_file_reader; }

  void set_complete() { m_is_complete = true; }

  bool is_complete() { return m_is_complete; }

  void set_assigned() { m_is_assigned = true; }

  bool is_assigned() { return m_is_assigned; }

  /* The number of normal events received by slave. */
  ulonglong trx_content_events_received = 0;

  /* The number of brr events received by slave. */
  ulonglong brr_events_received = 0;

  /*
    Whether the trx realtime applied by brr worker, this is used only by
    long trx.
  */
  bool realtime_apply_enabled = true;

 private:
  /*
    The offset of reverse space in front of the cache tmp file, reserved for
    relay log header and events.
  */
  std::atomic<my_off_t> m_file_reserved_size = 0;

  std::atomic<ulonglong> m_brr_index = 0;

  IO_CACHE_binlog_cache_storage m_file;

  std::atomic<my_off_t> m_brr_cache_end_pos = 0;

  std::atomic<Brr_trx_type> m_brr_type = Brr_trx_type::BRR_UNKNOWN;

  Gtid_specification m_brr_gtid_spec;

  /*
    When receiving Gtid_log_event or BRR_ROLLBACK_EVENT, the brr cache is
    complete.
  */
  bool m_is_complete = false;

  /* Whether the brr cache is assigned to a brr worker. */
  bool m_is_assigned = false;

  /*
    Brr transaction is assigned to brr worker, the brr worker will delete the
    brr cache when calling cleanup_brr_trx; otherwise, IO thread is responsible
    for deleting the brr cache.
  */
  Relay_log_info *m_worker = nullptr;

  Relaylog_file_reader *m_file_reader = nullptr;
};

/**
  This class provide functions to handle and queue the brr event.
*/
class Brr_rpl_info {
 public:
  void init_pthread_objects();

  void cleanup();

  /**
    Get brr cache from m_cache_map with brr index, and create a brr_cache if not
    found.

    @param index The brr index of target brr cache.
    @param reserved_size The reserved space size of current relay log cache.
    @param brr_type  The type of brr event.
    @param using_realtime_apply Whether the long trx using realtime apply.
    @return ptr of brr cache, nullptr if fail to create.
  */
  Brr_cache *get_cache_by_index(ulonglong index, ulong reserved_size,
                                Brr_magic_type brr_type,
                                bool using_realtime_apply);

  Format_description_log_event *get_description_event() {
    return m_description_event;
  }

  /**
    This function is called by the IO thread when the IO thread exits and before
    reconnecting.

    This function will clear the m_cache_map. If brr_cache in m_cache_map has
    been allocated, KILL_QUERY is set so that the brr worker thread rolls back
    brr_trx and destroys brr_cache, and the IO thread only erase brr_cache from
    m_cache_map; if brr_cache has not been allocated, the IO thread erase
    brr_cache from m_cache_map and destroys brr_cache.

    @param mi The Master_info object.
  */
  void clear_realtime_transmit(Master_info *mi);

  /**
    Queue the brr event to brr cache.

    @param mi The Master_info object.
    @param event_buf The buffer of current event packet.
    @param event_len The length of current event packet.
    @return false if success, true otherwise.
  */
  bool queue_brr_event(Master_info *mi, const char *event_buf, ulong event_len);

  /**
    Handle the brr gtid event.

    Handle this event means the all brr events of this transaction have been
    transmitted to the slave, and all transactions before this transaction in
    master's binlog have been written into relay log.

    For log transaction, this function will rename the brr cache tmp file to
    relay log file. For DDL, it will just write it to brr cache tmp file and
    the tmp file will be removed when brr cache is destroyed.

    This function determines whether the brr transaction is being applied by
    the brr worker, if it is being applied, the brr worker thread will acquire
    the ownership of transaction's gtid, otherwise the brr cache is destroyed,
    and the brr transaction will be applied by normal worker.

    @param mi The Master_info object.
    @param brr_cache The ptr of current brr cache.
    @param event_buf The buffer of current event packet.
    @param event_len The length of current event packet.
    @return false if success, true otherwise.
  */
  bool handle_gtid_event(Master_info *mi, Brr_cache *brr_cache,
                         const char *event_buf, ulong event_len);

  /**
    Handle the brr format description event. This is the first brr event
    received from master, use this event to handle following brr events.

    @param event_buf The buffer of current event packet.
    @param event_len The length of current event packet.
    @return false if success, true otherwise.
  */
  bool handle_format_description_event(const char *event_buf, ulong event_len);

  /**
   Handling BRR_ROLLBACK_EVENT.

   If the brr transaction is being applied by the brr worker, set KILL_QUERY
   to notify brr worker to roll back this transaction; otherwise just remove
   the brr cache.

   @param brr_cache The ptr of current brr cache.
   @param clean_map If true clean m_cache_map, false will not clean
   m_cache_map.
  */
  void handle_brr_rollback(Brr_cache *brr_cache, bool clean_map = true);

  void wait_for_update(bool need_lock) {
    if (need_lock)
      mysql_mutex_lock(&m_data_lock);
    else
      mysql_mutex_assert_owner(&m_data_lock);

    struct timespec abstime;
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds{
        binlog_realtime_apply_worker_heartbeat_period * 1000000ULL};
    set_timespec_nsec(&abstime, timeout.count());
    mysql_cond_timedwait(&m_data_cond, &m_data_lock, &abstime);
    if (need_lock) mysql_mutex_unlock(&m_data_lock);
  }

  Brr_cache *get_cache_for_worker(THD *thd);

  Log_event *read_next_event(THD *thd);

  bool check_gtid_executed(Gtid_set *gtid_executed);

  /**
    Clean up brr transaction information.

    This function is called in the following three situations:
    1. An error occurred when the brr worker applied the transaction.
    2. The brr worker received a kill signal.
    3. The brr worker completed the application of the transaction.

    This function will do the following:
    1. Clear the m_assigned and m_worker flags of brr_cache.
    2. For brr_cache with m_finish_flag set, delete brr_cache; for
       brr_cache without m_finish_flag set, add m_trx_to_be_assigned
       and do not delete brr_cache, so that the brr transaction can be
       applied by other brr worker or the brr cache tmp_file can be
       converted to relay log file.

    @param thd THD context.
    @param error Whether there is an error before this.
  */
  void cleanup_brr_trx(THD *thd, int error);

  /**
    Brr worker wait Gtid_log_event or BRR_ROLLBACK_EVENT from source.
    If received Gtid_log_event, brr worker will commit the transaction.
    If received BRR_ROLLBACK_EVENT, IO thread will set KILL_QUERY to the
    thd, brr worker will rollback the transaction.

    @param thd THD context.
    @param processor The processor to apply row log will waiting. It's only
    set when the brr DDL is inplace DDL.
    @return If true, rollback the brr DDL, else commit the brr DDL.
  */
  bool wait_for_gtid_event(THD *thd);

  /**
    This function is used by long transaction realtime apply. Long transaction
    need to wait for the IO thread to receive the Gtid_log_event before
    applying Xid_log_event.
    The waiting logic of DDL is not used because long transaction only transmit
    one copy, and the Gtid_log_event is written to the header of the relay log
    file.

    @param thd THD context.
    @return If true, rollback the brr long transaction, else commit the brr long
            transaction.
  */
  bool wait_for_gtid_spec(THD *thd);

  /**
    Check whether the brr worker receives an interrupt signal. When the IO
    thread exits or receives BRR_ROLLBACK_EVENT, it sets KILL_QUERY to roll back
    the brr trx; when the SQL thread exits, it sets KILL_QUERY to roll back the
    brr trx and sets brr_abort_running to exit the brr worker thread.

    @param thd THD context.
    @return If true, brr worker receives an interrupt signal.
  */
  bool brr_worker_interrupted(THD *thd);

  /**
    Add Gtid to owned_gtid so that normal worker threads can wait when applying
    Gtid_log_event.

    @param brr_index The index of brr cache.
    @param thread_id The thread id.
    @param spec The specification of a GTID for a statement to be executed.

    @return If true, acquire the gtid ownership failed.
  */
  bool brr_acquire_gtid_ownership(ulonglong brr_index, my_thread_id thread_id,
                                  const Gtid_specification &spec);

  /**
    Set gtid related variables on THD.

    @param thd  THD context.
    @param spec The specification of a GTID for a statement to be executed.

    @return If true, gtid related variables are set.
  */
  bool brr_set_gtid_var(THD *thd, const Gtid_specification &spec);

  /**
    Get information about each brr cache.

    @param brr_info_list List that stores brr cache information.
  */
  void get_brr_replica_info_for_i_s(
      std::list<Brr_replica_info_t> &brr_info_list);

  /**
   The mutex and cond are used to protect data of Brr_rpl_info and Brr_cache
   and coordinate IO thread and brr worker thread.
  */
  mysql_mutex_t m_data_lock;
  mysql_cond_t m_data_cond;

  /**
    The number of brr worker threads. Used to that all brr worker threads have
    exited during shutdown.
  */
  ulonglong m_brr_worker_cnt = 0;

 private:
  /**
    Release resource of brr cache, and remove it from m_cache_map.

    @param brr_cache The ptr of current brr cache.
    @param gno The gtid gno of brr trx if this cache is removed because brr trx
    complete. It will be zero if brr trx is rollback.
    @param clean_map If true clean m_cache_map, false will not clean
    m_cache_map.
    @return false if success, true otherwise.
  */
  void remove_cache(Brr_cache *brr_cache, rpl_gno gno, bool clean_map = true);

  void set_description_event(Format_description_log_event *fdle) {
    assert(m_description_event == nullptr);
    m_description_event = fdle;
  }

  /** The map of brr cache, key is brr_index, value is brr_cache ptr. */
  std::unordered_map<ulonglong, Brr_cache *> m_cache_map;

  Format_description_log_event *m_description_event = nullptr;

  /**
    The number of brr transactions waiting to be applied by the brr
    worker.
  */
  ulonglong m_trx_to_be_assigned = 0;

  /**
    This flag is incremented when the IO thread is disconnected, so each time
    the IO thread is started, the file name of the created brr cache is
    different, even if the brr cache was accepted when the IO thread was started
    last time.
  */
  ulonglong m_brr_io_version = 0;
};

extern Brr_rpl_info brr_rpl_info;

struct Brr_replica_info_t {
  /** The index of brr cache. */
  ulonglong index;

  /** The type of brr cache. */
  char type[BRR_TYPE_LEN];

  /** The name of binlog cache tmp file of this brr cache. */
  char file_name[FN_REFLEN];

  /** Record the Gtid of this transaction. */
  rpl_gno gno;

  /** The readable pos of the brr cache. */
  ulonglong received_pos;

  /** The number of brr events received by slave. */
  ulonglong brr_events_received;

  /** The number of normal events received by slave. */
  ulonglong trx_content_events_received;

  /**
    When receiving Gtid_log_event or BRR_ROLLBACK_EVENT, the brr cache is
    complete.
  */
  bool trx_complete;

  /** Whether the brr cache is assigned to a brr worker. */
  bool assigned_to_brr_worker;

  /** The pos of the brr cache being applied by the brr worker. */
  ulonglong apply_pos;
};

#endif  // RDS_MYSQL_8_0_RPL_REPLICA_EXT_H
