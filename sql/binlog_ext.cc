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

/**
  @file

  Extends the binlog code for AliSQL
*/

#include "sql/binlog_ext.h"

#include <atomic>
#include <cstring>

#include "handler.h"          // ha_flush_logs
#include "my_dir.h"           // my_dir
#include "my_loglevel.h"
#include "my_sys.h"           // my_micro_time, my_mkdir
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "sql/binlog_reader.h"
#include <mutex>
#include <sstream>
#include "mutex_lock.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_thread.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysqld_error.h"
#include "sql/clone_handler.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"

#include "sql/duckdb/duckdb_config.h"
#include "sql/duckdb/duckdb_context.h"
#include "sql/duckdb/duckdb_query.h"
#include "sql/mysqld.h"       // log_bin_basename
#include "sql/rpl_gtid.h"     // Gtid_set, gtid_state
#include "sql/rpl_handler.h"  // RUN_HOOK

Binlog_cache_free_flush binlog_cache_free_flush;

/*
  atomic<T>::value_type is not define in old versions, so micros
  are used instead of the template functions
*/
#define relax_load(t) t.load(std::memory_order_relaxed)
#define relax_store(t, v) t.store(v, std::memory_order_relaxed)

Binlog_ext mysql_bin_log_ext;
bool opt_persist_binlog_to_redo = false;
uint opt_persist_binlog_to_redo_size_limit = 1024 * 1024;
uint opt_sync_binlog_interval = 50000;
uint opt_binlog_buffer_size = 20 * 1024 * 1024;
bool opt_wait_binlog_flush = true;
uint opt_binlog_group_delay = 100;
uint opt_binlog_group_delay_running_threads = 100;

static PSI_thread_key key_thread_binlog_flush;
static PSI_thread_key key_thread_binlog_sync;
static PSI_mutex_key key_binlog_flush_mutex;
static PSI_mutex_key key_binlog_write_queue_node_mutex;
static PSI_mutex_key key_binlog_sync_mutex;
static PSI_mutex_key key_writeset_history_mutex;
static PSI_cond_key key_binlog_flush_cond;
static PSI_cond_key key_binlog_flushed_cond;
static PSI_cond_key key_binlog_write_queue_node_cond;
static PSI_cond_key key_binlog_sync_cond;
static PSI_cond_key key_binlog_synced_cond;
static PSI_memory_key key_memory_binlog_buffer;

static PSI_stage_info stage_waiting_for_binlog_write_queue_empty = {
    0, "Waiting for binlog write queue empty", 0, PSI_DOCUMENT_ME};
static PSI_stage_info stage_waiting_for_binlog_write_queue_node_available = {
    0, "Waiting for binlog write queue node available", 0, PSI_DOCUMENT_ME};
static PSI_stage_info stage_waiting_for_binlog_write_buffer_available = {
    0, "Waiting for binlog write buffer available", 0, PSI_DOCUMENT_ME};
static PSI_stage_info stage_waiting_for_binlog_written = {
    0, "Waiting for binlog events to be written into binlog file", 0,
    PSI_DOCUMENT_ME};

#ifdef HAVE_PSI_INTERFACE
static void binlog_ext_init_psi_keys() {
  const char *category = "sql";
  int count;

  static PSI_thread_info all_threads[] = {
      {&key_thread_binlog_flush, "binlog_flush_thread", "bl_flush",
       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
      {&key_thread_binlog_sync, "binlog_sync_thread", "bl_sync",
       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME}};
  static PSI_mutex_info all_mutexs[] = {
      {&key_binlog_flush_mutex, "Binlog_ext::Async_writer::m_flush_mutex",
       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
      {&key_binlog_sync_mutex, "Binlog_ext::Async_writer::m_sync_mutex",
       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
      {&key_binlog_write_queue_node_mutex,
       "Binlog_ext::Async_writer::Write_state::mutex", 0, 0, PSI_DOCUMENT_ME},
      {&key_writeset_history_mutex, "Binlog_ext::writeset_history_mutex", 0, 0,
       PSI_DOCUMENT_ME}};
  static PSI_cond_info all_conds[] = {
      {&key_binlog_flush_cond, "Binlog_ext::Async_writer::m_flush_cond",
       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
      {&key_binlog_flushed_cond, "Binlog_ext::Async_writer::m_flushed_cond",
       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
      {&key_binlog_write_queue_node_cond,
       "Binlog_ext::Async_writer::Write_state::cond", 0, 0, PSI_DOCUMENT_ME},
      {&key_binlog_sync_cond, "Binlog_ext::Async_writer::m_sync_cond",
       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
      {&key_binlog_synced_cond, "Binlog_ext::Async_writer::m_synced_cond",
       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME}};
  static PSI_stage_info *all_stages[] = {
      &stage_waiting_for_binlog_write_queue_empty,
      &stage_waiting_for_binlog_write_queue_node_available,
      &stage_waiting_for_binlog_write_buffer_available,
      &stage_waiting_for_binlog_written};
  static PSI_memory_info all_memories[] = {
      {&key_memory_binlog_buffer, "Binlog_ext::Async_writer::binlog_buffer", 0,
       0, PSI_DOCUMENT_ME}};
  count = static_cast<int>(array_elements(all_threads));
  mysql_thread_register(category, all_threads, count);

  count = static_cast<int>(array_elements(all_mutexs));
  mysql_mutex_register(category, all_mutexs, count);

  count = static_cast<int>(array_elements(all_conds));
  mysql_cond_register(category, all_conds, count);

  count = static_cast<int>(array_elements(all_stages));
  mysql_stage_register(category, all_stages, count);

  count = static_cast<int>(array_elements(all_memories));
  mysql_memory_register(category, all_memories, count);
}
#endif

/**
   Auxiliary function used in Binlog_ext::commit().
*/
static inline int call_after_sync_hook_ext(THD *thd) {
  int res = 0;
  THD *next_to_commit = thd->next_to_commit;
  thd->next_to_commit = nullptr;
  res = call_after_sync_hook(thd);
  thd->next_to_commit = next_to_commit;
  return res;
}

/**
  It provides a mechanism to write binlog events of transactions to binlog file
  and sync binlog file in separate threads.
  - Flush thread for flushing binlog events from binlog file buffer to file
  - Sync thread for persisting the binlog file to storage.
*/
class Binlog_ext::Async_writer {
 public:
  /** Writing context used in write process */
  struct Write_ctx {
    // start position of the reserved buffer
    uint64_t start_pos = 0;
    // Reserved buffer size
    uint length = 0;

    // Write queue index of the queue node for the reserved buffer
    uint64_t queue_index = 0;
  };

  /** It implements Basic_ostream interface for writting data into the buffer */
  class Buffer_ostream : public virtual Basic_ostream {
   public:
    Buffer_ostream(Async_writer &writer, const Write_ctx &ctx)
        : m_ctx(ctx), m_writer(writer) {}

    bool write(const unsigned char *buffer, my_off_t length) override {
      assert(m_bytes_written + length <= m_ctx.length);

      if (m_writer.write_to_buffer(m_ctx.start_pos + m_bytes_written, buffer,
                                   length))
        return true;
      m_bytes_written += length;
      return false;
    }

   private:
    const Write_ctx &m_ctx;
    Async_writer &m_writer;
    uint m_bytes_written = 0;
  };

  Async_writer() {}
  Async_writer(const Async_writer &) = delete;
  Async_writer(const Async_writer &&) = delete;
  Async_writer operator=(const Async_writer &) = delete;
  Async_writer operator=(const Async_writer &&) = delete;

  bool init() {
    m_buffer = (uchar *)my_malloc(key_memory_binlog_buffer,
                                  opt_binlog_buffer_size, MYF(0));
    if (m_buffer == nullptr) return true;

    mysql_mutex_init(key_binlog_flush_mutex, &m_flush_mutex,
                     MY_MUTEX_INIT_SLOW);
    mysql_cond_init(key_binlog_flush_cond, &m_flush_cond);
    mysql_cond_init(key_binlog_flushed_cond, &m_flushed_cond);

    mysql_mutex_init(key_binlog_sync_mutex, &m_sync_mutex, MY_MUTEX_INIT_SLOW);
    mysql_cond_init(key_binlog_sync_cond, &m_sync_cond);
    mysql_cond_init(key_binlog_synced_cond, &m_synced_cond);

    for (auto &flush_cond : m_flush_conds) {
      mysql_mutex_init(key_binlog_write_queue_node_mutex, &flush_cond.mutex,
                       MY_MUTEX_INIT_SLOW);
      mysql_cond_init(key_binlog_write_queue_node_cond, &flush_cond.cond);
    }
    reset_queue();

    m_inited = true;
    return false;
  }

  void cleanup() {
    if (m_inited) {
      my_free(m_buffer);
      m_buffer = nullptr;

      mysql_mutex_destroy(&m_flush_mutex);
      mysql_cond_destroy(&m_flush_cond);
      mysql_cond_destroy(&m_flushed_cond);

      mysql_mutex_destroy(&m_sync_mutex);
      mysql_cond_destroy(&m_sync_cond);
      mysql_cond_destroy(&m_synced_cond);

      for (auto &flush_cond : m_flush_conds) {
        mysql_mutex_destroy(&flush_cond.mutex);
        mysql_cond_destroy(&flush_cond.cond);
      }
    }
  }

  bool is_inited() { return m_inited; }

  /**
    Clear end_pos of all queue nodes. It is called after binlog is rotated.
  */
  void reset_queue() {
    for (auto &node : m_write_queue_nodes) node.end_pos = 0;
  }

  /**
    Wait until write queue is empty and all data is persisted into storage.
  */
  void wait_for_queue_empty(THD *thd) {
    wait_for_flush(thd, stage_waiting_for_binlog_write_queue_empty,
                   [&] { return write_queue_empty(); });

    MUTEX_LOCK(guard, &m_sync_mutex);
    while (!sync_queue_empty()) {
      // awake sync thread
      m_need_synced_signal = true;
      mysql_cond_signal(&m_sync_cond);
      mysql_cond_wait(&m_synced_cond, &m_sync_mutex);
    }
  }

  void wait_for_queue_node_available(THD *thd, uint64_t index) {
    wait_for_flush(
        thd, stage_waiting_for_binlog_write_queue_node_available,
        [&] { return index - relax_load(m_flushed_index) < QUEUE_NODE_COUNT; });
    assert(index > relax_load(m_flushed_index));
  }

  void wait_for_buffer_available(THD *thd, const Write_ctx &ctx) {
    wait_for_flush(thd, stage_waiting_for_binlog_write_buffer_available, [&] {
      return relax_load(m_flushed_pos) + opt_binlog_buffer_size >=
             ctx.start_pos + ctx.length;
    });
  }

  /**
    Before Innodb engine makes checkpoint, it has to wait until the binlog
    data which will be dropped by the checkpoint is flushed into binlog file.
  */
  void wait_for_binlog_before_lsn_synced(uint64_t lsn) {
    if (lsn <= relax_load(m_lsn_of_synced_binlog)) return;

    MUTEX_LOCK(guard, &m_sync_mutex);
    while (lsn > relax_load(m_lsn_of_synced_binlog) && !sync_queue_empty()) {
      m_need_synced_signal = true;
      mysql_cond_signal(&m_sync_cond);
      mysql_cond_wait(&m_synced_cond, &m_sync_mutex);
    }
  }

  /**
    Assign binlog buffer to a transaction.
  */
  void allocate_buffer(Write_ctx *ctx, uint buffer_size) {
    assert(buffer_size < opt_binlog_buffer_size);
    assert(buffer_size <= opt_persist_binlog_to_redo_size_limit);
    mysql_mutex_assert_owner(&mysql_bin_log.LOCK_log);
    /*
      When switching from sync mode to async mode, m_flushed_pos and
      m_allocated_pos are invalid, they should be updated here.
    */
    if (!m_async_mode) {
      MUTEX_LOCK(guard, &mysql_bin_log.LOCK_sync);

      m_allocated_pos = mysql_bin_log.get_binlog_file()->position();
      relax_store(m_flushed_pos, m_allocated_pos);
      m_async_mode = true;
    }

    ctx->start_pos = m_allocated_pos;
    ctx->length = buffer_size;
    ctx->queue_index =
        m_allocated_index.fetch_add(1, std::memory_order_relaxed) + 1;
    m_allocated_pos += buffer_size;
  }

  /**
    Writes data into binlog file buffer.
  */
  bool write_to_buffer(uint64_t start_pos, const unsigned char *data,
                       int length) {
    assert(static_cast<uint64_t>(length) < opt_binlog_buffer_size);
    assert(start_pos >= relax_load(m_flushed_pos));

    int buffer_offset = start_pos % opt_binlog_buffer_size;

    if (buffer_offset + length < static_cast<int>(opt_binlog_buffer_size)) {
      memcpy(m_buffer + buffer_offset, data, length);
    } else {
      int first_part_length = opt_binlog_buffer_size - buffer_offset;

      memcpy(m_buffer + buffer_offset, data, first_part_length);
      memcpy(m_buffer, data + first_part_length, length - first_part_length);
    }

    return false;
  }

  /**
    Tells flush thread that its buffer is ready for writing into binlog file.
  */
  void write_to_buffer_done(const Write_ctx &ctx) {
    relax_store(queue_node(ctx.queue_index)->end_pos,
                ctx.start_pos + ctx.length);

    /* Flush thread is waiting, when request count is 0. So send a signal. */
    if (m_flush_request_count.fetch_add(1, std::memory_order_release) == 0) {
      mysql_mutex_lock(&m_flush_mutex);
      mysql_cond_signal(&m_flush_cond);
      mysql_mutex_unlock(&m_flush_mutex);
    }
  }

  /**
    Write binlog events of a transaction group from binlog file buffer to
    redo log.
  */
  uint64_t write_to_redo(const Write_ctx &ctx, uint32_t log_num) {
    uint64_t lsn;
    uchar head[Binlog_redo_log_format::FILE_DATA_OFFSET];
    int buffer_offset = ctx.start_pos % opt_binlog_buffer_size;
    my_off_t start_pos = ctx.start_pos;

    head[0] = server_redo_log::LOG_TYPE_BINLOG;
    *reinterpret_cast<uint32_t *>(head + Binlog_redo_log_format::FILE_NUM_OFFSET) =
        log_num;

    DBUG_EXECUTE_IF("simulate_event_checksum_split", {
      uint event_len = uint4korr(m_buffer + buffer_offset + EVENT_LEN_OFFSET);
      start_pos = opt_binlog_buffer_size - event_len + 2;
    });
    DBUG_EXECUTE_IF("simulate_event_header_split",
                    { start_pos = opt_binlog_buffer_size - 8; });

    int8store(head + Binlog_redo_log_format::FILE_POS_OFFSET, start_pos);

    // look the comment in write_to_buffer()
    if (buffer_offset + ctx.length < opt_binlog_buffer_size) {
      lsn =
          server_redo_log::write(head, Binlog_redo_log_format::FILE_DATA_OFFSET,
                                 m_buffer + buffer_offset, ctx.length);
    } else {
      int first_part_length = opt_binlog_buffer_size - buffer_offset;
      lsn =
          server_redo_log::write(head, Binlog_redo_log_format::FILE_DATA_OFFSET,
                                 m_buffer + buffer_offset, first_part_length,
                                 m_buffer, ctx.length - first_part_length);
    }
    return lsn;
  }

  /**
    Waiting for redo to be synced to the lsn of the binlog of a transaction
    group.
  */
  void sync_to_redo(THD *thd, uint64_t lsn, const Write_ctx &ctx) {
    // wait queue node available before wait_for_flush to save time.
    wait_for_queue_node_available(thd, ctx.queue_index);

    server_redo_log::wait_for_flush(lsn);

    write_to_buffer_done(ctx);

    DBUG_EXECUTE_IF("crash_commit_after_log", DBUG_SUICIDE(););
    DBUG_EXECUTE_IF("crash_commit_after_sync_binlog", DBUG_SUICIDE(););
    DBUG_EXECUTE_IF("crash_commit_after_sync_to_redo", DBUG_SUICIDE(););

    if (call_after_sync_hook_ext(thd))
      mysql_bin_log.handle_binlog_flush_or_sync_error(thd, true, nullptr);
  }

  /** Write the data ready from binlog file buffer to binlog file. */
  bool flush() {
    uint64_t head = relax_load(m_flushed_index) + 1;
    my_off_t binlog_offset = relax_load(m_flushed_pos);

    uint ready_nodes = 0;

    while (relax_load(queue_node(head + ready_nodes)->end_pos) > binlog_offset &&
           ready_nodes < QUEUE_NODE_COUNT) {
      ready_nodes++;
    }

    if (ready_nodes > 0) {
      // Get the end_pos of the last node ready for writting
      my_off_t ready_end_pos =
          relax_load(queue_node(head + ready_nodes - 1)->end_pos);
      uint64_t length = ready_end_pos - binlog_offset;
      uint64_t buffer_offset = binlog_offset % opt_binlog_buffer_size;
      MYSQL_BIN_LOG::Binlog_ofile *file = mysql_bin_log.get_binlog_file();

      if (buffer_offset + length <= opt_binlog_buffer_size) {
        if (file->write(m_buffer + buffer_offset, length)) return true;
      } else {
        int part1_len = opt_binlog_buffer_size - buffer_offset;
        if (file->write(m_buffer + buffer_offset, part1_len) ||
            file->write(m_buffer, length - part1_len))
          return true;
      }

      if (file->flush()) return true;
      /*
        It has been synced to redo, so it can be exposed to users before sync
        binlog file.
      */
      mysql_bin_log.update_binlog_end_pos();

      m_flushed_pos.store(ready_end_pos, std::memory_order_seq_cst);

      // Wake up the transactions which are waiting
      for (uint i = 0; i < ready_nodes; i++) {
        Write_state *node = queue_node(head + i);

        if (node->wait_index.load(std::memory_order_seq_cst) >= head) {
          Flush_cond &flush_cond = m_flush_conds[(head + i) % FLUSH_COND_COUNT];
          MUTEX_LOCK(guard, &flush_cond.mutex);
          mysql_cond_broadcast(&flush_cond.cond);
        }
      }

      // Move flushed index forward
      relax_store(m_flushed_index, head + ready_nodes - 1);
    }

    return false;
  }

  /** Synchronizes binlog file to storage. */
  bool sync() {
    assert(m_synced_index <= m_flushed_index);

    uint64_t flushed_index = relax_load(m_flushed_index);
    if (relax_load(m_synced_index) < flushed_index) {
      MYSQL_BIN_LOG::Binlog_ofile *file = mysql_bin_log.get_binlog_file();

      if (file && file->is_open()) {
        if (file->sync()) return true;

        relax_store(m_synced_index, flushed_index);

        DBUG_EXECUTE_IF("sync_binlog_thread_print_log", {
          LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                 "binlog synced by Async_writer::sync");
        });
      }
    }

    if (m_synced_index >= m_current_lsn_index) {
      if (m_current_lsn > m_lsn_of_synced_binlog) {
        relax_store(m_lsn_of_synced_binlog, m_current_lsn);
      }

      m_current_lsn = server_redo_log::get_current_lsn();
      m_current_lsn_index = relax_load(m_allocated_index);
    }

    return false;
  }

  void run_flush(THD *, const bool &terminate) {
    while (!terminate || !write_queue_empty()) {
      uint64_t request_count =
          m_flush_request_count.load(std::memory_order_acquire);

      if (flush()) {
        const char *errmsg = "Failed to write binlog by async_writer_thread";
        exec_binlog_error_action_abort(errmsg);
        break;
      }

      if (relax_load(m_flush_request_count) == request_count ||
          m_need_flushed_signal) {
        mysql_mutex_lock(&m_flush_mutex);

        if (m_need_flushed_signal) {
          mysql_cond_broadcast(&m_flushed_cond);
          m_need_flushed_signal = false;
        }

        if (m_flush_request_count.compare_exchange_weak(
                request_count, 0, std::memory_order_relaxed)) {
          mysql_cond_wait(&m_flush_cond, &m_flush_mutex);
        }

        mysql_mutex_unlock(&m_flush_mutex);
      }
    }
  }

  void run_sync(const bool &terminate) {
    while (!terminate) {
      if (sync()) {
        const char *errmsg = "Failed to fsync binlog by async_writer_thread";
        exec_binlog_error_action_abort(errmsg);
        break;
      }

      mysql_mutex_lock(&m_sync_mutex);

      if (m_need_synced_signal) {
        mysql_cond_broadcast(&m_synced_cond);
        m_need_synced_signal = false;
      }

      struct timespec abstime;

      // opt_sync_binlog_interval is microsecond
      set_timespec_nsec(&abstime, opt_sync_binlog_interval * 1000);
      (void)mysql_cond_timedwait(&m_sync_cond, &m_sync_mutex, &abstime);

      mysql_mutex_unlock(&m_sync_mutex);
    }
  }

  void wait_for_binlog_flushed(THD *thd, const Write_ctx &ctx) {
    my_off_t end_pos = ctx.start_pos + ctx.length;
    if (end_pos <= relax_load(m_flushed_pos)) return;

    PSI_stage_info old_stage;
    Write_state *state = queue_node(ctx.queue_index);
    Flush_cond &flush_cond = m_flush_conds[ctx.queue_index % FLUSH_COND_COUNT];

    mysql_mutex_lock(&flush_cond.mutex);
    thd->ENTER_COND(&flush_cond.cond, &flush_cond.mutex,
                    &stage_waiting_for_binlog_written, &old_stage);

    state->wait_index.store(ctx.queue_index, std::memory_order_seq_cst);
    while (end_pos > m_flushed_pos.load(std::memory_order_seq_cst)) {
      mysql_cond_wait(&flush_cond.cond, &flush_cond.mutex);
    }
    mysql_mutex_unlock(&flush_cond.mutex);
    thd->EXIT_COND(&old_stage);
  }

  void awake_flush_thread() {
    if (!m_inited) return;
    mysql_mutex_lock(&m_flush_mutex);
    mysql_cond_signal(&m_flush_cond);
    mysql_mutex_unlock(&m_flush_mutex);
  }

  void awake_sync_thread() {
    if (!m_inited) return;
    mysql_mutex_lock(&m_sync_mutex);
    mysql_cond_signal(&m_sync_cond);
    mysql_mutex_unlock(&m_sync_mutex);
  }

  void switch_to_sync_mode(THD *thd) {
    mysql_mutex_assert_owner(&mysql_bin_log.LOCK_log);
    if (m_async_mode) {
      wait_for_queue_empty(thd);
      m_async_mode = false;
    }
  }

 private:
  /** It is set to true after init() is called successfully */
  bool m_inited = {false};

  bool m_async_mode = {false};

  uchar *m_buffer = {nullptr};
  /** All binlog events before m_flush_pos are flushed into file. */
  std::atomic_uint64_t m_flushed_pos = {0};
  /** The binlog end position that already assigned to transactions */
  uint64_t m_allocated_pos = 0;

  struct Write_state {
    std::atomic<my_off_t> wait_index;
    std::atomic<my_off_t> end_pos;
  };
  static const int QUEUE_NODE_COUNT = 10000;
  Write_state m_write_queue_nodes[QUEUE_NODE_COUNT];
  std::atomic_uint64_t m_flushed_index = {0};
  std::atomic_uint64_t m_allocated_index = {0};

  /** Mutex for protecting operations related to flush thread. */
  mysql_mutex_t m_flush_mutex;
  mysql_cond_t m_flush_cond;
  mysql_cond_t m_flushed_cond;
  bool m_need_flushed_signal = {false};

  std::atomic_uint64_t m_flush_request_count = {0};

  /** Mutex for pretecting operations related to sync thread. */
  mysql_mutex_t m_sync_mutex;
  mysql_cond_t m_sync_cond;
  mysql_cond_t m_synced_cond;
  bool m_need_synced_signal = {false};

  /** Current redo lsn */
  uint64_t m_current_lsn = 0;
  uint64_t m_current_lsn_index = 0;
  std::atomic_uint64_t m_synced_index = {0};
  std::atomic_uint64_t m_lsn_of_synced_binlog = {0};

  struct Flush_cond {
    mysql_mutex_t mutex;
    mysql_cond_t cond;
  };
  static const int FLUSH_COND_COUNT = 512;
  Flush_cond m_flush_conds[FLUSH_COND_COUNT];

  bool write_queue_empty() {
    return relax_load(m_flushed_index) == relax_load(m_allocated_index);
  }

  bool sync_queue_empty() {
    return relax_load(m_synced_index) == relax_load(m_allocated_index);
  }

  Write_state *queue_node(uint64_t index) {
    return &m_write_queue_nodes[index % QUEUE_NODE_COUNT];
  }

  template <class Predict>
  void wait_for_flush(THD *thd, PSI_stage_info &stage, const Predict &predict) {
    if (likely(predict())) return;

    PSI_stage_info old_stage;

    mysql_mutex_lock(&m_flush_mutex);
    if (thd)
      thd->ENTER_COND(&m_flushed_cond, &m_flush_mutex, &stage, &old_stage);

    while (!predict()) {
      m_need_flushed_signal = true;
      mysql_cond_wait(&m_flushed_cond, &m_flush_mutex);
    }

    mysql_mutex_unlock(&m_flush_mutex);
    if (thd) thd->EXIT_COND(&old_stage);
  }
};

static Binlog_ext::Async_writer async_writer;

static bool terminate_binlog_writer_threads = false;
static my_thread_handle binlog_flush_thread_id;
static my_thread_handle binlog_sync_thread_id;

bool update_sync_binlog_interval(sys_var *, THD *, enum_var_type) {
  async_writer.awake_sync_thread();
  return false;
}

void *binlog_flush_thread(void *) {
  my_thread_init();

  async_writer.run_flush(nullptr, terminate_binlog_writer_threads);

  my_thread_end();
  my_thread_exit(0);
  return 0;
}

void *binlog_sync_thread(void *) {
  my_thread_init();
  async_writer.run_sync(terminate_binlog_writer_threads);

  my_thread_end();
  my_thread_exit(0);
  return 0;
}

static bool create_binlog_writer_threads() {
  int error = 0;
  my_thread_attr_t attr;

  if ((error = my_thread_attr_init(&attr)) ||
      (error = pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM))) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "Failed to initialize binlog writer thread attribute");
    return true;
  }

  terminate_binlog_writer_threads = false;
  if ((error =
           mysql_thread_create(key_thread_binlog_flush, &binlog_flush_thread_id,
                               &attr, binlog_flush_thread, nullptr))) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "Failed to create binlog flush thread");
  }

  if ((error =
           mysql_thread_create(key_thread_binlog_sync, &binlog_sync_thread_id,
                               &attr, binlog_sync_thread, nullptr))) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "Failed to create binlog sync thread");
  }

  (void)my_thread_attr_destroy(&attr);
  return error != 0;
}

static void stop_binlog_writer_threads() {
  opt_persist_binlog_to_redo = false;
  terminate_binlog_writer_threads = true;

  if (async_writer.is_inited()) {
    async_writer.wait_for_queue_empty(nullptr);
    async_writer.awake_flush_thread();
    async_writer.awake_sync_thread();
  }

  if (binlog_flush_thread_id.thread != 0) {
    int error = my_thread_join(&binlog_flush_thread_id, nullptr);
    if (error)
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "Failed to join binlog flush thread");
    binlog_flush_thread_id.thread = 0;
  }

  if (binlog_sync_thread_id.thread != 0) {
    int error = my_thread_join(&binlog_sync_thread_id, nullptr);
    if (error)
      LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
             "Failed to join binlog sync thread");
    binlog_sync_thread_id.thread = 0;
  }
}

/**
  Find the name of the last binlog file from index file.

  @retval false  Succeed
  @retval true   Error
*/
static bool find_last_log(MYSQL_BIN_LOG *binlog, char *log_name) {
  LOG_INFO log_info;
  int error = 1;

  DBUG_TRACE;
  assert(log_name != nullptr);

  binlog->lock_index();
  error = binlog->find_log_pos(&log_info, NullS, false);
  if (error) {
    if (error != LOG_INFO_EOF)
      LogErr(ERROR_LEVEL, ER_BINLOG_CANT_FIND_LOG_IN_INDEX, error);
    else
      error = 0; /* must an empty index file */
    goto end;
  }

  while (!(error = binlog->find_next_log(&log_info, false))) {
  }

  if (error != LOG_INFO_EOF) {
    LogErr(ERROR_LEVEL, ER_BINLOG_CANT_FIND_LOG_IN_INDEX, error);
    goto end;
  }

  strmake(log_name, log_info.log_file_name, sizeof(log_info.log_file_name));
  error = 0;
end:
  binlog->unlock_index();
  return error != 0;
}

/**
  Transactions are binlogged and committed to engine in groups.
*/
class Binlog_group_committer {
 public:
  void init() {
    for (auto &group : m_group_conds) {
      mysql_mutex_init(0, &group.mutex, MY_MUTEX_INIT_FAST);
      mysql_cond_init(0, &group.cond);
    }
    mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
    m_inited = true;
  }

  void cleanup() {
    if (!m_inited) return;
    for (auto &group : m_group_conds) {
      mysql_mutex_destroy(&group.mutex);
      mysql_cond_destroy(&group.cond);
    }
    mysql_mutex_destroy(&m_mutex);
  }

  void fetch_group(uint64_t index) {
    if (opt_binlog_group_delay > 0 &&
        Global_THD_manager::get_instance()->get_num_thread_running() >
            (int)opt_binlog_group_delay_running_threads) {
      std::this_thread::sleep_for(
          std::chrono::nanoseconds{opt_binlog_group_delay});
    }

    MUTEX_LOCK(guard, &m_mutex);
    if (index == m_group_index) m_group_tail = nullptr;
  }

  uint64_t enroll_for(THD *thd) {
    bool is_leader = false;
    uint64_t group_index = 0;

    auto cache_data = thd_get_cache_mngr(thd)->get_binlog_cache_data(true);
    uint binlog_size = cache_data->get_byte_position() +
                       binary_log::Gtid_event::MAX_EVENT_LENGTH +
                       (cache_data->get_event_counter() + 1) *
                           BINLOG_CHECKSUM_LEN;

    {
      MUTEX_LOCK(guard, &m_mutex);

      /* The group's binlog is too big, so starts a new group */
      if (m_group_binlog_size + binlog_size > opt_binlog_buffer_size) {
        m_group_tail = nullptr;
        m_group_binlog_size = 0;
      }

      is_leader = (m_group_tail == nullptr);
      if (is_leader) {
        m_group_index++;
      } else {
        m_group_tail->next_to_commit = thd;
      }

      m_group_tail = thd;
      m_group_binlog_size += binlog_size;
      group_index = m_group_index;
    }

    if (is_leader) {
      fetch_group(group_index);
      return group_index;
    }

    /* Wait until leader signals it binlog commit is done */
    auto &group = m_group_conds[group_index % GROUP_COND_COUNT];

    mysql_mutex_lock(&group.mutex);
    while (thd->tx_commit_pending) mysql_cond_wait(&group.cond, &group.mutex);
    mysql_mutex_unlock(&group.mutex);
    return 0;
  }

  void signal_done(THD *thd, uint64_t group_index) {
    auto &group = m_group_conds[group_index % GROUP_COND_COUNT];
    if (thd->next_to_commit == nullptr) return;

    mysql_mutex_lock(&group.mutex);
    for (THD *current_thd = thd; current_thd != nullptr;
         current_thd = current_thd->next_to_commit) {
      current_thd->tx_commit_pending = false;
    }
    mysql_cond_broadcast(&group.cond);
    mysql_mutex_unlock(&group.mutex);
  }

 private:
  bool m_inited = false;
  mysql_mutex_t m_mutex;
  /** The last transaction of the group */
  THD *m_group_tail = nullptr;
  /** Total binlog size of the group of transactions */
  int m_group_binlog_size = 0;

  static const int GROUP_COND_COUNT = 512;
  struct Group_cond {
    mysql_mutex_t mutex;
    mysql_cond_t cond;
  };
  Group_cond m_group_conds[GROUP_COND_COUNT];
  uint64_t m_group_index = 0;
};
static Binlog_group_committer binlog_group_committer;

bool Binlog_ext::Redo_observer::before_checkpoint(uint64 lsn) {
  async_writer.wait_for_binlog_before_lsn_synced(lsn);
  return false;
}

Binlog_ext::Binlog_ext() {}

void Binlog_ext::init_pthread_objects() {
#ifdef HAVE_PSI_INTERFACE
  binlog_ext_init_psi_keys();
#endif
  mysql_mutex_init(key_writeset_history_mutex, &m_writeset_history_mutex,
                   MY_MUTEX_INIT_FAST);
}

bool Binlog_ext::init(bool is_normal_startup) {
  binlog_group_committer.init();
  m_log_num = 0;
  if (is_normal_startup) server_redo_log::register_observer(&m_redo_observer);

  open_binlog_file();
  if (async_writer.init() || create_binlog_writer_threads()) return true;
  m_current_server_sidno = gtid_state->get_server_sidno();

  m_inited = true;
  return false;
}

void Binlog_ext::cleanup() {
  /*
    The server can abort before init() has run, for instance when an invalid
    option is found. Nothing was set up in that case, and tearing the objects
    down would abort the server instead of letting it exit with the proper
    error. Note that init_pthread_objects() is called independently of init(),
    so m_writeset_history_mutex is always destroyed below.
  */
  if (m_inited) {
    server_redo_log::unregister_observer(&m_redo_observer);
    stop_binlog_writer_threads();
    async_writer.cleanup();
    binlog_group_committer.cleanup();
    m_inited = false;
  }
  mysql_mutex_destroy(&m_writeset_history_mutex);
}

bool Binlog_ext::open_binlog_file() {
  /* m_log_num is stored in big-endian  */
  int4store(reinterpret_cast<uchar *>(&m_log_num),
            static_cast<uint>(atoi(fn_ext(mysql_bin_log.get_log_fname()) + 1)));
  m_log_name =
      mysql_bin_log.log_file_name + dirname_length(mysql_bin_log.log_file_name);
  async_writer.reset_queue();
  return false;
}

bool Binlog_ext::should_persist_to_redo(THD *thd) {
  if (mysql_bin_log.get_sync_period() != 1 || !opt_persist_binlog_to_redo ||
      server_redo_log::redo_is_readonly())
    return false;

  // order commit is not supported by binlog in redo.
  if (opt_binlog_order_commits || Clone_handler::need_commit_order() ||
      opt_replica_preserve_commit_order)
    return false;

  auto mngr = thd_get_cache_mngr(thd);
  if (mngr == nullptr) return false;

  return !thd->get_transaction()->has_modified_non_trans_table(
             Transaction_ctx::SESSION) &&
         mngr->get_stmt_cache()->is_empty() &&
         mngr->get_trx_cache()->length() <=
             opt_persist_binlog_to_redo_size_limit;
}

/**
  Calculate the exact binlog events length of a group of transactions
*/
static int calculate_trx_group_binlog_size(THD *head, ulonglong ts) {
  // Dummy gtid event for calculating transaction size
  Gtid_specification gtid_spec = {ANONYMOUS_GTID, {0, 0}};
  Gtid_log_event ev(0, true, 0, 1, true, 0, ts, gtid_spec, 0,
                    do_server_version_int(::server_version));
  int size = 0;

  for (THD *thd = head; thd != nullptr; thd = thd->next_to_commit) {
    binlog_cache_data *cache_data =
        thd_get_cache_mngr(thd)->get_binlog_cache_data(true);

    ev.original_commit_timestamp = thd->variables.original_commit_timestamp;
    ev.original_server_version = thd->variables.original_server_version;

    if (ev.original_commit_timestamp == UNDEFINED_COMMIT_TIMESTAMP) {
      if (thd->slave_thread || thd->is_binlog_applier())
        ev.original_commit_timestamp = 0;
      else
        ev.original_commit_timestamp = ev.immediate_commit_timestamp;
    }
    if (ev.original_server_version == UNDEFINED_SERVER_VERSION) {
      if (thd->slave_thread || thd->is_binlog_applier())
        ev.original_server_version = UNKNOWN_SERVER_VERSION;
      else
        ev.original_server_version = ev.immediate_server_version;
    }

    ev.set_trx_length_by_cache_size(
        cache_data->get_byte_position(),
        binlog_checksum_options != binary_log::BINLOG_CHECKSUM_ALG_OFF,
        cache_data->get_event_counter());
    size += ev.transaction_length;
  }
  return size;
}

void Binlog_ext::update_gtid_after_commit(THD *first_seen) {
  bool is_locked = false;

  for (THD *head = first_seen; head; head = head->next_to_commit) {
    /* Update GTID state in a group to avoid large amounts of locking and
       unlocking */
    if (!head->owned_gtid_is_empty() &&
        head->owned_gtid.sidno == m_current_server_sidno) {
      if (!is_locked) {
        global_sid_lock->rdlock();
        gtid_state->lock_sidno(m_current_server_sidno);
        is_locked = true;
      }

      gtid_state->update_gtids_impl_own_gtid(
          head, head->commit_error == THD::CE_NONE);
      DBUG_PRINT("gtid_prefetcher_release",
                 ("Transaction's GTID released by gtid_prefetcher."));
    }
  }

  if (is_locked) {
    gtid_state->unlock_sidno(m_current_server_sidno);
    global_sid_lock->unlock();
  }
}

bool Binlog_ext::commit(THD *thd) {
  DBUG_TRACE;
  uint64_t group_index = 0;

  group_index = binlog_group_committer.enroll_for(thd);
  if (group_index == 0) {
    // It is a follower, leader will handle commit process for it.
    return mysql_bin_log.finish_commit(thd);
  }

  bool do_rotate = false;
  my_off_t bytes_written = 0;
  Async_writer::Write_ctx write_ctx;
  int group_binlog_size = 0;
  ulonglong immediate_commit_ts = 0;

  /*
    Step 1: Below actions are protected by LOCK_log
  */
  mysql_mutex_lock(&mysql_bin_log.LOCK_log);

  immediate_commit_ts = my_micro_time();
  group_binlog_size = calculate_trx_group_binlog_size(thd, immediate_commit_ts);

  (void)mysql_bin_log.assign_automatic_gtids_to_flush_group(thd);

  for (THD *trx_thd = thd; trx_thd != nullptr;
       trx_thd = trx_thd->next_to_commit) {
    trx_thd->get_transaction()->sequence_number =
        mysql_bin_log.m_dependency_tracker.step();
  }

  /*
    It guarantees that binlog doesn't rotate after LOCK_log is released.
  */
  mysql_bin_log.inc_prep_xids(thd);

  async_writer.allocate_buffer(&write_ctx, group_binlog_size);

  my_off_t end_pos = write_ctx.start_pos + write_ctx.length;

  thd->set_trans_pos(m_log_name, end_pos);
  if (RUN_HOOK(binlog_storage, after_flush, (thd, m_log_name, end_pos))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_FAILED_TO_RUN_AFTER_FLUSH_HOOK);
    mysql_bin_log.handle_binlog_flush_or_sync_error(thd, true, nullptr);
  }

  do_rotate =
      end_pos + mysql_bin_log.get_binlog_file()->get_encrypted_header_size() >=
          (my_off_t)mysql_bin_log.max_size ||
      DBUG_EVALUATE_IF("simulate_max_binlog_size", true, false) ||
      DBUG_EVALUATE_IF("force_rotate", true, false);

  if (xa_delay_rotate(thd, do_rotate)) do_rotate = false;

  // LOCK_log will not be released if rotation is required
  if (!do_rotate) mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

  DEBUG_SYNC(thd, "before_flush_binlog_cache");
  DBUG_EXECUTE_IF("crash_commit_before_log", DBUG_SUICIDE(););

  /*
    Step 2: Wait until the reserved buffer is available and then write
            binlog events of the transactions into the reserved buffer.
  */
  uint64_t lsn = 0;
  Async_writer::Buffer_ostream ostream(async_writer, write_ctx);
  Binlog_event_writer event_writer(&ostream, write_ctx.start_pos);

  async_writer.wait_for_buffer_available(thd, write_ctx);

  for (THD *trx_thd = thd; trx_thd != nullptr;
       trx_thd = trx_thd->next_to_commit) {
    if (thd_get_cache_mngr(trx_thd)->trx_cache.flush(
            trx_thd, &bytes_written, &event_writer, immediate_commit_ts)) {
      mysql_bin_log.handle_binlog_flush_or_sync_error(trx_thd, true, nullptr);
    }
  }

  // Step 3: persist the binlog events into redo log
  DBUG_EXECUTE_IF("binlog_write_redo_crash", DBUG_SUICIDE(););
  lsn = async_writer.write_to_redo(write_ctx, m_log_num);
  DEBUG_SYNC(thd, "after_write_to_redo");

  async_writer.sync_to_redo(thd, lsn, write_ctx);
  DEBUG_SYNC(thd, "bgc_after_sync_stage_before_commit_stage");

  // Step 4: Commit
  for (THD *trx_thd = thd; trx_thd != nullptr;
       trx_thd = trx_thd->next_to_commit) {
    /*
      update_max_committed() asserts that the commit order part of the
      dependency tracker is protected. The ordinary binlog group commit takes
      LOCK_replica_trans_dep_tracker around it, so do the same here: in the
      binlog-in-redo commit path the transactions commit in parallel.
    */
    {
      mysql_mutex_lock(&LOCK_replica_trans_dep_tracker);
      mysql_bin_log.m_dependency_tracker.update_max_committed(trx_thd);
      mysql_mutex_unlock(&LOCK_replica_trans_dep_tracker);
    }

    Thd_backup_and_restore switch_thd(thd, trx_thd);
    bool all = trx_thd->get_transaction()->m_flags.real_commit;
    if (trx_thd->get_transaction()->m_flags.commit_low) {
      if (ha_commit_low(trx_thd, all, false))
        trx_thd->commit_error = THD::CE_COMMIT_ERROR;
    }
  }

  mysql_bin_log.process_after_commit_stage_queue(thd, thd);

  /*
    Step 5: wait for binlog events to be written into binlog file here.
  */
  if (opt_wait_binlog_flush) {
    async_writer.wait_for_binlog_flushed(thd, write_ctx);
  }

  mysql_bin_log.dec_prep_xids(thd);
  DEBUG_SYNC(thd, "after_dec_prep_xids");

  update_gtid_after_commit(thd);

  binlog_group_committer.signal_done(thd, group_index);
  (void)mysql_bin_log.finish_commit(thd);

  if (do_rotate) rotate(thd);
  return thd->commit_error == THD::CE_COMMIT_ERROR;
}

void Binlog_ext::rotate(THD *thd) {
  if (thd->commit_error == THD::CE_NONE) {
    DEBUG_SYNC(thd, "ready_to_do_rotation");

    bool check_purge = false;
    int error = mysql_bin_log.rotate(true, &check_purge);
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

    if (error)
      thd->commit_error = THD::CE_COMMIT_ERROR;
    else if (check_purge)
      mysql_bin_log.auto_purge();
  } else {
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);
  }
}

void Binlog_ext::switch_to_sync_mode(THD *thd) {
  async_writer.switch_to_sync_mode(thd);
}

bool Binlog_ext::force_ha_flush_log(THD *) {
  /*
    The recovery-apply-binlog machinery that made use of this was reverted, so
    persisting engine log is never forced here.
  */
  return false;
}

/**
  XA transacitons don't have xid, but inc_prep_xids() is necessary for them
  to avoid binlog rotation happening before engine prepare/commit.
*/
void Binlog_ext::xa_inc_prep_xids(THD *thd) {
  mysql_mutex_assert_owner(&mysql_bin_log.LOCK_log);
  if (thd->m_se_gtid_flags[THD::Se_GTID_flag::SE_GTID_PIN]) {
    mysql_bin_log.m_atomic_prep_xids++;
    thd->get_transaction()->xid_state()->m_ctx.set_increased_prep_xids();
  }
}

bool Binlog_ext::xa_delay_rotate(THD *thd, bool do_rotate) {
  if (thd->m_se_gtid_flags[THD::Se_GTID_flag::SE_GTID_PIN] && do_rotate) {
    thd->get_transaction()->xid_state()->m_ctx.set_do_binlog_rotate();
    return true;
  }
  return false;
}

Binlog_ext::XA_rotate_guard::~XA_rotate_guard() {
  THD *thd = m_thd;
  XID_STATE *xs = thd->get_transaction()->xid_state();

  // It has to decrease xids no matter error happens or not
  if (xs->m_ctx.increased_prep_xp_xids()) {
    mysql_bin_log.dec_prep_xids(thd);
    xs->m_ctx.reset_increased_prep_xids();
  }

  if (xs->m_ctx.do_binlog_rotate()) {
    xs->m_ctx.reset_do_binlog_rotate();
    if (*m_has_error) return;

    DEBUG_SYNC(thd, "ready_to_do_rotation");

    bool check_purge = false;
    mysql_mutex_lock(&mysql_bin_log.LOCK_log);
    int error = mysql_bin_log.rotate(false, &check_purge);
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

    if (!error && check_purge) mysql_bin_log.auto_purge();

    // Since the XA action has succeeded, it will not return error to user.
    if (thd->is_error()) thd->clear_error();
  }
}

class Binlog_event_updater : public Basic_ostream {
  bool have_checksum;
  ha_checksum initial_checksum;
  ha_checksum checksum;
  uint32 end_log_pos = 0;
  uchar header[LOG_EVENT_HEADER_LEN];
  my_off_t header_len = 0;
  uint32 event_len = 0;
  Basic_ostream *m_ostream;
  uint32 skip_len = 0;

 public:
  Binlog_event_updater(Basic_ostream *ostream, uint32_t end_log_pos_arg,
                       bool checksum)
      : have_checksum(checksum),
        initial_checksum(my_checksum(0L, nullptr, 0)),
        checksum(initial_checksum),
        end_log_pos(end_log_pos_arg),
        m_ostream(ostream) {}

  void update_header() {
    event_len = uint4korr(header + EVENT_LEN_OFFSET);

    // update end log pos
    end_log_pos += event_len;
    int4store(header + LOG_POS_OFFSET, end_log_pos);
    // update the checksum
    if (have_checksum) checksum = my_checksum(checksum, header, header_len);
  }

  bool write(const unsigned char *buffer, my_off_t length) override {
    DBUG_TRACE;

    while (length > 0) {
      /* Write event header into binlog */
      if (event_len == 0) {
        // Skip checksum
        if (skip_len > 0) {
          if (length > skip_len) {
            buffer += skip_len;
            length -= skip_len;
            skip_len = 0;
          } else {
#ifndef NDEBUG
            if (length < BINLOG_CHECKSUM_LEN) {
              LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                     "event checksum is split");
            }
#endif
            skip_len -= length;
            return false;
          }
        }
        /* data in the buf may be smaller than header size.*/
        uint32 header_incr =
            std::min<uint32>(LOG_EVENT_HEADER_LEN - header_len, length);

        memcpy(header + header_len, buffer, header_incr);
        header_len += header_incr;
        buffer += header_incr;
        length -= header_incr;

        if (header_len == LOG_EVENT_HEADER_LEN) {
          update_header();
          if (m_ostream->write(header, header_len)) return true;

          event_len -= header_len;
          header_len = 0;
          if (have_checksum) {
            skip_len = BINLOG_CHECKSUM_LEN;
            event_len -= BINLOG_CHECKSUM_LEN;
          }
        } else {
#ifndef NDEBUG
          LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, "event header is split");
#endif
        }
      } else {
        my_off_t write_bytes = std::min<uint64>(length, event_len);

        if (m_ostream->write(buffer, write_bytes)) return true;

        // update the checksum
        if (have_checksum)
          checksum = my_checksum(checksum, buffer, write_bytes);

        event_len -= write_bytes;
        length -= write_bytes;
        buffer += write_bytes;

        // The whole event is copied, now add the checksum
        if (have_checksum && event_len == 0) {
          uchar checksum_buf[BINLOG_CHECKSUM_LEN];

          int4store(checksum_buf, checksum);
          if (m_ostream->write(checksum_buf, BINLOG_CHECKSUM_LEN)) return true;
          checksum = initial_checksum;
        }
      }
    }
    return false;
  }
};

Binlog_redo_recovery::Binlog_redo_recovery() : m_log_num(0) {
  server_redo_log::register_applier(server_redo_log::LOG_TYPE_BINLOG, this);
}
Binlog_redo_recovery::~Binlog_redo_recovery() { recovery_end(); }

bool Binlog_redo_recovery::recovery_begin() {
  // unregister it first. It will registered again if recovery is needed
  server_redo_log::unregister_applier(server_redo_log::LOG_TYPE_BINLOG);

  // Check wether binlog recovery is needed or not.
  if (!opt_bin_log) return false;

  m_log_name[0] = '\0';
  if (find_last_log(&mysql_bin_log, m_log_name)) return true;
  if (m_log_name[0] == '\0') return false;

  Binlog_file_reader reader(false);
  if (reader.open(m_log_name)) {
    /*
      The last binlog file cannot be read. It may be truncated, corrupted or
      encrypted with a key which is not available at this early stage of the
      startup. Recovering binlog events from the redo log is impossible then,
      but that must not stop the server: the ordinary binlog crash recovery
      still runs later and handles the file. Skip the redo based recovery and
      keep going.
    */
    LogErr(WARNING_LEVEL, ER_BINLOG_FILE_OPEN_FAILED, reader.get_error_str());
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
           "Could not read the last binlog file, skipping the recovery of "
           "binlog events from the redo log");
    return false;
  }

  // recovery is needed if the last binlog file has IN_USE flag.
  std::unique_ptr<Log_event> ev(reader.read_event_object());
  if (ev && ev->get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT &&
      (ev->common_header->flags & LOG_EVENT_BINLOG_IN_USE_F)) {
    my_off_t log_length = reader.ifile()->length();

    m_have_checksum = reader.format_description_event().footer()->checksum_alg;
    int4store(reinterpret_cast<uchar *>(&m_log_num),
              static_cast<uint>(atoi(fn_ext(m_log_name) + 1)));

    server_redo_log::register_applier(server_redo_log::LOG_TYPE_BINLOG, this);

    m_buffer = (uchar *)my_malloc(key_memory_binlog_buffer,
                                  opt_binlog_buffer_size, MYF(0));
    if (m_buffer == nullptr) {
      LogErr(ERROR_LEVEL, ER_OOM);
      goto err;
    }
    m_buffer_size = opt_binlog_buffer_size;

    m_ofile = MYSQL_BIN_LOG::Binlog_ofile::open_existing(key_file_binlog,
                                                         m_log_name, MYF(MY_WME));
    if (m_ofile == nullptr || m_ofile->seek(log_length)) goto err;

    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           "Start to recover binlog from redo log");
  }

  return false;
err:
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
         "Failed to recover binlog from redo log");
  return true;
}

bool Binlog_redo_recovery::recovery_end() {
  server_redo_log::unregister_applier(server_redo_log::LOG_TYPE_BINLOG);
  if (m_ofile) {
    for (auto &block : m_binlog_blocks) {
      /*
        If a transaction has assigned binlog buffer, but not yet be written into
        redo log, it made a hole in the binlog position. Thus
        Binlog_event_updater is called to update binlog end pos of binlog events
        following the hole.
      */
      Binlog_event_updater updater(&*m_ofile, m_ofile->position(),
                                   m_have_checksum);
      copy_to_file(block.first, block.second, &updater);
    }
    m_binlog_blocks.clear();

    if (m_ofile->flush_and_sync()) {
      char errbuf[MYSYS_STRERROR_SIZE];
      LogErr(ERROR_LEVEL, ER_FAILED_TO_WRITE_TO_FILE, m_log_name, errno,
             my_strerror(errbuf, sizeof(errbuf), errno));
    }
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
           "Finished recovering binlog from redo log");
    m_ofile = nullptr;
  }

  my_free(m_buffer);
  m_buffer = nullptr;
  return false;
}

bool Binlog_redo_recovery::apply(const unsigned char *ptr, uint len) {
  Binlog_redo_log_reader reader(ptr, len);

  if (m_log_num != reader.log_num()) return false;

  my_off_t data_pos = reader.log_pos();
  if (data_pos + reader.data_len() <= m_ofile->position()) return false;

  // It's the first consecutive block at the end. Writes it into binlog file
  if (data_pos <= m_ofile->position()) {
    my_off_t write_offset = m_ofile->position() - data_pos;
    if (m_ofile->write(reader.data() + write_offset,
                       reader.data_len() - write_offset))
      goto err;

    // Find all the consecutive binlog blocks following m_ofile->position()
    my_off_t data_len = 0;
    auto it = m_binlog_blocks.begin();

    for (; it != m_binlog_blocks.end(); it++) {
      if (it->first > m_ofile->position() + data_len) break;

      assert(it->first == m_ofile->position() + data_len);
      data_len += it->second;
    }

    // Copy all the consecutive binlog blocks into file
    if (data_len > 0) {
      assert(m_ofile->position() == m_binlog_blocks.begin()->first);

      copy_to_file(m_binlog_blocks.begin()->first, data_len, &*m_ofile);

      m_binlog_blocks.erase(m_binlog_blocks.begin(), it);
    }
  } else {
    size_t size = data_pos + reader.data_len() - m_ofile->position();
    if (size > m_buffer_size && increase_buffer(size)) {
      LogErr(ERROR_LEVEL, ER_OOM);
      goto err;
    }

    // The blocks before it is missing, so copy it into buffer.
    copy_to_buffer(reader.data(), reader.data_len(), data_pos);

    assert(m_binlog_blocks.find(data_pos) == m_binlog_blocks.end());
    m_binlog_blocks[data_pos] = reader.data_len();
  }
  return false;
err:
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
         "Failed to recover binlog from redo log");
  flush_error_log_messages();
  abort();
  return true;
}

bool Binlog_redo_recovery::copy_to_file(my_off_t log_pos, size_t len,
                                        Basic_ostream *ostream) {
  size_t buffer_offset = log_pos % m_buffer_size;

  if (buffer_offset + len <= m_buffer_size) {
    if (ostream->write(m_buffer + buffer_offset, len)) return true;
  } else {
    size_t part1_len = m_buffer_size - buffer_offset;

    if (ostream->write(m_buffer + buffer_offset, part1_len) ||
        ostream->write(m_buffer, len - part1_len))
      return true;
  }
  return false;
}

void Binlog_redo_recovery::copy_to_buffer(const uchar *src, size_t len,
                                          my_off_t log_pos) {
  size_t buffer_offset = log_pos % m_buffer_size;

  if (buffer_offset + len <= m_buffer_size) {
    memcpy(m_buffer + buffer_offset, src, len);
  } else {
    size_t part1_len = m_buffer_size - buffer_offset;

    memcpy(m_buffer + buffer_offset, src, part1_len);
    memcpy(m_buffer, src + part1_len, len - part1_len);
  }
}

bool Binlog_redo_recovery::increase_buffer(size_t size) {
  size_t old_size = m_buffer_size;
  uchar *old_buffer = m_buffer;
  size_t old_offset = m_ofile->position() % m_buffer_size;

  assert(size > m_buffer_size);
  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
         "Increase binlog recovery buffer from %llu to %llu",
         static_cast<ulonglong>(m_buffer_size), static_cast<ulonglong>(size));

  m_buffer_size = size;
  m_buffer = (uchar *)my_malloc(key_memory_binlog_buffer, size, MYF(0));
  if (m_buffer == nullptr) return true;

  if (old_offset == 0) {
    copy_to_buffer(old_buffer, old_size, m_ofile->position());
  } else {
    size_t part1_len = old_size - old_offset;
    copy_to_buffer(old_buffer + old_offset, part1_len, m_ofile->position());
    copy_to_buffer(old_buffer, old_offset, m_ofile->position() + part1_len);
  }
  my_free(old_buffer);
  return false;
}

/******************************************************************************
  DuckDB specific binlog extension.
******************************************************************************/

bool Binlog_ext::duckdb_commit(THD *thd) {
  DBUG_TRACE;
  my_off_t total_bytes = 0;
  bool do_rotate = false;
  bool error = false;

  thd->get_transaction()->m_flags.run_hooks = false;

  DBUG_EXECUTE_IF("crash_commit_before_log", DBUG_SUICIDE(););

  mysql_mutex_lock(&mysql_bin_log.LOCK_log);
  // used for truncate binlog if error happens
  my_off_t before_pos = mysql_bin_log.m_binlog_file->position();

  mysql_bin_log.assign_automatic_gtids_to_flush_group(thd);
  {
    std::pair<int, my_off_t> result = mysql_bin_log.flush_thread_caches(thd);
    error = result.first;
    total_bytes = result.second;
  }

  if (error == 0 && total_bytes > 0) {
    my_off_t flush_end_pos;
    error = mysql_bin_log.flush_cache_to_file(&flush_end_pos);
  }

  if (error == 0 && total_bytes > 0) {
    std::pair<bool, bool> result = mysql_bin_log.sync_binlog_file(false);
    error = result.first;
  }

  if (error) {
    mysql_bin_log.handle_binlog_flush_or_sync_error(
        thd, false /* need_lock_log */,
        (thd->commit_error == THD::CE_FLUSH_GNO_EXHAUSTED_ERROR)
            ? ER_THD(thd, ER_GNO_EXHAUSTED)
            : nullptr);
  }
  DBUG_EXECUTE_IF("crash_after_binlog_sync", DBUG_SUICIDE(););
  DEBUG_SYNC(thd, "after_binlog_sync");

  /*
    For the case DuckDB engine does not envovled into the transaction,
    A duckdb transaction should be started and the handlerton is registered
    to Server layer here for updating duckdb_binlog_position table.
  */

  auto duckdb_ctx = thd->get_duckdb_context();
  if (!duckdb_ctx->has_transaction() && duckdb_ctx->duckdb_trans_begin())
    assert(1);
  if (thd->get_transaction()->is_active(Transaction_ctx::SESSION)) {
    trans_register_ha(thd, true, myduck_hton, nullptr);
  } else if (thd->get_transaction()->is_active(Transaction_ctx::STMT)) {
    trans_register_ha(thd, false, myduck_hton, nullptr);
  }

  std::ostringstream stmt;
  stmt << "INSERT INTO mysql.duckdb_binlog_position VALUES('" << m_log_name
       << "', " << mysql_bin_log.m_binlog_file->get_real_file_size() << ")";

  auto query_res = myduck::duckdb_query(thd, stmt.str(), false);

  // For the case SQL does nothing, commit DuckDB Engine here.
  if (!query_res->HasError() &&
      !thd->get_transaction()->is_active(Transaction_ctx::SESSION) &&
      !thd->get_transaction()->is_active(Transaction_ctx::STMT)) {
    query_res = myduck::duckdb_query(thd, "COMMIT", false);
    DBUG_EXECUTE_IF("crash_after_duckdb_commit", DBUG_SUICIDE(););
  }

  if (query_res->HasError()) {
    thd->commit_error = THD::CE_COMMIT_ERROR;
    my_error(ER_DUCKDB_COMMIT_ERROR, MYF(0), query_res->GetError().c_str());
  }

  if (thd->commit_error == THD::CE_NONE) {
    ::finish_transaction_in_engines(
        thd, thd->get_transaction()->m_flags.real_commit, false);
  }

  // finish_transaction_in_engines may return CE_COMMIT_ERROR
  if (thd->commit_error == THD::CE_COMMIT_ERROR) {
    mysql_bin_log.m_binlog_file->truncate(before_pos);
    (void)mysql_bin_log.finish_commit(thd);
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

    return true;
  }

  mysql_bin_log.update_binlog_end_pos();

  do_rotate = (mysql_bin_log.m_binlog_file->get_real_file_size() >=
               (my_off_t)mysql_bin_log.max_size);
  (void)mysql_bin_log.finish_commit(thd);
  mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

  if (DBUG_EVALUATE_IF("force_rotate", 1, 0) ||
      (do_rotate && thd->commit_error == THD::CE_NONE)) {
    bool check_purge = false;
    mysql_mutex_lock(&mysql_bin_log.LOCK_log);
    (void)mysql_bin_log.rotate(false, &check_purge);
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);

    if (check_purge) mysql_bin_log.auto_purge();
  }
  return false;
}

bool Binlog_ext::duckdb_binlog_rotate() {
  if (!myduck::global_mode_on()) return false;

  // Ignore truncate error here. It should not crash the server.
  auto query_res =
      myduck::duckdb_query("TRUNCATE TABLE mysql.duckdb_binlog_position");

  DBUG_EXECUTE_IF("crash_during_duckdb_binlog_rotate", DBUG_SUICIDE(););

  std::ostringstream stmt;
  stmt << "INSERT INTO mysql.duckdb_binlog_position VALUES('" << m_log_name
       << "', " << mysql_bin_log.m_binlog_file->get_real_file_size() << ")";
  fprintf(stderr, "query:%s\n", stmt.str().c_str());
  query_res = myduck::duckdb_query(stmt.str());

  return DBUG_EVALUATE_IF("simulate_duckdb_binlog_roate_error", true,
                          query_res->HasError());
}

bool Binlog_ext::duckdb_binlog_init() {
  using namespace myduck;
  std::string query =
      "SELECT 1 FROM information_schema.tables "
      "WHERE table_schema = 'mysql' AND table_name = 'duckdb_binlog_position'";
  auto res = duckdb_query(query);
  if (res->HasError()) {
    LogErr(ERROR_LEVEL, ER_DUCKDB, "Failed to SELECT information_schema.tables");
    return true;
  }

  auto mres = res->Cast<duckdb::StreamQueryResult>().Materialize();
  if (mres->RowCount() > 0) return false;

  LogErr(INFORMATION_LEVEL, ER_DUCKDB, "Create duckdb_binlog_position table");

  query = "CREATE SCHEMA IF NOT EXISTS mysql";
  res = duckdb_query(query);
  if (res->HasError()) goto err;

  query =
      "CREATE TABLE mysql.duckdb_binlog_position("
      "  file VARCHAR(128) NOT NULL,"
      "  position BIGINT NOT NULL)";
  res = duckdb_query(query);
  if (res->HasError()) goto err;

  return false;
err:
  LogErr(ERROR_LEVEL, ER_DUCKDB, "Failed to initialize duckdb_binlog_position");
  return true;
}

bool Binlog_ext::duckdb_recover(const char *log_name) {
  Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
  if (binlog_file_reader.open(log_name)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_FILE_OPEN_FAILED,
           binlog_file_reader.get_error_str());
    return true;
  }

  if (!mysql_bin_log.read_binlog_in_use_flag(binlog_file_reader)) return false;

  std::string stmt;
  stmt = "SELECT max(position) FROM mysql.duckdb_binlog_position WHERE file = '";
  stmt += log_name + dirname_length(log_name);
  stmt += "'";
  auto res = myduck::duckdb_query(stmt);

  if (res->HasError()) {
    LogErr(ERROR_LEVEL, ER_DUCKDB,
           "Failed to read positon from mysql.duckdb_binlog_position");
    return true;
  }

  auto mres = res->Cast<duckdb::StreamQueryResult>().Materialize();
  LogErr(INFORMATION_LEVEL, ER_DUCKDB, mres->ToString().c_str());

  if (mres->RowCount() == 0) return false;
  if (mres->GetValue(0, 0).IsNull()) return false;

  auto pos = mres->GetValue<int64_t>(0, 0);
  if (pos == 0) return false;

  truncate(log_name, pos);
  std::ostringstream errmsg;
  errmsg << "Truncate last binlog file to position " << pos;
  LogErr(INFORMATION_LEVEL, ER_DUCKDB, errmsg.str().c_str());
  return false;
}

void trx_cache_write_event(THD *thd, Log_event *event) {
  binlog_cache_mngr *cache_mngr = thd_get_cache_mngr(thd);
  cache_mngr->trx_cache.write_event(event);
}

/**
  Calculate the exact binlog events length of a group of transactions

  @param[in]  thd  THD object of the transaction
  @param[in]  ts   timestamp which will be written into immediate_commit_ts
  @param[in]  gtid_only Only calculate gtid event length.

  @return Total size of the binlog events of the group of transactions
*/
inline int calculate_trx_group_binlog_size(THD *head, ulonglong ts,
                                           bool gtid_only = false) {
  // Dummy gtid event for calculating transaction size
  Gtid_specification gtid_spec = {ANONYMOUS_GTID, {0, 0}};
  Gtid_log_event ev(0, true, 0, 1, true, 0, ts, gtid_spec, 0,
                    do_server_version_int(::server_version));
  int size = 0;

  for (THD *thd = head; thd != nullptr; thd = thd->next_to_commit) {
    binlog_cache_data *cache_data =
        thd_get_cache_mngr(thd)->get_binlog_cache_data(true);

    /*
      For getting correct length,
      below code has the same logic with write_transaction().
    */
    ev.original_commit_timestamp = thd->variables.original_commit_timestamp;
    ev.original_server_version = thd->variables.original_server_version;

    if (ev.original_commit_timestamp == UNDEFINED_COMMIT_TIMESTAMP) {
      if (thd->slave_thread || thd->is_binlog_applier())
        ev.original_commit_timestamp = 0;
      else
        ev.original_commit_timestamp = ev.immediate_commit_timestamp;
    }
    if (ev.original_server_version == UNDEFINED_SERVER_VERSION) {
      if (thd->slave_thread || thd->is_binlog_applier())
        ev.original_server_version = UNKNOWN_SERVER_VERSION;
      else
        ev.original_server_version = ev.immediate_server_version;
    }

    ev.set_trx_length_by_cache_size(
        cache_data->get_byte_position(),
        binlog_checksum_options != binary_log::BINLOG_CHECKSUM_ALG_OFF,
        cache_data->get_event_counter());

    if (gtid_only)
      size += ev.get_event_length();
    else
      size += ev.transaction_length;
  }
  return size;
}

Binlog_cache_free_flush::Binlog_cache_free_flush()
    : m_limit_size(0),
      m_is_enabled(false),
      m_is_dir_initialized(false),
      m_is_free_flushing(false) {
  m_reserved_size.store(0);
  m_tmp_file_count.store(0);
}

void Binlog_cache_free_flush::init_binlog_cache_dir() {
  char target_dir[FN_REFLEN];
  size_t length;

  dirname_part(target_dir, log_bin_basename, &length);

  uint max_tmp_file_name_len = 2 /* prefix */ + strlen(BINLOG_CACHE_NAME) +
                               10 /* max len of thread_id */ +
                               2 /* underline */;

  /*
    Must ensure the full name of the tmp file is shorter than FN_REFLEN, to
    avoid overflowing the name buffer in write and commit.
  */
  if (length + strlen(BINLOG_CACHE_DIR) + max_tmp_file_name_len >= FN_REFLEN) {
    char errmsg[FN_REFLEN + 128];
    const char *errmsg_fmt =
        "binlog cache dir %s%s is too long, disable binlog cache free flush.";

    sprintf(errmsg, errmsg_fmt, target_dir, BINLOG_CACHE_DIR);

    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH, errmsg);
    m_is_enabled = false;
    return;
  }

  memcpy(target_dir + length, BINLOG_CACHE_DIR, strlen(BINLOG_CACHE_DIR));
  target_dir[length + strlen(BINLOG_CACHE_DIR)] = 0;

  MY_DIR *dir_info = my_dir(target_dir, MYF(0));

  m_is_dir_initialized = true;

  if (!dir_info) {
    /* Make a dir for binlog cache temp files if not exist. */
    if (my_mkdir(target_dir, (S_IRWXU | S_IRGRP | S_IXGRP), MYF(0))) {
      char errmsg[FN_REFLEN + 128];
      const char *errmsg_fmt =
          "failed to create binlog cache dir : %s, disable binlog cache free "
          "flush.";

      sprintf(errmsg, errmsg_fmt, target_dir);

      LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH, errmsg);
      m_is_enabled = false;
      m_is_dir_initialized = false;
    }
    return;
  }

  if (dir_info->number_off_files > 2) {
    /* Delete all files in directory */
    for (uint i = 0; i < dir_info->number_off_files; i++) {
      FILEINFO *file = dir_info->dir_entry + i;

      /* Skip the names "." and ".." */
      if (!std::strcmp(file->name, ".") || !std::strcmp(file->name, ".."))
        continue;

      char file_path[FN_REFLEN];
      fn_format(file_path, file->name, target_dir, "", MYF(MY_REPLACE_DIR));

      my_delete(file_path, MYF(0));
    }
  }

  my_dirend(dir_info);
}

void Binlog_cache_free_flush::save_new_file_name(const char *new_name) {
  memcpy(m_current_new_name, new_name, FN_REFLEN);
  m_current_new_name[FN_REFLEN - 1] = '\0';
}

bool Binlog_cache_free_flush::rename_temp_to_binlog() {
  char file_dir[FN_REFLEN], file_path[FN_REFLEN];
  size_t length;

  mysql_mutex_assert_owner(&mysql_bin_log.LOCK_index);

  /* Update log_file_name, should call inside lock_index. */
  if (mysql_bin_log.init_and_set_log_file_name(nullptr, m_current_new_name,
                                               0)) {
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to generate new file name.");
    return true;
  }

  /* Open purge index file before rename to ensure crash safe. */
  if (mysql_bin_log.open_purge_index_file(true) ||
      mysql_bin_log.register_create_index_entry(m_current_new_name) ||
      mysql_bin_log.sync_purge_index_file()) {
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to open purge index file.");
    return true;
  }

  DBUG_EXECUTE_IF("crash_before_rename_file", DBUG_SUICIDE(););

  /* Rename. */
  dirname_part(file_dir, log_bin_basename, &length);
  fn_format(file_path, m_current_new_name, file_dir, "", MYF(MY_REPLACE_DIR));

  if (DBUG_EVALUATE_IF("simulate_binlog_cache_free_flush_rename_fail", 1, 0) ||
      my_rename(m_tmp_file_name, file_path, MYF(0))) {
    mysql_bin_log.close_purge_index_file();
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to rename temp file.");
    return true;
  }

  DBUG_EXECUTE_IF("crash_after_rename_file", DBUG_SUICIDE(););

  /* Add log name to index file. */
  if (mysql_bin_log.add_log_to_index((uchar *)mysql_bin_log.log_file_name,
                                     strlen(mysql_bin_log.log_file_name),
                                     false /*need_lock_index=false*/)) {
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to write binlog index file.");
    return true;
  }

  /* Close purge index file. */
  mysql_bin_log.close_purge_index_file();

  DBUG_EXECUTE_IF("crash_after_free_flush_add_index_file", DBUG_SUICIDE(););
  return false;
}

bool Binlog_cache_free_flush::commit(THD *thd) {
  binlog_cache_mngr *cache_mngr = thd_get_cache_mngr(thd);
  binlog_cache_data *cache_data = cache_mngr->get_binlog_cache_data(true);
  Binlog_cache_storage *cache_storage = cache_data->get_cache();
  bool check_purge;
  bool flush_error = false;
  const char *message = nullptr;
  const char *file_name_ptr;
  Empty_log_event empty_event(thd, 0);
  ulonglong immediate_commit_ts;

  my_off_t file_end_pos =
      cache_storage->get_rds_cache_storage()->get_file_end_pos();
  my_off_t reserved_size =
      cache_storage->get_rds_cache_storage()->get_file_reserved_size();

  /*
    Sync binlog cache temp file and redo log before enter log_lock, to reduce
    time of holding LOCK_log.
  */
  if (cache_storage->get_rds_cache_storage()->flush_and_sync()) return false;
  ha_flush_logs(true);

  mysql_mutex_lock(&mysql_bin_log.LOCK_log);

  /*
    Double-check, write and sync front event inside the lock. If failed, switch
    to normal binlog flush mode.
  */
  if (!check_reserved_space_enough(reserved_size)) {
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);
    return false;
  }

  m_is_free_flushing = true;
  LogErr(SYSTEM_LEVEL, ER_MESSAGE_BINLOG_CACHE_FREE_FLUSH,
         "Enter binlog cache free flush.");

  /* Save tmp file name */
  strcpy(m_tmp_file_name, cache_storage->tmp_file_name());

  /* Detach tmp file. */
  cache_storage->get_rds_cache_storage()->detach_temp_file();

#ifndef NDEBUG
  /* Use to test binlog position in debug mode. */
  my_off_t debug_binlog_position = mysql_bin_log.atomic_binlog_end_pos;
  char debug_binlog_file_name[FN_REFLEN];
  memcpy(debug_binlog_file_name, mysql_bin_log.log_file_name, FN_REFLEN);
#endif

  /*
    Close last binlog file, open tmp file as the new binlog file.

    This function will open the cache tmp file as binlog file, but will not
    update log_file_name and write index file.
  */
  if ((flush_error = mysql_bin_log.rotate(true, &check_purge))) {
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to open the cache tmp file as binlog file.");
    thd->commit_error = THD::CE_FLUSH_ERROR;
    goto err;
  }

  /* Write empty event. */
  immediate_commit_ts = my_micro_time();
  empty_event.set_size(
      reserved_size - mysql_bin_log.m_binlog_file->position() -
      calculate_trx_group_binlog_size(thd, immediate_commit_ts, true));
  mysql_bin_log.write_event_to_binlog(&empty_event);

  /* Flush transaction. GTID event will be written in this step. */
  (void)mysql_bin_log.assign_automatic_gtids_to_flush_group(thd);
  thd->get_transaction()->sequence_number =
      mysql_bin_log.m_dependency_tracker.step();

  {
    /* Define and use the writer in a local scope to avoid affecting goto. */
    Binlog_event_writer writer(mysql_bin_log.get_binlog_file());
    flush_error = cache_data->flush(thd, nullptr, &writer, immediate_commit_ts);
  }

  if (flush_error) {
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to flush gtid event");
    if (thd->commit_error == THD::CE_FLUSH_GNO_EXHAUSTED_ERROR)
      message = ER_THD(thd, ER_GNO_EXHAUSTED);
    goto err;
  }

  if (mysql_bin_log.get_binlog_file()->position() != reserved_size) {
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to fill reserve space exactly.");
    thd->commit_error = THD::CE_FLUSH_ERROR;
    flush_error = true;
    goto err;
  }

  /* Flush and sync empty and gtid event to binlog file before rename. */
  if ((flush_error = mysql_bin_log.flush_and_sync(true /* Force sync */))) {
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to flush and sync binary logs.");
    thd->commit_error = THD::CE_FLUSH_ERROR;
    goto err;
  }

#ifndef NDEBUG
  assert(debug_binlog_position == mysql_bin_log.atomic_binlog_end_pos);
  assert(mysql_bin_log.is_active(debug_binlog_file_name));
#endif

  mysql_bin_log.lock_index();

  /* Rename tmp file to binlog file. */
  if ((flush_error = rename_temp_to_binlog())) {
    mysql_bin_log.unlock_index();
    goto err;
  }

  /* Seek to the end of binlog file. */
  mysql_bin_log.get_binlog_file()->seek(file_end_pos);

  /*
    update_binlog_end_pos() must be executed before unlock LOCK_index.

    When Dump thread finishes sending the last binlog, it will take LOCK_index,
    get current binlog file name and start reading current binlog. Update binlog
    end pos should be done before dump thread gets current binlog, to avoid
    using end_pos of the last binlog to read current binlog.
  */
  mysql_bin_log.update_binlog_end_pos();

  assert(file_end_pos == mysql_bin_log.atomic_binlog_end_pos);

  mysql_bin_log.unlock_index();

  mysql_bin_log.update_thd_next_event_pos(thd);

  thd->set_trans_pos(mysql_bin_log.log_file_name, file_end_pos);

  /*
    The inc_prep_xid() is not called in this function, because lock_log is held
    by this thread during the whole commit process, there is no need to avoid
    other threads rotating the binlog.
  */

  m_is_free_flushing = false;

  /* Process after_flush and after_sync hook. */
  file_name_ptr =
      mysql_bin_log.log_file_name + dirname_length(mysql_bin_log.log_file_name);
  if (RUN_HOOK(binlog_storage, after_flush,
               (thd, file_name_ptr, mysql_bin_log.m_binlog_file->position()))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_FAILED_TO_RUN_AFTER_FLUSH_HOOK);
    flush_error = ER_ERROR_ON_WRITE;
    goto err;
  }

  if ((flush_error = call_after_sync_hook(thd))) {
    LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "failed to execute after sync function. ");
    goto err;
  }

  /* Finish commit. */
  (void)mysql_bin_log.finish_commit(thd);

  /* Rotate to the next binlog file. */
  if (mysql_bin_log.rotate(true, &check_purge)) {
    thd->commit_error = THD::CE_FLUSH_ERROR;
    goto err;
  }

  /* Clear checksum_alg_reset after last rotate. */
  mysql_bin_log.checksum_alg_reset = binary_log::BINLOG_CHECKSUM_ALG_UNDEF;

err:
  if (flush_error)
    mysql_bin_log.handle_binlog_flush_or_sync_error(
        thd, false /* need_lock_log */, message);

  m_is_free_flushing = false;
  mysql_mutex_unlock(&mysql_bin_log.LOCK_log);
  return true;
}

ulong Binlog_cache_free_flush::calculate_front_event_length() {
  /* 0. Binlog header. */
  ulong total_length = BIN_LOG_HEADER_SIZE;

  /* 1. Format description event. FD always have checksum. */
  total_length += Binary_log_event::FORMAT_DESCRIPTION_HEADER_LEN +
                  BINLOG_CHECKSUM_ALG_DESC_LEN + LOG_EVENT_HEADER_LEN +
                  BINLOG_CHECKSUM_LEN;

  /* 2. Previous gtids event */
  /* Need wait prep_xids equal to 0 to get accurate previous gtid. */
  mysql_mutex_assert_owner(&mysql_bin_log.LOCK_log);
  mysql_mutex_lock(&mysql_bin_log.LOCK_xids);
  /*
    We need to ensure that the number of prepared XIDs are 0, to make sure all
    gtid before this binlog file has been added into gtid_executed. Otherwise,
    the calculation of previous gtid event size may get wrong value.
  */
  while (mysql_bin_log.get_prep_xids() > 0) {
    DEBUG_SYNC(current_thd, "before_waiting_xids_cond");
    mysql_cond_wait(&mysql_bin_log.m_prep_xids_cond, &mysql_bin_log.LOCK_xids);
  }
  mysql_mutex_unlock(&mysql_bin_log.LOCK_xids);

  /* Generate previous gtid event. */
  Gtid_set logged_gtids_binlog(global_sid_map, global_sid_lock);

  global_sid_lock->wrlock();

  const Gtid_set *executed_gtids = gtid_state->get_executed_gtids();
  const Gtid_set *gtids_only_in_table = gtid_state->get_gtids_only_in_table();
  /* logged_gtids_binlog= executed_gtids - gtids_only_in_table */
  if (logged_gtids_binlog.add_gtid_set(executed_gtids) != RETURN_STATUS_OK) {
    global_sid_lock->unlock();
    LogErr(WARNING_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
           "fail to get previous gtid event size.");
    return 0;
  }
  logged_gtids_binlog.remove_gtid_set(gtids_only_in_table);

  /* Add previous gtid event length. */
  total_length +=
      logged_gtids_binlog.get_encoded_length() + LOG_EVENT_HEADER_LEN;

  global_sid_lock->unlock();

  /* 3. Gtid event */
  total_length += binary_log::Gtid_event::MAX_EVENT_LENGTH;

  /* 4. Min value of ignorable event, only event header. */
  total_length += LOG_EVENT_HEADER_LEN;

  return total_length;
}

inline bool Binlog_cache_free_flush::check(THD *thd) {
  binlog_cache_mngr *cache_mngr = thd_get_cache_mngr(thd);
  Binlog_cache_storage *cache_storage = cache_mngr->get_trx_cache();

  if (!m_is_enabled) return false;

  /*
    Free flush is incompatible with a second non-binlog 2PC engine such as
    DuckDB. DuckDB registers itself as a 2PC engine whenever binlog is
    enabled (ha_duckdb.cc: duckdb_hton->prepare is set when opt_bin_log),
    which makes total_ha_2pc == 3 regardless of duckdb_mode.

    Free flush closes the old binlog cleanly (clearing LOG_EVENT_IN_USE_F)
    before renaming the temp file to the new binlog. A crash in between
    therefore leaves a *cleanly-closed* binlog together with a still
    prepared transaction -- a combination the normal recovery path assumes
    is impossible. On restart open_binlog() sees the clean binlog and calls
    ha_recover(nullptr), which only force-rolls-back such a prepared
    transaction when total_ha_2pc <= opt_bin_log + 1 (binlog + a single
    engine). With DuckDB present that guard fails, recovery keeps dry_run
    and the server aborts with "Found N prepared transactions".

    So when more than one non-binlog engine participates in 2PC, fall back
    to the normal binlog flush path instead of doing a free flush.
  */
  if (total_ha_2pc > (ulong)opt_bin_log + 1) return false;

  /* Do not free flush if total_bytes smaller than limit size. */
  if (DBUG_EVALUATE_IF("free_flush_skip_limit_size_check", 0, 1) &&
      likely(cache_storage->length() <= m_limit_size))
    return false;

  /* Do not free flush if stmt storage engine enable. */
  if (!cache_mngr->stmt_cache.is_binlog_empty()) return false;

  if (!cache_mngr->trx_cache.is_finalized() ||
      cache_mngr->trx_cache.has_incident())
    return false;

  /*
    Do not free flush if binlog encryption is enabled. Binlog cache tmp file
    uses a different file_password with binlog file.
  */
  if (rpl_encryption.is_enabled() ||
      cache_storage->get_rds_cache_storage()->is_encryption_enabled())
    return false;

  if (unlikely(!mysql_bin_log.is_open())) return false;

  if (thd->get_transaction()
          ->get_rds_transaction_ctx()
          ->get_modified_gtid_executed_table()) {
    LogErr(
        WARNING_LEVEL, ER_MESSAGE_BINLOG_CACHE_FREE_FLUSH,
        "the trx has modified gtid_executed table. To avoid the deadlock with "
        "attachable_trx_rw, commit will go back to original logic.");
    return false;
  }

  /*
    Do not free flush if not write tmp file, happened when limit_size is
    smaller than binlog cache size.
  */
  if (cache_storage->disk_writes() == 0) return false;

  /* Do not free flush if reserve space equal to zero. */
  if (cache_storage->get_rds_cache_storage()->get_file_reserved_size() == 0)
    return false;

  return true;
}

inline bool Binlog_cache_free_flush::check_reserved_space_enough(
    my_off_t reserved_size) {
  ulonglong front_len;

  if (!(front_len = calculate_front_event_length())) return false;

  if (DBUG_EVALUATE_IF("simulate_reserve_size_not_enough", 1, 0) ||
      front_len > reserved_size) {
    char errmsg[FN_REFLEN + 128];
    const char *errmsg_fmt =
        "check reserve space for front events is not enough, go back to "
        "original logic. reserve size = %llu, front event size = %llu";

    sprintf(errmsg, errmsg_fmt, reserved_size, front_len);

    LogErr(ERROR_LEVEL, ER_MESSAGE_BINLOG_CACHE_FREE_FLUSH, errmsg);
    return false;
  }

  return true;
}
