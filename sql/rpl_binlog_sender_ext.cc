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

#include "mysql/psi/mysql_memory.h"

#include "sql/basic_ostream.h"
#include "sql/mysqld.h"
#include "sql/rpl_binlog_sender_ext.h"
#include "sql/sql_lex.h"

/*
  atomic<T>::value_type is not define in old versions, so  micros
  are used instead of the template functions
*/
#if 0
template <class ATOMIC_T>
typename ATOMIC_T::value_type relax_load(const ATOMIC_T &t) {
  return t.load(std::memory_order_relaxed);
}

template <class ATOMIC_T>
void relax_store(ATOMIC_T &t, typename ATOMIC_T::value_type value) {
  return t.store(value, std::memory_order_relaxed);
}
#else
#define relax_load(t) t.load(std::memory_order_relaxed)
#define relax_store(t, v) t.store(v, std::memory_order_relaxed)
#endif

uint binlog_realtime_ddl_time_limit = DEFAULT_BINLOG_REALTIME_DDL_TIME_LIMIT;
bool binlog_realtime_apply_ddl_source_enabled = false;
bool optimize_table_write_binlog_advance = false;
bool binlog_realtime_apply_long_trx_enabled = false;

Brr_trx::Brr_trx() {
  index = 0;
  end_position = 0;
  status = Brr_trx_status::ACTIVE;
  file_name[0] = '\0';
  start_read_pos = 0;
  ref_count = 0;
  gno = 0;
}

Brr_trx_manager brr_trx_mngr;
std::unordered_set<Brr_binlog_sender *> brr_binlog_sender_set;

static PSI_memory_key key_memory_brr_source;
void Brr_trx_manager::init_pthread_objects() {
#ifdef HAVE_PSI_INTERFACE
  const char *category = "sql";
  int count;

  static PSI_memory_info all_memories[] = {{&key_memory_brr_source,
                                            "binlog_realtime_transmit_source", 0,
                                            0, PSI_DOCUMENT_ME}};
  count = static_cast<int>(array_elements(all_memories));
  mysql_memory_register(category, all_memories, count);
#endif
}

Brr_trx_manager::Brr_trx_manager() {
  m_max_index = 0;
  m_enabled = DEFAULT_BRR_SOURCE_ENABLED;
  m_limit_size = DEFAULT_BRR_LONG_TRANSACTION_LIMIT_SIZE;
  m_brr_max_send_size = DEFAULT_BRR_READ_BLOCK_SIZE;
  m_cache_read_block_size = DEFAULT_BRR_READ_BLOCK_SIZE;
}

Brr_trx *Brr_trx_manager::open_brr_trx(const char *file_name, my_off_t end_pos,
                                       my_off_t offset, uchar *buffer) {
  Brr_trx *brr_trx = (Brr_trx *)my_malloc(key_memory_brr_source,
                                          sizeof(Brr_trx), MYF(MY_ZEROFILL));
  if (brr_trx == nullptr) return nullptr;

  brr_trx->end_position = end_pos;
  brr_trx->start_read_pos = offset;
  
  if (file_name) {
    memset(brr_trx->file_name, 0, FN_REFLEN);
    memcpy(brr_trx->file_name, file_name,
           std::min(strlen(file_name), static_cast<size_t>(FN_REFLEN - 1)));
  } else {
    assert(buffer);
    brr_trx->ddl_query_buffer = buffer;
  }

  brr_trx->status = Brr_trx_status::ACTIVE;
  brr_trx->gno = 0;
  brr_trx->ref_count = 0;
  brr_trx->brr_thread_id = current_thd->thread_id();
  brr_trx->last_gtid_executed = nullptr;
  brr_trx->last_gtid_executed_len = 0;
  brr_trx->ddl_start_time = now_in_nanosecs();
  brr_trx->is_xa_trx = false;

  if (current_thd && current_thd->is_extrenal_xa()) brr_trx->is_xa_trx = true;

  mysql_bin_log.lock_binlog_end_pos();
  m_max_index.fetch_add(1);
  brr_trx->index = m_max_index;

  m_brr_trx_list[brr_trx->index] = brr_trx;

  source_brr_trx_count++;

  mysql_bin_log.signal_update();
  mysql_bin_log.unlock_binlog_end_pos();

  LOG_BRR_MESSAGE(
      "brr trx start on source, brr index: %llu, tmp file name: %s, start read "
      "pos: %llu, current end pos of tmp file: %llu",
      (ulonglong)brr_trx->index.load(), brr_trx->file_name,
      brr_trx->start_read_pos, (ulonglong)brr_trx->end_position.load());

  return brr_trx;
}

bool Brr_trx_manager::try_remove_trx(Brr_trx *brr_trx) {
  assert(brr_trx != nullptr);
  mysql_mutex_assert_owner(mysql_bin_log.get_binlog_end_pos_lock());

  /*
    We only remove brr_trx in the following status.

    1. The brr trx status is not active, which means the user thread of this
    transaction has finished modifying the brr trx.

    2. Ref_count = 0, which means we are the last dump thread reading this trx;
    or we are the user thread committing this trx, updating the status, but
    there is no dump thread reading this trx.
  */
  if (brr_trx->status != Brr_trx_status::ACTIVE && brr_trx->ref_count == 0) {
    m_brr_trx_list.erase(brr_trx->index);
    source_brr_trx_count--;
    LOG_BRR_MESSAGE(
        "brr trx removed on source, brr index: %llu, brr status: %u, brr trx "
        "gno: %lld",
        (ulonglong)brr_trx->index.load(), (uint)brr_trx->status.load(),
        (longlong)brr_trx->gno.load());

    brr_trx->~Brr_trx();
    my_free(brr_trx);

    return true;
  }

  return false;
}

void Brr_trx_manager::update_brr_trx_gtid(Brr_trx *brr_trx,
                                          const rpl_sid *sid_ptr, rpl_gno gno) {
  assert(brr_trx != nullptr);

  {
    /* Record the current gtid_executed in Brr_trx::last_gtid_executed. */
    global_sid_lock->wrlock();

    const Gtid_set *gtid_set = gtid_state->get_executed_gtids();
    ulonglong gtid_set_len = gtid_set->get_encoded_length();
    brr_trx->last_gtid_executed =
        (uchar *)my_malloc(key_memory_brr_source, gtid_set_len, MYF(MY_WME));

    if (brr_trx->last_gtid_executed == nullptr) {
      global_sid_lock->unlock();
      LOG_BRR_ERROR("%s allocate memory failed, brr index: %llu", __func__,
                    (ulonglong)brr_trx->index.load());
      close_brr_trx(brr_trx, Brr_trx_status::ROLLBACK);
      return;
    }
    brr_trx->last_gtid_executed_len = gtid_set_len;
    gtid_set->encode(brr_trx->last_gtid_executed);

    global_sid_lock->unlock();
  }

  /*
    We store sid first and gno next in memory_order_release way, to make sure
    sid is initialized before gno, which make other thread can get the sid
    after check the value of gno in memory_order_release way.
  */
  brr_trx->sid.copy_from(*sid_ptr);
  brr_trx->gno.store(gno, std::memory_order_release);

  /* Signal_update will be called in this function. */
  close_brr_trx(brr_trx, Brr_trx_status::FINISHED);
}

void Brr_trx_manager::close_brr_trx(Brr_trx *brr_trx, Brr_trx_status status) {
  assert(brr_trx != nullptr);

  mysql_bin_log.lock_binlog_end_pos();
  assert(m_brr_trx_list[brr_trx->index] == brr_trx);
  assert(status != Brr_trx_status::ACTIVE);

  brr_trx->status.store(status);

  /*
    We dont signal update if the brr trx has been removed, because in this case
    there are no dump thread we need to signal up.
  */
  if (!try_remove_trx(brr_trx)) mysql_bin_log.signal_update();

  mysql_bin_log.unlock_binlog_end_pos();
}

Brr_binlog_sender::Brr_binlog_sender(THD *thd, Binlog_sender *sender)
    : m_thd(thd), m_sender(sender) {
  m_max_index = 0;
}

Brr_binlog_sender::~Brr_binlog_sender() {
  auto it = m_brr_reader_list.begin();
  while (it != m_brr_reader_list.end()) {
    Brr_reader *brr_reader = *it;
    it++;
    remove_reader(brr_reader, false);
  }
  LOG_BRR_MESSAGE("Brr sender finished");
}

class Brr_binlog_sender::Brr_reader {
  typedef Basic_binlog_file_reader<Binlog_ifile, Binlog_event_data_istream,
                                   Binlog_event_object_istream,
                                   Binlog_sender::Event_allocator>
      File_reader;

 public:
  Brr_reader(Brr_trx *brr_trx, Brr_binlog_sender *sender)
      : m_brr_trx_ptr(brr_trx), m_brr_sender(sender) {}

  ~Brr_reader() {
    if (m_file_reader != nullptr) {
      m_file_reader->~File_reader();
      my_free(m_file_reader);
      m_file_reader = nullptr;
    }
  }

  bool init_reader(uint max_event_size) {
    /* For DDL, we use read the buffer of Brr_trx.*/
    if (m_brr_trx_ptr->is_brr_ddl()) {
      assert(m_brr_trx_ptr->ddl_query_buffer);
      m_buffer = m_brr_trx_ptr->ddl_query_buffer;
      return false;
    }

    /*
      Whether long transactions use realtime apply is determined at
      Brr_reader creation based on the value of
      binlog_realtime_apply_long_trx_enabled.

      If the long trx don't use realtime apply, we will not send the
      Gtid_executed_log_event any more.

      Now we don't support XA transaction, we will support it later.
    */
    if (!binlog_realtime_apply_long_trx_enabled || m_brr_trx_ptr->is_xa_trx) {
      finish_send_gtid_execute_event = true;
    }

    m_file_reader = (File_reader *)my_malloc(
        key_memory_brr_source, sizeof(File_reader), MYF(MY_ZEROFILL));
    if (!m_file_reader) {
      return true;
    }

    m_file_reader =
        new (m_file_reader) File_reader(false, max_event_size, true);
    m_file_reader->allocator()->set_sender(m_brr_sender->m_sender);

    if (m_file_reader->open(m_brr_trx_ptr->file_name)) {
      return true;
    }

    m_file_reader->set_format_description_event(m_brr_sender->m_fde);
    return m_file_reader->seek(m_brr_trx_ptr->start_read_pos);
  }

  my_off_t position() {
    if (m_buffer) {
      return m_buffer_position;
    } else {
      assert(m_file_reader);
      return m_file_reader->position();
    }
  }

  bool seek(my_off_t offset) {
    assert(m_file_reader);
    return m_file_reader->seek(offset);
  }

  bool read(uchar **event_ptr, uint32 *event_len, ulonglong index) {
    if (m_buffer) {
      *event_len = uint4korr(m_buffer + m_buffer_position + EVENT_LEN_OFFSET);

      /*
        Set the type of first BRR_GTID_EXECUTED_EVENT as BRR_DDL_BEGIN_EVENT.
        When IO thread receive the BRR_DDL_BEGIN_EVENT, it will create brr
        cache. So that when brr worker fail to apply brr trx and destroy brr
        cache, IO thread will not create brr_cache again when receive flowing
        brr events of the brr trx.
      */
      if (m_brr_sender->brr_reset_packet(
              *event_len, index,
              m_brr_sender->check_has_sent_brr_events(this)
                  ? Brr_magic_type::BRR_DDL_EVENT
                  : Brr_magic_type::BRR_DDL_BEGIN_EVENT))
        return true;

      size_t packet_header_len = m_brr_sender->m_sender->m_packet.length();
      *event_ptr =
          pointer_cast<uchar *>(m_brr_sender->m_sender->m_packet.ptr()) +
          packet_header_len;
      memcpy(*event_ptr, m_buffer + m_buffer_position, *event_len);
      m_brr_sender->m_sender->m_packet.length(packet_header_len + *event_len);
      m_buffer_position += *event_len;
    } else {
      assert(m_file_reader);
      if (m_brr_sender->brr_reset_packet(
              *event_len, index,
              m_brr_sender->check_has_sent_brr_events(this)
                  ? Brr_magic_type::BRR_LONG_TRX_EVENT
                  : Brr_magic_type::BRR_LONG_TRX_BEGIN_EVENT))
        return true;

      if (m_file_reader->read_event_data(event_ptr, event_len) ||
          event_ptr == nullptr)
        return true;
    }

    return false;
  }

  Brr_trx *get_trx_ptr() { return m_brr_trx_ptr; }

  Brr_binlog_sender *get_binlog_sender() { return m_brr_sender; }

  /* The number of brr events sent to slave. */
  ulonglong brr_events_sent = 0;

  /* The number of normal events sent to slave. */
  ulonglong trx_content_events_sent = 0;

  /*
    Whether this transaction has event exceed the brr max_send_size.

    If the event exceed the max_send_size, this variable will be set to true,
    which means the rest of events of this trx will not be sent realtime, they
    will be sent after trx commit, in find_and_finish_brr_transaction().
  */
  bool event_exceed_max_send_size = false;

  /*
    The replica determines the replay timing of a group of events based
    on BRR_GTID_EXECUTED_EVENT.
    last_gtid_executed_send_pos indicates the end position of the group
    of events in the binlog cache.
  */
  my_off_t last_gtid_executed_send_pos = 0;

  /*
    If finish_send_gtid_execute_event is true, we will not send
    gtid_executed_log_event any more.
  */
  bool finish_send_gtid_execute_event = false;

 private:
  /* The ptr of brr_trx this reader reading on. */
  Brr_trx *m_brr_trx_ptr = nullptr;

  /* The ptr of brr binlog sender. */
  Brr_binlog_sender *m_brr_sender = nullptr;

  /* The file reader to read the binlog cache tmp file of brr_trx. */
  File_reader *m_file_reader = nullptr;

  /* For DDL, we read the buffer directly. */
  uchar *m_buffer = nullptr;

  /* The position of the buffer that we are reading. */
  my_off_t m_buffer_position = 0;
};

bool Brr_binlog_sender::brr_reset_packet(size_t event_len, ulonglong brr_index,
                                         Brr_magic_type magic_type) {
  assert(m_sender->m_packet.alloced_length() >=
         1 + BRR_HEADER_MAGIC_SIZE + BRR_HEADER_INDEX_SIZE);
  m_sender->m_packet.length(1 + BRR_HEADER_MAGIC_SIZE +
                            BRR_HEADER_INDEX_SIZE);  // size of the content

  m_sender->m_packet.ptr()[0] = 0;  // Set this as an OK packet

  m_sender->m_packet.ptr()[1] = BRR_MAGIC_NUM;
  m_sender->m_packet.ptr()[2] = (uint)magic_type;

  int8store((uchar *)m_sender->m_packet.ptr() + 1 + BRR_HEADER_MAGIC_SIZE,
            brr_index);

  /* Resizes the buffer if needed. */
  if (event_len > 0 && m_sender->grow_packet(event_len)) return 1;
  return 0;
}

bool Brr_binlog_sender::update_brr_reader_list(bool have_lock) {
  assert(m_max_index <= brr_trx_mngr.get_max_index());
  if (m_max_index == brr_trx_mngr.get_max_index()) return false;

  if (have_lock) {
    mysql_mutex_assert_owner(mysql_bin_log.get_binlog_end_pos_lock());
  } else {
    mysql_bin_log.lock_binlog_end_pos();
  }

  DBUG_EXECUTE_IF("wait_before_update_brr_reader_list", {
    mysql_bin_log.unlock_binlog_end_pos();
    const char act[] = "now WAIT_FOR continue_update_brr_reader_list";
    assert(!debug_sync_set_action(current_thd, STRING_WITH_LEN(act)));
    mysql_bin_log.lock_binlog_end_pos();
  });

  int added_brr_reader_count = 0;

  unsigned int max_event_size =
      std::max(m_thd->variables.max_allowed_packet,
               binlog_row_event_max_size + MAX_LOG_EVENT_HEADER);

  auto *list_ptr = brr_trx_mngr.get_brr_trx_list();
  auto it = list_ptr->begin();
  while (it != list_ptr->end()) {
    Brr_trx *brr_trx = it->second;
    it++;

    assert(brr_trx->index <= brr_trx_mngr.get_max_index());
    if (brr_trx->index <= m_max_index) continue;

    if (brr_trx->status != Brr_trx_status::ACTIVE) continue;

    /*
      Begin to prepare the brr_reader for current brr_trx. This operation will
      be done after checking brr_trx->status == Brr_trx_status::ACTIVE,
      inside the same lock, which means this brr_reader is opening the binlog
      cache tmp file before transaction commit.

      If prepare failed, this brr trx will be ignored by current dump thread,
      dump thread will not corrupt.
    */
    Brr_reader *brr_reader = (Brr_reader *)my_malloc(
        key_memory_brr_source, sizeof(Brr_reader), MYF(MY_ZEROFILL));
    if (!brr_reader) {
      LOG_BRR_ERROR("Fail to alloc memory for brr reader, brr trx index: %llu",
                    (ulonglong)brr_trx->index.load());
      continue;
    }

    brr_reader = new (brr_reader) Brr_reader(brr_trx, this);
    if (brr_reader->init_reader(max_event_size)) {
      brr_reader->~Brr_reader();
      my_free(brr_reader);
      LOG_BRR_ERROR("Fail to init brr reader, brr trx index: %llu",
                    (ulonglong)brr_trx->index.load());

      continue;
    }

    LOG_BRR_MESSAGE(
        "brr reader start on source, brr trx start realtime transmit by dump "
        "thread, thread_id: %d, replica server_id: %d, brr index: %llu, "
        "tmp file name: %s, start read pos: %llu, end pos: %llu, is_ddl: %d, "
        "using realtime_apply: %d",
        m_thd->thread_id(), m_thd->server_id, (ulonglong)brr_trx->index.load(),
        brr_trx->file_name, brr_trx->start_read_pos,
        (ulonglong)brr_trx->end_position.load(), brr_trx->is_brr_ddl(),
        !brr_reader->finish_send_gtid_execute_event);

    source_brr_reader_count++;

    brr_trx->ref_count++;

    /*
      An updated brr list does not guarantee a readable brr trx. This is
      because DDL-type brr trx must meet the binlog_realtime_ddl_time_limit
      threshold. This prevents the situation in which the wait_new_events
      function in get_binlog_end_pos ends the wait, but the subsequent
      check_brr_reader_readable function returns false, causing the dump
      function to abort.
    */
    if (brr_reader->get_trx_ptr()->check_send_brr_ddl_event())
      added_brr_reader_count++;

    m_brr_reader_list.emplace_back(brr_reader);
  }

  m_max_index = brr_trx_mngr.get_max_index();
  if (!have_lock) mysql_bin_log.unlock_binlog_end_pos();
  return (added_brr_reader_count > 0);
}

bool Brr_binlog_sender::check_brr_reader_readable(
    std::chrono::nanoseconds *brr_ddl_wait_time) {
  auto it = m_brr_reader_list.begin();
  while (it != m_brr_reader_list.end()) {
    Brr_reader *brr_reader = *it;
    it++;
    if (brr_reader->get_trx_ptr()->status != Brr_trx_status::ACTIVE ||
        (brr_reader->get_trx_ptr()->end_position > brr_reader->position() &&
         !brr_reader->event_exceed_max_send_size &&
         brr_reader->get_trx_ptr()->check_send_brr_ddl_event(
             brr_ddl_wait_time)))
      return true;
  }

  return false;
}

bool Brr_binlog_sender::find_and_finish_brr_transaction(
    const uchar *gtid_event_ptr,
    binary_log::enum_binlog_checksum_alg checksum_alg, my_off_t trx_start_pos,
    bool in_exclude_group, my_off_t *trx_end_pos) {
  Format_description_log_event s;
  s.common_footer->checksum_alg = checksum_alg;
  Gtid_log_event gtid_ev(reinterpret_cast<const char *>(gtid_event_ptr), &s);
  gtid_ev.common_footer->checksum_alg = binary_log::BINLOG_CHECKSUM_ALG_OFF;
  *trx_end_pos = 0;
  String tmp_buffer;

  /* Find brr_reader in brr_reader_list according to gtid. */
  Brr_reader *brr_reader = nullptr;
  auto it = m_brr_reader_list.begin();
  while (it != m_brr_reader_list.end()) {
    Brr_reader *brr_reader_it = *it;
    it++;

    if (brr_reader_it->get_trx_ptr()->gno.load(std::memory_order_acquire) ==
        gtid_ev.get_gno()) {
      /*
        We load gno in memory_order_acquire way, so if gno is initialized, sid
        is initialized too, see logic in update_brr_trx_gtid. This means we can
        access brr_reader->get_trx_ptr()->sid without lock.
      */
      if (gtid_ev.get_sid()->equals(brr_reader_it->get_trx_ptr()->sid))
        brr_reader = brr_reader_it;
      if (brr_reader != nullptr) break;
    }
  }
  if (brr_reader == nullptr) return false;

  /* If we have never sent any brr events, we just remove reader. */
  if (!check_has_sent_brr_events(brr_reader)) {
    remove_reader(brr_reader, true);
    return false;
  }

  /* Have Found brr_reader for this transaction. */
  if (brr_reader->get_trx_ptr()->is_brr_ddl()) {
    /*
      Save a copy of the buffer content. For brr_trx of BRR_DDL_EVENT type,
      we need to send normal Gtid_log_event and Query_log_event.
    */
    tmp_buffer.copy(m_sender->m_packet);
    tmp_buffer.length(m_sender->m_packet.length());
  }

  /* Send rollback brr message event if need to skip this transaction. */
  if (in_exclude_group) return rollback_brr_transaction(brr_reader);

  /*
    Send rest of brr events of this transaction, gtid event, finish brr message
    event and a heartbeat event with ask_ack.
  */
  assert(brr_reader->get_trx_ptr()->status == Brr_trx_status::FINISHED);

  uchar *event_ptr = nullptr;
  uint32 event_len = 0;

  while (brr_reader->get_trx_ptr()->end_position > brr_reader->position()) {
    /*
      For DDL, we will send Gtid_execute_log_event in following code. For long
      transaction, this is the last Gtid_execute_log_event we send.
    */
    if (!brr_reader->get_trx_ptr()->is_brr_ddl() &&
        send_brr_gtid_executed_event(brr_reader))
      return true;

    if (DBUG_EVALUATE_IF("simulate_read_rest_brr_event_error", 1,
                         brr_reader->read(&event_ptr, &event_len,
                                          brr_reader->get_trx_ptr()->index))) {
      LOG_BRR_ERROR(
          "Read rest of brr event from binlog cache tmp file failed, rollback "
          "this brr_trx, brr status: %u, brr index: %llu",
          (uint)brr_reader->get_trx_ptr()->status.load(),
          (ulonglong)brr_reader->get_trx_ptr()->index.load());
      return true;
    }

    if (unlikely(m_sender->send_packet())) {
      LOG_BRR_ERROR("Send brr event error, brr index: %llu",
                    (ulonglong)brr_reader->get_trx_ptr()->index.load());
      return true;
    }
    brr_reader->trx_content_events_sent++;
    source_sent_brr_events_count++;
  }

  if (brr_reader->get_trx_ptr()->is_brr_ddl()) {
    /*
      For brr_trx of BRR_DDL_EVENT type, send brr_gtid_executed_event before
      send brr Gtid_log_event, so replica can determine the time of committing
      this brr transaction.
      For brr_trx of BRR_DDL_EVENT type, all binlog events will be transmit
      again, so there is no need to send heartbeat event.
    */
    if (send_brr_gtid_executed_event(brr_reader)) return true;

    if (send_brr_gtid_event(brr_reader, &gtid_ev)) return true;

    /* Restore the copy back. */
    m_sender->m_packet.copy(tmp_buffer);
  } else {
    *trx_end_pos = trx_start_pos + gtid_ev.transaction_length;
    if (send_brr_gtid_event(brr_reader, &gtid_ev) ||
        m_sender->send_heartbeat_event_v2(*trx_end_pos, true))
      return true;
  }

  remove_reader(brr_reader, true);
  return false;
}

bool Brr_binlog_sender::send_brr_gtid_event(Brr_reader *brr_reader,
                                            Gtid_log_event *gtid_ev) {
  uchar buf[binary_log::Gtid_event::MAX_EVENT_LENGTH];
  Buffer_ostream writer(buf, binary_log::Gtid_event::MAX_EVENT_LENGTH);
  if (gtid_ev->write(&writer)) return true;

  if (brr_reset_packet(writer.position, brr_reader->get_trx_ptr()->index,
                       brr_reader->get_trx_ptr()->is_brr_ddl()
                           ? Brr_magic_type::BRR_DDL_EVENT
                           : Brr_magic_type::BRR_LONG_TRX_EVENT))
    return true;

  size_t packet_header_len = m_sender->m_packet.length();
  uchar *event_ptr =
      pointer_cast<uchar *>(m_sender->m_packet.ptr()) + packet_header_len;
  memcpy(event_ptr, buf, writer.position);

  m_sender->m_packet.length(writer.position + packet_header_len);
  if (m_sender->send_packet_and_flush()) {
    LOG_BRR_ERROR("Send brr gtid event error, brr index: %llu",
                  (ulonglong)brr_reader->get_trx_ptr()->index.load());
    return true;
  }
  brr_reader->trx_content_events_sent++;
  source_sent_brr_events_count++;
  return false;
}

bool Brr_binlog_sender::rollback_brr_transaction(Brr_reader *brr_reader) {
  /*
    If we have never sent any brr events, we just remove the reader and
    we will not send rollback message.
  */
  if (!check_has_sent_brr_events(brr_reader)) {
    remove_reader(brr_reader, false);
    return false;
  }

  if (brr_reset_packet(LOG_EVENT_HEADER_LEN, brr_reader->get_trx_ptr()->index,
                       Brr_magic_type::BRR_ROLLBACK_EVENT))
    return true;
  size_t packet_header_len = m_sender->m_packet.length();
  m_sender->m_packet.length(packet_header_len + LOG_EVENT_HEADER_LEN + 1);

  uchar *header =
      pointer_cast<uchar *>(m_sender->m_packet.ptr()) + packet_header_len;

  /* Timestamp field */
  int4store(header, 0);
  header[EVENT_TYPE_OFFSET] = binary_log::IGNORABLE_LOG_EVENT;
  int4store(header + SERVER_ID_OFFSET, server_id);
  int4store(header + LOG_POS_OFFSET, 0);
  int2store(header + FLAGS_OFFSET, LOG_EVENT_IGNORABLE_F);
  int4store(header + EVENT_LEN_OFFSET, LOG_EVENT_HEADER_LEN);

  if (m_sender->send_packet_and_flush()) {
    LOG_BRR_ERROR("Send brr rollback event error, brr index: %llu",
                  (ulonglong)brr_reader->get_trx_ptr()->index.load());
    return true;
  }
  brr_reader->brr_events_sent++;
  source_sent_brr_events_count++;
  remove_reader(brr_reader, false);
  return false;
}

struct Malloc_guard {
  uchar *ptr;
  bool owned;
  Malloc_guard(uchar *p = nullptr, bool o = false) : ptr(p), owned(o) {}
  ~Malloc_guard() {
    if (owned && ptr) my_free(ptr);
  }
  void reset(uchar *p, bool o) {
    if (owned && ptr) my_free(ptr);
    ptr = p;
    owned = o;
  }
};

bool Brr_binlog_sender::send_brr_gtid_executed_event(Brr_reader *brr_reader) {
  if (brr_reader->finish_send_gtid_execute_event) return false;

  Malloc_guard gtid_set_buf_guard;
  ulonglong gtid_set_len = 0;
  Brr_magic_type brr_magic_type;

  Brr_trx *brr_trx = brr_reader->get_trx_ptr();

  if (brr_trx->is_brr_ddl() && !brr_trx->last_gtid_executed) {
    LOG_BRR_ERROR("last_gtid_executed is empty, brr index: %llu",
                  (ulonglong)brr_trx->index.load());
    return true;
  }

  if (brr_trx->last_gtid_executed) {
    gtid_set_buf_guard.reset(brr_trx->last_gtid_executed, false);
    gtid_set_len = brr_trx->last_gtid_executed_len;

    /*
      If we find last_gtid_executed is set, then we will send the
      last_gtid_executed, and we will finish send gtid_executed_event,
      so that we can avoid mistakenly getting more dependent transactions.
    */
    assert(!brr_reader->finish_send_gtid_execute_event);
    brr_reader->finish_send_gtid_execute_event = true;
  } else {
    brr_reader->last_gtid_executed_send_pos =
        brr_reader->get_trx_ptr()->end_position;

    /* Get gtid_executed. */
    global_sid_lock->wrlock();

    const Gtid_set *gtid_set = gtid_state->get_executed_gtids();
    gtid_set_len = gtid_set->get_encoded_length();
    uchar *gtid_set_buf =
        (uchar *)my_malloc(key_memory_brr_source, gtid_set_len, MYF(MY_WME));

    if (gtid_set_buf == nullptr) {
      global_sid_lock->unlock();
      LOG_BRR_ERROR("%s allocate memory failed, brr index: %llu", __func__,
                    (ulonglong)brr_trx->index.load());
      return true;
    }
    gtid_set->encode(gtid_set_buf);
    gtid_set_buf_guard.reset(gtid_set_buf, true);

    global_sid_lock->unlock();
  }

  Brr_gtid_executed_log_event gtid_executed_log_event(m_thd);

  size_t gtid_executed_log_ev_len = gtid_set_len + LOG_EVENT_HEADER_LEN;

  if (brr_reader->get_trx_ptr()->is_brr_ddl()) {
    brr_magic_type = Brr_magic_type::BRR_DDL_EVENT;
  } else {
    if (check_has_sent_brr_events(brr_reader))
      brr_magic_type = Brr_magic_type::BRR_LONG_TRX_EVENT;
    else
      brr_magic_type = Brr_magic_type::BRR_LONG_TRX_BEGIN_EVENT;
  }

  if (brr_reset_packet(gtid_executed_log_ev_len, brr_trx->index,
                       brr_magic_type))
    return true;

  size_t packet_header_len = m_sender->m_packet.length();
  m_sender->m_packet.length(packet_header_len + gtid_executed_log_ev_len);

  uchar *header =
      pointer_cast<uchar *>(m_sender->m_packet.ptr()) + packet_header_len;

  Buffer_ostream writer(header, gtid_executed_log_ev_len);

  if (gtid_executed_log_event.write_buffer(gtid_set_buf_guard.ptr, gtid_set_len,
                                           &writer)) {
    LOG_BRR_ERROR("Write Brr_gtid_executed_log_event error, brr index: %llu",
                  (ulonglong)brr_trx->index.load());
    return true;
  }

  if (m_sender->send_packet_and_flush()) {
    LOG_BRR_ERROR("Send Brr_gtid_executed_log_event error, brr index: %llu",
                  (ulonglong)brr_trx->index.load());
    return true;
  }
  brr_reader->brr_events_sent++;
  source_sent_brr_events_count++;
  return false;
}

bool Brr_binlog_sender::check_has_sent_brr_events(Brr_reader *brr_reader) {
  return brr_reader->brr_events_sent + brr_reader->trx_content_events_sent;
}

bool Brr_binlog_sender::send_brr_fde_event() {
  m_fde.common_footer->checksum_alg = binary_log::BINLOG_CHECKSUM_ALG_OFF;

  uchar buf[Binary_log_event::FORMAT_DESCRIPTION_HEADER_LEN +
            BINLOG_CHECKSUM_ALG_DESC_LEN + LOG_EVENT_HEADER_LEN +
            BINLOG_CHECKSUM_LEN];

  Buffer_ostream writer(buf, Binary_log_event::FORMAT_DESCRIPTION_HEADER_LEN +
                                 BINLOG_CHECKSUM_ALG_DESC_LEN +
                                 LOG_EVENT_HEADER_LEN + BINLOG_CHECKSUM_LEN);
  if (m_fde.write(&writer)) return true;

  if (brr_reset_packet(writer.position, 0, Brr_magic_type::BRR_LONG_TRX_EVENT))
    return true;

  size_t packet_header_len = m_sender->m_packet.length();
  uchar *event_ptr =
      pointer_cast<uchar *>(m_sender->m_packet.ptr()) + packet_header_len;

  memcpy(event_ptr, buf, writer.position);

  m_sender->m_packet.length(writer.position + packet_header_len);
  if (m_sender->send_packet_and_flush()) {
    LOG_BRR_ERROR("Send brr format description event error");
    return true;
  }

  source_sent_brr_events_count++;
  return false;
}

void Brr_binlog_sender::remove_reader(Brr_reader *brr_reader,
                                      bool is_complete) {
  mysql_bin_log.lock_binlog_end_pos();

  /* Need to reduce the ref_count for brr_trx. */
  brr_reader->get_trx_ptr()->ref_count--;

  Brr_trx_status brr_status = brr_reader->get_trx_ptr()->status;
  ulonglong brr_index = brr_reader->get_trx_ptr()->index;
  rpl_gno gno = brr_reader->get_trx_ptr()->gno;
  bool is_ddl = brr_reader->get_trx_ptr()->is_brr_ddl();

  source_brr_reader_count--;

  brr_trx_mngr.try_remove_trx(brr_reader->get_trx_ptr());

  mysql_bin_log.unlock_binlog_end_pos();

  LOG_BRR_MESSAGE(
      "remove brr reader by dump thread, thread_id: %d, replica server_id: %d, "
      "brr index: %llu, is trx send complete: %d. The trx type is %s, total "
      "trx content events sent: "
      "%llu, total brr events sent: %llu, brr status: %u, brr trx gno : %lld",
      m_thd->thread_id(), m_thd->server_id, brr_index, is_complete,
      is_ddl ? "DDL" : "LONG TRANSACTION", brr_reader->trx_content_events_sent,
      brr_reader->brr_events_sent, (uint)brr_status, (longlong)gno);

  m_brr_reader_list.remove(brr_reader);
  brr_reader->~Brr_reader();
  my_free(brr_reader);
}

bool Brr_binlog_sender::send_brr_events() {
  uchar *event_ptr = nullptr;
  uint32 event_len = 0;

  DBUG_EXECUTE_IF("skip_send_brr_events", { return false; });

  auto it = m_brr_reader_list.begin();
  ulonglong total_event_len = 0;

  DBUG_EXECUTE_IF("send_brr_events_wait_before_loop", {
    const char act[] = "now WAIT_FOR continue_brr_before_loop";
    assert(!debug_sync_set_action(current_thd, STRING_WITH_LEN(act)));
  });

  while (it != m_brr_reader_list.end()) {
    Brr_reader *brr_reader = *it;
    my_off_t read_pos = brr_reader->position();
    it++;

    if (brr_reader->get_trx_ptr()->status == Brr_trx_status::ROLLBACK) {
      rollback_brr_transaction(brr_reader);
      continue;
    }

    if (!brr_reader->get_trx_ptr()->check_send_brr_ddl_event()) continue;

    DBUG_EXECUTE_IF("send_brr_events_wait_before_read", {
      const char act[] = "now WAIT_FOR continue_before_read_brr_event";
      assert(!debug_sync_set_action(current_thd, STRING_WITH_LEN(act)));
    });

    if (read_pos < brr_reader->get_trx_ptr()->end_position &&
        !brr_reader->event_exceed_max_send_size) {
      /*
        For DDL, Gtid_executed_log_event is already write to buffer, see
        function write_start_alter(). So we don't send Gtid_executed_log_event
        here.
      */
      if (!brr_reader->get_trx_ptr()->is_brr_ddl()) {
        /*
          Send new gtid_executed_event if we have finished send the group events
          that rely last gtid_executed_event to replay on replica.
        */
        if (brr_reader->last_gtid_executed_send_pos == 0 ||
            brr_reader->last_gtid_executed_send_pos == brr_reader->position())
          if (send_brr_gtid_executed_event(brr_reader)) return true;
      }

      if (brr_reader->read(&event_ptr, &event_len,
                           brr_reader->get_trx_ptr()->index)) {
        /*
          It is reading events before end_position of binlog cache tmp file.
          In theory, it should never return nullptr. But rollback doesn't check
          if there is any dump thread reading this file. So it is possible that
          binlog cache tmp file is reopened and truncated by rollback. In this
          situation, send the rollback brr message, remove and skip this reader.
        */
        assert(brr_reader->get_trx_ptr()->status == Brr_trx_status::ROLLBACK);

        LOG_BRR_WARNING(
            "Read brr event from binlog cache tmp file failed, rollback this "
            "brr_trx, brr status: %u, brr index: %llu, previous read position: "
            "%llu",
            (uint)brr_reader->get_trx_ptr()->status.load(),
            (ulonglong)brr_reader->get_trx_ptr()->index.load(), read_pos);
        rollback_brr_transaction(brr_reader);
        continue;
      }

      if (brr_reader->get_trx_ptr()->status == Brr_trx_status::ROLLBACK) {
        rollback_brr_transaction(brr_reader);
        continue;
      }

      /*
        If single event of this trx exceed the max_send_size, we will never send
        any event of this trx realtimely. This trx will be finished sending in
        find_and_finish_brr_transaction() after commit.
      */
      if (!brr_reader->get_trx_ptr()->is_brr_ddl() &&
          event_len > brr_trx_mngr.get_max_send_size()) {
        LOG_BRR_WARNING(
            "Send brr event failed, event length is too long, brr max send "
            "size: %llu, event length: %u, event position : %llu, brr index: "
            "%llu",
            brr_trx_mngr.get_max_send_size(), event_len, read_pos,
            (ulonglong)brr_reader->get_trx_ptr()->index.load());
        brr_reader->event_exceed_max_send_size = true;
        brr_reader->seek(read_pos);
        continue;
      }

      if (unlikely(m_sender->send_packet())) {
        LOG_BRR_ERROR(
            "Send brr event error, brr index: %llu, start send pos: %llu",
            (ulonglong)brr_reader->get_trx_ptr()->index.load(), read_pos);
        return true;
      }

      /*
        For brr trx of DDL type, the first event is Brr_gtid_executed_log_event,
        it's a brr event.
      */
      if (brr_reader->get_trx_ptr()->is_brr_ddl() && !read_pos)
        brr_reader->brr_events_sent++;
      else
        brr_reader->trx_content_events_sent++;
      source_sent_brr_events_count++;

      /*
        interrupt the realtime send if the total_event_len of current loop has
        exceeded the max_send_size.
      */
      total_event_len += event_len;
      if (total_event_len > brr_trx_mngr.get_max_send_size()) {
        source_brr_exceed_max_send_size_count++;
        break;
      }
    }
  }

  return false;
}

void get_brr_sender_trx_info_for_i_s(Brr_binlog_sender::Brr_reader *brr_reader,
                                     Brr_sender_trx_info_t *brr_info) {
  brr_info->dump_thread_id =
      brr_reader->get_binlog_sender()->get_dump_thread_id();
  brr_info->replica_server_id =
      brr_reader->get_binlog_sender()->get_replica_server_id();
  brr_info->brr_idx = brr_reader->get_trx_ptr()->index;
  brr_info->send_pos = brr_reader->position();
  brr_info->events_send = brr_reader->brr_events_sent;
  brr_info->trx_content_events_send = brr_reader->trx_content_events_sent;
}

extern void get_brr_source_trx_info_for_i_s(const Brr_trx *brr_trx,
                                            Brr_source_trx_info_t *brr_info) {
  brr_info->brr_thread_id = brr_trx->brr_thread_id;
  brr_info->index = brr_trx->index;
  strcpy(brr_info->type, brr_trx->is_brr_ddl() ? "DDL" : "LONG_TRANSACTION");
  brr_info->status = brr_trx->status;
  brr_info->start_read_pos = brr_trx->start_read_pos;
  brr_info->end_position = brr_trx->end_position;
  memcpy(brr_info->file_name, brr_trx->file_name, FN_REFLEN);
  brr_info->gno = brr_trx->gno;
}

bool Brr_ddl_write_guard::should_start_brr_ddl(THD *thd) {
  /*
    binlog_realtime_apply_ddl_source_enabled to be set to ON.

    DDL applied on a replica is not supported.

    Binlog must be enabled, and this DDL must be recorded in the
    binlog.
  */
  if (!binlog_realtime_apply_ddl_source_enabled || thd->slave_thread ||
      !mysql_bin_log.is_open() ||
      /*
        For optimize table, MySQL disable_binlog_guard is set to prevent binlog
        writes when execute mysql_recreate_table. However, for BRR, we need to
        send the optimize table statement in advance when mysql_recreate_table.
        Therefore, we make additional judgments here.
      */
      (!(thd->variables.option_bits & OPTION_BIN_LOG) &&
       !thd->get_rds_context().get_brr_optimize_query_str()) ||
      thd->lex->no_write_to_binlog) {
    return false;
  }

  return true;
}

/**
  Send a rollback event if a DDL execution fails.
*/
Brr_ddl_write_guard::~Brr_ddl_write_guard() {
  if (!m_ddl_res) return;

  /* We don't send rollback message, if we have not sent SA. */
  Brr_trx *brr_trx = m_thd->get_rds_context().get_brr_trx();
  if (!brr_trx) return;

  brr_trx_mngr.close_brr_trx(brr_trx, Brr_trx_status::ROLLBACK);

  /* Reset brr_source_index. */
  m_thd->get_rds_context().set_brr_trx(nullptr);

  LOG_BRR_MESSAGE("%s succeed, brr_index: %llu", __func__,
                  (ulonglong)brr_trx->index.load());
}

/**
  We transmit the DDL query_log_event through realtime transmit after
  we obtain the MDL X lock.
  We refer to this query_log_event as SA (start alter).

  @param    thd               The thread handle.
  @param    is_trans          If the event changes either
                              a trans or non-trans engine.
  @param    is_instant        Whether the DDL uses the instant algorithm.
*/
void Brr_ddl_write_guard::write_start_alter(THD *thd, bool is_trans,
                                            bool is_instant) {
  /* Non-transactional DDL is not supported. */
  if (!is_trans) return;

  /* Instant DDL is not supported. */
  if (is_instant) return;

  if (!should_start_brr_ddl(thd)) return;

  int end_pos = 0;
  Brr_trx *brr_trx = nullptr;

  const char *query_str = nullptr;
  size_t query_str_len = 0;

  if (thd->get_rds_context().get_brr_optimize_query_str()) {
    query_str = thd->get_rds_context().get_brr_optimize_query_str()->ptr();
    query_str_len =
        thd->get_rds_context().get_brr_optimize_query_str()->length();
  } else {
    query_str = thd->query().str;
    query_str_len = thd->query().length;
  }

  /* Construct SA. */
  Query_log_event query_event(thd, query_str, query_str_len, is_trans, false,
                              false, 0);
  uint64 query_event_length =
      MAX_LOG_EVENT_HEADER + query_event.db_len + query_str_len;

  /* Construct Gtid_executed_log_event. */
  Brr_gtid_executed_log_event brr_gtid_executed_event(thd, true);
  size_t gtid_executed_event_length = brr_gtid_executed_event.get_data_size();

  size_t reserved_size = query_event_length + gtid_executed_event_length;

  uchar *buf = new uchar[reserved_size];
  if (!buf) {
    LOG_BRR_ERROR("%s, allocate buffer failed.", __func__);
    return;
  }

  Buffer_ostream writer(buf, reserved_size);

  /* Write CA and Gtid_executed_log_event.*/
  if (brr_gtid_executed_event.write(&writer) || query_event.write(&writer)) {
    LOG_BRR_ERROR(
        "%s, write Brr_gtid_executed_log_event or Query_log_event failed.",
        __func__);
    delete[] buf;
    return;
  }

  end_pos = writer.position;

  brr_trx = brr_trx_mngr.open_brr_trx(nullptr, end_pos, 0, buf);

  if (!brr_trx) {
    LOG_BRR_ERROR("%s, create Brr_trx failed.", __func__);
    delete[] buf;
    return;
  }

  /* We set this variable to indicate this a brr DDL. */
  thd->get_rds_context().set_brr_trx(brr_trx);
  LOG_BRR_MESSAGE("%s succeed, brr_index: %llu", __func__,
                  (ulonglong)brr_trx->index.load());
  return;
}