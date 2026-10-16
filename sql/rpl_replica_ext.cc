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
#include "sql/auth/sql_auth_cache.h"
#include "sql/binlog_ext.h"
#include "sql/rpl_replica.h"
#include "sql/sys_vars.h"

const char *RDS_BRR_VERSION = "3";
bool binlog_realtime_transmit_enable_log_message = false;
uint binlog_realtime_apply_worker_heartbeat_period =
    DEFAULT_REALTIME_APPLY_WORKER_HEARTBEAT_PERIOD;
ulong binlog_realtime_apply_workers = DEFAULT_REALTIME_APPLY_WORKERS;
Brr_rpl_info brr_rpl_info;

static PSI_memory_key key_memory_brr_replica;

static PSI_mutex_key key_brr_rpl_info_data_lock;

static PSI_cond_key key_brr_rpl_info_data_cond;

static PSI_stage_info stage_wait_brr_trx = {0, "Waiting for brr transaction", 0,
                                            PSI_DOCUMENT_ME};
static PSI_stage_info stage_wait_for_brr_event = {0, "Waiting for brr events",
                                                  0, PSI_DOCUMENT_ME};
static PSI_stage_info stage_wait_dependent_trx = {
    0, "Brr worker Waiting for dependent transactions to commit", 0,
    PSI_DOCUMENT_ME};

void init_brr_replica_psi_keys() {
#ifdef HAVE_PSI_INTERFACE
  const char *category = "sql";
  int count;

  static PSI_memory_info all_memories[] = {{&key_memory_brr_replica,
                                            "binlog_realtime_transmit_replica",
                                            0, 0, PSI_DOCUMENT_ME}};

  static PSI_mutex_info all_mutexs[] = {{&key_brr_rpl_info_data_lock,
                                         "Brr_rpl_info::m_data_lock", 0, 0,
                                         PSI_DOCUMENT_ME}};

  static PSI_cond_info all_conds[] = {{&key_brr_rpl_info_data_cond,
                                       "Brr_rpl_info::m_data_cond", 0, 0,
                                       PSI_DOCUMENT_ME}};

  static PSI_stage_info *all_stages[] = {&stage_wait_brr_trx,
                                         &stage_wait_for_brr_event,
                                         &stage_wait_dependent_trx};

  count = static_cast<int>(array_elements(all_memories));
  mysql_memory_register(category, all_memories, count);

  count = static_cast<int>(array_elements(all_mutexs));
  mysql_mutex_register(category, all_mutexs, count);

  count = static_cast<int>(array_elements(all_conds));
  mysql_cond_register(category, all_conds, count);

  count = static_cast<int>(array_elements(all_stages));
  mysql_stage_register(category, all_stages, count);
#endif
}

bool Brr_cache::open(my_off_t cache_size, ulonglong brr_index,
                     ulonglong reserved_size, ulonglong brr_io_version) {
  m_brr_index = brr_index;
  m_file_reserved_size = reserved_size;

  if (m_file.open(nullptr, "RL", cache_size, ~(my_off_t)0)) {
    LOG_BRR_ERROR("failed to open brr cache, brr index: %llu", brr_index);
    return true;
  }

  IO_CACHE *info = m_file.get_io_cache();
  reinit_io_cache(info, WRITE_CACHE, m_file_reserved_size, false, true);
  info->end_of_file = m_file.get_max_cache_size();

  char name_buff[FN_REFLEN];
  size_t length;
  dirname_part(name_buff, log_bin_basename, &length);

  sprintf(name_buff + length, "%s/%s_%s_%llu_%llu", BINLOG_CACHE_DIR,
          m_file.get_io_cache()->prefix, RELAY_LOG_CACHE_NAME, brr_io_version,
          m_brr_index.load());

  if ((info->file = mysql_file_open(info->file_key, name_buff, O_CREAT | O_RDWR,
                                    MYF(MY_WME))) < 0) {
    LOG_BRR_ERROR("failed to open brr cache tmp file %s, brr index: %llu",
                  name_buff, brr_index);
    return true;
  }

  /*
    We update the brr_cache_end_pos here, so that we don't read the reserved
    space.
  */
  m_brr_cache_end_pos = m_file.length();
  LOG_BRR_MESSAGE(
      "brr cache opened, brr index: %llu, brr cache tmp file: "
      "%s, reserved size: %llu",
      brr_index, name_buff, reserved_size);
  return false;
}

bool Brr_cache::flush_and_sync() {
  if (my_b_flush_io_cache(m_file.get_io_cache(), 1)) return true;
  if (mysql_file_sync(m_file.get_io_cache()->file, MYF(MY_WME))) return true;
  return false;
}

void Brr_cache::detach_temp_file() {
  IO_CACHE *info = m_file.get_io_cache();

  /* Close file. */
  mysql_file_close(info->file, MYF(0));

  /* Reset tmp file fd for binlog cache. */
  info->file = -1;
}

void Brr_cache::close() {
  /* unlink file. */
  if (m_file.get_io_cache()->file != -1) unlink(tmp_file_name());
  m_file.close();
}

void Brr_cache::update_end_pos(bool need_lock) {
  if (need_lock)
    mysql_mutex_lock(&brr_rpl_info.m_data_lock);
  else
    mysql_mutex_assert_owner(&brr_rpl_info.m_data_lock);

  m_brr_cache_end_pos = m_file.length();
  mysql_cond_broadcast(&brr_rpl_info.m_data_cond);

  if (need_lock) mysql_mutex_unlock(&brr_rpl_info.m_data_lock);
}

void Brr_rpl_info::init_pthread_objects() {
  mysql_mutex_init(key_brr_rpl_info_data_lock, &m_data_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_brr_rpl_info_data_cond, &m_data_cond);

  init_brr_replica_psi_keys();
}

void Brr_rpl_info::cleanup() {
  mysql_mutex_destroy(&m_data_lock);
  mysql_cond_destroy(&m_data_cond);
}

void Brr_rpl_info::clear_realtime_transmit(Master_info *mi) {
  MUTEX_LOCK(guard, &m_data_lock);

  auto it = m_cache_map.begin();
  while (it != m_cache_map.end()) {
    Brr_cache *brr_cache = it->second;
    handle_brr_rollback(brr_cache, false);
    it++;
  }
  mysql_cond_broadcast(&m_data_cond);

  m_cache_map.clear();
  mi->m_realtime_transmit_enabled = false;
  replica_brr_io_thread_count--;

  if (m_description_event) {
    delete m_description_event;
    m_description_event = nullptr;
  }
  
  /* Increate m_brr_io_version. */
  LOG_BRR_MESSAGE("Brr IO thread finished");
  m_brr_io_version++;
}

Brr_cache *Brr_rpl_info::get_cache_by_index(ulonglong index,
                                            ulong reserved_size,
                                            Brr_magic_type brr_type,
                                            bool using_realtime_apply) {
  {
    MUTEX_LOCK(guard, &m_data_lock);
    auto it = m_cache_map.find(index);
    if (it != m_cache_map.end()) return it->second;
  }

  assert(brr_type == Brr_magic_type::BRR_DDL_BEGIN_EVENT ||
         brr_type == Brr_magic_type::BRR_LONG_TRX_BEGIN_EVENT);

  Brr_cache *cache = (Brr_cache *)my_malloc(
      key_memory_brr_replica, sizeof(Brr_cache), MYF(MY_ZEROFILL));
  if (!cache) {
    LOG_BRR_ERROR("brr cache memory allocated fail brr index: %llu", index);
    return nullptr;  // Didn't manage to set it up
  }
  cache = new (cache) Brr_cache;
  if (cache->open(IO_SIZE, index, reserved_size, m_brr_io_version)) {
    cache->~Brr_cache();
    my_free(cache);
    return nullptr;
  }

  cache->set_brr_type(brr_type);

  if (!using_realtime_apply) {
    assert(brr_type == Brr_magic_type::BRR_LONG_TRX_BEGIN_EVENT);
    cache->realtime_apply_enabled = false;
    LOG_BRR_MESSAGE("Trx not using realtime apply brr index: %llu", index);
  }

  replica_brr_cache_count++;

  {
    MUTEX_LOCK(guard, &m_data_lock);

    m_cache_map[index] = cache;

    if (cache->realtime_apply_enabled) m_trx_to_be_assigned++;

    mysql_cond_broadcast(&m_data_cond);
  }

  return cache;
}

void Brr_rpl_info::remove_cache(Brr_cache *brr_cache, rpl_gno gno,
                                bool clean_map) {
  mysql_mutex_assert_owner(&m_data_lock);

  ulonglong brr_index = brr_cache->get_brr_index();
  bool is_old = true;

  if (m_cache_map.find(brr_index) != m_cache_map.end()) {
    Brr_cache *brr_cache_in_map = m_cache_map[brr_index];

    /*
      TODO: We will refactor this function in subsequent versions to give full
      ownership of the brr map to the IO thread.
    */

    /*
      When a brr worker thread acquires a brr cache, it owns the cache. When the
      I/O thread exits, it clears m_cache_map and sets KILL_QUERY to notify the
      brr worker thread to roll back the transaction and destroy the brr cache.
      The I/O thread and the BRR worker lack synchronization. When the I/O
      thread reconnects, it may receive a brr transaction with the same index
      (the brr transaction has not been committed on the master), create a brr
      cache, and add it to m_cache_map. Although the old and new brr caches have
      the same index, their pointers are different. Here, we judge the pointer
      to prevent the brr worker holding the old brr cache from mistakenly
      deleting the new brr cache.
    */
    if (brr_cache_in_map == brr_cache) {
      is_old = false;
      if (clean_map) m_cache_map.erase(brr_index);
    }
  }

  LOG_BRR_MESSAGE(
      "brr cache with gno: %ld, brr index: %llu, trx content events received: "
      "%llu, brr events received: %llu, is_old: %d, is_complete: %d, "
      "is_assigned: %d",
      gno, brr_index, brr_cache->trx_content_events_received,
      brr_cache->brr_events_received, is_old, brr_cache->is_complete(),
      brr_cache->is_assigned());

  brr_cache->~Brr_cache();
  my_free(brr_cache);
  brr_cache = nullptr;
  replica_brr_cache_count--;
}

bool Brr_rpl_info::handle_gtid_event(Master_info *mi, Brr_cache *brr_cache,
                                     const char *event_buf, ulong event_len) {
  my_off_t end_pos = 0;
  my_off_t gtid_start_pos = 0;
  char gtid_str[Gtid::MAX_TEXT_LENGTH + 1];

  if (brr_cache->get_brr_type() == Brr_trx_type::BRR_LONG_TRX) {
    replica_brr_long_trx_need_commit++;
    brr_cache->flush_and_sync();
    /*
      Write gtid event to the reserve space of tmp file before the first
      event.
    */
    end_pos = brr_cache->get_file_end_pos();

    gtid_start_pos = brr_cache->get_file_reserved_size() - event_len;

    if (brr_cache->file_seek(gtid_start_pos) ||
        brr_cache->write((const uchar *)event_buf, event_len)) {
      return true;
    }
    brr_cache->flush_and_sync();
  } else {
    assert(brr_cache->get_brr_type() == Brr_trx_type::BRR_DDL);
    replica_brr_ddl_need_commit++;
    /*
      For DDL-type brr trx, we write the Gtid_log_event at the end of the brr
      cache file. Because source transmits two sets of binlog events: one set
      brr events and one set normal binlong event. Therefore, we don't need
      to convert the brr cache file into a real log file. Secondly, writing
      the  Gtid_log_event at the end of the file makes it easier to process,
      eliminating the need for the brr worker's file reader to perform seek
      operations.
    */
    if (brr_cache->write((const uchar *)event_buf, event_len)) return true;
  }

  /* Set gtid info for brr_cache. */
  Gtid_log_event gtid_ev(event_buf, get_description_event());
  if (!gtid_ev.is_valid()) return true;

  /* Check this trx is not received from IO thread before. */
  mi->rli->get_sid_lock()->rdlock();
  Gtid gtid;
  gtid.sidno = gtid_ev.get_sidno(mi->rli->get_sid_map());
  gtid.gno = gtid_ev.get_gno();
  gtid.to_string(mi->rli->get_sid_map(), gtid_str);
  if (mi->rli->get_gtid_set()->contains_gtid(gtid)) {
    LOG_BRR_ERROR(
        "brr trx has the same gtid with normal trx, brr index: "
        "%llu, gtid: %s",
        brr_cache->get_brr_index(), gtid_str);
    mi->rli->get_sid_lock()->unlock();
    return true;
  }
  mi->rli->get_sid_lock()->unlock();

  MUTEX_LOCK(guard, &m_data_lock);

  /*
    The detach_temp_file` operation should be performed under lock
    protection to prevent the brr worker from reading the brr_cache
    but being unable to open the temp file.
  */
  if (brr_cache->get_brr_type() == Brr_trx_type::BRR_LONG_TRX) {
    if (mi->rli->relay_log.rename_tmp_to_relay(
            mi, brr_cache->tmp_file_name(), gtid_start_pos, end_pos, &gtid_ev))
      return true;

    brr_cache->detach_temp_file();
  }

  brr_cache->set_complete();

  if (brr_cache->get_brr_worker()) {
    THD *thd = brr_cache->get_brr_worker()->info_thd;
    assert(thd);

    /*
      The brr worker acquire the lock of gtid, so when worker thread
      get the normal Gtid_log_event, it will wait the brr worker
      release the ownership of gtid reference set_gtid_next. And worker
      thread will skip the transaction reference gtid_pre_statement_checks.
    */
    global_sid_lock->rdlock();

    const Gtid_specification &spec = gtid_ev.get_spec();

    assert(spec.type == ASSIGNED_GTID);
    if (gtid_ev.get_sidno(false) <
            0 /* make sure that sid has been converted to sidno */
        || brr_acquire_gtid_ownership(brr_cache->get_brr_index(),
                                      thd->thread_id(), spec)) {
      LOG_BRR_ERROR("acquire gtid ownership failed,  brr index: %llu, gtid: %s",
                    brr_cache->get_brr_index(), gtid_str);
      handle_brr_rollback(brr_cache);
    }
    global_sid_lock->unlock();

    LOG_BRR_MESSAGE("acquire gtid ownership succeed, brr index: %llu, gtid: %s",
                    brr_cache->get_brr_index(), gtid_str);

    brr_cache->set_brr_gtid_spec(spec);

    /*
      After set_gtid_next, notify the brr worker to end the wait, so as to
      avoid the situation where the brr worker thread does not have a gtid
      when the transaction is committed and cause normal worker to reply the
      transaction again.
    */
    brr_cache->flush_and_update_pos(false);
  } else {
    /*
      When we received Gtid_log_event, if this brr trx has not been applied
      by brr worker, or the brr worker has failed to apply it before. Then
      this brr trx is applied by normal worker.

      For the second case, because the brr worker has already decremented
      m_trx_to_be_assigned in get_cache_for_worker, it is no longer decremented
      here.
    */

    if (!brr_cache->is_assigned()) {
      if (brr_cache->realtime_apply_enabled) m_trx_to_be_assigned--;
      replica_brr_trx_not_assigned_to_brr_worker++;
      LOG_BRR_MESSAGE(
          "brr cache not assigned to brr worker until receive Gtid_log_event, "
          "brr index: %llu, brr_type: %s, gtid: %s",
          brr_cache->get_brr_index(),
          brr_cache->get_brr_type() == Brr_trx_type::BRR_DDL
              ? "DDL"
              : "LONG_TRANSACTION",
          gtid_str);
    }

    remove_cache(brr_cache, gtid.gno);
  }

  return false;
}

bool Brr_rpl_info::handle_format_description_event(const char *event_buf,
                                                   ulong event_len) {
  /* Process brr format description event. */
  Log_event *ev = nullptr;
  Format_description_log_event fde;

  if (binlog_event_deserialize(
          reinterpret_cast<const unsigned char *>(event_buf), event_len, &fde,
          true, &ev) != Binlog_read_error::SUCCESS) {
    LOG_BRR_ERROR("failed to decode brr format description event.");
    return true;
  }

  Format_description_log_event *fdle =
      dynamic_cast<Format_description_log_event *>(ev);
  assert(fdle->common_footer->checksum_alg ==
         binary_log::BINLOG_CHECKSUM_ALG_OFF);
  set_description_event(fdle);

  return false;
}

bool Brr_rpl_info::queue_brr_event(Master_info *mi, const char *event_buf,
                                   ulong event_len) {
  assert(event_buf[0] == BRR_MAGIC_NUM);
  Brr_magic_type brr_type = (Brr_magic_type)event_buf[1];

  event_buf += BRR_HEADER_MAGIC_SIZE;
  event_len -= BRR_HEADER_MAGIC_SIZE;

  ulonglong index = uint8korr(event_buf);
  event_buf += BRR_HEADER_INDEX_SIZE;
  event_len -= BRR_HEADER_INDEX_SIZE;

  replica_received_brr_events_count++;

  Log_event_type event_type = (Log_event_type)event_buf[EVENT_TYPE_OFFSET];

  if (event_type == binary_log::FORMAT_DESCRIPTION_EVENT) {
    assert(index == 0);
    return handle_format_description_event(event_buf, event_len);
  } else if (m_description_event == nullptr) {
    LOG_BRR_ERROR(
        "format description event is not initialized, replica interrupt.");
    return true;
  }

  bool is_gtid_executed_log_ev = false;
  if (event_type == binary_log::IGNORABLE_LOG_EVENT &&
      (uint2korr(event_buf + FLAGS_OFFSET) & LOG_EVENT_RDS_F))
    is_gtid_executed_log_ev = true;

  /*
    The first event of long trx using realtime apply is the
    Gtid_executed_log_event, otherwise not.
  */
  Brr_cache *brr_cache =
      get_cache_by_index(index, mi->rli->relay_log.relay_log_reserved_size,
                         brr_type, is_gtid_executed_log_ev);

  if (brr_cache == nullptr) {
    LOG_BRR_MESSAGE("%s, brr_cache not found, index is %llu", __func__, index);
    return true;
  }

  assert(index != 0);

  switch (brr_type) {
    case Brr_magic_type::BRR_ROLLBACK_EVENT: {
      assert(event_type == binary_log::IGNORABLE_LOG_EVENT);
      brr_cache->brr_events_received++;
      if (brr_cache->get_brr_type() == Brr_trx_type::BRR_DDL)
        replica_brr_ddl_need_rollback++;
      if (brr_cache->get_brr_type() == Brr_trx_type::BRR_LONG_TRX)
        replica_brr_long_trx_need_rollback++;
      MUTEX_LOCK(guard, &m_data_lock);
      handle_brr_rollback(brr_cache);
      mysql_cond_broadcast(&m_data_cond);
      return false;
    }
    default: {
      assert((brr_type == Brr_magic_type::BRR_LONG_TRX_EVENT) ||
             (brr_type == Brr_magic_type::BRR_LONG_TRX_BEGIN_EVENT) ||
             (brr_type == Brr_magic_type::BRR_DDL_EVENT) ||
             (brr_type == Brr_magic_type::BRR_DDL_BEGIN_EVENT));

      /* Brr_gtid_executed_log_event is not considered a content event. */
      if (event_type != binary_log::IGNORABLE_LOG_EVENT)
        brr_cache->trx_content_events_received++;
      else
        brr_cache->brr_events_received++;

      if (event_type == binary_log::GTID_LOG_EVENT)
        return handle_gtid_event(mi, brr_cache, event_buf, event_len);

      if (brr_cache->write((const uchar *)event_buf, event_len)) return true;

      /*
        For brr DDL, we need to flush and update pos, so that brr worker will
        read and apply it.
      */
      if (brr_cache->get_brr_type() == Brr_trx_type::BRR_DDL &&
          event_type == binary_log::QUERY_EVENT)
        return brr_cache->flush_and_update_pos(true);

      /*
        For brr long transaction, we need to flush and update pos, when the
        event we receive is not Gtid_executed_log_event.
      */
      if (brr_cache->get_brr_type() == Brr_trx_type::BRR_LONG_TRX &&
          !is_gtid_executed_log_ev && brr_cache->realtime_apply_enabled)
        return brr_cache->flush_and_update_pos(true);
    }
  }
  return false;
}

void Brr_rpl_info::handle_brr_rollback(Brr_cache *brr_cache, bool clean_map) {
  mysql_mutex_assert_owner(&m_data_lock);
  brr_cache->set_complete();
  Relay_log_info *brr_worker = brr_cache->get_brr_worker();

  if (brr_worker) {
    THD *worker_thd = brr_worker->info_thd;
    assert(worker_thd);
    mysql_mutex_lock(&worker_thd->LOCK_thd_data);
    worker_thd->awake(THD::KILL_QUERY);
    worker_thd->rli_slave->brr_abort_query = true;
    mysql_mutex_unlock(&worker_thd->LOCK_thd_data);
  } else {
    if (brr_cache->realtime_apply_enabled) m_trx_to_be_assigned--;
    remove_cache(brr_cache, 0, clean_map);
  }
}

Brr_cache *Brr_rpl_info::get_cache_for_worker(THD *thd) {
  MUTEX_LOCK(guard, &m_data_lock);

  THD_STAGE_INFO(thd, stage_wait_brr_trx);

  while (!m_trx_to_be_assigned && !brr_worker_interrupted(thd)) {
    /*
      Wake up at a fixed time to check if thd->killed is set. We don't use
      thd->ENTER_COND, because we will set current_mutex as m_data_lock there
      and release m_data_lock when we wait. In handle_brr_rollback, we will
      lock m_data_lock and call THD::awake, in THD::awake, we will lock
      current_mutex which is m_data_lock, then the problem arises at this time.
    */
    wait_for_update(false);
  }

  auto it = m_cache_map.begin();
  while (!brr_worker_interrupted(thd) && it != m_cache_map.end()) {
    Brr_cache *brr_cache = it->second;

    if (brr_cache->is_assigned() || !brr_cache->realtime_apply_enabled) {
      it++;
      continue;
    }

    assert(thd->rli_slave);
    brr_cache->set_brr_worker(thd->rli_slave);
    brr_cache->set_assigned();

    /*
      Initialize the binlog file reader so that the brr worker can read
      and apply the binlog event of the brr transaction.
    */
    Relaylog_file_reader *reader = new Relaylog_file_reader(false);
    brr_cache->set_file_reader(reader);

    reader->open(brr_cache->tmp_file_name());
    reader->set_format_description_event(*m_description_event);
    reader->seek(brr_cache->get_file_reserved_size());

    thd->rli_slave->set_brr_cache(brr_cache);

    assert(m_trx_to_be_assigned > 0);
    m_trx_to_be_assigned--;
    replica_brr_trx_assigned_to_brr_worker++;
    return brr_cache;
  }

  return nullptr;
}

Log_event *Brr_rpl_info::read_next_event(THD *thd) {
  Brr_cache *brr_cache = thd->rli_slave->get_brr_cache();
  assert(brr_cache);
  Relaylog_file_reader *brr_file_reader = brr_cache->get_file_reader();
  assert(brr_file_reader);

  PSI_stage_info old_stage;

  thd->enter_stage(&stage_wait_for_brr_event, &old_stage, __func__, __FILE__,
                   __LINE__);

  while (!brr_worker_interrupted(thd) &&
         brr_file_reader->position() == brr_cache->get_readable_pos()) {
    wait_for_update(true);
  }

  THD_STAGE_INFO(thd, old_stage);

  if (brr_worker_interrupted(thd)) return nullptr;

  Log_event *ev = brr_file_reader->read_event_object();

  if (!ev) {
    LOG_BRR_ERROR("%s, read event failed.", __func__);
    return nullptr;
  }

  if (unlikely(ev->get_type_code() == binary_log::IGNORABLE_LOG_EVENT) &&
      (ev->common_header->flags & LOG_EVENT_RDS_F)) {
    Brr_gtid_executed_log_event *brr_gtid_executed_ev =
        dynamic_cast<Brr_gtid_executed_log_event *>(ev);
    uchar *data_buf = brr_gtid_executed_ev->get_data_buf();
    ulonglong data_size = brr_gtid_executed_ev->get_data_size();

    char *gtid_string = nullptr;
    Sid_map sid_map(nullptr);
    Gtid_set set(&sid_map, nullptr);
    if (set.add_gtid_encoding(reinterpret_cast<const uchar *>(data_buf),
                              data_size) != RETURN_STATUS_OK)
      return nullptr;

    set.to_string(&gtid_string);

    DBUG_EXECUTE_IF("simulate_long_gtid_executed", {
      std::string s(LOG_BUFF_MAX, 'a');
      LOG_BRR_MESSAGE("%s, long_gtid_executed, gtid: %s.", __func__, s.c_str());
    };);

    LOG_BRR_MESSAGE("%s, read Brr_gtid_executed_log_event, gtid: %s.", __func__,
                    gtid_string);

    my_free(gtid_string);

    thd->enter_stage(&stage_wait_dependent_trx, &old_stage, __func__, __FILE__,
                     __LINE__);
    while (!brr_worker_interrupted(thd) && !check_gtid_executed(&set)) {
      /* TODO: We will using condition to signal, not just sleep. */
      std::this_thread::sleep_for(std::chrono::milliseconds(
          binlog_realtime_apply_worker_heartbeat_period));
    }
    THD_STAGE_INFO(thd, old_stage);

    delete ev;
    if (!brr_worker_interrupted(thd))
      return read_next_event(thd);
    else
      return nullptr;
  }

  return ev;
}

bool Brr_rpl_info::check_gtid_executed(Gtid_set *gtid_executed) {
  global_sid_lock->wrlock();
  if (gtid_executed->is_subset(gtid_state->get_executed_gtids())) {
    global_sid_lock->unlock();
    return true;
  }
  global_sid_lock->unlock();
  return false;
}

void Brr_rpl_info::cleanup_brr_trx(THD *thd, int error) {
  DBUG_EXECUTE_IF("brr_worker_wait_clean_up", {
    /* clear_error to avoid debug_sync_set_action return true */
    thd->clear_error();
    const char act[] = "now WAIT_FOR brr_worker_continue";
    assert(opt_debug_sync_timeout > 0);
    assert(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
  };);

  MUTEX_LOCK(guard, &m_data_lock);

  Brr_cache *brr_cache = thd->rli_slave->get_brr_cache();

  if (!brr_cache) return;

  Gtid brr_gtid = brr_cache->get_brr_gtid_spec().gtid;

  ulonglong brr_index = brr_cache->get_brr_index();

  if (error) {
    /*
      Check and roll back Gtid here to avoid the situation where the IO thread
      has called brr_acquire_gtid_ownership but the brr worker exits before
      calling brr_set_gtid_var.
    */
    if (brr_gtid.gno > 0 &&
        brr_set_gtid_var(thd, brr_cache->get_brr_gtid_spec())) {
      gtid_state->update_on_rollback(thd);
      LOG_BRR_MESSAGE("%s, brr index: %llu, gno: %ld, rollback gtid", __func__,
                      brr_index, brr_gtid.gno);
    }
  }

  /* Close file of brr cache. */
  Relaylog_file_reader *brr_file_reader = brr_cache->get_file_reader();
  brr_file_reader->close();
  delete brr_file_reader;
  brr_cache->set_file_reader(nullptr);
  brr_cache->set_brr_worker(nullptr);

  /*
    The BRR worker destroys the BRR cache only when it is complete.

    The IO thread and worker thread have no synchronization mechanism. This
    prevents the IO thread from accidentally accessing the destroyed brr cache.
  */
  if (brr_cache->is_complete()) remove_cache(brr_cache, brr_gtid.gno);
  thd->rli_slave->set_brr_cache(nullptr);
  LOG_BRR_MESSAGE("%s remove brr_cache index: %llu, gno: %ld", __func__,
                  brr_index, brr_gtid.gno);
}

bool Brr_rpl_info::wait_for_gtid_event(THD *thd) {
  bool ret = false;

  if (!thd->rli_slave || !thd->rli_slave->get_brr_cache()) return ret;

  Log_event *ev = nullptr;

  Brr_cache *brr_cache = thd->rli_slave->get_brr_cache();
  ulonglong brr_index = brr_cache->get_brr_index();

  LOG_BRR_MESSAGE("%s, brr index: %llu start waiting", __func__, brr_index);

  ev = read_next_event(thd);

  if (ev) {
    assert(ev->get_type_code() == binary_log::GTID_LOG_EVENT);

    brr_set_gtid_var(thd, brr_cache->get_brr_gtid_spec());

    delete ev;
    ev = nullptr;
  } else {
    ret = true;
  }

  LOG_BRR_MESSAGE("%s, brr index: %llu, get %s signal", __func__, brr_index,
                  ret ? "rollback" : "commit");

  DBUG_EXECUTE_IF("simulate_error_after_brr_set_gtid_var", ret = true;);

  return ret;
}

bool Brr_rpl_info::wait_for_gtid_spec(THD *thd) {
  Brr_cache *brr_cache = thd->rli_slave->get_brr_cache();

  PSI_stage_info old_stage;

  thd->enter_stage(&stage_wait_for_brr_event, &old_stage, __func__, __FILE__,
                   __LINE__);

  while (!brr_worker_interrupted(thd) &&
         brr_cache->get_brr_gtid_spec().gtid.gno == 0) {
    wait_for_update(true);
  }

  THD_STAGE_INFO(thd, old_stage);

  return brr_worker_interrupted(thd);
}

bool Brr_rpl_info::brr_worker_interrupted(THD *thd) {
  assert(thd->rli_slave);
  return thd->killed || thd->rli_slave->brr_abort_running ||
         thd->rli_slave->brr_abort_query;
}

bool Brr_rpl_info::brr_acquire_gtid_ownership(ulonglong brr_index,
                                              my_thread_id thread_id,
                                              const Gtid_specification &spec) {
  assert(spec.type == ASSIGNED_GTID && spec.gtid.sidno >= 1 &&
         spec.gtid.gno >= 1 && spec.gtid.gno < GNO_END);

  global_sid_lock->assert_some_lock();

  if (global_gtid_mode.get() == Gtid_mode::OFF) {
    LOG_BRR_ERROR("%s failed because gtid_mode is off, brr index: %llu",
                  __func__, brr_index);
    return true;
  }

  int ret = false;

  /* acquire lock before checking conditions */
  gtid_state->lock_sidno(spec.gtid.sidno);
  if (gtid_state->is_executed(spec.gtid)) {
    LOG_BRR_ERROR("%s failed because gtid is in gtid_executed, brr index: %llu",
                  __func__, brr_index);
    ret = true;
  } else {
    if (!gtid_state->is_owned(spec.gtid)) {
      if (const_cast<Owned_gtids *>(gtid_state->get_owned_gtids())
              ->add_gtid_owner(spec.gtid, thread_id) != RETURN_STATUS_OK) {
        LOG_BRR_ERROR(
            "%s failed because add_gtid_owner failed, brr index: %llu",
            __func__, brr_index);
        ret = true;
      }
    } else {
      LOG_BRR_ERROR(
          "%s failed because gtid is owner by other thead, brr index: %llu",
          __func__, brr_index);
      ret = true;
    }
  }

  gtid_state->unlock_sidno(spec.gtid.sidno);
  return ret;
}

bool Brr_rpl_info::brr_set_gtid_var(THD *thd, const Gtid_specification &spec) {
  bool ret = false;
  global_sid_lock->rdlock();
  gtid_state->lock_sidno(spec.gtid.sidno);

  if (gtid_state->get_owned_gtids()->is_owned_by(spec.gtid, thd->thread_id())) {
    thd->owned_gtid = spec.gtid;
    thd->owned_gtid.dbug_print(nullptr, "set owned_gtid in brr_set_gtid_var");
    thd->owned_sid = global_sid_map->sidno_to_sid(spec.gtid.sidno);
    thd->rpl_thd_ctx.last_used_gtid_tracker_ctx().set_last_used_gtid(spec.gtid);
    thd->variables.gtid_next = spec;
    ret = true;
  }

  gtid_state->unlock_sidno(spec.gtid.sidno);
  global_sid_lock->unlock();
  return ret;
}

void Brr_rpl_info::get_brr_replica_info_for_i_s(
    std::list<Brr_replica_info_t> &brr_info_list) {
  MUTEX_LOCK(guard, &m_data_lock);
  auto it = m_cache_map.begin();
  while (it != m_cache_map.end()) {
    Brr_cache *brr_cache = it->second;
    Brr_replica_info_t brr_replica_info;

    brr_replica_info.index = brr_cache->get_brr_index();

    strcpy(brr_replica_info.type,
           brr_cache->get_brr_type() == Brr_trx_type::BRR_DDL
               ? "DDL"
               : "LONG_TRANSACTION");
    strcpy(brr_replica_info.file_name, brr_cache->tmp_file_name());

    brr_replica_info.gno = brr_cache->get_brr_gtid_spec().gtid.gno;
    brr_replica_info.received_pos = brr_cache->get_file_end_pos();
    brr_replica_info.brr_events_received = brr_cache->brr_events_received;
    brr_replica_info.trx_content_events_received =
        brr_cache->trx_content_events_received;
    brr_replica_info.trx_complete = brr_cache->is_complete();
    brr_replica_info.assigned_to_brr_worker = brr_cache->is_assigned();
    brr_replica_info.apply_pos = brr_cache->get_file_reader()
                                     ? brr_cache->get_file_reader()->position()
                                     : 0;

    brr_info_list.push_back(brr_replica_info);
    it++;
  }
}