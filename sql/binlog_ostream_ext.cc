/* Copyright (c) 2008, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#include "binlog_ostream.h"
#include "mysys/mysys_priv.h"
#include "sql/binlog.h"
#include "sql/binlog_ext.h"

RDS_binlog_cache_storage::RDS_binlog_cache_storage()
    : m_trx_flag(false), m_enabled(false), m_file_reserved_size(0) {}

RDS_binlog_cache_storage::~RDS_binlog_cache_storage() { close(); }

bool RDS_binlog_cache_storage::open(const char *dir, const char *prefix,
                                    my_off_t cache_size,
                                    my_off_t max_cache_size,
                                    bool trx_flag_arg) {
  m_trx_flag = trx_flag_arg;

  return m_file.open(dir, prefix, cache_size, max_cache_size);
}

void RDS_binlog_cache_storage::close() {
  /*
    The m_file.close() only close tmp file, need to unlink tmp file if this
    cache is enabled for free flush.
  */
  if (m_enabled && m_file.get_io_cache()->file != -1) {
    unlink(tmp_file_name());
  }

  m_file.close();
}

void RDS_binlog_cache_storage::init_file_reserved_size() {
  bool should_enabled = (m_trx_flag && binlog_cache_free_flush.is_enabled());
  m_file_reserved_size =
      should_enabled ? binlog_cache_free_flush.get_reserved_size() : 0;

  /*
    Aligned reserved_size with IO_SIZE, to avoid cache buffer will reduce in
    reinit_io_cache().
  */
  if (m_file_reserved_size % IO_SIZE > 0) {
    m_file_reserved_size =
        m_file_reserved_size - (m_file_reserved_size % IO_SIZE) + IO_SIZE;
  }

  DBUG_EXECUTE_IF("simulate_small_rds_binlog_cache_reserved_space",
                  m_file_reserved_size = 100;);

  /* Need to close the tmp file when the free flush is switched. */
  if (should_enabled != m_enabled) {
    if (m_file.get_io_cache()->file != -1) {
      /* Need to unlink the KEEP tmp file when switch off the free flush. */
      if (m_enabled) {
        unlink(tmp_file_name());
      }

      mysql_file_close(m_file.get_io_cache()->file, MYF(0));
      m_file.get_io_cache()->file = -1;
    }

    m_enabled = should_enabled;
  }

  /*
    Call reinit_io_cache() to seek the reserve space in the front of tmp file.

    This function will set pos_in_file to m_file_reserved_size, and set
    seek_not_done to true.

    The tmp file will be created when the buffer is full, and will be sought to
    pos_in_file before writing into it.
  */
  if (m_file_reserved_size != 0) {
    IO_CACHE *info = m_file.get_io_cache();
    reinit_io_cache(info, WRITE_CACHE, m_file_reserved_size, false, true);
    info->end_of_file = m_file.get_max_cache_size();
  }
}

my_off_t RDS_binlog_cache_storage::get_file_reserved_size() {
  if (m_file.length() == 0) init_file_reserved_size();

  return m_file_reserved_size;
}

void RDS_binlog_cache_storage::generate_tmp_file_name(char *name) {
  size_t length;

  dirname_part(name, log_bin_basename, &length);
  name += length;

  binlog_cache_free_flush.inc_tmp_file_count();

  assert(current_thd);

  // The sprintf will add '\0' to the end of string.
  sprintf(name, "%s/%s_%s_%u", BINLOG_CACHE_DIR, m_file.get_io_cache()->prefix,
          BINLOG_CACHE_NAME, current_thd->thread_id());
}

bool RDS_binlog_cache_storage::write(const unsigned char *buffer,
                                     my_off_t length) {
  if (m_file_reserved_size > 0) {
    /*
      Create a KEEP tmp file outside m_file.write() to avoid creating an
      unlinked tmp file.
    */
    IO_CACHE *info = m_file.get_io_cache();

    if (info->write_pos + length > info->write_end && info->file == -1) {
      char name_buff[FN_REFLEN];
      generate_tmp_file_name(name_buff);
      if ((info->file = mysql_file_open(info->file_key, name_buff,
                                        O_CREAT | O_RDWR, MYF(MY_WME))) < 0) {
        LogErr(ERROR_LEVEL, ER_FAILED_BINLOG_CACHE_FREE_FLUSH,
               "failed to open binlog cache tmp file %s.", name_buff);
        return true;
      }
    }
  }

  return m_file.write(buffer, length);
}

void RDS_binlog_cache_storage::detach_temp_file() {
  IO_CACHE *info = m_file.get_io_cache();

  /*
    If there was a rollback_to_savepoint happened before, the real length of tmp
    file can be greater than the file_end_pos. Truncate the cache tmp file to
    file_end_pos of this cache.
  */
  my_chsize(info->file, get_file_end_pos(), 0, MYF(MY_WME));

  /* Close file. */
  mysql_file_close(info->file, MYF(0));

  /* Reset tmp file fd for binlog cache. */
  info->file = -1;
}

bool RDS_binlog_cache_storage::flush_and_sync() {
  IO_CACHE *info = m_file.get_io_cache();

  if (my_b_flush_io_cache(info, 1)) return true;
  if (mysql_file_sync(info->file, MYF(MY_WME))) return true;
  return false;
}
