/* Copyright (c) 2008, 2025, Alibaba and/or its affiliates. All rights reserved.

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

/**
  @file

  The code for alisql
*/

#include "ha_innodb_ext.h"
#include <atomic>
#include <cstdio>
#include "log0buf.h"
#include "log0chkp.h"
#include "log0log.h"
#include "log0sys.h"
#include "log0write.h"
#include "mach0data.h"
#include "mtr0log_ext.h"
#include "mtr0mtr.h"
#include "srv0srv.h"
#include "trx0sys.h"
#include "trx0types.h"

static ulonglong innobase_log_server_log(const uchar *buf1, uint32 len1,
                                         const uchar *buf2, uint32 len2,
                                         const uchar *buf3, uint32 len3) {
  mtr_t mtr;
  mtr_start(&mtr);
  mlog_log_server_log(buf1, len1, buf2, len2, buf3, len3, &mtr);
  mtr_commit(&mtr);
  return mtr.commit_lsn();
}

static void innobase_wait_for_write(ulonglong lsn) {
  (void)log_write_up_to(*log_sys, lsn, false);
  ut_a(log_sys->write_lsn.load() >= lsn);
}

static void innobase_wait_for_flush(ulonglong lsn) {
  (void)log_write_up_to(*log_sys, lsn, true);
  ut_a(log_sys->flushed_to_disk_lsn.load() >= lsn);
}

ulonglong innobase_get_current_lsn() {
  return log_sys->write_lsn.load(std::memory_order_relaxed);
}

/**
  Reset binlog offset when RESET MASTER

  @param[in]  fn  type of binlog operation.

  @return  return 0 if succeeds.
*/
static int innobase_binlog_func(handlerton *, THD *, enum_binlog_func fn,
                                void *) {
  if (srv_read_only_mode) return false;

  /* RESET MASTER is called, so reset binlog info to invalid */
  if (fn == BFN_RESET_LOGS) {
    log_buffer_flush_to_disk(true);

    /*
      Make a checkpoint to current lsn, thus the binlog records before reset
      master are invalid and will never be redo again.
    */
    log_make_latest_checkpoint(*log_sys);
  }
  return 0;
}

/**
  Initialize innobase extension.

  param[in]  innobase_hton  handlerton of innobase.
*/
void innobase_init_ext(handlerton *innobase_hton) {
  innobase_hton->binlog_func = innobase_binlog_func;
  innobase_hton->ext.log_server_data = innobase_log_server_log;
  innobase_hton->ext.wait_for_write = innobase_wait_for_write;
  innobase_hton->ext.wait_for_flush = innobase_wait_for_flush;
  innobase_hton->ext.get_current_lsn = innobase_get_current_lsn;
}
