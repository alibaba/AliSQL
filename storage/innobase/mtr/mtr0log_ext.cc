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

#include "mtr0log.h"
#include "mtr0log_ext.h"
#include "mysql/plugin_engine.h"

/**
  Logs a server log.

  @param[in]  ptr1  buffer of the first part of server data.
  @param[in]  len1  length of the first part of server data.
  @param[in]  ptr2  buffer of the second part of server data.
  @param[in]  len2  length of the second part of server data.
  @param[in]  ptr3  buffer of the third part of server data.
  @param[in]  len3  length of the third part of server data.

  @param[in]  mtr  mini-transaction handle.
*/
void mlog_log_server_log(const byte *ptr1, ulint len1, const byte *ptr2,
                         ulint len2, const byte *ptr3, ulint len3, mtr_t *mtr) {
  byte *log_ptr;

  ut_ad(ptr1 != nullptr && len1 != 0 && mtr);
  ut_ad(len2 == 0 || ptr2 != nullptr);
  ut_ad(len3 == 0 || ptr3 != nullptr);

  ulint total_len = len1 + len2 + len3;
  /*
    Open 30 bytes for Log Type and Log Length fields. The unused space will be
    released by mlog_close automatically.
  */
  if (!mlog_open(mtr, 30, log_ptr)) return;

  // Log Type
  mach_write_to_1(log_ptr, MLOG_SERVER_DATA);
  log_ptr++;

  // Log length
  log_ptr += mach_write_compressed(log_ptr, total_len);

  mlog_close(mtr, log_ptr);

  // Log content
  mlog_catenate_string(mtr, ptr1, len1);
  if (len2 > 0) mlog_catenate_string(mtr, ptr2, len2);
  if (len3 > 0) mlog_catenate_string(mtr, ptr3, len3);
  mtr->added_rec();
}

/**
  Parses a server data log record.

  @param[in]  ptr       buffer of the record without Log Type.
  @param[in]  end_ptr   end of the buffer.

  @return  parsed record end, nullptr if not a complete record.
*/
byte *mlog_parse_server_log(const byte *ptr, const byte *end_ptr) {
  uint len;

  len = mach_parse_compressed(&ptr, end_ptr);
  if (len == 0 || ptr + len > end_ptr) return nullptr;

  if (apply_server_redo_log(ptr, len)) return nullptr;
  return const_cast<byte *>(ptr) + len;
}
