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

  handler extension for alisql.
*/

#ifndef HANDLER_EXT_INCLUDED
#define HANDLER_EXT_INCLUDED

/**
  Log server data into redo log. It will concatenate three buffers into one redo
  record if they are not null.
*/
typedef ulonglong (*log_server_data_t)(const uchar *buf1, uint32 len1,
                                       const uchar *buf2, uint32 len2,
                                       const uchar *buf3, uint32 len3);
typedef void (*wait_for_write_t)(ulonglong lsn);
typedef void (*wait_for_flush_t)(ulonglong lsn);
typedef ulonglong (*get_current_lsn_t)();
/**
  The extension of handlerton struct.
*/
struct handlerton_ext {
  log_server_data_t log_server_data;
  wait_for_write_t wait_for_write;
  wait_for_flush_t wait_for_flush;
  get_current_lsn_t get_current_lsn;
};

#endif
