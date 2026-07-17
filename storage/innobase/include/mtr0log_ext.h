#ifndef MTR0LOG_EXT_INCLUDED
#define MTR0LOG_EXT_INCLUDED
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

  log extension for alisql.
*/
void mlog_log_server_log(const byte *ptr1, ulint len1, const byte *ptr2,
                         ulint len2, const byte *ptr3, ulint len3, mtr_t *mtr);
byte *mlog_parse_server_log(const byte *ptr, const byte *end_ptr);

#endif  // MTR0LOG_EXT_INCLUDED
