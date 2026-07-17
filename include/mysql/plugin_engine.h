#ifndef MYSQL_PLUGIN_ENGINE_INCLUDED
#define MYSQL_PLUGIN_ENGINE_INCLUDED
/* Copyright (c) 2018, 2025, Alibaba and/or its affiliates. All rights reserved.

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

#include <mysql/plugin.h>

/**
  Server side hooks that InnoDB's redo log framework calls into. They allow the
  server layer to persist arbitrary data into InnoDB's redo log and recover it
  at server startup. See sql/server_redo_log.h for details.
*/
bool apply_server_redo_log(const unsigned char *ptr, uint len);
bool notify_server_before_redo_checkpoint(uint64 checkpoint_lsn);
bool notify_server_redo_recovery_begin();
bool notify_server_redo_recovery_end();

#endif  // MYSQL_PLUGIN_ENGINE_INCLUDED
