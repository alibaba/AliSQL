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

#include "sql/server_redo_log.h"

#include <algorithm>
#include <atomic>
#include <vector>

#include "mutex_lock.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/handler.h"
#include "sql/mysqld.h"

namespace server_redo_log {

static Applier *appliers[LOG_TYPE_COUNT] = {nullptr};
static std::vector<Observer *> observers;

/** It is used for protect the operations on observers. */
static mysql_mutex_t mutex;
static PSI_mutex_key key_mutex;

/*
  Whether the module has been initialized. InnoDB's background log_checkpointer
  thread may call before_checkpoint() (via notify_server_before_redo_checkpoint)
  during InnoDB bring-up, possibly before init() has initialized the mutex. In
  that window no observer can be registered yet, so the notification is a no-op.
*/
static std::atomic<bool> g_inited{false};

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_info mutex_info[] = {{&key_mutex, "server_redo_log::mutex",
                                       PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME}};
#endif

void init() {
#ifdef HAVE_PSI_INTERFACE
  mysql_mutex_register("sql", mutex_info, array_elements(mutex_info));
#endif

  mysql_mutex_init(key_mutex, &mutex, MY_MUTEX_INIT_FAST);
  g_inited.store(true);
}

void deinit() {
  /*
    The server may abort before init() has run (for instance on an invalid
    option) and still call deinit() while cleaning up. Destroying the
    uninitialized mutex would abort the server instead of letting it exit with
    the proper error.
  */
  if (!g_inited.exchange(false)) return;

  mysql_mutex_destroy(&mutex);
}

void register_applier(Log_type type, Applier *applier) {
  appliers[type] = applier;
}
void unregister_applier(Log_type type) { appliers[type] = nullptr; }

inline bool redo_recovery_begin() {
  for (auto applier : appliers) {
    if (applier && applier->recovery_begin()) return true;
  }
  return false;
}

inline bool redo_recovery_end() {
  for (auto applier : appliers) {
    if (applier && applier->recovery_end()) return true;
  }
  return false;
}

/**
  It checks type of the log and call the applier of the type to handle the log.

  @param[in]  ptr  Buffer of the server redo log record.
  @param[in]  len  Lenght of the server redo log record.

  @retval  false  Applied the log successfully or skipped.
  @retval  true   Failed to apply the log.
*/
inline bool apply_log(const unsigned char *ptr, uint len) {
  if (ptr[0] >= LOG_TYPE_COUNT) {
    LogErr(ERROR_LEVEL, ER_SERVER_REDO_LOG_INVALID_LOG_TYPE, ptr[0]);
    return true;
  }

  uchar type = ptr[0];
  if (type < LOG_TYPE_COUNT && appliers[type]) {
    return appliers[type]->apply(ptr, len);
  }
  return false;
}

void register_observer(Observer *observer) {
  /*
    Not initialized yet: the mutex does not exist. Observers are only
    registered after init(), so there is nothing to do.
  */
  if (!g_inited.load()) return;

  MUTEX_LOCK(guard, &mutex);
  if (std::find(observers.begin(), observers.end(), observer) ==
      observers.end())
    observers.push_back(observer);
}

void unregister_observer(Observer *observer) {
  /*
    The server may abort early (for instance on a bad option) before init()
    has run, and unregister the observers while cleaning up. In that case no
    observer can have been registered, so there is nothing to do. Locking the
    uninitialized mutex here would abort the server instead of letting it exit
    with the proper error.
  */
  if (!g_inited.load()) return;

  MUTEX_LOCK(guard, &mutex);
  auto it = std::remove(observers.begin(), observers.end(), observer);
  observers.erase(it, observers.end());
}

/**
  Notify all observers before making checkpoint.

  @param[in]  checkpoint_lsn  The lsn of next checkpoint.

  @retval  false  Succeed.
  @retval  true   Error happened.
*/
inline bool before_checkpoint(uint64 checkpoint_lsn) {
  /* Not initialized yet: no observer can be registered, nothing to notify. */
  if (!g_inited.load()) return false;

  MUTEX_LOCK(guard, &mutex);
  for (auto observer : observers) {
    if (observer->before_checkpoint(checkpoint_lsn)) return true;
  }
  return false;
}

}  // namespace server_redo_log

bool apply_server_redo_log(const unsigned char *ptr, uint len) {
  return server_redo_log::apply_log(ptr, len);
}

bool notify_server_before_redo_checkpoint(uint64 checkpoint_lsn) {
  return server_redo_log::before_checkpoint(checkpoint_lsn);
}

bool notify_server_redo_recovery_begin() {
  return server_redo_log::redo_recovery_begin();
}

bool notify_server_redo_recovery_end() {
  return server_redo_log::redo_recovery_end();
}
