#ifndef SERVER_REDO_LOG_INCLUDED
#define SERVER_REDO_LOG_INCLUDED
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

  Server data for written into redo log
*/

#include "sql/handler.h"
#include "sql/mysqld.h"

/**
  Server redo log provides a framework to log something into InnoDB's redo log
  and recover them at server startup.
  When logging something into redo log, it follow below process.
  * Define a Log Type
    First, a log type should be added into Log_type.

  * Logging
    - Call write() to write a piece of data into redo log.
    - Call flush() to wait the redo log to be flushed to disk.
    - Log Format
      +---------------+-----------------------+
      | Log Type(1B)  | Data Content(1B-10MB) |
      +---------------+-----------------------+

  * Redo Observer
    It provides the interfaces to observe redo's actions. Thus observers have
    chance to do something before or after the actions.
    - Implement a subclass of Observer
    - Register the observer object through register_redo_observer()

  * Redo Applier
    Applier is used to recover the data logged into redo log when server
    startup. It is embedded into redo's recovery.
    - Implement a subclass of Applier.
    - Register the object through register_redo_applier(). It should be
      registered before starting InnoDB redo recovery. When redo recovery
      process finds a server redo log, it will pass the log content to
      server redo log module. And then a specific applier is called according
      to log type.

    Note: the applier should be idempotence like innodb redo log.
*/
namespace server_redo_log {
enum Log_type { LOG_TYPE_BINLOG = 0, LOG_TYPE_COUNT };

void init();
void deinit();

/**
   Whether the redo log is readonly. If it is readonly, then nothing can be
   written into redo log.

   @retval  true   It it readonly
   @retval  false  It is not readonly
*/
inline bool redo_is_readonly() { return innodb_hton->is_dict_readonly(); }

/**
   write the data in \@buffer into a redo record.

   @param[in]  buf1  The data will be written into redo log. buf1[0] must be
                     log type.
   @param[in]  len1  length of the data.

   @return  end lsn of the redo record.
*/
inline uint64 write(const unsigned char *buf1, uint32_t len1,
                    const unsigned char *buf2 = nullptr, uint32_t len2 = 0,
                    const unsigned char *buf3 = nullptr, uint32_t len3 = 0) {
  assert(buf1 != nullptr && len1 != 0);
  assert(len2 == 0 || buf2 != nullptr);
  assert(len3 == 0 || buf3 != nullptr);
  assert(buf1[0] < LOG_TYPE_COUNT);

  return innodb_hton->ext.log_server_data(buf1, len1, buf2, len2, buf3, len3);
}

/**
  Signal innodb to write redo log. It waits until the given lsn is write to
  redo file.

  @param[in]  lsn  The lsn should be written into redo file
*/
inline void wait_for_write(uint64 lsn) { innodb_hton->ext.wait_for_write(lsn); }

/**
  Signal innodb to flush redo log. It waits until the given lsn is persisted to
  disk.

  @param[in]  lsn  The lsn should be persisted.
*/
inline void wait_for_flush(uint64 lsn) { innodb_hton->ext.wait_for_flush(lsn); }

inline uint64 get_current_lsn() { return innodb_hton->ext.get_current_lsn(); }

/**
  It declares the interface for observing InnoDB's redo actions.
*/
class Observer {
 public:
  virtual ~Observer() {}

  /**
    It is called just before InnoDB making checkpoint. If you have to do
    something just before InnoDB checkpoint, this is the correct place to do.

    @param[in]  checkpoint_lsn  The lsn of the next checkpoint.
  */
  virtual bool before_checkpoint(uint64 checkpoint_lsn [[maybe_unused]]) {
    return false;
  }
};
void register_observer(Observer *observer);
void unregister_observer(Observer *observer);

/**
  It declares the interface for recovery data from server redo log.
*/
class Applier {
 public:
  virtual ~Applier() {}
  virtual bool recovery_begin() = 0;
  virtual bool recovery_end() = 0;
  /**
    It is called when redo recovery process find a server redo log at server
    startup. You need to implement a applier for recovering data from certain
    type of server redo log.

    @param[in]  log  The buffer of the log. It includes the Log Type field.
    @param[in]  len  The length of the log.
  */
  virtual bool apply(const unsigned char *log, uint len) = 0;
};

/**
  Register an applier for certain type of server redo log.

  @param[in]  type  which type the applier will work on.
  @param[in]  applier  pointer of the applier.
*/
void register_applier(Log_type type, Applier *applier);
/**
  Unregister the applier of certain type of server redo log.

  @param[in]  type  The type of the applier will be unregistered.
*/
void unregister_applier(Log_type type);

};  // namespace server_redo_log

#endif  // SERVER_REDO_LOG
