/* Copyright (c) 2008, 2018, Alibaba and/or its affiliates. All rights reserved.

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
#ifndef SPIN_LOCK_INCLUDED
#define SPIN_LOCK_INCLUDED
#include <atomic>
#include "my_systime.h"

/** Spin lock implemented by using std::atomic_bool */
class Spin_lock {
 public:
  Spin_lock() {}
  Spin_lock(uint32_t wait_usec) : m_wait_usec(wait_usec) {}

  void lock() {
    while (m_lock.exchange(true, std::memory_order_acquire)) {
      my_sleep(m_wait_usec);
    }
  }

  /**
     @retval true   locked successfully
     @retval false  It is locked by another thread.
  */
  bool lock_no_wait() {
    return !m_lock.exchange(true, std::memory_order_acquire);
  }
  void unlock() { m_lock.store(false, std::memory_order_release); }

 private:
  std::atomic_bool m_lock = {false};

  /** Microseconds to wait if someone else is holding the lock. */
  uint32_t m_wait_usec = 10;

  Spin_lock &operator=(const Spin_lock &) = delete;
  Spin_lock(const Spin_lock &) = delete;
  Spin_lock(Spin_lock &&) = delete;
};

#endif  // SPIN_LOCK_INCLUDED
