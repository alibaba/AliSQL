/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#ifndef TABLE_EXT_INCLUDED
#define TABLE_EXT_INCLUDED

#include <assert.h>

#include "mysql.h"

struct TABLE;
class THD;
struct LEX;
class Item;
struct Parse_context;
class Table_ref;

namespace im {

/**
  Snapshot clause info.
*/
class Snapshot_info_t {
  enum class Snapshot_type { SNAPSHOT_NONE, SNAPSHOT_TIMESTAMP };

 public:
  explicit Snapshot_info_t()
      : m_type(Snapshot_type::SNAPSHOT_NONE), m_ptr(nullptr) {}

  bool valid() const { return (m_type == Snapshot_type::SNAPSHOT_TIMESTAMP); }

  Snapshot_type get_type() const { return m_type; }

  uint64_t get_timestamp() const {
    assert(m_type == Snapshot_type::SNAPSHOT_TIMESTAMP);
    return m_ts;
  }

  void set_timestamp(uint64_t ts) {
    assert(m_type == Snapshot_type::SNAPSHOT_NONE);
    m_type = Snapshot_type::SNAPSHOT_TIMESTAMP;
    m_ts = ts;
  }

  void *get_ptr() const { return m_ptr; }

  void set_ptr(void *ptr) { m_ptr = ptr; }

  void set_value(const Snapshot_info_t &input, bool force_overwite) {
    if (m_type == Snapshot_type::SNAPSHOT_NONE || force_overwite) {
      *this = input;
    }
  }

  void reset() {
    m_type = Snapshot_type::SNAPSHOT_NONE;
    m_ptr = nullptr;
  }

 private:
  Snapshot_type m_type;
  uint64_t m_ts;
  void *m_ptr;
};

/**
  Reset snapshot, increase the snapshot table count.
*/
extern void init_table_snapshot(TABLE *table, THD *thd);

/**
  Evaluate table snapshot expressions.
*/
extern bool evaluate_snapshot(THD *thd, const LEX *lex);

/**
  Fix fields, and make sure expr is constant.
*/
extern bool fix_snapshot_fields(THD *thd, Item *&ts);

extern bool itemize_snapshot(Parse_context *pc, Item *opt_snapshot,
                             Table_ref *owner);

}  // namespace im

#endif