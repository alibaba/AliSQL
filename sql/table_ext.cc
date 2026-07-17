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

#include "sql/table_ext.h"

#include "sql/common/component.h"
#include "sql/item.h"
#include "sql/item_timefunc.h"
#include "sql/parse_tree_node_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

namespace im {

/**
  Evaluate timestamp value.

  @param ts    Timestamp item
  @param out   UTC value caculated
  @return true Some error occurs.
*/
static bool evaluate_timestamp(Item *ts, uint64_t *out) {
  assert(ts && out);
  assert(current_thd);

  int unused;
  my_timeval tm;

  if (ts->get_timeval(&tm, &unused)) {
    my_error(ER_AS_OF_BAD_TIMESTAMP_TYPE, MYF(0));
    return true;
  }

  if (current_thd->is_error()) return true;

  if (ts->null_value) {
    my_error(ER_AS_OF_BAD_TIMESTAMP_TYPE, MYF(0));
    return true;
  }

  *out = tm.m_tv_sec;
  return false;
}

static bool evaluate_snapshot_item(Item *snapshot_item,
                                   Snapshot_info_t *snapshot) {
  if (snapshot_item == nullptr) return false;

  uint64_t val;
  if (evaluate_timestamp(snapshot_item, &val)) return true;
  snapshot->set_timestamp(val);

  return false;
}

/**
  Reset snapshot, increase the snapshot table count.
*/
void init_table_snapshot(TABLE *table, THD *thd) {
  assert(thd && thd->lex);

  table->snapshot.reset();

  if (table->pos_in_table_list->snapshot_expr() != nullptr)
    thd->lex->table_snap_expr_count_to_evaluate++;
}

/**
  Evaluate table snapshot expressions.

  @return true   If some error occurs.
*/
bool evaluate_snapshot(THD *thd, const LEX *lex) {
  assert(thd->lex->table_snap_expr_count_to_evaluate >= 0);

  /* Cases that need not do evaluating */
  if ((thd->lex->table_snap_expr_count_to_evaluate == 0) || /* No snapshot */
      (lex->is_explain() && !lex->is_explain_analyze))      /* Not analyze */
    return false;

  assert(thd->open_tables);

  for (TABLE *table = thd->open_tables; table; table = table->next) {
    assert(table->pos_in_table_list);
    assert(!table->snapshot.valid());

    if (evaluate_snapshot_item(table->pos_in_table_list->snapshot_expr(),
                               &table->snapshot))
      return true;
  }

  return false;
}

/**
  Fix fields, and make sure expr is constant.
*/
bool fix_snapshot_fields(THD *thd, Item *&ts) {
  if (ts != nullptr) {
    if (!ts->fixed && ts->fix_fields(thd, &ts)) return true;

    if (!ts->const_for_execution()) {
      my_error(ER_AS_OF_EXPR_NOT_CONSTANT, MYF(0));
      return true;
    }

    if (ts->data_type() == MYSQL_TYPE_INVALID) {
      /* MySQL only support INT parameter propagation,
      so not support `as of timestamp ?` now */
      goto ts_error;
    } else if (!ts->is_temporal_with_date_and_time()) {
      if (try_cast_to_datetime(thd, &ts)) goto ts_error;
    }
  }

  return false;

ts_error:
  if (!thd->is_error()) my_error(ER_AS_OF_BAD_TIMESTAMP_TYPE, MYF(0));
  return true;
}

bool itemize_snapshot(Parse_context *pc, Item *opt_snapshot,
                      Table_ref *owner) {
  assert(pc && pc->thd && pc->thd->lex);
  assert(owner && owner->snapshot_expr() == nullptr);

  if (opt_snapshot) {
    Query_block *select = owner->query_block;
    assert(select);

    if (select->select_number == 1 && pc->thd->lex->disallow_table_snapshot) {
      my_error(ER_AS_OF_NOT_SELECT, MYF(0));
      return true;
    }

    if (opt_snapshot->itemize(pc, &opt_snapshot)) return true;
    owner->set_snapshot_expr(opt_snapshot);

    /* Cannot invoke query cache if snapshot expression is set. */
    pc->thd->lex->safe_to_cache_query = false;
  }

  return false;
}

}  // namespace im