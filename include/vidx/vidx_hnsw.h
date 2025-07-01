#ifndef VIDX_HNSW_INCLUDED
#define VIDX_HNSW_INCLUDED

/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates. All rights reserved.

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

class THD;
class KEY;
struct TABLE;

namespace dd {
class Table;
}  // namespace dd

namespace vidx {
namespace hnsw {
static constexpr uint32_t DEF_CACHE_SIZE = 16 * 1024 * 1024;
static constexpr uint max_ef = 10000;

extern ulonglong max_cache_size;
extern void *trx_handler;

std::unique_ptr<dd::Table> create_dd_table(THD *thd, const char *table_name,
                                           KEY *key, dd::Table *dd_table,
                                           TABLE *table, const char *db_name,
                                           const uint tref_len);
int mhnsw_insert(TABLE *table, KEY *keyinfo);
int mhnsw_read_first(TABLE *table, KEY *keyinfo, Item *dist);
int mhnsw_read_next(TABLE *table);
int mhnsw_read_end(TABLE *table);
int mhnsw_invalidate(TABLE *table, const uchar *rec, KEY *keyinfo);
int mhnsw_delete_all(TABLE *table, KEY *keyinfo);
void mhnsw_free(TABLE_SHARE *share);
}  // namespace hnsw
}  // namespace vidx

#endif /* VIDX_HNSW_INCLUDED */
