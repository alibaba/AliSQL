/*****************************************************************************

Copyright (c) 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

#include "sql/duckdb/duckdb_charset_collation.h"

#include <sstream>
#include <string>

#include "m_ctype.h"
#include "sql/derror.h"
#include "sql/duckdb/duckdb_query.h"
#include "sql/field.h"

namespace myduck {

std::string get_duckdb_collation(const CHARSET_INFO *cs,
                                 std::string &warn_msg) {
  /* Charsets other than utf8mb3 and utf8mb4 use POSIX Collation directly
  Duckdb treats posix same as binary. We cannot use binary because binary is
  a keyword, so we use POSIX instead. */
  if (strcmp(cs->csname, "utf8mb3") && strcmp(cs->csname, "utf8mb4") &&
      strcmp(cs->csname, "ascii")) {
    std::ostringstream osst;
    osst << "Variable 'collation_connection' is set to " << cs->m_coll_name
         << " BINARY Collation is used for literal string in DuckDB."
         << " Recommend using collations of 'utf8mb3', 'utf8mb4' or 'ascii'.";
    warn_msg = osst.str();
    return COLLATION_BINARY;
  }

  /* _bin Collation */
  if (cs->state & MY_CS_BINSORT) return COLLATION_BINARY;

  /* utf8mb3_tolower_ci is _as_ci actually */
  if (cs->state & MY_CS_LOWER_SORT) return COLLATION_NOCASE;

  /* _ai_ci Collation */
  if (cs->levels_for_compare == 1) return COLLATION_NOCASE_NOACCENT;

  /* _as_ci Collation */
  if (cs->levels_for_compare == 2) return COLLATION_NOCASE;

  /* _as_cs Collation */
  return COLLATION_BINARY;
}

}  // namespace myduck