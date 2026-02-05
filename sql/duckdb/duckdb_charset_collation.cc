#include "duckdb/duckdb_charset_collation.h"

#include <sstream>
#include <string>

#include "m_ctype.h"
#include "derror.h"
#include "duckdb/duckdb_query.h"
#include "field.h"

namespace myduck {

std::string get_duckdb_collation(const CHARSET_INFO *cs,
                                 std::string &warn_msg) {
  /* Charsets other than utf8mb3 and utf8mb4 use POSIX Collation directly
  Duckdb treats posix same as binary. We cannot use binary because binary is
  a keyword, so we use POSIX instead. */
  if (strcmp(cs->csname, "utf8mb3") && strcmp(cs->csname, "utf8mb4") &&
      strcmp(cs->csname, "ascii") && strcmp(cs->csname, "utf8")) {
    std::ostringstream osst;
    osst << "BINARY Collation is used for literal string in DuckDB."
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