/*****************************************************************************

Copyright (c) 2026, Alibaba and/or its affiliates. All Rights Reserved.

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

#include "my_inttypes.h"
#include "sql/dd/impl/tables/dd_properties.h"
#include "sql/dd/string_type.h"
#include "sql/sql_class.h"

#include "sql/dd/dd_minor_upgrade.h"

namespace dd {
/**
  Version for server I_S tables and System Views.
  PLS increase it if update server I_S tables or System Views.


  Historical I_S version number published:

  1. Fixed VECTOR column DATA_TYPE in INFORMATION_SCHEMA views
  ---------------------------
  Strip versioned comment markers from VECTOR column DATA_TYPE in
    INFORMATION_SCHEMA.COLUMNS
    INFORMATION_SCHEMA.PARAMETERS
    INFORMATION_SCHEMA.ROUTINES
  to correctly show the underlying storage type.
*/
static const uint EXTRA_IS_DD_VERSION = 1;

static const String_type EXTRA_IS_DD_VERSION_STRING("EXTRA_IS_VERSION");

// Get the singleton instance
Minor_upgrade_ctx *Minor_upgrade_ctx::instance() {
  static Minor_upgrade_ctx ctx;
  return &ctx;
}

// Get compiled extra I_S version
uint Minor_upgrade_ctx::get_target_extra_I_S_version() {
  return EXTRA_IS_DD_VERSION;
}

// Get persisted extra I_S version
uint Minor_upgrade_ctx::get_actual_extra_I_S_version(THD *thd) {
  bool exists = false;
  uint version = 0;
  bool error MY_ATTRIBUTE((unused)) = tables::DD_properties::instance().get(
      thd, EXTRA_IS_DD_VERSION_STRING.c_str(), &version, &exists);
  if (error || !exists)
    return UNKNOWN_EXTRA_VERSION;
  else
    return version;
}

// Persist extra I_S version into dd_properties
uint Minor_upgrade_ctx::set_extra_I_S_version(THD *thd, uint version) {
  return tables::DD_properties::instance().set(
      thd, EXTRA_IS_DD_VERSION_STRING.c_str(), version);
}

}  // namespace dd
