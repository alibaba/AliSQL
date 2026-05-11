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

#ifndef SQL_DD_DD_MINOR_UPGRADE_H_INCLUDED
#define SQL_DD_DD_MINOR_UPGRADE_H_INCLUDED

#include "my_inttypes.h"

class THD;

/**
  Minor Upgrade is designed to upgrade I_S System Views without changing
  the upstream IS_DD_VERSION.

  This strategy adds EXTRA_IS_VERSION into the DD_properties table.
  Increase EXTRA_IS_DD_VERSION when modifying I_S system view definitions.

  DD restart will reconstruct I_S system views if the persisted version
  does not match the compiled target version.
*/
namespace dd {

static const uint UNKNOWN_EXTRA_VERSION = -1;

class Minor_upgrade_ctx {
 public:
  Minor_upgrade_ctx() {}
  ~Minor_upgrade_ctx() {}

  static Minor_upgrade_ctx *instance();

  static uint get_target_extra_I_S_version();

  virtual uint get_actual_extra_I_S_version(THD *thd);

  uint set_extra_I_S_version(THD *thd, uint version);
};

}  // namespace dd

#endif
