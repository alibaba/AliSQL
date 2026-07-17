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

#ifndef FLASHBACK_RESULT_INCLUDED
#define FLASHBACK_RESULT_INCLUDED

#include "my_dbug.h"
#include "my_inttypes.h"

#include <string>
#include <vector>

namespace im {

typedef struct snapshot_transform_result_t {
  ulonglong trx_id;
  ulonglong utc;
  std::string memo;
} snapshot_transform_result_t;

typedef struct snapshot_analyze_result_t {
  ulonglong min_utc;
  ulonglong max_utc;
  ulonglong size;
  ulonglong undo_size;

  bool valid() { return min_utc > 0 && max_utc > 0; }
} snapshot_analyze_result_t;

typedef std::vector<snapshot_transform_result_t> snapshot_transform_result_list;

typedef struct snapshot_transform_result_container_t {
  snapshot_transform_result_list m_list;
  ulonglong m_size;
} snapshot_transform_result_container_t;

}  // namespace im

#endif