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

#ifndef FLASHBACK_INCLUDED
#define FLASHBACK_INCLUDED

#include "flashback_result.h"

typedef void (*get_snapshot_func)(
    im::snapshot_transform_result_container_t *result_list, ulonglong utc,
    bool is_desc);

typedef void (*delete_snapshot_func)(ulonglong utc, bool is_desc);

typedef void (*analyze_snapshot_func)(im::snapshot_analyze_result_t *result);

typedef struct flashback_service_t {
  /* Get snapshots */
  get_snapshot_func get_snapshots;
  /* Delete snapshots */
  delete_snapshot_func delete_snapshots;
  /* Analyze snapshots */
  analyze_snapshot_func analyze_snapshots;
} flashback_service_t;

extern flashback_service_t *flashback_service;

extern void register_flashback_service(flashback_service_t *service);

#define FLASHBACK_CALL(M) flashback_service->M

#endif