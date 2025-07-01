#ifndef VIDX_COMMON_INCLUDED
#define VIDX_COMMON_INCLUDED

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

#include <assert.h>
#include <cstdio>
#include <string>

#include "my_inttypes.h"

/* RDS comment version. The comment having version bigger than it should not be
treated as comment. */
#define RDS_COMMENT_VERSION 99999

#define RDS_COMMENT_VIDX_START "/*!99999 "
#define RDS_COMMENT_VIDX_END " */"

namespace vidx {
static constexpr uint32_t MAX_DIMENSIONS = 16383;
static constexpr uint32_t VECTOR_PRECISION = sizeof(float);

static const char *distance_names[] = {"EUCLIDEAN", "COSINE", nullptr};
static constexpr uint METRIC_DEF = 0;
static constexpr uint METRIC_MAX =
    sizeof(distance_names) / sizeof(distance_names[0]) - 1;

namespace hnsw {
static constexpr uint M_DEF = 6;
static constexpr uint M_MAX = 200;
static constexpr uint M_MIN = 3;

static inline bool validate_index_option_m(const uint option) {
  return option <= M_MAX && option >= M_MIN;
}
}  // namespace hnsw

static inline bool validate_index_option_distance(const uint option) {
  return option <= METRIC_MAX;
}

static inline uint32_t get_dimensions_low(const uint32_t length,
                                          const uint32_t precision) {
  if (length % precision > 0) {
    return UINT_MAX32;
  }
  return length / precision;
}
}  // namespace vidx

#endif /* VIDX_COMMON_INCLUDED */
