# Copyright (c) 2025, Alibaba and/or its affiliates. All Rights Reserved.
# 
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License, version 2.0, as published by the
# Free Software Foundation.
# 
# This program is also distributed with certain software (including but not
# limited to OpenSSL) that is licensed under separate terms, as designated in a
# particular file or component or in included license documentation. The authors
# of MySQL hereby grant you an additional permission to link the program and
# your derivative works with the separately licensed software that they have
# included with MySQL.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
# for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

DISABLE_MISSING_PROFILE_WARNING()
ADD_DEFINITIONS(-DMYSQL_SERVER)

SET(DUCKDB_SOURCES
  ha_duckdb.cc
  duckdb_types.cc
  delta_appender.cc
  ddl_convertor.cc
  dml_convertor.cc
  duckdb_select.cc
)
option(WITH_DUCKDB_STORAGE_ENGINE "Enable DuckDB storage engine" ON)

IF(WITH_DUCKDB_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(duckdb ${DUCKDB_SOURCES}
    STORAGE_ENGINE MANDATORY
    LINK_LIBRARIES ${DUCKDB_LIBRARY} extra::rapidjson extra::unordered_dense
  )
ELSE()
  MYSQL_ADD_PLUGIN(my_duckdb ${DUCKDB_SOURCES}
    STORAGE_ENGINE
    MODULE_ONLY
    LINK_LIBRARIES ext::zlib ${DUCKDB_LIBRARY} extra::rapidjson extra::unordered_dense
  )
ENDIF()
