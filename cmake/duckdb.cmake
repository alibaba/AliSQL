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

INCLUDE(ExternalProject)

SET(DUCKDB_NAME "duckdb")
SET(DUCKDB_DIR "extra/${DUCKDB_NAME}")
SET(DUCKDB_SOURCE_DIR "${CMAKE_SOURCE_DIR}/${DUCKDB_DIR}")

IF(CMAKE_BUILD_TYPE STREQUAL "Debug")
  SET(DUCKDB_BUILD_TYPE "bundle-library-debug")
  SET(DUCKDB_BUILD_DIR "debug")
ELSE()
  SET(DUCKDB_BUILD_TYPE "bundle-library")
  SET(DUCKDB_BUILD_DIR "release")
ENDIF()

MACRO (MYSQL_USE_BUNDLED_DUCKDB)
  MESSAGE(STATUS "=== Setting up DuckDB from local source (${DUCKDB_DIR}) ===")

  IF(NOT EXISTS "${DUCKDB_SOURCE_DIR}/Makefile")
    MESSAGE(FATAL_ERROR
      "DuckDB source not found or incomplete at: ${DUCKDB_SOURCE_DIR}\n"
      "Expected Makefile. Please put DuckDB source under extra/duckdb."
    )
  ENDIF()

  SET(BINARY_DIR "${CMAKE_BINARY_DIR}/${CMAKE_CFG_INTDIR}/${DUCKDB_DIR}/build")
  SET(DUCKDB_INCLUDE_DIR "${DUCKDB_SOURCE_DIR}/src/include")

  ExternalProject_Add(duckdb_proj
    PREFIX      "${DUCKDB_DIR}"
    SOURCE_DIR  "${DUCKDB_SOURCE_DIR}"
    BINARY_DIR  "${BINARY_DIR}"
    STAMP_DIR   "${BINARY_DIR}/${DUCKDB_BUILD_DIR}/stamp"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make -C "${DUCKDB_SOURCE_DIR}" "${DUCKDB_BUILD_TYPE}" > /dev/null 2>&1
    INSTALL_COMMAND ""
    BUILD_ALWAYS OFF
  )

  SET(MY_DUCKDB_LIB "${BINARY_DIR}/${DUCKDB_BUILD_DIR}/libduckdb_bundle.a")
  MESSAGE(STATUS "DuckDB include: ${DUCKDB_INCLUDE_DIR}")
  MESSAGE(STATUS "DuckDB library:  ${MY_DUCKDB_LIB}")

  ADD_LIBRARY(libduckdb STATIC IMPORTED GLOBAL)
  SET_TARGET_PROPERTIES(libduckdb PROPERTIES IMPORTED_LOCATION "${MY_DUCKDB_LIB}")
  ADD_DEPENDENCIES(libduckdb duckdb_proj)

  INCLUDE_DIRECTORIES(BEFORE SYSTEM "${DUCKDB_INCLUDE_DIR}")
ENDMACRO()

MACRO (MYSQL_CHECK_DUCKDB)
  MYSQL_USE_BUNDLED_DUCKDB()
  SET(DUCKDB_LIBRARY libduckdb)
ENDMACRO()
