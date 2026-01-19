# Copyright (c) 2024, 2025, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

ADD_LIBRARY(xxhash_interface INTERFACE)
TARGET_LINK_LIBRARIES(xxhash_interface INTERFACE xxhash_lib)
ADD_LIBRARY(ext::xxhash ALIAS xxhash_interface)

SET(XXHASH_VERSION_DIR "xxHash-0.8.3")
SET(BUNDLED_XXHASH_PATH
  "${CMAKE_SOURCE_DIR}/extra/xxhash/${XXHASH_VERSION_DIR}")

# Dispatching is only supported on x86 and x86_64, not ARM.
ADD_STATIC_LIBRARY(xxhash_lib
  ${BUNDLED_XXHASH_PATH}/xxhash.c
# ${BUNDLED_XXHASH_PATH}/xxh_x86dispatch.c
  COMPILE_DEFINITIONS PRIVATE XXH_NAMESPACE=MY_
  )

# xxhash.h:2450:42:
# error: "__cplusplus" is not defined, evaluates to 0 [-Werror=undef]
IF(SOLARIS)
  TARGET_COMPILE_OPTIONS(xxhash_lib PRIVATE "-Wno-undef")
ENDIF()

IF(UNIX)
  TARGET_COMPILE_OPTIONS(xxhash_lib PRIVATE "-fvisibility=hidden")
ENDIF()
