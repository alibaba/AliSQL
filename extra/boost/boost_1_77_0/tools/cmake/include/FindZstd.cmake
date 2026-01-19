# Copyright 2021 Peter Dimov
# Distributed under the Boost Software License, Version 1.0.
# See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt

include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

# find_path and find_library already look into Zstd_ROOT

find_path(ZSTD_INCLUDE_DIR NAMES zstd.h)

find_library(ZSTD_LIBRARY_DEBUG NAMES zstdd zstd_staticd)
find_library(ZSTD_LIBRARY_RELEASE NAMES zstd zstd_static)

select_library_configurations(ZSTD)

if(ZSTD_INCLUDE_DIR AND EXISTS "${ZSTD_INCLUDE_DIR}/zstd.h")

  file(STRINGS "${ZSTD_INCLUDE_DIR}/zstd.h" _zstd_h REGEX "#define ZSTD_VERSION_[A-Z]+[ \t]+[0-9]+")

  string(REGEX REPLACE ".*#define ZSTD_VERSION_MAJOR[ \t]+([0-9]+).*" "\\1" ZSTD_VERSION_MAJOR "${_zstd_h}")
  string(REGEX REPLACE ".*#define ZSTD_VERSION_MINOR[ \t]+([0-9]+).*" "\\1" ZSTD_VERSION_MINOR "${_zstd_h}")
  string(REGEX REPLACE ".*#define ZSTD_VERSION_RELEASE[ \t]+([0-9]+).*" "\\1" ZSTD_VERSION_PATCH "${_zstd_h}")

  set(ZSTD_VERSION_STRING "${ZSTD_VERSION_MAJOR}.${ZSTD_VERSION_MINOR}.${ZSTD_VERSION_PATCH}")

  unset(_zstd_h)

endif()

find_package_handle_standard_args(Zstd REQUIRED_VARS ZSTD_LIBRARIES ZSTD_INCLUDE_DIR VERSION_VAR ZSTD_VERSION_STRING)

mark_as_advanced(ZSTD_INCLUDE_DIR)

if(ZSTD_FOUND AND NOT TARGET Zstd::Zstd)

  add_library(Zstd::Zstd UNKNOWN IMPORTED)

  set_target_properties(Zstd::Zstd PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${ZSTD_INCLUDE_DIR}
    IMPORTED_LINK_INTERFACE_LANGUAGES C)

  if(ZSTD_LIBRARY_RELEASE)

    set_property(TARGET Zstd::Zstd APPEND PROPERTY
      IMPORTED_CONFIGURATIONS RELEASE)

    set_target_properties(Zstd::Zstd PROPERTIES
      IMPORTED_LOCATION_RELEASE "${ZSTD_LIBRARY_RELEASE}")

  endif()

  if(ZSTD_LIBRARY_DEBUG)

    set_property(TARGET Zstd::Zstd APPEND PROPERTY
      IMPORTED_CONFIGURATIONS DEBUG)

    set_target_properties(Zstd::Zstd PROPERTIES
      IMPORTED_LOCATION_DEBUG "${ZSTD_LIBRARY_DEBUG}")

  endif()

endif()
