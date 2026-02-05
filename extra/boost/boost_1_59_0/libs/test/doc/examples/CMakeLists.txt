#
# Copyright (c) 2013-2015 Raffi Enficiaud
# 
#  Distributed under the Boost Software License, Version 1.0. (See accompanying
#  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
# 


cmake_minimum_required(VERSION 2.8.11)
project(examples)
enable_testing()

# not meant to be included in the documentation
set(BOOST_ROOT ${CMAKE_SOURCE_DIR}/../../../..)
message(STATUS "$BOOST_ROOT = ${BOOST_ROOT}")

set(EXAMPLES_SRC
    example12.cpp
    dataset_example64.cpp
    dataset_example68.cpp
    )

# source files needing variadic macros
set(EXAMPLES_VARIADIC_MACROS_SRC
    boost_test_macro_overview.cpp
    boost_test_macro_workaround.cpp
    boost_test_macro2.cpp
    boost_test_macro3.cpp
    boost_test_bitwise.cpp
    boost_test_string.cpp
    boost_test_message.cpp

    boost_test_container_default.cpp
    boost_test_sequence_per_element.cpp
    boost_test_container_lex.cpp
    boost_test_macro_container_c_array.cpp
    )
    
if(CMAKE_CXX_COMPILER_ID STREQUAL Clang)
  set_source_files_properties(${EXAMPLES_VARIADIC_MACROS_SRC}
                              PROPERTIES 
                                COMPILE_FLAGS -std=c++11)
endif()

set(ALL_SRC ${EXAMPLES_SRC} ${EXAMPLES_VARIADIC_MACROS_SRC})


foreach(example_file IN LISTS ALL_SRC)

  get_filename_component(example_project ${example_file} NAME_WE)

  # creates the executable
  add_executable(test_${example_project} ${example_file})
  # indicates the include paths
  target_include_directories(test_${example_project} PRIVATE ${BOOST_ROOT})

  # declares a test with our executable
  add_test(NAME test-${example_project} COMMAND test_${example_project})

endforeach(example_file)

