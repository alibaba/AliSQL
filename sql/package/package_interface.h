/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_PACKAGE_PACKAGE_INTERFACE_INCLUDED
#define SQL_PACKAGE_PACKAGE_INTERFACE_INCLUDED

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>
#include "proc.h"
#include "malloc_allocator.h"

class sp_name;
class THD;

/**
  Interface of native package module.
*/

namespace im {
/* All package memory usage aggregation point */
static PSI_memory_key key_memory_package;

template <class Key, class Value, class Less = std::less<Key> >
class malloc_map
    : public std::map<Key, Value, Less,
                      Malloc_allocator<std::pair<const Key, Value> > > {
 public:
  malloc_map(PSI_memory_key psi_key)
      : std::map<Key, Value, Less,
                 Malloc_allocator<std::pair<const Key, Value> > >(
            Less(), Malloc_allocator<>(psi_key)) {}
};

template <typename F, typename S>
class Pair_key_icase_type : public std::pair<F, S> {
 public:
  explicit Pair_key_icase_type() : std::pair<F, S>() {}
  explicit Pair_key_icase_type(F f, S s) : std::pair<F, S>(f, s) {}

  bool operator<(const Pair_key_icase_type<F, S> &rhs) const;
};

template <typename K, typename T>
class Pair_key_map : public malloc_map<K, const T *> {
 public:
  explicit Pair_key_map(PSI_memory_key key) : malloc_map<K, const T *>(key) {}
};

typedef Pair_key_icase_type<std::string, std::string> Package_key_type;
typedef Pair_key_map<Package_key_type, Proc> Package_element_map;

class Package : public PSI_memory_base {
  typedef Package_element_map Proc_map;

 public:
  explicit Package(PSI_memory_key key)
      : PSI_memory_base(key), m_proc_map(key) {}

  ~Package() { clear_elements(); }

  /* Disable copy and assign function */
  Package(const Package &);
  Package &operator=(const Package &);

  /**
    Global singleton package container.

    @retval     package instance
  */
  static Package *instance();

  /**
    Register native element

    @param[in]      schema_name     Element schema.
    @param[in]      element_name    Element name
    @param[in]      element         Object instance

    @retval         false           Failure
    @retval         true            Success
  */
  bool register_element(const std::string &schema_name,
                        const std::string &element_name, Proc *element) {
    return m_proc_map.insert(Package_element_map::value_type(
        Package_element_map::key_type(schema_name, element_name), element))
        .second;
  }

  /**
    Lookup element

    @param[in]      schema_name     Element schema.
    @param[in]      element_name    Element name

    @retval         element         Object instance
 */
  const Proc *lookup_element(const std::string &schema_name,
                             const std::string &element_name) {
    Package_element_map::const_iterator it;

    it = m_proc_map.find(
        Package_element_map::key_type(schema_name, element_name));

    if (it == m_proc_map.end())
      return NULL;
    else
      return it->second;
  }

  const Proc_map *get_all_element() { return &m_proc_map; }

 private:
  /* Clear all elements */
  void clear_elements() {
    Package_element_map::const_iterator it;
    for (it = m_proc_map.begin(); it != m_proc_map.end(); ++it) {
      delete it->second;
    }
    m_proc_map.clear();
  }

 private:
  /* Native proc container */
  Proc_map m_proc_map;
};

/* Initialize Package context. */
void package_context_init();

/**
  whether exist native proc by schema_name and proc_name

  @retval       true              Exist
  @retval       false             Not exist
*/
bool exist_native_proc(const char *db, const char *name);

/**
  Find the native proc and evoke make_cmd

  @param[in]    THD               Thread context
  @param[in]    sp_name           Proc name

  @retval       Sql_cmd
*/
extern Sql_cmd *native_make_cmd(THD *thd, sp_name *sp_name);

} /* namespace im */

#endif

