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

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <boost/algorithm/string/case_conv.hpp>
#include <functional>

#include "package_interface.h"
#include "package_parse.h"
#include "sp_head.h"
#include "duckdb/duckdb_proc.h"

namespace im {

const char *PACKAGE_SCHEMA = "mysql";

static bool package_inited = false;

#ifdef HAVE_PSI_INTERFACE
static PSI_memory_info package_memory[] = {
    {&key_memory_package, "im::package", 0}};

static void init_package_psi_key() {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(package_memory));
  mysql_memory_register(category, package_memory, count);
}
#endif

template <typename F, typename S>
bool Pair_key_icase_type<F, S>::operator<(
    const Pair_key_icase_type<F, S> &rhs) const {
  int p_k = my_strcasecmp(system_charset_info, this->first.c_str(),
                          rhs.first.c_str());
  if (p_k) return p_k < 0;

  int s_k = my_strcasecmp(system_charset_info, this->second.c_str(),
                          rhs.second.c_str());
  return s_k < 0;
}

/**
  Global singleton package container.

  @retval     package instance
*/
Package *Package::instance() {
  static Package container(key_memory_package);

  return &container;
}

/* Register all the native package element */
template <typename T>
static void register_package(const LEX_STRING &schema) {
  if (package_inited) {
    Package::instance()->register_element(
        std::string(schema.str), T::instance()->str(), T::instance());
  }
}

/* Template of search package element */
static const Proc *find_package_element(const std::string &schema_name,
                                        const std::string &element_name) {
  return Package::instance()->lookup_element(schema_name, element_name);
}

/**
  whether exist native proc by schema_name and proc_name

  @retval       true              Exist
  @retval       false             Not exist
*/
bool exist_native_proc(const char *db, const char *name) {
  return find_package_element(std::string(db), std::string(name)) ? true
                                                                  : false;
}

/**
  Find the native proc and evoke make_cmd

  @param[in]    THD               Thread context
  @param[in]    sp_name           Proc name

  @retval       Sql_cmd
*/
Sql_cmd *native_make_cmd(THD *thd, sp_name *sp_name) {
  const Proc *proc = find_package_element(
      std::string(sp_name->m_db.str), std::string(sp_name->m_name.str));

  if (!proc) return NULL;

  List<Item> *para_list = &thd->lex->call_value_list;
  PT_package_base *pb = proc->PT_evoke(thd, para_list, proc);
  return pb ? pb->make_cmd(thd) : NULL;
}

/**
  Initialize Package context.
*/
void package_context_init() {
#ifdef HAVE_PSI_INTERFACE
  init_package_psi_key();
#endif
  package_inited = true;

  /* dbms_duckdb.query() */
  register_package<myduck::Duckdb_proc_query>(myduck::DUCKDB_PROC_SCHEMA);
}

/**
  Generate the proc execution command.

  @param[in]      THD       Thread context

  @retval         Sql_cmd   The Sql_cmd_proc object
*/
Sql_cmd *PT_package_proc::make_cmd(THD *thd) {
  LEX *const lex = thd->lex;

  Sql_cmd *sql_cmd = m_proc->evoke_cmd(thd, m_opt_expr_list);

  if (sql_cmd)
    lex->sql_command = sql_cmd->sql_command_code();

  return sql_cmd;
}

} /* namespace im */

