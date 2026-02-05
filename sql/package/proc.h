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

#ifndef SQL_PACKAGE_PROC_INCLUDED
#define SQL_PACKAGE_PROC_INCLUDED

#include "prealloced_array.h"
#include "package_parse.h"
#include "sql_cmd.h"
#include <sstream>

class THD;
class sp_name;

/**
  Interface of native procedure.

  Any native procedure should implement these two interface:

    1) Proc
        It's abstract class declaration.

        - Parser structure
          All subclass of proc will has the same parse tree root,
            PT_package_proc(sp_name, item_list)

        - Sql command
          All subclass of proc need to implement itself command class.

        - Procedure name
          All subclass should define itself name.

    2) Sql_cmd_proc
        It's abstract class declaration.

        - Execute logic (pc_execute())
          All subclass need to implement it.

        - SQL command type
          All subclass has the same command type (SQLCOM_PROC).

        - Default behaviour (pls override these if individualization)
            1. send_result
            2. check_access
            3. check_parameter
            4. prepare


  Revision History:
  =================
  R1. Proc will be classified into two categories, administrator proc and
      transactional proc. They has the same base class PROC interface, but
      ADMIN_PROC and TRANS_PROC will have different SQL command, ADMIN_PROC will
      trigger implicit commit, and TRANS_PROC will inherit the transaction state
      context.
*/
namespace im {

/**
  PSI memory detect interface;
*/
class PSI_memory_base {
 public:
  PSI_memory_base(PSI_memory_key key) : m_key(key) {}

  virtual ~PSI_memory_base() {}

  /* Getting */
  PSI_memory_key psi_key() { return m_key; }

  /* Setting */
  void set_psi_key(PSI_memory_key key) { m_key = key; }

 private:
  PSI_memory_key m_key;
};

class Disable_copy_base {
 public:
  Disable_copy_base() {}
  virtual ~Disable_copy_base() {}

 private:
  Disable_copy_base(const Disable_copy_base &);
  Disable_copy_base &operator=(const Disable_copy_base &);
};
/**
   Native procedure interface
*/
class Proc : public PSI_memory_base {

  static const unsigned int PROC_PREALLOC_SIZE = 10;
 public:
  /**
     All the native procedures have uniform parse tree root.
     It includes:
        - sp_name
        - pt_expr_list
      from sql_yacc.yy

        - Proc
      from searching native proc map
  */
  typedef PT_package_proc PT_proc_type;

  /* Container of proc parameters */
  typedef Prealloced_array<enum_field_types, PROC_PREALLOC_SIZE>
      Parameters;

  /* Column element */
  typedef struct st_column_element {
    enum enum_field_types type;
    const char *name;
    std::size_t name_len;
    std::size_t size;

  } Column_element;

  /* Container of proc columns */
  typedef Prealloced_array<Column_element, PROC_PREALLOC_SIZE> Columns;

  typedef std::vector<const Parameters *> Parameters_list;

  /**
  */
  enum Result_type {
    RESULT_NONE,  // Initiail state
    RESULT_OK,    // Only OK or ERROR protocal
    RESULT_SET    // Send result set
  };

 public:
  explicit Proc(PSI_memory_key key)
      : PSI_memory_base(key),
        m_result_type(RESULT_NONE),
        m_parameters(key),
        m_parameters_list(),
        m_columns(key) {}

  virtual ~Proc() {}

  /**
    Generate the parse tree root.

    @param[in]    THD           Thread context
    @param[in]    pt_expr_list  Parameters
    @param[in]    proc          Native predefined proc

    @retval       PT_package_base 
  */
  PT_package_base *PT_evoke(THD *thd, List<Item> *pt_expr_list,
                            const Proc *proc) const;

  /**
    Interface of generating proc execution logic.

    @param[in]    THD           Thread context
    @param[in]    pt_expr_list  Parameters

    @retval       Sql cmd
  */
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const = 0;

  Result_type get_result_type() const { return m_result_type; }

  const Parameters *get_parameters() const { return &m_parameters; }

  const Parameters_list &get_parameters_list() const {
    return m_parameters_list;
  }

  const Columns &get_columns() const { return m_columns; }

  /**
    Send the result meta data by columns definition.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  bool send_result_metadata(THD *thd) const;

  /**
    Interface of proc name.

    @retval       string        Proc name
  */
  virtual const std::string str() const = 0;
  /**
    Interface of sp name.

    @retval       string        sp name
  */
  virtual const std::string qname() const = 0;

  /* Disable copy and assign function */
  Proc(const Proc &);
  Proc &operator=(const Proc &);

 protected:
  /* The type of result packet */
  Result_type m_result_type;
  /* The list of proc parameters, the default parameter format */
  Parameters m_parameters;
  /* Now support multiple parameter formats */
  Parameters_list m_parameters_list;
  /* The list of proc columns */
  Columns m_columns;
};


/**
  Interface of proc execution.

  Should implement pc_execute() function at least!
*/
class Sql_cmd_proc : public Sql_cmd {
 protected:
  enum Priv_type { PRIV_NONE_ACL = 0, PRIV_SUPER_ACL };

 public:
  explicit Sql_cmd_proc(THD *thd, List<Item> *list, const Proc *proc,
                        Priv_type priv_type)
      : m_thd(thd), m_list(list), m_proc(proc), m_priv_type(priv_type) {}
  /**
    Interface of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool execute(THD *thd);
  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) = 0;

  /**
    SQLCOM_ADMIN_PROC or SQLCOM_TRANS_PROC.
  */
  virtual enum_sql_command sql_command_code() const = 0;

  /**
    Send the ok or error packet defaultly,
    Override it if any result set.
  */
  virtual void send_result(THD *thd, bool error);

  /**
    Check access, require SUPER_ACL defaultly.
    Override it if any other requirement.
  */
  virtual bool check_access(THD *thd);

  /**
    Check the parameters
    Override it if any other requirement.
  */
  virtual bool check_parameter();

  /**
    Prepare the proc before execution.
  */
  virtual bool prepare(THD *thd);

 protected:
  void set_priv_type(Priv_type priv_type) { m_priv_type = priv_type; }

 protected:
  THD *m_thd;
  List<Item> *m_list;
  const Proc *m_proc;
  Priv_type m_priv_type;
};

/**
  Base class for administrator procedure.

  Require SUPER_ACL default.
*/
class Sql_cmd_admin_proc : public Sql_cmd_proc {
 public:
  explicit Sql_cmd_admin_proc(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_proc(thd, list, proc, PRIV_SUPER_ACL) {}

  virtual enum_sql_command sql_command_code() const {
    return SQLCOM_ADMIN_PROC;
  }
};

} /* namespace im */

#endif

