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

#ifndef FLASHBACK_PROC_INCLUDED
#define FLASHBACK_PROC_INCLUDED

#include <assert.h>

#include "sql/package/proc.h"

namespace im {

namespace flashback {

/**
  Proc base for dbms_admin

  1) Uniform schema: dbms_admin
*/
class Flashback_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Flashback_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << ADMIN_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  1) dbms_admin.show_flashback_snapshots();

*/
class Sql_cmd_flashback_proc_show : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_flashback_proc_show(THD *thd, mem_root_deque<Item *> *list,
                                       const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;
};

class Flashback_proc_show : public Flashback_proc_base {
  using Sql_cmd_type = Sql_cmd_flashback_proc_show;

  /* All the parameters */
  enum enum_parameter {
    FLASHBACK_PARAM_TIMESTAMP = 0,
    FLASHBACK_PARAM_ORDER,
    FLASHBACK_PARAM_LIMIT,
    FLASHBACK_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case FLASHBACK_PARAM_TIMESTAMP:
      case FLASHBACK_PARAM_ORDER:
        return MYSQL_TYPE_VARCHAR;
      case FLASHBACK_PARAM_LIMIT:
        return MYSQL_TYPE_LONGLONG;
      case FLASHBACK_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

  enum enum_column {
    COLUMN_TRX_ID = 0,
    COLUMN_TIMESTAMP,
    COLUMN_READVIEW,
    COLUMN_LAST
  };

 public:
  explicit Flashback_proc_show(PSI_memory_key key = 0)
      : Flashback_proc_base(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, STRING_WITH_LEN("TRX_ID"), 0},
        {MYSQL_TYPE_TIMESTAMP, STRING_WITH_LEN("TIMESTAMP"), 0},
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("READ_VIEW"), 512}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }

    /* Init parameters */
    for (size_t i = FLASHBACK_PARAM_TIMESTAMP; i < FLASHBACK_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  /* Singleton instance for show_flashback_snapshots */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for show_flashback_snapshots() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  virtual ~Flashback_proc_show() {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("show_flashback_snapshots");
  }
};

/**
  2) dbms_admin.del_flashback_snapshots();

*/
class Sql_cmd_flashback_proc_del : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_flashback_proc_del(THD *thd, mem_root_deque<Item *> *list,
                                      const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
};

class Flashback_proc_del : public Flashback_proc_base {
  using Sql_cmd_type = Sql_cmd_flashback_proc_del;

  /* All the parameters */
  enum enum_parameter {
    FLASHBACK_PARAM_TIMESTAMP = 0,
    FLASHBACK_PARAM_ORDER,
    FLASHBACK_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case FLASHBACK_PARAM_TIMESTAMP:
      case FLASHBACK_PARAM_ORDER:
        return MYSQL_TYPE_VARCHAR;
      case FLASHBACK_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Flashback_proc_del(PSI_memory_key key = 0)
      : Flashback_proc_base(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_OK;

    /* Init parameters */
    for (size_t i = FLASHBACK_PARAM_TIMESTAMP; i < FLASHBACK_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  /* Singleton instance for del_flashback_snapshots */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for del_flashback_snapshots() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  virtual ~Flashback_proc_del() {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("del_flashback_snapshots");
  }
};

/**
  3) dbms_admin.analyze_flashback_snapshots();

*/
class Sql_cmd_flashback_proc_analyze : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_flashback_proc_analyze(THD *thd,
                                          mem_root_deque<Item *> *list,
                                          const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    /**
      Require not any privileges when execute analyze_flashback_snapshots()
    */
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;
};

class Flashback_proc_analyze : public Flashback_proc_base {
  using Sql_cmd_type = Sql_cmd_flashback_proc_analyze;

  enum enum_column {
    COLUMN_MIN_TIMESTAMP = 0,
    COLUMN_MAX_TIMESTAMP,
    COLUMN_UNDO_SIZE,
    COLUMN_LAST
  };

 public:
  explicit Flashback_proc_analyze(PSI_memory_key key = 0)
      : Flashback_proc_base(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_TIMESTAMP, STRING_WITH_LEN("MIN_TIMESTAMP"), 0},
        {MYSQL_TYPE_TIMESTAMP, STRING_WITH_LEN("MAX_TIMESTAMP"), 0},
        {MYSQL_TYPE_LONGLONG, STRING_WITH_LEN("UNDO_SIZE(MB)"), 0}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }
  /* Singleton instance for analyze_flashback_snapshots */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for analyze_flashback_snapshots() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  virtual ~Flashback_proc_analyze() {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("analyze_flashback_snapshots");
  }
};

}  // namespace flashback

}  // namespace im

#endif
