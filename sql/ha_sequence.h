#ifndef HA_SEQUENCE_INCLUDED
#define HA_SEQUENCE_INCLUDED
/*
   Copyright (c) 2005, 2016, Aliyun and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/
#include "sql_sequence.h"
#include "table.h"
#include "handler.h"

/* Sequence share cache state */
enum enum_cache_state
{
  CACHE_INVALID,
  CACHE_ROUND_OUT,
  CACHE_HIT,
  CACHE_ERROR
};
/*
  The sequence caches will be stored here,
  allowed to be accessed simultaneously that protected by seq_mutex.
*/
class Sequence_share
{
  ulonglong caches[FIELD_NUM_END];
  ulonglong cache_end;
public:
  bool seq_initialized;
  mysql_mutex_t seq_mutex;

  bool cache_valid;
  uint ref_count;

  /* db_name + table_name */
  const char *table_name;

  /* All setted read/write set. */
  MY_BITMAP read_set;
  MY_BITMAP write_set;

public:
  Sequence_share() {};
  ~Sequence_share()
  {
    DBUG_ENTER("~Sequence_share");
    DBUG_ASSERT(ref_count == 0);
    mysql_mutex_destroy(&seq_mutex);
    if (table_name)
    {
      my_free((char *) table_name);
      table_name= NULL;
    }
    bitmap_free(&read_set);
    bitmap_free(&write_set);

    seq_initialized= false;
    DBUG_VOID_RETURN;
  };

  void init(const char *table_name);
  enum enum_cache_state quick_read(TABLE *table);
  int reload_cache(TABLE *table,
                   enum enum_cache_state state,
                   bool *changed);
  ulonglong *get_field_ptr(enum enum_sequence_field field_num);
  void set_valid(bool valid);
};

/*
  Sequence engine handler.

  The sequence engine is a logic engine. it didn't store any data.
  All the sequence data stored into the based table that default table engine is InnoDB.

  CACHE RULES:
    Sequence_share is used to cache values that sequence defined.
      1. If hit cache, we can query back the sequence nextval directly
         instead of scanning InnoDB table.

      2. When run out the caches. sequence engine will start autonomous transaction
         to update InnoDB table, and get the new values.

      3. Invalid the caches if any update on based table.
*/
class ha_sequence :public handler
{
private:
  handler *m_file;
  plugin_ref m_engine;
  Sequence_create_info *m_seq_create_info;
  Sequence_share *share;

  /* Control that only first record is valid within sequence table. */
  ulong start_of_scan;

  /* Whether iterator the sequence nextval */
  bool iter_sequence;

public:
  ha_sequence(handlerton *hton, TABLE_SHARE *share);

  ha_sequence(handlerton *hton,
               Sequence_create_info *seq_create_info);

  ~ha_sequence();

  bool initialize_sequence(MEM_ROOT *mem_root);
  void init_variables();
  bool setup_handler(MEM_ROOT *mem_root);
  bool setup_engine();
  bool get_from_handler_file(const char *name, MEM_ROOT *mem_root);
  bool new_handler_from_seq_create_info(MEM_ROOT *mem_root);
  void clear_handler_file();

  /* virtual function */
  virtual int rnd_init(bool scan);
  virtual int rnd_next(uchar *buf);
  int rnd_end();
  virtual int rnd_pos(uchar *buf, uchar *pos);
  virtual void position(const uchar * record);
  virtual int info(uint);
  virtual const char *table_type() const;
  virtual ulong index_flags(uint inx, uint part, bool all_parts) const;
  virtual THR_LOCK_DATA **store_lock(THD * thd, THR_LOCK_DATA ** to,
				     enum thr_lock_type lock_type);

  virtual int open(const char *name, int mode, uint test_if_locked);
  virtual int close(void);
  virtual Table_flags table_flags() const;
  virtual int create(const char *name, TABLE *form,
		     HA_CREATE_INFO *create_info);
  virtual const char **bas_ext() const;
  uint8 table_cache_type();

  int delete_table(const char* name);

  int write_row(uchar *buf);
  int update_row(const uchar *old_data, uchar *new_data);
  int delete_row(const uchar *buf);
  int external_lock(THD *thd, int lock_type);
  int update_and_reload(uchar *buf,
                        enum enum_cache_state state);

  int rename_table(const char* from, const char* to);
  void print_error(int error, myf errflag);

  /* Autonomous transaction */
  int begin_autonomous();
  int end_autonomous();
  int commit_autonomous();

  /*
    Bind the table/handler thread to track table i/o.
  */
  virtual void unbind_psi();
  virtual void rebind_psi();

  void lock_share()
  {
    DBUG_ENTER("ha_sequence::lock_share");
    DBUG_ASSERT(share && share->seq_initialized);
    mysql_mutex_lock(&share->seq_mutex);
    DBUG_VOID_RETURN;
  };

  void unlock_share()
  {
    DBUG_ENTER("ha_sequence::unlock_share");
    DBUG_ASSERT(share && share->seq_initialized);
    mysql_mutex_unlock(&share->seq_mutex);
    DBUG_VOID_RETURN;
  };

};


#endif
