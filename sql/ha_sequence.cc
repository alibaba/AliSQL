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
#include "ha_sequence.h"
#include "sql_plugin.h"
#include "mysql/plugin.h"
#include "sql_priv.h"
#include "sql_parse.h"
#include "sql_table.h"
#include "sql_update.h"
#include "sql_base.h"

#define SEQUENCE_ENABLED_TABLE_FLAGS  (HA_FILE_BASED | \
                                       HA_REC_NOT_IN_SEQ)
#define SEQUENCE_DISABLED_TABLE_FLAGS  (HA_CAN_GEOMETRY | \
                                        HA_CAN_FULLTEXT | \
                                        HA_DUPLICATE_POS | \
                                        HA_CAN_SQL_HANDLER | \
                                        HA_CAN_INSERT_DELAYED | \
                                        HA_READ_BEFORE_WRITE_REMOVAL)
#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_sequence_mutex;
static PSI_mutex_key key_sequence_share_mutex;
#endif

/* Protect the sequence_open_shares. */
static mysql_mutex_t sequence_share_mutex;
/* Sequence open shares */
static HASH sequence_open_shares;

static bool sequence_inited= false;

static handler *sequence_create_handler(handlerton *hton,
                                        TABLE_SHARE *share,
                                        MEM_ROOT *mem_root);
/* Sequence share get method.
   Squence handler must be related with share when open.

   SYNOPSIS
     name            db_name and table_name

   RETURN VALUES
     Sequence_share
*/
static Sequence_share *get_share(const char *name)
{
  Sequence_share *share;
  DBUG_ENTER("get_share");

  mysql_mutex_lock(&sequence_share_mutex);
  if (!(share= (Sequence_share *)my_hash_search(&sequence_open_shares,
                                                (uchar *)name,
                                                strlen(name))))
  {
    share= new Sequence_share();
    share->init(name);
    if (my_hash_insert(&sequence_open_shares, (uchar*) share))
    {
      delete share;
      share= NULL;
    }
  }
  if (share)
    share->ref_count++;

  mysql_mutex_unlock(&sequence_share_mutex);

  DBUG_RETURN(share);
}

/* Sequence share close method.
   squence handler must be disassociated from share when close.

   SYNOPSIS
     share            handler related sequence_share
*/
static void close_share(Sequence_share *share)
{
  DBUG_ENTER("close_share");

  mysql_mutex_lock(&sequence_share_mutex);

#ifndef DBUG_OFF
  Sequence_share *share2;
  share2= (Sequence_share *)my_hash_search(&sequence_open_shares,
                                           (uchar *)(share->table_name),
                                           strlen(share->table_name));
  DBUG_ASSERT(share2 == share);
#endif

  DBUG_ASSERT(share->ref_count > 0);
  --share->ref_count;
  mysql_mutex_unlock(&sequence_share_mutex);
  DBUG_VOID_RETURN;
}
/* Sequence share destroy method.

   SYNOPSIS
     name            db_name and table_name
*/
static void destroy_share(const char *name)
{
  Sequence_share *share= NULL;
  DBUG_ENTER("destory_share");
  mysql_mutex_lock(&sequence_share_mutex);

  share= (Sequence_share *)my_hash_search(&sequence_open_shares,
                                           (uchar*)(name),
                                           strlen(name));
  if (share)
  {
    DBUG_ASSERT(share->ref_count == 0);
    my_hash_delete(&sequence_open_shares, (uchar*) share);
  }
  mysql_mutex_unlock(&sequence_share_mutex);
  DBUG_VOID_RETURN;
}
/*
  Get sequence share table_name

  SYNOPSIS
    get_sequence_share()
    share                      sequence_share
    length                     table_name length

  RETURN VALUES
    table_name
*/
static uchar* get_sequence_share(Sequence_share *share, size_t *length,
                                 my_bool not_used MY_ATTRIBUTE((unused)))
{
  DBUG_ENTER("get_sequence_share");
  *length= strlen(share->table_name);
  DBUG_RETURN((uchar*) share->table_name);
}
/*
  Free sequence share object
*/
void free_sequence_share(Sequence_share *share)
{
  DBUG_ENTER("free_sequence_share");
  delete share;
  DBUG_VOID_RETURN;
}
/* Sequence_share init method.

   SYNOPSIS
     name            db_name and table_name
*/
void Sequence_share::init(const char *name)
{
  DBUG_ENTER("Sequence_share::init");
  mysql_mutex_init(key_sequence_mutex,
                   &seq_mutex,
                   MY_MUTEX_INIT_FAST);

  ref_count= 0;
  uint length= (uint)strlen(name);
  table_name= my_strndup(name, length, MYF(MY_FAE | MY_ZEROFILL));

  cache_valid= false;

  bitmap_init(&read_set, NULL, FIELD_NUM_END, false);
  bitmap_init(&write_set, NULL, FIELD_NUM_END, false);
  bitmap_set_all(&read_set);
  bitmap_set_all(&write_set);

  seq_initialized= true;
  DBUG_VOID_RETURN;
}
/*
  Get sequence share cache field value pointer

  SYNOPSIS
    field_num           sequence field number

  RETURN VALUES
    field value pointer
*/
ulonglong *Sequence_share::get_field_ptr(enum enum_sequence_field field_num)
{
  DBUG_ENTER("Sequence_share::get_field_ptr");
  DBUG_ASSERT(field_num < FIELD_NUM_END);
  DBUG_RETURN(&caches[field_num]);
}
/*
  Store the values into table->record from
  sequence share caches directly if caches has not been run out.

  SYNOPSIS
    table       TABLE object
    share       sequence share object
*/
static void sequence_prepare_field_value(TABLE *table,
                                         Sequence_share *share)
{
  ST_SEQ_FIELD_INFO *field_info;
  Field **field;
  ulonglong *value;
  MY_BITMAP *save_set;
  DBUG_ENTER("sequence_prepare_field_value");

  /* Save table write bitmap */
  save_set= table->write_set;
  table->write_set= &(share->write_set);

  for (field= table->field, field_info= seq_fields;
       *field;
       field++, field_info++)
  {
    DBUG_ASSERT(!memcmp(field_info->field_name,
                        (*field)->field_name,
                        strlen(field_info->field_name)));
    value= share->get_field_ptr(field_info->field_num);
    (*field)->set_notnull();
    (*field)->store(*value, true);
  }
  table->write_set= save_set;
  DBUG_VOID_RETURN;
}
/*
  Change the sequence share cache valid state.
*/
void Sequence_share::set_valid(bool valid)
{
  DBUG_ENTER("Sequence_share::set_valid");
  mysql_mutex_assert_owner(&seq_mutex);
  cache_valid= valid;
  DBUG_VOID_RETURN;
}
/*
  Quick read sequence value from cache.

  SYNOPSIS
    table       opened TABLE

  RETURN VALUES
    CACHE_INVALID       cache invalid, need reload from based table.
    CACHE_ROUND_OUT     cache run out, need reload next batch.
    CACHE_HIT           cache hit
*/
enum enum_cache_state Sequence_share::quick_read(TABLE *table)
{
  ulonglong *nextval_ptr;
  ulonglong *increment_ptr;
  bool last_round;
  DBUG_ENTER("Sequence_share::quick_read");

  mysql_mutex_assert_owner(&seq_mutex);
  nextval_ptr= &caches[FIELD_NUM_NEXTVAL];
  increment_ptr= &caches[FIELD_NUM_INCREMENT];

  if (!cache_valid)
    DBUG_RETURN(CACHE_INVALID);

  /* If cache_end roll upon maxvalue, then it is last round */
  last_round = (caches[FIELD_NUM_MAXVALUE] == cache_end);

  if (!last_round && ulonglong(*nextval_ptr) >= cache_end)
  {
    DBUG_RETURN(CACHE_ROUND_OUT);
  }
  else if (last_round)
  {
    if (*nextval_ptr > cache_end)
      DBUG_RETURN(CACHE_ROUND_OUT);
  }

  /* Retrieve values from cache directly */
  {
    DBUG_ASSERT(*nextval_ptr <= cache_end);
    sequence_prepare_field_value(table, this);
    if ((cache_end - *nextval_ptr) >= *increment_ptr)
      *nextval_ptr+= *increment_ptr;
    else
    {
      *nextval_ptr= cache_end;
      cache_valid= false;
    }
  }
  DBUG_RETURN(CACHE_HIT);
}
/*
  Sequence reload the cache from the table if cache has run out.

  SYNOPSIS
    table       TABLE object
    state       sequence cache state
    changed     whether sequence values changed

  RETURN VALUES
    false       Success
    true        Failure
*/
int Sequence_share::reload_cache(TABLE *table,
                                  enum enum_cache_state state,
                                  bool *changed)
{
  ST_SEQ_FIELD_INFO *field_info;
  Field **field;
  ulonglong durable[FIELD_NUM_END];
  enum enum_sequence_field field_num;
  DBUG_ENTER("Sequence_share::reload_cache");

  DBUG_ASSERT(state == CACHE_INVALID || state == CACHE_ROUND_OUT);
  mysql_mutex_assert_owner(&seq_mutex);

  /* Read the durable values */
  for (field= table->field, field_info= seq_fields;
       *field;
       field++, field_info++)
  {
    field_num= field_info->field_num;
    durable[field_num]= (ulonglong)((*field)->val_int());
  }

  /* If someone update the table directly, need this check again. */
  if (check_sequence_values_valid(durable))
    DBUG_RETURN(HA_ERR_SEQUENCE_INVALID);

  /* Calculate the next round cache values */
  ulonglong begin;

  /* Step 1: overlap the cache using durable values */
  for (field_info= seq_fields; field_info->field_name; field_info++)
    caches[field_info->field_num]= durable[field_info->field_num];

  /* Step 2: decide the begin value */
  if (caches[FIELD_NUM_NEXTVAL] == 0)
  {
    if (caches[FIELD_NUM_ROUND] == 0)
      begin= caches[FIELD_NUM_START]; /* from the begining start */
    else
      begin= caches[FIELD_NUM_MINVALUE]; /* next round from minvalue */
  }
  else if (caches[FIELD_NUM_NEXTVAL] == caches[FIELD_NUM_MAXVALUE])
    DBUG_RETURN(HA_ERR_SEQUENCE_RUN_OUT); /* run out value when nocycle */
  else
    begin= caches[FIELD_NUM_NEXTVAL];

  DBUG_ASSERT(begin <= caches[FIELD_NUM_MAXVALUE]);
  if (begin > caches[FIELD_NUM_MAXVALUE])
  {
    DBUG_RETURN(HA_ERR_SEQUENCE_INVALID);
  }
  /* step 3: calc the left counter to cache */
  longlong left = (caches[FIELD_NUM_MAXVALUE] - begin)
                  / caches[FIELD_NUM_INCREMENT] - 1;

  /* the left counter is less than cache size */
  if (left < 0 || ((ulonglong)left) <= caches[FIELD_NUM_CACHE])
  {
    /* if cycle, start again; else will report error! */
    cache_end= caches[FIELD_NUM_MAXVALUE];
    if (caches[FIELD_NUM_CYCLE] > 0)
    {
      durable[FIELD_NUM_NEXTVAL]= 0;
      durable[FIELD_NUM_ROUND]++;
    }
    else
      durable[FIELD_NUM_NEXTVAL]= caches[FIELD_NUM_MAXVALUE];
  }
  else
  {
    cache_end= begin + (caches[FIELD_NUM_CACHE] + 1) *
                        caches[FIELD_NUM_INCREMENT];
    durable[FIELD_NUM_NEXTVAL]= cache_end;
    DBUG_ASSERT(cache_end < caches[FIELD_NUM_MAXVALUE]);
  }
  caches[FIELD_NUM_NEXTVAL]= begin;

  /* step 4: Write back durable values*/
  store_record(table,record[1]);
  for (field= table->field, field_info= seq_fields;
       *field;
       field++, field_info++)
  {
    (*field)->set_notnull();
    (*field)->store(durable[field_info->field_num],true);
  }
  *changed = compare_records(table);

#ifndef DBUG_OFF
  fprintf(stderr, "Sequence will write values: "
                        "currval %llu "
                        "nextval %llu "
                        "minvalue %llu "
                        "maxvalue %llu "
                        "start %llu "
                        "increment %llu "
                        "cache %llu "
                        "cycle %llu \n",
                        durable[FIELD_NUM_CURRVAL],
                        durable[FIELD_NUM_NEXTVAL],
                        durable[FIELD_NUM_MINVALUE],
                        durable[FIELD_NUM_MAXVALUE],
                        durable[FIELD_NUM_START],
                        durable[FIELD_NUM_INCREMENT],
                        durable[FIELD_NUM_CACHE],
                        durable[FIELD_NUM_CYCLE]);
#endif
  DBUG_RETURN(0);
}

/*
   Sequence base table db engine setup.
*/
bool ha_sequence::setup_engine()
{
  handlerton *hton;
  DBUG_ENTER("ha_sequence::setup_engine");
  DBUG_ASSERT((table_share && table_share->is_sequence)
              || !table_share);

  /* TODO: we need .seq file to record base_db_type */
  if (table_share)
  {
    hton= table_share->seq_db_type;
    m_engine= ha_lock_engine(NULL, hton);
  }
  else
  {
    LEX_STRING engine_name= {C_STRING_WITH_LEN("InnoDB")};
    m_engine= ha_resolve_by_name(NULL, &engine_name, false);
  }
  if (!m_engine)
    goto err;

  DBUG_RETURN(FALSE);
err:
  clear_handler_file();
  DBUG_RETURN(TRUE);
}
/*
  Clear the locked sequence base table engine
*/
void ha_sequence::clear_handler_file()
{
  DBUG_ENTER("ha_sequence::clear_handler_file");
  if (m_engine)
  {
    plugin_unlock(NULL, m_engine);
    m_engine= NULL;
  }
  DBUG_VOID_RETURN;
}
/*
  Init sequence handler variables
*/
void ha_sequence::init_variables()
{
  DBUG_ENTER("ha_sequence::init_variables");
  m_file= NULL;
  m_engine= NULL;
  m_seq_create_info= NULL;

  start_of_scan= 0;
  DBUG_VOID_RETURN;
}
/*
  Init sequence handler when create sequence.
*/
bool ha_sequence::new_handler_from_seq_create_info(MEM_ROOT *mem_root)
{
  DBUG_ENTER("ha_sequence::new_handler_from_seq_create_info");

  DBUG_ASSERT(m_seq_create_info);

  if (!(m_file= get_new_handler(table_share, mem_root,
                                m_seq_create_info->base_db_type)))
  {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR),
             static_cast<int>(sizeof(handler)));

    DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}
/*
  Init sequence handler circumstances:
    1. Init from sequence_create_info when create sequence.
    2. Init from NULL when delete table.
    3. Init from table_name
*/
bool ha_sequence::initialize_sequence(MEM_ROOT *mem_root)
{
  DBUG_ENTER("ha_sequence::initialize_sequence");

  if (m_seq_create_info)
  {
    if (new_handler_from_seq_create_info(mem_root))
      DBUG_RETURN(TRUE);
  }
  else if (!table_share)
  {
    DBUG_RETURN(FALSE);
  }
  else if (get_from_handler_file(table_share->normalized_path.str,
                                 mem_root))
  {
    DBUG_RETURN(TRUE);
  }
  DBUG_EXECUTE_IF("sequence_handler_error",
                  {
                    my_error(ER_SEQUENCE_ACCESS_ERROR, MYF(0), NULL, NULL);
                    DBUG_RETURN(TRUE);
                  });

  DBUG_RETURN(FALSE);
}

static handler *sequence_create_handler(handlerton *hton,
                                        TABLE_SHARE *share,
                                        MEM_ROOT *mem_root)
{
  DBUG_ENTER("sequence_create_handler");
  ha_sequence *file= new (mem_root) ha_sequence(hton, share);
  if (file && file->initialize_sequence(mem_root))
  {
    delete file;
    file= 0;
  }
  DBUG_RETURN(file);
}

/* Constructor method*/
ha_sequence::ha_sequence(handlerton *hton, TABLE_SHARE *share)
  :handler(hton, share)
{
  init_variables();
}

ha_sequence::ha_sequence(handlerton *hton, Sequence_create_info *info)
  :handler(hton, 0)
{
  init_variables();
  m_seq_create_info= info;
}

/* Destructor method */
ha_sequence::~ha_sequence()
{
  if (m_file)
  {
    delete m_file;
    m_file= NULL;
  }
  clear_handler_file();
}
/* virtual function */
int ha_sequence::info(uint flag)
{
  DBUG_ENTER("ha_sequence::info");
  DBUG_RETURN(false);
}
const char *ha_sequence::table_type() const
{
  DBUG_ENTER("ha_sequence::table_type");
  DBUG_RETURN(m_file->table_type());
}
ulong ha_sequence::index_flags(uint inx, uint part, bool all_parts) const
{
  DBUG_ENTER("ha_sequence::index_flags");
  DBUG_RETURN(m_file->index_flags(inx, part, all_parts));
}
THR_LOCK_DATA **ha_sequence::store_lock(THD *thd,
                                        THR_LOCK_DATA **to,
                                        enum thr_lock_type lock_type)
{
  DBUG_ENTER("ha_sequence::store_lock");
  DBUG_RETURN(m_file->store_lock(thd, to, lock_type));
}
/* Sequence table open method
   SYNOPIS
     name               dbname and tablename
     mode               mode
     test_if_locked
   RETURN VALUES
     0                  Success
     !=0                Failure
 */
int ha_sequence::open(const char *name, int mode, uint test_if_locked)
{
  int error;
  DBUG_ENTER("ha_sequence::open");
  DBUG_ASSERT(table->s == table_share);
  error= HA_ERR_INITIALIZATION;

  if (!(share= get_share(name)))
    DBUG_RETURN(error);

  if (get_from_handler_file(name, &table->mem_root))
  {
    close_share(share);
    DBUG_RETURN(error);
  }
  DBUG_ASSERT(m_engine && m_file);

  if ((error= m_file->ha_open(table, name, mode, test_if_locked)))
  {
    close_share(share);
    goto err_handler;
  }
  clear_handler_file();

err_handler:
  DBUG_RETURN(error);
}
/*
  Close sequence handler.
  We didn't destroy share although the ref_count == 0,
  the cached values will be lost if we do that.
*/
int ha_sequence::close(void)
{
  DBUG_ENTER("ha_sequence::close");
  close_share(share);
  DBUG_RETURN(m_file->ha_close());
}
/*
  Sequence write row method.
  It should be used when create sequence.

  Attention:
    The sequence will only query the first row if you inserted many rows,
    so left rows existed in table are invalid, but still allowed.

  RETURN VALUE
    0           Success
    != 0        Failure
*/
int ha_sequence::write_row(uchar *buf)
{
  int error= 0;
  THD *thd;
  DBUG_ENTER("ha_sequence::write_row");
  DBUG_ASSERT(m_file);
  DBUG_ASSERT(share);
  thd= ha_thd();

  lock_share();

  /* Binlog will decided by m_file engine. so disable here */
  tmp_disable_binlog(thd);
  share->set_valid(false);

  error= m_file->ha_write_row(buf);

  DBUG_EXECUTE_IF("sequence_write_error",
                  {
                    error= HA_ERR_SEQUENCE_ACCESS_ERROR;
                  });
  reenable_binlog(thd);
  unlock_share();

  DBUG_RETURN(error);
}

int ha_sequence::update_row(const uchar *old_data, uchar *new_data)
{
  int error= 0;
  THD *thd;
  DBUG_ENTER("ha_sequence::update_row");
  DBUG_ASSERT(m_file);
  DBUG_ASSERT(share);
  thd= ha_thd();

  lock_share();

  /* Binlog will decided by m_file engine. so disable here */
  tmp_disable_binlog(thd);
  share->set_valid(false);
  error= m_file->ha_update_row(old_data, new_data);
  reenable_binlog(thd);

  unlock_share();

  DBUG_RETURN(error);
}
int ha_sequence::delete_row(const uchar *buf)
{
  int error= 0;
  THD *thd;
  DBUG_ENTER("ha_sequence::delete_row");
  DBUG_ASSERT(m_file);
  DBUG_ASSERT(share);
  thd= ha_thd();

  lock_share();

  /* Binlog will decided by m_file engine. so disable here */
  tmp_disable_binlog(thd);
  share->set_valid(false);
  error= m_file->ha_delete_row(buf);
  reenable_binlog(thd);

  unlock_share();

  DBUG_RETURN(error);
}

int ha_sequence::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_sequence::external_lock");
  DBUG_ASSERT(m_file);
  DBUG_RETURN(m_file->ha_external_lock(thd, lock_type));
}

int ha_sequence::rnd_init(bool scan)
{
  DBUG_ENTER("ha_sequence::rnd_init");
  DBUG_ASSERT(m_file);
  DBUG_ASSERT(share);
  DBUG_ASSERT(table_share && table);

  start_of_scan= 1;
  iter_sequence= false;

  /* Inherit the iter_sequence option. */
  if (table->iter_sequence)
    iter_sequence= true;

  DBUG_RETURN(m_file->ha_rnd_init(scan));
}
/*
  Sequence engine main logic.
  Embedded into the table scan process.

  Logics:
    1.Skip sequence cache to scan the based table record if
      a. update;
      b. session set sequence_read_skip_cache=true;
      c. select_from clause;

    2.Only scan the first row that controlled by
      variable 'start_of_scan'

    3.Lock strategy
      a. lock MDL_SHARE_WRITE on table when query cache
      b. lock global read lock when query cache
      c. lock commit when updating base table.

    4.Transaction
      a. begine autonomous transaction when updating base table.
*/
int ha_sequence::rnd_next(uchar *buf)
{
  int error= 0;
  enum enum_cache_state state;
  THD *thd;
  DBUG_ENTER("ha_sequence::rnd_next");
  error= 0;
  thd= ha_thd();

  DBUG_ASSERT(m_file);
  DBUG_ASSERT(share);
  DBUG_ASSERT(thd && table_share && table);

  /* Read the based record directly
     When: 1. Update
           2. Session variable setting
           3. Select_from clause
  */
  if (get_lock_type() == F_WRLCK
      || !iter_sequence
      || thd->variables.sequence_read_skip_cache)
  {
    DBUG_RETURN(m_file->ha_rnd_next(buf));
  }

  if (start_of_scan)
  {
    /* step 0: lock sequence table and global read */
    if (lock_sequence_table(thd, table)
        || check_lock_sequence_table(thd, table))
      DBUG_RETURN(HA_ERR_SEQUENCE_ACCESS_ERROR);

    start_of_scan= 0;
    lock_share();

    /* Step 1: quick read from cache */
    state= share->quick_read(table);
    switch (state)
    {
      /* If hit, query back quickly */
      case CACHE_HIT:
        goto end;
      case CACHE_ERROR:
        {
          /* Unlikely error.*/
          error= HA_ERR_SEQUENCE_ACCESS_ERROR;
          goto err;
        }
      case CACHE_INVALID:
      case CACHE_ROUND_OUT:
        {
          /* Step 2: cache reload */
          if ((error= update_and_reload(buf, state)))
            goto err;

          /* Step 3: Read from cache data again */
          share->set_valid(true);
          state= share->quick_read(table);
          switch (state)
          {
            case CACHE_HIT:
              { goto end; }
            case CACHE_ROUND_OUT:
              {
                error= HA_ERR_SEQUENCE_RUN_OUT;
                goto err;
              }
            default:
              {
                error= HA_ERR_SEQUENCE_ACCESS_ERROR;
                goto err;
              }
          }/* step 3  switch end */
        }
    }/* step 1 switch end */
  }
  else
    DBUG_RETURN(HA_ERR_END_OF_FILE);

err:
  share->set_valid(false);
end:
  unlock_share();
  DBUG_RETURN(error);
}

/*
  Begin autonomous transaction to:
    1. query based table;
    2. reload sequence cache;
    3. write back based table;

  SYNOPSIS
    buf         table->record[0]

  RETURN VALUES
    0           Success
    !=0         Failure
*/
int ha_sequence::update_and_reload(uchar *buf,
                                   enum enum_cache_state state)
{
  int error= 0;
  bool changed;
  MY_BITMAP *save_read_set;
  MY_BITMAP *save_write_set;
  DBUG_ENTER("ha_sequence::update_and_reload");
  error= 0;

  DBUG_ASSERT(m_file);
  DBUG_ASSERT(share);
  DBUG_ASSERT(table_share && table);

  /* Save read/write bitmap set */
  save_read_set= table->read_set;
  save_write_set= table->write_set;
  table->read_set= &(share->read_set);
  table->write_set= &(share->write_set);

  /* Step 1 begin the autonomous transaction */
  if ((error= begin_autonomous()))
    goto err;

  /* Step 2: query data */
  if ((error= m_file->ha_rnd_next(buf)))
    goto err_trans;

  /* Step 3: flush cache and ready data */
  if ((error= share->reload_cache(table, state, &changed)))
    goto err_trans;

  /* Step 4: write back new data */
  if (changed)
  {
    if ((error= m_file->ha_atm_update_row(
                                table->record[1],
                                table->record[0])))
    {
      goto err_trans;
    }
    /* Step 5: commit autonomous transaction */
    if ((error= commit_autonomous()))
      goto err_trans;
  }
err_trans:
  /* End the autonomous transaction */
  end_autonomous();
err:
  /* Restore the read/write bitmap set */
  table->write_set= save_write_set;
  table->read_set= save_read_set;
  DBUG_RETURN(error);
}
/*
  Begin autonomous transaction.
  Firstly backup binlog_cache and based_table engine trx,
  then we can begin autonomous transaction.

  RETURN VALUE:
    0           Success
    !=0         Failure
*/
int ha_sequence::begin_autonomous()
{
  THD *thd;
  int error= 0;
  DBUG_ENTER("ha_sequence::begin_autonomous");
  thd= ha_thd();

  DBUG_ASSERT(ha_thd());
  DBUG_ASSERT(m_file);

  if (thd->begin_autonomous_binlog())
  {
    error= HA_ERR_SEQUENCE_ACCESS_ERROR;
    DBUG_RETURN(error);
  }
  if ((error= m_file->begin_autonomous_trans()))
  {
    thd->end_autonomous_binlog();
    DBUG_RETURN(error);
  }
  DBUG_RETURN(error);
}
/*
  End autonomous transaction.

  RETURN VALUE:
    0           Success
    !=0         Failure
*/
int ha_sequence::end_autonomous()
{
  THD *thd;
  DBUG_ENTER("ha_sequence::end_autonomous");
  DBUG_ASSERT(ha_thd());
  DBUG_ASSERT(m_file);
  thd= ha_thd();
  m_file->end_autonomous_trans();
  thd->end_autonomous_binlog();
  DBUG_RETURN(FALSE);
}
/*
  2pc to commit autonomous transaction.

  RETURN VALUES
    0           Success
    !=0         Failure
*/
int ha_sequence::commit_autonomous()
{
  THD *thd;
  int error= 0;
  DBUG_ENTER("ha_sequence::commit_autonomous");
  thd= ha_thd();

  DBUG_ASSERT(ha_thd());
  DBUG_ASSERT(m_file);

  /* autonomous transaction commit process */
  ha_coalesce_atm_trx(thd);

  error= ha_prepare_low(thd, true);

  if (error || (error = tc_log->commit(thd, true)))
  {
    ha_rollback_trans(thd, true);
    DBUG_RETURN(HA_ERR_SEQUENCE_ACCESS_ERROR);
  }
  DBUG_RETURN(error);
}
int ha_sequence::rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_ENTER("ha_sequence::rnd_pos");
  DBUG_ASSERT(m_file);
  DBUG_RETURN(m_file->ha_rnd_pos(buf, pos));
}
void ha_sequence::position(const uchar *record)
{
  DBUG_ENTER("ha_sequence::positioin");
  DBUG_ASSERT(m_file);
  m_file->position(record);
}
int ha_sequence::rnd_end()
{
  DBUG_ENTER("ha_sequence::rnd_end");
  DBUG_ASSERT(m_file);
  DBUG_ASSERT(share);
  DBUG_ASSERT(table_share && table);
  DBUG_RETURN(m_file->ha_rnd_end());
}
void ha_sequence::unbind_psi()
{
  DBUG_ENTER("ha_sequence::unbind_psi");
  handler::unbind_psi();

  DBUG_ASSERT(m_file != NULL);
  m_file->unbind_psi();
  DBUG_VOID_RETURN;
}

void ha_sequence::rebind_psi()
{
  DBUG_ENTER("ha_sequence::rebind_psi");
  handler::rebind_psi();

  DBUG_ASSERT(m_file != NULL);
  m_file->rebind_psi();
  DBUG_VOID_RETURN;
}

/*
  Inherit the sequence base table flags.
  The stats is not exact, so never using 'const' when explain.
*/
handler::Table_flags ha_sequence::table_flags() const
{
  DBUG_ENTER("ha_sequence::table_flags");
  if (!m_file)
  {
    DBUG_RETURN(SEQUENCE_ENABLED_TABLE_FLAGS);
  }
  DBUG_RETURN(m_file->ha_table_flags()
              & ~(HA_STATS_RECORDS_IS_EXACT | HA_REQUIRE_PRIMARY_KEY));
}
/*
  Create sequence base table handler
*/
bool ha_sequence::setup_handler(MEM_ROOT *mem_root)
{
  handlerton *hton;

  DBUG_ENTER("ha_sequence::setup_handler");
  DBUG_ASSERT(m_engine);

  hton= plugin_data(m_engine, handlerton*);
  if (!(m_file= get_new_handler(table_share, mem_root, hton)))
  {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR),
             static_cast<int>(sizeof(handler)));
    DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}
/* Setup the sequence table engine and file handler.
     Default:  InnoDB storage engine.
 */
bool ha_sequence::get_from_handler_file(const char *name,
                                        MEM_ROOT *mem_root)
{
  DBUG_ENTER("ha_sequence::get_from_handler_file");

  if (m_file)
    DBUG_RETURN(FALSE);

  /*TODO: read from sequence meta data file */
  if(setup_engine())
    goto err;

  if (setup_handler(mem_root))
    goto err;

  DBUG_RETURN(FALSE);
err:
  clear_handler_file();
  DBUG_RETURN(TRUE);
}
/*
  Sequence table create method.
*/
int ha_sequence::create(const char *name, TABLE *table_arg,
                        HA_CREATE_INFO *create_info)
{
  int error;
  DBUG_ENTER("ha_sequence::create");

  if (get_from_handler_file(name, ha_thd()->mem_root))
    DBUG_RETURN(TRUE);

  DBUG_ASSERT(m_engine && m_file);
  if ((error= m_file->ha_create(name, table_arg, create_info)))
    goto create_error;

  DBUG_RETURN(FALSE);
create_error:
  m_file->ha_delete_table(name);
  handler::delete_table(name);
  DBUG_RETURN(error);
}
/*
  Sequence table drop method.
  we will destroy sequence share.
*/
int ha_sequence::delete_table(const char *name)
{
  DBUG_ENTER("ha_sequence::delete_table");
  if (get_from_handler_file(name, ha_thd()->mem_root))
    DBUG_RETURN(TRUE);

  destroy_share(name);
  DBUG_RETURN(m_file->ha_delete_table(name));
}

/*
  Sequence table rename method.
  we will destroy sequence share.
*/
int ha_sequence::rename_table(const char* from, const char* to)
{
  DBUG_ENTER("ha_sequence::rename_table");
  if (get_from_handler_file(from, ha_thd()->mem_root))
    DBUG_RETURN(TRUE);

  destroy_share(from);
  DBUG_RETURN(m_file->ha_rename_table(from, to));
}
/*
  Squence engine error deal method
*/
void ha_sequence::print_error(int error, myf errflag)
{
  THD *thd= ha_thd();
  char *sequence_db= (char *) "???";
  char *sequence_name= (char *) "???";
  DBUG_ENTER("ha_sequence::print_error");

  if (table_share)
  {
    sequence_db= table_share->db.str;
    sequence_name= table_share->table_name.str;
  }
  switch (error) {
    case HA_ERR_SEQUENCE_INVALID:
    {
      my_error(ER_SEQUENCE_INVALID, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
    case HA_ERR_SEQUENCE_RUN_OUT:
    {
      my_error(ER_SEQUENCE_RUN_OUT, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
    /*
      We has reported error using my_error, so this unkown error
      is used to prevent from repeating error definition
     */
    case HA_ERR_SEQUENCE_ACCESS_ERROR:
    {
      if (thd->is_error())
        DBUG_VOID_RETURN;

      my_error(ER_SEQUENCE_ACCESS_ERROR, MYF(0), sequence_db, sequence_name);
      DBUG_VOID_RETURN;
    }
  }
  if (m_file)
    m_file->print_error(error, errflag);
  else
    handler::print_error(error, errflag);

  DBUG_VOID_RETURN;
}
/*
  Sequence engine end.

  SYNOPSIS
    sequence_end()
    p                           handlerton.
    type                        panic type.
  RETURN VALUES
    0           Success
    !=0         Failure
*/
static int sequence_end(handlerton* hton,
                        ha_panic_function type __attribute__((unused)))
{
  DBUG_ENTER("sequence_end");
  if (sequence_inited)
  {
    my_hash_free(&sequence_open_shares);
    mysql_mutex_destroy(&sequence_share_mutex);
  }
  sequence_inited= false;
  DBUG_RETURN(0);
}
/*
  Sequence engine init.

  SYNOPSIS
    sequence_initialize()
    p                           handlerton.
  RETURN VALUES
    0           Success
    !=0         Failure
*/
static int sequence_initialize(void *p)
{
  handlerton *sequence_hton;
  DBUG_ENTER("sequence_initialize");

  sequence_hton= (handlerton *)p;

  sequence_hton->state= SHOW_OPTION_YES;
  sequence_hton->db_type= DB_TYPE_SEQUENCE_DB;
  sequence_hton->create= sequence_create_handler;
  sequence_hton->panic= sequence_end;
  sequence_hton->flags= HTON_HIDDEN
                          | HTON_TEMPORARY_NOT_SUPPORTED
                          | HTON_ALTER_NOT_SUPPORTED
                          | HTON_NO_PARTITION;

  mysql_mutex_init(key_sequence_share_mutex,
                   &sequence_share_mutex,
                   MY_MUTEX_INIT_FAST);
  if (my_hash_init(&sequence_open_shares, system_charset_info,
                   128, 0, 0, (my_hash_get_key) get_sequence_share,
                   (my_hash_free_key) free_sequence_share, HASH_UNIQUE))
  {
    mysql_mutex_destroy(&sequence_share_mutex);
    DBUG_RETURN(1);
  }
  sequence_inited= true;
  DBUG_RETURN(0);
}

/* Sequence meta file ext names */
static const char *ha_sequence_ext[]=
{ NullS };

/*
  Sequence meta data file ext name

  SYNOPSIS
    bas_ext()

  RETURN VALUE
    ext name string array.
*/
const char **ha_sequence::bas_ext() const
{
  DBUG_ENTER("ha_sequence::bas_ext");
  DBUG_RETURN(ha_sequence_ext);
}

uint8 ha_sequence::table_cache_type()
{
  DBUG_ENTER("ha_sequence::table_cache_type");
  DBUG_RETURN(HA_CACHE_TBL_NOCACHE);
}

/* Sequence storage definition */
struct st_mysql_storage_engine sequence_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(sequence)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &sequence_storage_engine,
  "sequence",
  "jianwei.zhao, Aliyun",
  "Sequence Storage Engine Helper",
  PLUGIN_LICENSE_GPL,
  sequence_initialize, /* Plugin Init */
  NULL, /* Plugin Deinit */
  0x0100, /* 1.0 */
  NULL,                       /* status variables                */
  NULL,                       /* system variables                */
  NULL,                       /* config options                  */
  0,                          /* flags                           */
}
mysql_declare_plugin_end;

