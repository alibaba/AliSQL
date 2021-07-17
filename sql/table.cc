/*
   Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/* Some general useful functions */

#include "my_global.h"                          /* NO_EMBEDDED_ACCESS_CHECKS */
#include "sql_priv.h"
#include "unireg.h"                    // REQUIRED: for other includes
#include "table.h"
#include "frm_crypt.h"           // get_crypt_for_frm
#include "key.h"                                // find_ref_key
#include "sql_table.h"                          // build_table_filename,
                                                // primary_key_name
#include "sql_trigger.h"
#include "sql_parse.h"                          // free_items
#include "strfunc.h"                            // unhex_type2
#include "sql_partition.h"       // mysql_unpack_partition,
                                 // fix_partition_func, partition_info
#include "sql_acl.h"             // *_ACL, acl_getroot_no_password
#include "sql_base.h"            // release_table_share
#include "sql_derived.h"
#include <m_ctype.h>
#include "my_md5.h"
#include "my_bit.h"
#include "sql_select.h"
#include "mdl.h"                 // MDL_wait_for_graph_visitor
#include "opt_trace.h"           // opt_trace_disable_if_no_security_...
#include "table_cache.h"         // table_cache_manager
#include "sql_view.h"
#include "debug_sync.h"

/* INFORMATION_SCHEMA name */
LEX_STRING INFORMATION_SCHEMA_NAME= {C_STRING_WITH_LEN("information_schema")};

/* PERFORMANCE_SCHEMA name */
LEX_STRING PERFORMANCE_SCHEMA_DB_NAME= {C_STRING_WITH_LEN("performance_schema")};

/* MYSQL_SCHEMA name */
LEX_STRING MYSQL_SCHEMA_NAME= {C_STRING_WITH_LEN("mysql")};

/* GENERAL_LOG name */
LEX_STRING GENERAL_LOG_NAME= {C_STRING_WITH_LEN("general_log")};

/* SLOW_LOG name */
LEX_STRING SLOW_LOG_NAME= {C_STRING_WITH_LEN("slow_log")};

/* RLI_INFO name */
LEX_STRING RLI_INFO_NAME= {C_STRING_WITH_LEN("slave_relay_log_info")};

/* MI_INFO name */
LEX_STRING MI_INFO_NAME= {C_STRING_WITH_LEN("slave_master_info")};

/* WORKER_INFO name */
LEX_STRING WORKER_INFO_NAME= {C_STRING_WITH_LEN("slave_worker_info")};

	/* Functions defined in this file */

void open_table_error(TABLE_SHARE *share, int error, int db_errno,
                      myf errortype, int errarg);
static int open_binary_frm(THD *thd, TABLE_SHARE *share,
                           uchar *head, File file);
static void fix_type_pointers(const char ***array, TYPELIB *point_to_type,
			      uint types, char **names);
static uint find_field(Field **fields, uchar *record, uint start, uint length);

static Item *create_view_field(THD *thd, TABLE_LIST *view, Item **field_ref,
                               const char *name,
                               Name_resolution_context *context);

inline bool is_system_table_name(const char *name, uint length);

static ulong get_form_pos(File file, uchar *head);

/**************************************************************************
  Object_creation_ctx implementation.
**************************************************************************/

Object_creation_ctx *Object_creation_ctx::set_n_backup(THD *thd)
{
  Object_creation_ctx *backup_ctx;
  DBUG_ENTER("Object_creation_ctx::set_n_backup");

  backup_ctx= create_backup_ctx(thd);
  change_env(thd);

  DBUG_RETURN(backup_ctx);
}

void Object_creation_ctx::restore_env(THD *thd, Object_creation_ctx *backup_ctx)
{
  if (!backup_ctx)
    return;

  backup_ctx->change_env(thd);

  delete backup_ctx;
}

/**************************************************************************
  Default_object_creation_ctx implementation.
**************************************************************************/

Default_object_creation_ctx::Default_object_creation_ctx(THD *thd)
  : m_client_cs(thd->variables.character_set_client),
    m_connection_cl(thd->variables.collation_connection)
{ }

Default_object_creation_ctx::Default_object_creation_ctx(
  const CHARSET_INFO *client_cs, const CHARSET_INFO *connection_cl)
  : m_client_cs(client_cs),
    m_connection_cl(connection_cl)
{ }

Object_creation_ctx *
Default_object_creation_ctx::create_backup_ctx(THD *thd) const
{
  return new Default_object_creation_ctx(thd);
}

void Default_object_creation_ctx::change_env(THD *thd) const
{
  thd->variables.character_set_client= m_client_cs;
  thd->variables.collation_connection= m_connection_cl;

  thd->update_charset();
}

/**************************************************************************
  View_creation_ctx implementation.
**************************************************************************/

View_creation_ctx *View_creation_ctx::create(THD *thd)
{
  View_creation_ctx *ctx= new (thd->mem_root) View_creation_ctx(thd);

  return ctx;
}

/*************************************************************************/

View_creation_ctx * View_creation_ctx::create(THD *thd,
                                              TABLE_LIST *view)
{
  View_creation_ctx *ctx= new (thd->mem_root) View_creation_ctx(thd);

  /* Throw a warning if there is NULL cs name. */

  if (!view->view_client_cs_name.str ||
      !view->view_connection_cl_name.str)
  {
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE,
                        ER_VIEW_NO_CREATION_CTX,
                        ER(ER_VIEW_NO_CREATION_CTX),
                        (const char *) view->db,
                        (const char *) view->table_name);

    ctx->m_client_cs= system_charset_info;
    ctx->m_connection_cl= system_charset_info;

    return ctx;
  }

  /* Resolve cs names. Throw a warning if there is unknown cs name. */

  bool invalid_creation_ctx;

  invalid_creation_ctx= resolve_charset(view->view_client_cs_name.str,
                                        system_charset_info,
                                        &ctx->m_client_cs);

  invalid_creation_ctx= resolve_collation(view->view_connection_cl_name.str,
                                          system_charset_info,
                                          &ctx->m_connection_cl) ||
                        invalid_creation_ctx;

  if (invalid_creation_ctx)
  {
    sql_print_warning("View '%s'.'%s': there is unknown charset/collation "
                      "names (client: '%s'; connection: '%s').",
                      (const char *) view->db,
                      (const char *) view->table_name,
                      (const char *) view->view_client_cs_name.str,
                      (const char *) view->view_connection_cl_name.str);

    push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE,
                        ER_VIEW_INVALID_CREATION_CTX,
                        ER(ER_VIEW_INVALID_CREATION_CTX),
                        (const char *) view->db,
                        (const char *) view->table_name);
  }

  return ctx;
}

/*************************************************************************/

/* Get column name from column hash */

static uchar *get_field_name(Field **buff, size_t *length,
                             my_bool not_used MY_ATTRIBUTE((unused)))
{
  *length= (uint) strlen((*buff)->field_name);
  return (uchar*) (*buff)->field_name;
}


/*
  Returns pointer to '.frm' extension of the file name.

  SYNOPSIS
    fn_rext()
    name       file name

  DESCRIPTION
    Checks file name part starting with the rightmost '.' character,
    and returns it if it is equal to '.frm'. 

  TODO
    It is a good idea to get rid of this function modifying the code
    to garantee that the functions presently calling fn_rext() always
    get arguments in the same format: either with '.frm' or without '.frm'.

  RETURN VALUES
    Pointer to the '.frm' extension. If there is no extension,
    or extension is not '.frm', pointer at the end of file name.
*/

char *fn_rext(char *name)
{
  char *res= strrchr(name, '.');
  if (res && !strcmp(res, reg_ext))
    return res;
  return name + strlen(name);
}

TABLE_CATEGORY get_table_category(const LEX_STRING *db, const LEX_STRING *name)
{
  DBUG_ASSERT(db != NULL);
  DBUG_ASSERT(name != NULL);

  if (is_infoschema_db(db->str, db->length))
    return TABLE_CATEGORY_INFORMATION;

  if ((db->length == PERFORMANCE_SCHEMA_DB_NAME.length) &&
      (my_strcasecmp(system_charset_info,
                     PERFORMANCE_SCHEMA_DB_NAME.str,
                     db->str) == 0))
    return TABLE_CATEGORY_PERFORMANCE;

  if ((db->length == MYSQL_SCHEMA_NAME.length) &&
      (my_strcasecmp(system_charset_info,
                     MYSQL_SCHEMA_NAME.str,
                     db->str) == 0))
  {
    if (is_system_table_name(name->str, name->length))
      return TABLE_CATEGORY_SYSTEM;

    if ((name->length == GENERAL_LOG_NAME.length) &&
        (my_strcasecmp(system_charset_info,
                       GENERAL_LOG_NAME.str,
                       name->str) == 0))
      return TABLE_CATEGORY_LOG;

    if ((name->length == SLOW_LOG_NAME.length) &&
        (my_strcasecmp(system_charset_info,
                       SLOW_LOG_NAME.str,
                       name->str) == 0))
      return TABLE_CATEGORY_LOG;

    if ((name->length == RLI_INFO_NAME.length) &&
        (my_strcasecmp(system_charset_info,
                      RLI_INFO_NAME.str,
                      name->str) == 0))
      return TABLE_CATEGORY_RPL_INFO;

    if ((name->length == MI_INFO_NAME.length) &&
        (my_strcasecmp(system_charset_info,
                      MI_INFO_NAME.str,
                      name->str) == 0))
      return TABLE_CATEGORY_RPL_INFO;

    if ((name->length == WORKER_INFO_NAME.length) &&
        (my_strcasecmp(system_charset_info,
                      WORKER_INFO_NAME.str,
                      name->str) == 0))
      return TABLE_CATEGORY_RPL_INFO;
  }

  return TABLE_CATEGORY_USER;
}


/*
  Allocate a setup TABLE_SHARE structure

  SYNOPSIS
    alloc_table_share()
    TABLE_LIST		Take database and table name from there
    key			Table cache key (db \0 table_name \0...)
    key_length		Length of key

  RETURN
    0  Error (out of memory)
    #  Share
*/

TABLE_SHARE *alloc_table_share(TABLE_LIST *table_list, const char *key,
                               uint key_length)
{
  MEM_ROOT mem_root; //wangyang �ṹ�� ��ʼ�� ��һ���յĶ���, ������ NULL
  TABLE_SHARE *share;  //�����ʼ����� ��ַ����ʹ��
  char *key_buff, *path_buff;
  char path[FN_REFLEN];
  uint path_length;
  Table_cache_element **cache_element_array;
  DBUG_ENTER("alloc_table_share");
  DBUG_PRINT("enter", ("table: '%s'.'%s'",
                       table_list->db, table_list->table_name));

  path_length= build_table_filename(path, sizeof(path) - 1,
                                    table_list->db,
                                    table_list->table_name, "", 0);
  /**
   * wangyang @@@ table_share ����������ʼ�� mem_root
   * ��ʼ�� mem_root
   */
  init_sql_alloc(&mem_root, TABLE_ALLOC_BLOCK_SIZE, 0);
  /**
   * wangyang ��������� ������ʼ�������ǵ�������ķ���
   * ���淽���������� ��Ӧ��ָ����г�ʼ���� ���Կ������������� ���س�ʼ�� ����һ��������ָ�룬
   * �ڶ��������� ��Ӧ���ڴ�Ĵ�С
   */
  if (multi_alloc_root(&mem_root,
                       &share, sizeof(*share), //share �Ǳ�ʾ   table_share ����
                       &key_buff, key_length, // key_buf �Ǳ�ʾ key(��ʾһ�ű��key) �ĳ���
                       &path_buff, path_length + 1, // ��ʾ ���·����Ϣ
                       &cache_element_array,
                       table_cache_instances * sizeof(*cache_element_array),
                       NULL))
  {

      /*
       # include <string.h>
void *memset(void *s, int c, unsigned long n);
�����Ĺ����ǣ���ָ����� s ��ָ���ǰ n �ֽڵ��ڴ浥Ԫ��һ���������� c �滻��ע�� c �� int �͡�s �� void* �͵�ָ�����������������Ϊ�κ����͵����ݽ��г�ʼ����
       */
    //����������� �� share ָ��ָ��� �ڴ�ȫ����ʼ��Ϊ0
    memset(share, 0, sizeof(*share));

    share->set_table_cache_key(key_buff, key, key_length);

    share->path.str= path_buff;
    share->path.length= path_length;
    strmov(share->path.str, path);
    share->normalized_path.str=    share->path.str;
    share->normalized_path.length= path_length;

    share->version=       refresh_version;

    /*
      Since alloc_table_share() can be called without any locking (for
      example, ha_create_table... functions), we do not assign a table
      map id here.  Instead we assign a value that is not used
      elsewhere, and then assign a table map id inside open_table()
      under the protection of the LOCK_open mutex.
    */
    share->table_map_id= ~0ULL;
    share->cached_row_logging_check= -1;

    share->m_flush_tickets.empty();

    /**
     * �������ڳ�ʼ�� cache_element_array ��ַ��Ӧ���ڴ棬����Ϊ len
     * ȫ����ʼ��Ϊ0
     */
    memset(cache_element_array, 0,
           table_cache_instances * sizeof(*cache_element_array));
    share->cache_element= cache_element_array;

    /**
     * wangyang �� �ڶ�������ָ����ڴ� ָ�� ��һ������ָ����ڴ�
     *
     * �ڴ濽�� �� �ֲ����� &mem_root ��Ӧ���ڴ� ���Ƶ� share �ж�Ӧ�� mem_root��
     * ��Ӧ�ĵ�ַ share �� mem_root �ǽṹ�壬��ֵ����null �� ���� �սṹ��
     */
    memcpy((char*) &share->mem_root, (char*) &mem_root, sizeof(mem_root));
    mysql_mutex_init(key_TABLE_SHARE_LOCK_ha_data,
                     &share->LOCK_ha_data, MY_MUTEX_INIT_FAST);
  }
  DBUG_RETURN(share);
}


/*
  Initialize share for temporary tables

  SYNOPSIS
    init_tmp_table_share()
    thd         thread handle
    share	Share to fill
    key		Table_cache_key, as generated from create_table_def_key.
		must start with db name.    
    key_length	Length of key
    table_name	Table name
    path	Path to file (possible in lower case) without .frm

  NOTES
    This is different from alloc_table_share() because temporary tables
    don't have to be shared between threads or put into the table def
    cache, so we can do some things notable simpler and faster

    If table is not put in thd->temporary_tables (happens only when
    one uses OPEN TEMPORARY) then one can specify 'db' as key and
    use key_length= 0 as neither table_cache_key or key_length will be used).
*/

void init_tmp_table_share(THD *thd, TABLE_SHARE *share, const char *key,
                          uint key_length, const char *table_name,
                          const char *path)
{
  DBUG_ENTER("init_tmp_table_share");
  DBUG_PRINT("enter", ("table: '%s'.'%s'", key, table_name));

  memset(share, 0, sizeof(*share));
  init_sql_alloc(&share->mem_root, TABLE_ALLOC_BLOCK_SIZE, 0);
  share->table_category=         TABLE_CATEGORY_TEMPORARY;
  share->tmp_table=              INTERNAL_TMP_TABLE;
  share->db.str=                 (char*) key;
  share->db.length=		 strlen(key);
  share->table_cache_key.str=    (char*) key;
  share->table_cache_key.length= key_length;
  share->table_name.str=         (char*) table_name;
  share->table_name.length=      strlen(table_name);
  share->path.str=               (char*) path;
  share->normalized_path.str=    (char*) path;
  share->path.length= share->normalized_path.length= strlen(path);
  share->frm_version= 		 FRM_VER_TRUE_VARCHAR;

  share->cached_row_logging_check= -1;

  /*
    table_map_id is also used for MERGE tables to suppress repeated
    compatibility checks.
  */
  share->table_map_id= (ulonglong) thd->query_id;

  share->m_flush_tickets.empty();

  DBUG_VOID_RETURN;
}

/*
 Aggregate TABLE_SHARE table_stats into global_table_stats.
 assumed that has hold LOCK_open if no_tmp_table.
*/
void TABLE_SHARE::add_table_stats()
{
  if (!rows_read && !rows_changed)
    return;

  if (!table_cache_key.str || !table_name.str)
    return;

  TABLE_STATS* table_stats;
  char key[NAME_LEN * 2 + 2];
  sprintf(key, "%s.%s", table_cache_key.str, table_name.str);

  mysql_mutex_lock(&LOCK_global_table_stats);
  if (!(table_stats = (TABLE_STATS *) my_hash_search(&global_table_stats,
                                                     (uchar*)key,
                                                     strlen(key))))
  {
    if (!(table_stats = ((TABLE_STATS *)
                         my_malloc(sizeof(TABLE_STATS), MYF(MY_WME | MY_ZEROFILL)))))
    {
      sql_print_error("Allocating table stats failed.");
      goto end;
    }
    strncpy(table_stats->table, key, sizeof(table_stats->table));
    table_stats->rows_read= 0;
    table_stats->rows_changed= 0;
    table_stats->rows_changed_x_indexes= 0;
    table_stats->rows_inserted= 0;
    table_stats->rows_deleted= 0;
    table_stats->rows_updated= 0;

    if (my_hash_insert(&global_table_stats, (uchar *) table_stats))
    {
      sql_print_error("Inserting table stats failed.");
      my_free((char *) table_stats);
      goto end;
    }
  }

  table_stats->rows_read+= rows_read;
  table_stats->rows_changed+= rows_changed;
  table_stats->rows_inserted+= rows_inserted;
  table_stats->rows_deleted+= rows_deleted;
  table_stats->rows_updated+= rows_updated;
  table_stats->rows_changed_x_indexes+= rows_changed_x_indexes;

  rows_read= 0;
  rows_changed= 0;
  rows_inserted= 0;
  rows_deleted= 0;
  rows_updated= 0;
  rows_changed_x_indexes= 0;

end:
  mysql_mutex_unlock(&LOCK_global_table_stats);
}

/*
  Aggregate TABLE_SHARE index_stats into global_index_stats.
  assumed that has hold LOCK_open if no_tmp_table.
*/
void TABLE_SHARE::add_index_stats()
{
  INDEX_STATS *index_stats;
  char key[NAME_LEN * 3 + 3];

  if (!table_cache_key.str || !table_name.str)
    return;

  for (uint x= 0; x < keys; x++)
  {
    if (index_rows_read[x])
    {
      KEY *info= key_info+x;
      sprintf(key, "%s.%s.%s", table_cache_key.str, table_name.str, info->name);

      mysql_mutex_lock(&LOCK_global_index_stats);
      if (!(index_stats = (INDEX_STATS *) my_hash_search(&global_index_stats,
                                                         (uchar *) key,
                                                         strlen(key))))
      {
        if (!(index_stats = ((INDEX_STATS *)
                             my_malloc(sizeof(INDEX_STATS), MYF(MY_WME | MY_ZEROFILL)))))
        {
          sql_print_error("Allocating index stats failed.");
          goto end;
        }
        strncpy(index_stats->index, key, sizeof(index_stats->index));
        index_stats->rows_read= 0;

        if (my_hash_insert(&global_index_stats, (uchar *) index_stats))
        {
          sql_print_error("Inserting index stats failed.");
          my_free((char *) index_stats);
          goto end;
        }
      }
      index_stats->rows_read+= index_rows_read[x];
      index_rows_read[x]= 0;
    end:
      mysql_mutex_unlock(&LOCK_global_index_stats);
    }
  }
}

/**
  Release resources (plugins) used by the share and free its memory.
  TABLE_SHARE is self-contained -- it's stored in its own MEM_ROOT.
  Free this MEM_ROOT.
*/

void TABLE_SHARE::destroy()
{
  uint idx;
  KEY *info_it;

  DBUG_ENTER("TABLE_SHARE::destroy");
  DBUG_PRINT("info", ("db: %s table: %s", db.str, table_name.str));
  if (ha_share)
  {
    delete ha_share;
    ha_share= NULL;
  }
  /* The mutex is initialized only for shares that are part of the TDC */
  if (tmp_table == NO_TMP_TABLE)
    mysql_mutex_destroy(&LOCK_ha_data);
  my_hash_free(&name_hash);

  plugin_unlock(NULL, db_plugin);
  db_plugin= NULL;

  /* Release fulltext parsers */
  info_it= key_info;
  for (idx= keys; idx; idx--, info_it++)
  {
    if (info_it->flags & HA_USES_PARSER)
    {
      plugin_unlock(NULL, info_it->parser);
      info_it->flags= 0;
    }
  }

#ifdef HAVE_PSI_TABLE_INTERFACE
  PSI_TABLE_CALL(release_table_share)(m_psi);
#endif

  /*
    Make a copy since the share is allocated in its own root,
    and free_root() updates its argument after freeing the memory.
  */
  MEM_ROOT own_root= mem_root;
  free_root(&own_root, MYF(0));
  DBUG_VOID_RETURN;
}

/*
  Free table share and memory used by it

  SYNOPSIS
    free_table_share()
    share		Table share
*/

void free_table_share(TABLE_SHARE *share)
{
  DBUG_ENTER("free_table_share");
  DBUG_PRINT("enter", ("table: %s.%s", share->db.str, share->table_name.str));
  DBUG_ASSERT(share->ref_count == 0);

  if (share->m_flush_tickets.is_empty())
  {
    /*
      No threads are waiting for this share to be flushed (the
      share is not old, is for a temporary table, or just nobody
      happens to be waiting for it). Destroy it.
    */
    share->destroy();
  }
  else
  {
    Wait_for_flush_list::Iterator it(share->m_flush_tickets);
    Wait_for_flush *ticket;
    /*
      We're about to iterate over a list that is used
      concurrently. Make sure this never happens without a lock.
    */
    mysql_mutex_assert_owner(&LOCK_open);

    while ((ticket= it++))
      (void) ticket->get_ctx()->m_wait.set_status(MDL_wait::GRANTED);
    /*
      If there are threads waiting for this share to be flushed,
      the last one to receive the notification will destroy the
      share. At this point the share is removed from the table
      definition cache, so is OK to proceed here without waiting
      for this thread to do the work.
    */
  }
  DBUG_VOID_RETURN;
}


/**
  Return TRUE if a table name matches one of the system table names.
  Currently these are:

  help_category, help_keyword, help_relation, help_topic,
  proc, event
  time_zone, time_zone_leap_second, time_zone_name, time_zone_transition,
  time_zone_transition_type

  This function trades accuracy for speed, so may return false
  positives. Presumably mysql.* database is for internal purposes only
  and should not contain user tables.
*/

inline bool is_system_table_name(const char *name, uint length)
{
  CHARSET_INFO *ci= system_charset_info;

  return (
           /* mysql.proc table */
           (length == 4 &&
             my_tolower(ci, name[0]) == 'p' && 
             my_tolower(ci, name[1]) == 'r' &&
             my_tolower(ci, name[2]) == 'o' &&
             my_tolower(ci, name[3]) == 'c') ||

           (length > 4 &&
             (
               /* one of mysql.help* tables */
               (my_tolower(ci, name[0]) == 'h' &&
                 my_tolower(ci, name[1]) == 'e' &&
                 my_tolower(ci, name[2]) == 'l' &&
                 my_tolower(ci, name[3]) == 'p') ||

               /* one of mysql.time_zone* tables */
               (my_tolower(ci, name[0]) == 't' &&
                 my_tolower(ci, name[1]) == 'i' &&
                 my_tolower(ci, name[2]) == 'm' &&
                 my_tolower(ci, name[3]) == 'e') ||

               /* mysql.event table */
               (my_tolower(ci, name[0]) == 'e' &&
                 my_tolower(ci, name[1]) == 'v' &&
                 my_tolower(ci, name[2]) == 'e' &&
                 my_tolower(ci, name[3]) == 'n' &&
                 my_tolower(ci, name[4]) == 't')
             )
           )
         );
}


/**
  Check if a string contains path elements
*/  

static inline bool has_disabled_path_chars(const char *str)
{
  for (; *str; str++)
    switch (*str)
    {
      case FN_EXTCHAR:
      case '/':
      case '\\':
      case '~':
      case '@':
        return TRUE;
    }
  return FALSE;
}


/*
  Read table definition from a binary / text based .frm file
  
  SYNOPSIS
  open_table_def()
  thd		Thread handler
  share		Fill this with table definition
  db_flags	Bit mask of the following flags: OPEN_VIEW

  NOTES
    This function is called when the table definition is not cached in
    table_def_cache
    The data is returned in 'share', which is alloced by
    alloc_table_share().. The code assumes that share is initialized.

  RETURN VALUES
   0	ok
   1	Error (see open_table_error)
   2    Error (see open_table_error)
   3    Wrong data in .frm file
   4    Error (see open_table_error)
   5    Error (see open_table_error: charset unavailable)
   6    Unknown .frm version
   8    Error while reading view definition from .FRM file.
   9    Wrong type in view's .frm file.
*/

int open_table_def(THD *thd, TABLE_SHARE *share, uint db_flags)
{
  int error, table_type;
  bool error_given;
  File file;
  uchar head[64];
  char	path[FN_REFLEN];
  MEM_ROOT **root_ptr, *old_root;
  DBUG_ENTER("open_table_def");
  DBUG_PRINT("enter", ("table: '%s'.'%s'  path: '%s'", share->db.str,
                       share->table_name.str, share->normalized_path.str));

  error= 1;
  error_given= 0;

  strxmov(path, share->normalized_path.str, reg_ext, NullS);
  if ((file= mysql_file_open(key_file_frm,
                             path, O_RDONLY | O_SHARE, MYF(0))) < 0)
  {
    /*
      We don't try to open 5.0 unencoded name, if
      - non-encoded name contains '@' signs, 
        because '@' can be misinterpreted.
        It is not clear if '@' is escape character in 5.1,
        or a normal character in 5.0.
        
      - non-encoded db or table name contain "#mysql50#" prefix.
        This kind of tables must have been opened only by the
        mysql_file_open() above.
    */
    if (has_disabled_path_chars(share->table_name.str) ||
        has_disabled_path_chars(share->db.str) ||
        !strncmp(share->db.str, MYSQL50_TABLE_NAME_PREFIX,
                 MYSQL50_TABLE_NAME_PREFIX_LENGTH) ||
        !strncmp(share->table_name.str, MYSQL50_TABLE_NAME_PREFIX,
                 MYSQL50_TABLE_NAME_PREFIX_LENGTH))
      goto err_not_open;

    /* Try unencoded 5.0 name */
    uint length;
    strxnmov(path, sizeof(path)-1,
             mysql_data_home, "/", share->db.str, "/",
             share->table_name.str, reg_ext, NullS);
    length= unpack_filename(path, path) - reg_ext_length;
    /*
      The following is a safety test and should never fail
      as the old file name should never be longer than the new one.
    */
    DBUG_ASSERT(length <= share->normalized_path.length);
    /*
      If the old and the new names have the same length,
      then table name does not have tricky characters,
      so no need to check the old file name.
    */
    if (length == share->normalized_path.length ||
        ((file= mysql_file_open(key_file_frm,
                                path, O_RDONLY | O_SHARE, MYF(0))) < 0))
      goto err_not_open;

    /* Unencoded 5.0 table name found */
    path[length]= '\0'; // Remove .frm extension
    strmov(share->normalized_path.str, path);
    share->normalized_path.length= length;
  }

  error= 4;
  if (mysql_file_read(file, head, 64, MYF(MY_NABP)))
    goto err;

  if (head[0] == (uchar) 254 && head[1] == 1)
  {
    if (head[2] == FRM_VER || head[2] == FRM_VER+1 ||
        (head[2] >= FRM_VER+3 && head[2] <= FRM_VER+4))
    {
      /* Open view only */
      if (db_flags & OPEN_VIEW_ONLY)
      {
        error_given= 1;
        goto err;
      }
      table_type= 1;
    }
    else
    {
      error= 6;                                 // Unkown .frm version
      goto err;
    }
  }
  else if (memcmp(head, STRING_WITH_LEN("TYPE=")) == 0)
  {
    error= 5;
    if (memcmp(head+5, "VIEW", 4) == 0)
    {
      share->is_view= 1;
      if (db_flags & OPEN_VIEW)
        table_type= 2;
      else
        goto err;
    }
    else
      goto err;
  }
  else
    goto err;

  if (table_type == 1)
  {
    root_ptr= my_pthread_getspecific_ptr(MEM_ROOT**, THR_MALLOC);
    old_root= *root_ptr;
    *root_ptr= &share->mem_root;
    error= open_binary_frm(thd, share, head, file); //wangyang @@@ �����򿪶������ļ� ������ʼ�� table_share ����
    *root_ptr= old_root;
    error_given= 1;
  }
  else if (table_type == 2)
  {
    LEX_STRING pathstr= { path, strlen(path) };

    /*
      Create view file parser and hold it in TABLE_SHARE member
      view_def.
      */ 
    share->view_def= sql_parse_prepare(&pathstr, &share->mem_root, true);
    if (!share->view_def)
      error= 8;
    else if (!is_equal(&view_type, share->view_def->type()))
      error= 9;
    else
      error= 0;
  }

  share->table_category= get_table_category(& share->db, & share->table_name);

  if (!error)
    thd->status_var.opened_shares++;

err:
  mysql_file_close(file, MYF(MY_WME));

err_not_open:
  if (error && !error_given)
  {
    share->error= error;
    open_table_error(share, error, (share->open_errno= my_errno), 0);
  }

  DBUG_RETURN(error);
}


/**
  Initialize key_part_flag from source field.
*/

void KEY_PART_INFO::init_flags()
{
  DBUG_ASSERT(field);
  if (field->type() == MYSQL_TYPE_BLOB ||
      field->type() == MYSQL_TYPE_GEOMETRY)
    key_part_flag|= HA_BLOB_PART;
  else if (field->real_type() == MYSQL_TYPE_VARCHAR)
    key_part_flag|= HA_VAR_LENGTH_PART;
  else if (field->type() == MYSQL_TYPE_BIT)
    key_part_flag|= HA_BIT_PART;
}


/**
  Initialize KEY_PART_INFO from the given field.

  @param fld The field to initialize keypart from
*/

void KEY_PART_INFO::init_from_field(Field *fld)
{
  field= fld;
  fieldnr= field->field_index + 1;
  null_bit= field->null_bit;
  null_offset= field->null_offset();
  offset= field->offset(field->table->record[0]);
  length= (uint16) field->key_length();
  store_length= length;
  key_part_flag= 0;

  if (field->real_maybe_null())
    store_length+= HA_KEY_NULL_LENGTH;
  if (field->type() == MYSQL_TYPE_BLOB ||
      field->real_type() == MYSQL_TYPE_VARCHAR ||
      field->type() == MYSQL_TYPE_GEOMETRY)
  {
    store_length+= HA_KEY_BLOB_LENGTH;
  }
  init_flags();

  type=  (uint8) field->key_type();
  key_type =
    ((ha_base_keytype) type == HA_KEYTYPE_TEXT ||
     (ha_base_keytype) type == HA_KEYTYPE_VARTEXT1 ||
     (ha_base_keytype) type == HA_KEYTYPE_VARTEXT2) ?
    0 : FIELDFLAG_BINARY;
}


/**
  Setup key-related fields of Field object for given key and key part.

  @param[in]     share         Pointer to TABLE_SHARE
  @param[in]     handler       Pointer to handler
  @param[in]     primary_key_n Primary key number
  @param[in]     keyinfo       Pointer to processed key
  @param[in]     key_n         Processed key number
  @param[in]     key_part_n    Processed key part number
  @param[in,out] usable_parts  Pointer to usable_parts variable
*/

static void setup_key_part_field(TABLE_SHARE *share, handler *handler_file,
                                 uint primary_key_n, KEY *keyinfo, uint key_n,
                                 uint key_part_n, uint *usable_parts)
{
  KEY_PART_INFO *key_part= &keyinfo->key_part[key_part_n];
  Field *field= key_part->field;

  /* TokuDB: Flag field as unique and/or clustering if it is the only keypart in a
             unique/clustering index */
  if (key_part_n == 0 && key_n != primary_key_n)
  {
    field->flags |= (((keyinfo->flags & HA_NOSAME) &&
                      (keyinfo->user_defined_key_parts == 1)) ?
                     UNIQUE_KEY_FLAG : MULTIPLE_KEY_FLAG);

    if (((keyinfo->flags & HA_CLUSTERING) &&
         (keyinfo->user_defined_key_parts == 1)))
      field->flags|= CLUSTERING_FLAG;
  }

  if (key_part_n == 0)
    field->key_start.set_bit(key_n);
  if (field->key_length() == key_part->length &&
      !(field->flags & BLOB_FLAG))
  {
    if (handler_file->index_flags(key_n, key_part_n, 0) & HA_KEYREAD_ONLY)
    {
      share->keys_for_keyread.set_bit(key_n);
      field->part_of_key.set_bit(key_n);
      field->part_of_key_not_clustered.set_bit(key_n);
    }
    if (handler_file->index_flags(key_n, key_part_n, 1) & HA_READ_ORDER)
      field->part_of_sortkey.set_bit(key_n);
  }

  if (!(key_part->key_part_flag & HA_REVERSE_SORT) &&
      *usable_parts == key_part_n)
    (*usable_parts)++;			// For FILESORT
}


/**
  Generate extended secondary keys by adding primary key parts to the
  existing secondary key. A primary key part is added if such part doesn't
  present in the secondary key or the part in the secondary key is a
  prefix of the key field. Key parts are added till:
  .) all parts were added
  .) number of key parts became bigger that MAX_REF_PARTS
  .) total key length became longer than MAX_REF_LENGTH
  depending on what occurs first first.
  Unlike existing secondary key parts which are initialized at
  open_binary_frm(), newly added ones are initialized here by copying
  KEY_PART_INFO structure from primary key part and calling
  setup_key_part_field().

  Function updates sk->actual/unused_key_parts and sk->actual_flags.

  @param[in]     sk            Secondary key
  @param[in]     sk_n          Secondary key number
  @param[in]     pk            Primary key
  @param[in]     pk_n          Primary key number
  @param[in]     share         Pointer to TABLE_SHARE
  @param[in]     handler       Pointer to handler
  @param[in,out] usable_parts  Pointer to usable_parts variable

  @retval                      Number of added key parts
*/

static uint add_pk_parts_to_sk(KEY *sk, uint sk_n, KEY *pk, uint pk_n,
                               TABLE_SHARE *share, handler *handler_file,
                               uint *usable_parts)
{
  uint max_key_length= sk->key_length;
  bool is_unique_key= false;
  KEY_PART_INFO *current_key_part= &sk->key_part[sk->user_defined_key_parts];
  ulong *current_rec_per_key= &sk->rec_per_key[sk->user_defined_key_parts];

  /* 
     For each keypart in the primary key: check if the keypart is
     already part of the secondary key and add it if not.
  */
  for (uint pk_part= 0; pk_part < pk->user_defined_key_parts; pk_part++)
  {
    KEY_PART_INFO *pk_key_part= &pk->key_part[pk_part];
    /* MySQL does not supports more key parts than MAX_REF_LENGTH */
    if (sk->actual_key_parts >= MAX_REF_PARTS)
      goto end;

    bool pk_field_is_in_sk= false;
    for (uint j= 0; j < sk->user_defined_key_parts; j++)
    {
      if (sk->key_part[j].fieldnr == pk_key_part->fieldnr &&
          share->field[pk_key_part->fieldnr - 1]->key_length() ==
          sk->key_part[j].length)
      {
        pk_field_is_in_sk= true;
        break;
      }
    }

    /* Add PK field to secondary key if it's not already  part of the key. */
    if (!pk_field_is_in_sk)
    {
      /* MySQL does not supports keys longer than MAX_KEY_LENGTH */
      if (max_key_length + pk_key_part->length > MAX_KEY_LENGTH)
        goto end;

      *current_key_part= *pk_key_part;
      setup_key_part_field(share, handler_file, pk_n, sk, sk_n,
                           sk->actual_key_parts, usable_parts);
      *current_rec_per_key++= 0;
      sk->actual_key_parts++;
      sk->unused_key_parts--;
      current_key_part++;
      max_key_length+= pk_key_part->length;
      /*
        Secondary key will be unique if the key  does not exceed
        key length limitation and key parts limitation.
      */
      is_unique_key= true;
    }
  }
  if (is_unique_key)
    sk->actual_flags|= HA_NOSAME;

end:
  return (sk->actual_key_parts - sk->user_defined_key_parts);
}


/*
  Read data from a binary .frm file from MySQL 3.23 - 5.0 into TABLE_SHARE

  NOTE: Much of the logic here is duplicated in create_tmp_table()
  (see sql_select.cc). Hence, changes to this function may have to be
  repeated there.
*/

static int open_binary_frm(THD *thd, TABLE_SHARE *share, uchar *head,
                           File file)
{
  int error, errarg= 0;
  uint new_frm_ver, field_pack_length, new_field_pack_flag;
  uint interval_count, interval_parts, read_length, int_length;
  uint db_create_options, keys, key_parts, n_length;
  uint key_info_length, com_length, null_bit_pos;
  uint extra_rec_buf_length;
  uint i,j;
  bool use_extended_sk;   // Supported extending of secondary keys with PK parts
  bool use_hash;
  char *keynames, *names, *comment_pos;
  uchar forminfo[288];
  uchar *record;
  uchar *disk_buff, *strpos, *null_flags, *null_pos;
  ulong pos, record_offset, *rec_per_key, rec_buff_length;
  handler *handler_file= 0;
  KEY	*keyinfo;
  KEY_PART_INFO *key_part;
  SQL_CRYPT *crypted=0;
  Field  **field_ptr, *reg_field;
  const char **interval_array;
  enum legacy_db_type legacy_db_type;
  my_bitmap_map *bitmaps;
  uchar *extra_segment_buff= 0;
  const uint format_section_header_size= 8;
  uchar *format_section_fields= 0;
  DBUG_ENTER("open_binary_frm");

  new_field_pack_flag= head[27];
  new_frm_ver= (head[2] - FRM_VER);
  field_pack_length= new_frm_ver < 2 ? 11 : 17;
  disk_buff= 0;

  error= 3;
  /* Position of the form in the form file. */
  if (!(pos= get_form_pos(file, head)))
    goto err;                                   /* purecov: inspected */

  mysql_file_seek(file,pos,MY_SEEK_SET,MYF(0));
  if (mysql_file_read(file, forminfo,288,MYF(MY_NABP)))
    goto err;
  share->frm_version= head[2];
  /*
    Check if .frm file created by MySQL 5.0. In this case we want to
    display CHAR fields as CHAR and not as VARCHAR.
    We do it this way as we want to keep the old frm version to enable
    MySQL 4.1 to read these files.
  */
  if (share->frm_version == FRM_VER_TRUE_VARCHAR -1 && head[33] == 5)
    share->frm_version= FRM_VER_TRUE_VARCHAR;

  legacy_db_type= (enum legacy_db_type) (uint) *(head+3);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (*(head+61) && legacy_db_type == DB_TYPE_PARTITION_DB &&
      !(share->default_part_db_type= 
        ha_checktype(thd, (enum legacy_db_type) (uint) *(head+61), 1, 0)))
    goto err;
  DBUG_PRINT("info", ("default_part_db_type = %u", head[61]));
#endif

  if (legacy_db_type == DB_TYPE_SEQUENCE_DB)
  {
    share->seq_db_type= ha_checktype(thd,
                         (enum legacy_db_type) (uint) *(head+61), 1, 0);
    share->is_sequence= true;
  }
  DBUG_ASSERT(share->db_plugin == NULL);
  /*
    if the storage engine is dynamic, no point in resolving it by its
    dynamically allocated legacy_db_type. We will resolve it later by name.
  */
  if (legacy_db_type > DB_TYPE_UNKNOWN && 
      legacy_db_type < DB_TYPE_FIRST_DYNAMIC)
    share->db_plugin= ha_lock_engine(NULL, 
                                     ha_checktype(thd, legacy_db_type, 0, 0));
  share->db_create_options= db_create_options= uint2korr(head+30);
  share->db_options_in_use= share->db_create_options;
  share->mysql_version= uint4korr(head+51);
  share->null_field_first= 0;
  if (!head[32])				// New frm file in 3.23
  {
    share->avg_row_length= uint4korr(head+34);
    share->row_type= (row_type) head[40];
    share->table_charset= get_charset((((uint) head[41]) << 8) + 
                                        (uint) head[38],MYF(0));
    share->null_field_first= 1;
    share->stats_sample_pages= uint2korr(head+42);
    share->stats_auto_recalc= static_cast<enum_stats_auto_recalc>(head[44]);
  }
  if (!share->table_charset)
  {
    /* unknown charset in head[38] or pre-3.23 frm */
    if (use_mb(default_charset_info))
    {
      /* Warn that we may be changing the size of character columns */
      sql_print_warning("'%s' had no or invalid character set, "
                        "and default character set is multi-byte, "
                        "so character column sizes may have changed",
                        share->path.str);
    }
    share->table_charset= default_charset_info;
  }
  share->db_record_offset= 1;
  /* Set temporarily a good value for db_low_byte_first */
  share->db_low_byte_first= MY_TEST(legacy_db_type != DB_TYPE_ISAM);
  error=4;
  share->max_rows= uint4korr(head+18);
  share->min_rows= uint4korr(head+22);

  /* Read keyinformation */
  key_info_length= (uint) uint2korr(head+28);
  mysql_file_seek(file, (ulong) uint2korr(head+6), MY_SEEK_SET, MYF(0));
  if (read_string(file,(uchar**) &disk_buff,key_info_length))
    goto err;                                   /* purecov: inspected */
  if (disk_buff[0] & 0x80)
  {
    share->keys=      keys=      (disk_buff[1] << 7) | (disk_buff[0] & 0x7f);
    share->key_parts= key_parts= uint2korr(disk_buff+2);
  }
  else
  {
    share->keys=      keys=      disk_buff[0];
    share->key_parts= key_parts= disk_buff[1];
  }
  share->keys_for_keyread.init(0);
  share->keys_in_use.init(keys);
  share->visible_indexes.init(0);

  strpos=disk_buff+6;  

  use_extended_sk=
    ha_check_storage_engine_flag(share->db_type(),
                                 HTON_SUPPORTS_EXTENDED_KEYS);

  uint total_key_parts;
  if (use_extended_sk)
  {
    uint primary_key_parts= keys ?
      (new_frm_ver >= 3) ? (uint) strpos[4] : (uint) strpos[3] : 0;
    total_key_parts= key_parts + primary_key_parts * (keys - 1);
  }
  else
    total_key_parts= key_parts;
  n_length= keys * sizeof(KEY) + total_key_parts * sizeof(KEY_PART_INFO);

  if (!(keyinfo = (KEY*) alloc_root(&share->mem_root,
				    n_length + uint2korr(disk_buff+4))))
    goto err;                                   /* purecov: inspected */
  memset(keyinfo, 0, n_length);
  share->key_info= keyinfo;
  key_part= reinterpret_cast<KEY_PART_INFO*>(keyinfo+keys);

  if (!(rec_per_key= (ulong*) alloc_root(&share->mem_root,
                                         sizeof(ulong) * total_key_parts)))
    goto err;

  for (i=0 ; i < keys ; i++, keyinfo++)
  {
    keyinfo->table= 0;                           // Updated in open_frm
    if (new_frm_ver >= 3)
    {
      keyinfo->flags=	   (uint) uint2korr(strpos) ^ HA_NOSAME;
      /*
        TokuDB: Replace HA_FULLTEXT & HA_SPATIAL with HA_CLUSTERING. This way we
              support TokuDB clustering key definitions without changing the FRM format.
      */
       if (keyinfo->flags & HA_SPATIAL && keyinfo->flags & HA_FULLTEXT)
       {
         if (!ha_check_storage_engine_flag(share->db_type(), HTON_SUPPORTS_CLUSTERED_KEYS))
             goto err;

         keyinfo->flags|= HA_CLUSTERING;
         keyinfo->flags&= ~HA_SPATIAL;
         keyinfo->flags&= ~HA_FULLTEXT;
       }

      /*
         Replace HA_SORT_ALLOWS_SAME with HA_INVISIBLE_KEY. This way we can support
         invisible index without changing the FRM format.
      */
      if (keyinfo->flags & HA_SORT_ALLOWS_SAME)
      {
        keyinfo->flags|= HA_INVISIBLE_KEY;
        keyinfo->flags&= ~HA_SORT_ALLOWS_SAME;
        keyinfo->is_visible= FALSE;
      }
      else
      {
        share->visible_indexes.set_bit(i);
        keyinfo->is_visible= TRUE;
      }

      keyinfo->key_length= (uint) uint2korr(strpos+2);
      keyinfo->user_defined_key_parts= (uint) strpos[4];
      keyinfo->algorithm=  (enum ha_key_alg) strpos[5];
      keyinfo->block_size= uint2korr(strpos+6);
      strpos+=8;
    }
    else
    {
      keyinfo->flags=	 ((uint) strpos[0]) ^ HA_NOSAME;
      keyinfo->key_length= (uint) uint2korr(strpos+1);
      keyinfo->user_defined_key_parts= (uint) strpos[3];
      keyinfo->algorithm= HA_KEY_ALG_UNDEF;
      strpos+=4;
    }

    keyinfo->key_part=	 key_part;
    keyinfo->rec_per_key= rec_per_key;
    for (j=keyinfo->user_defined_key_parts ; j-- ; key_part++)
    {
      *rec_per_key++=0;
      key_part->fieldnr=	(uint16) (uint2korr(strpos) & FIELD_NR_MASK);
      key_part->offset= (uint) uint2korr(strpos+2)-1;
      key_part->key_type=	(uint) uint2korr(strpos+5);
      // key_part->field=	(Field*) 0;	// Will be fixed later
      if (new_frm_ver >= 1)
      {
	key_part->key_part_flag= *(strpos+4);
	key_part->length=	(uint) uint2korr(strpos+7);
	strpos+=9;
      }
      else
      {
	key_part->length=	*(strpos+4);
	key_part->key_part_flag=0;
	if (key_part->length > 128)
	{
	  key_part->length&=127;		/* purecov: inspected */
	  key_part->key_part_flag=HA_REVERSE_SORT; /* purecov: inspected */
	}
	strpos+=7;
      }
      key_part->store_length=key_part->length;
    }
    /*
      Add primary key parts if engine supports primary key extension for
      secondary keys. Here we add unique first key parts to the end of
      secondary key parts array and increase actual number of key parts.
      Note that primary key is always first if exists. Later if there is no
      primary key in the table then number of actual keys parts is set to
      user defined key parts.
    */
    keyinfo->actual_key_parts= keyinfo->user_defined_key_parts;
    keyinfo->actual_flags= keyinfo->flags;
    if (use_extended_sk && i && !(keyinfo->flags & HA_NOSAME))
    {
      const uint primary_key_parts= share->key_info->user_defined_key_parts;
      keyinfo->unused_key_parts= primary_key_parts;
      key_part+= primary_key_parts;
      rec_per_key+= primary_key_parts;
      share->key_parts+= primary_key_parts;
    }
  }
  keynames=(char*) key_part;
  strpos+= (strmov(keynames, (char *) strpos) - keynames)+1;

  //reading index comments
  for (keyinfo= share->key_info, i=0; i < keys; i++, keyinfo++)
  {
    if (keyinfo->flags & HA_USES_COMMENT)
    {
      keyinfo->comment.length= uint2korr(strpos);
      keyinfo->comment.str= strmake_root(&share->mem_root, (char*) strpos+2,
                                         keyinfo->comment.length);
      strpos+= 2 + keyinfo->comment.length;
    } 
    DBUG_ASSERT(MY_TEST(keyinfo->flags & HA_USES_COMMENT) ==
               (keyinfo->comment.length > 0));
  }

  share->reclength = uint2korr((head+16));
  if (*(head+26) == 1)
    share->system= 1;				/* one-record-database */
#ifdef HAVE_CRYPTED_FRM
  else if (*(head+26) == 2)
  {
    crypted= get_crypt_for_frm();
    share->crypted= 1;
  }
#endif

  record_offset= (ulong) (uint2korr(head+6)+
                          ((uint2korr(head+14) == 0xffff ?
                            uint4korr(head+47) : uint2korr(head+14))));
 
  if ((n_length= uint4korr(head+55)))
  {
    /* Read extra data segment */
    uchar *next_chunk, *buff_end;
    DBUG_PRINT("info", ("extra segment size is %u bytes", n_length));
    if (!(extra_segment_buff= (uchar*) my_malloc(n_length, MYF(MY_WME))))
      goto err;
    next_chunk= extra_segment_buff;
    if (mysql_file_pread(file, extra_segment_buff,
                         n_length, record_offset + share->reclength,
                         MYF(MY_NABP)))
    {
      goto err;
    }
    share->connect_string.length= uint2korr(next_chunk);
    if (!(share->connect_string.str= strmake_root(&share->mem_root,
                                                  (char*) next_chunk + 2,
                                                  share->connect_string.
                                                  length)))
    {
      goto err;
    }
    next_chunk+= share->connect_string.length + 2;
    buff_end= extra_segment_buff + n_length;
    if (next_chunk + 2 < buff_end)
    {
      uint str_db_type_length= uint2korr(next_chunk);
      LEX_STRING name;
      name.str= (char*) next_chunk + 2;
      name.length= str_db_type_length;

      plugin_ref tmp_plugin= ha_resolve_by_name(thd, &name, FALSE);
      if (tmp_plugin != NULL && !plugin_equals(tmp_plugin, share->db_plugin))
      {
        if (legacy_db_type > DB_TYPE_UNKNOWN &&
            legacy_db_type < DB_TYPE_FIRST_DYNAMIC &&
            legacy_db_type != ha_legacy_type(
                plugin_data(tmp_plugin, handlerton *)))
        {
          /* bad file, legacy_db_type did not match the name */
          goto err;
        }
        /*
          tmp_plugin is locked with a local lock.
          we unlock the old value of share->db_plugin before
          replacing it with a globally locked version of tmp_plugin
        */
        plugin_unlock(NULL, share->db_plugin);
        share->db_plugin= my_plugin_lock(NULL, &tmp_plugin);
        DBUG_PRINT("info", ("setting dbtype to '%.*s' (%d)",
                            str_db_type_length, next_chunk + 2,
                            ha_legacy_type(share->db_type())));
      }
#ifdef WITH_PARTITION_STORAGE_ENGINE
      else if (str_db_type_length == 9 &&
               !strncmp((char *) next_chunk + 2, "partition", 9))
      {
        /*
          Use partition handler
          tmp_plugin is locked with a local lock.
          we unlock the old value of share->db_plugin before
          replacing it with a globally locked version of tmp_plugin
        */
        /* Check if the partitioning engine is ready */
        if (!plugin_is_ready(&name, MYSQL_STORAGE_ENGINE_PLUGIN))
        {
          error= 8;
          my_error(ER_OPTION_PREVENTS_STATEMENT, MYF(0),
                   "--skip-partition");
          goto err;
        }
        plugin_unlock(NULL, share->db_plugin);
        share->db_plugin= ha_lock_engine(NULL, partition_hton);
        DBUG_PRINT("info", ("setting dbtype to '%.*s' (%d)",
                            str_db_type_length, next_chunk + 2,
                            ha_legacy_type(share->db_type())));
      }
#endif
      else if (!tmp_plugin)
      {
        /* purecov: begin inspected */
        error= 8;
        name.str[name.length]=0;
        my_error(ER_UNKNOWN_STORAGE_ENGINE, MYF(0), name.str);
        goto err;
        /* purecov: end */
      }
      next_chunk+= str_db_type_length + 2;
    }
    if (next_chunk + 5 < buff_end)
    {
      uint32 partition_info_str_len = uint4korr(next_chunk);
#ifdef WITH_PARTITION_STORAGE_ENGINE
      if ((share->partition_info_buffer_size=
             share->partition_info_str_len= partition_info_str_len))
      {
        if (!(share->partition_info_str= (char*)
              memdup_root(&share->mem_root, next_chunk + 4,
                          partition_info_str_len + 1)))
        {
          goto err;
        }
      }
#else
      if (partition_info_str_len)
      {
        DBUG_PRINT("info", ("WITH_PARTITION_STORAGE_ENGINE is not defined"));
        goto err;
      }
#endif
      next_chunk+= 5 + partition_info_str_len;
    }
#if MYSQL_VERSION_ID < 50200
    if (share->mysql_version >= 50106 && share->mysql_version <= 50109)
    {
      /*
         Partition state array was here in version 5.1.6 to 5.1.9, this code
         makes it possible to load a 5.1.6 table in later versions. Can most
         likely be removed at some point in time. Will only be used for
         upgrades within 5.1 series of versions. Upgrade to 5.2 can only be
         done from newer 5.1 versions.
      */
      next_chunk+= 4;
    }
    else
#endif
    if (share->mysql_version >= 50110 && next_chunk < buff_end)
    {
      /* New auto_partitioned indicator introduced in 5.1.11 */
#ifdef WITH_PARTITION_STORAGE_ENGINE
      share->auto_partitioned= *next_chunk;
#endif
      next_chunk++;
    }
    keyinfo= share->key_info;
    for (i= 0; i < keys; i++, keyinfo++)
    {
      if (keyinfo->flags & HA_USES_PARSER)
      {
        LEX_STRING parser_name;
        if (next_chunk >= buff_end)
        {
          DBUG_PRINT("error",
                     ("fulltext key uses parser that is not defined in .frm"));
          goto err;
        }
        parser_name.str= (char*) next_chunk;
        parser_name.length= strlen((char*) next_chunk);
        next_chunk+= parser_name.length + 1;
        keyinfo->parser= my_plugin_lock_by_name(NULL, &parser_name,
                                                MYSQL_FTPARSER_PLUGIN);
        if (! keyinfo->parser)
        {
          my_error(ER_PLUGIN_IS_NOT_LOADED, MYF(0), parser_name.str);
          goto err;
        }
      }
    }
    if (forminfo[46] == (uchar)255)
    {
      //reading long table comment
      if (next_chunk + 2 > buff_end)
      {
          DBUG_PRINT("error",
                     ("long table comment is not defined in .frm"));
          goto err;
      }
      share->comment.length = uint2korr(next_chunk);
      if (! (share->comment.str= strmake_root(&share->mem_root,
             (char*)next_chunk + 2, share->comment.length)))
      {
          goto err;
      }
      next_chunk+= 2 + share->comment.length;
    }

    if (next_chunk + format_section_header_size < buff_end)
    {
      /*
        New extra data segment called "format section" with additional
        table and column properties introduced by MySQL Cluster
        based on 5.1.20

        Table properties:
        TABLESPACE <ts> and STORAGE [DISK|MEMORY]

        Column properties:
        COLUMN_FORMAT [DYNAMIC|FIXED] and STORAGE [DISK|MEMORY]
      */
      DBUG_PRINT("info", ("Found format section"));

      /* header */
      const uint format_section_length= uint2korr(next_chunk);
      const uint format_section_flags= uint4korr(next_chunk+2);
      /* 2 bytes unused */

      if (next_chunk + format_section_length > buff_end)
      {
        DBUG_PRINT("error", ("format section length too long: %u",
                             format_section_length));
        goto err;
      }
      DBUG_PRINT("info", ("format_section_length: %u, format_section_flags: %u",
                          format_section_length, format_section_flags));

      share->default_storage_media=
        (enum ha_storage_media) (format_section_flags & 0x7);

      /* tablespace */
      const char *tablespace=
        (const char*)next_chunk + format_section_header_size;
      const uint tablespace_length= strlen(tablespace);
      if (tablespace_length &&
          !(share->tablespace= strmake_root(&share->mem_root,
                                            tablespace, tablespace_length+1)))
      {
        goto err;
      }
      DBUG_PRINT("info", ("tablespace: '%s'",
                          share->tablespace ? share->tablespace : "<null>"));

      /* pointer to format section for fields */
      format_section_fields=
        next_chunk + format_section_header_size + tablespace_length + 1;

      next_chunk+= format_section_length;
    }
  }
  share->key_block_size= uint2korr(head+62);

  error=4;
  extra_rec_buf_length= uint2korr(head+59);
  rec_buff_length= ALIGN_SIZE(share->reclength + 1 + extra_rec_buf_length);
  share->rec_buff_length= rec_buff_length;
  if (!(record= (uchar *) alloc_root(&share->mem_root,
                                     rec_buff_length)))
    goto err;                                   /* purecov: inspected */
  share->default_values= record;
  if (mysql_file_pread(file, record, (size_t) share->reclength,
                       record_offset, MYF(MY_NABP)))
    goto err;                                   /* purecov: inspected */

  mysql_file_seek(file, pos+288, MY_SEEK_SET, MYF(0));
#ifdef HAVE_CRYPTED_FRM
  if (crypted)
  {
    crypted->decode((char*) forminfo+256,288-256);
    if (sint2korr(forminfo+284) != 0)		// Should be 0
      goto err;                                 // Wrong password
  }
#endif

  share->fields= uint2korr(forminfo+258);
  pos= uint2korr(forminfo+260);   /* Length of all screens */
  n_length= uint2korr(forminfo+268);
  interval_count= uint2korr(forminfo+270);
  interval_parts= uint2korr(forminfo+272);
  int_length= uint2korr(forminfo+274);
  share->null_fields= uint2korr(forminfo+282);
  com_length= uint2korr(forminfo+284);
  if (forminfo[46] != (uchar)255)
  {
    share->comment.length=  (int) (forminfo[46]);
    share->comment.str= strmake_root(&share->mem_root, (char*) forminfo+47,
                                     share->comment.length);
  }

  DBUG_PRINT("info",("i_count: %d  i_parts: %d  index: %d  n_length: %d  int_length: %d  com_length: %d", interval_count,interval_parts, share->keys,n_length,int_length, com_length));

  if (!(field_ptr = (Field **)
	alloc_root(&share->mem_root,
		   (uint) ((share->fields+1)*sizeof(Field*)+
			   interval_count*sizeof(TYPELIB)+
			   (share->fields+interval_parts+
			    keys+3)*sizeof(char *)+
			   (n_length+int_length+com_length)))))
    goto err;                                   /* purecov: inspected */

  share->field= field_ptr;
  read_length=(uint) (share->fields * field_pack_length +
		      pos+ (uint) (n_length+int_length+com_length));
  if (read_string(file,(uchar**) &disk_buff,read_length))
    goto err;                                   /* purecov: inspected */
#ifdef HAVE_CRYPTED_FRM
  if (crypted)
  {
    crypted->decode((char*) disk_buff,read_length);
    delete crypted;
    crypted=0;
  }
#endif
  strpos= disk_buff+pos;

  share->intervals= (TYPELIB*) (field_ptr+share->fields+1);
  interval_array= (const char **) (share->intervals+interval_count);
  names= (char*) (interval_array+share->fields+interval_parts+keys+3);
  if (!interval_count)
    share->intervals= 0;			// For better debugging
  memcpy((char*) names, strpos+(share->fields*field_pack_length),
	 (uint) (n_length+int_length));
  comment_pos= names+(n_length+int_length);
  memcpy(comment_pos, disk_buff+read_length-com_length, com_length);

  fix_type_pointers(&interval_array, &share->fieldnames, 1, &names);
  if (share->fieldnames.count != share->fields)
    goto err;
  fix_type_pointers(&interval_array, share->intervals, interval_count,
		    &names);

  {
    /* Set ENUM and SET lengths */
    TYPELIB *interval;
    for (interval= share->intervals;
         interval < share->intervals + interval_count;
         interval++)
    {
      uint count= (uint) (interval->count + 1) * sizeof(uint);
      if (!(interval->type_lengths= (uint *) alloc_root(&share->mem_root,
                                                        count)))
        goto err;
      for (count= 0; count < interval->count; count++)
      {
        char *val= (char*) interval->type_names[count];
        interval->type_lengths[count]= strlen(val);
      }
      interval->type_lengths[count]= 0;
    }
  }

  if (keynames)
    fix_type_pointers(&interval_array, &share->keynames, 1, &keynames);

 /* Allocate handler */
  if (!(handler_file= get_new_handler(share, thd->mem_root,
                                      share->db_type())))
    goto err;

  if (handler_file->set_ha_share_ref(&share->ha_share))
    goto err;

  record= share->default_values-1;              /* Fieldstart = 1 */
  if (share->null_field_first)
  {
    null_flags= null_pos= (uchar*) record+1;
    null_bit_pos= (db_create_options & HA_OPTION_PACK_RECORD) ? 0 : 1;
    /*
      null_bytes below is only correct under the condition that
      there are no bit fields.  Correct values is set below after the
      table struct is initialized
    */
    share->null_bytes= (share->null_fields + null_bit_pos + 7) / 8;
  }
#ifndef WE_WANT_TO_SUPPORT_VERY_OLD_FRM_FILES
  else
  {
    share->null_bytes= (share->null_fields+7)/8;
    null_flags= null_pos= (uchar*) (record + 1 +share->reclength -
                                    share->null_bytes);
    null_bit_pos= 0;
  }
#endif

  use_hash= share->fields >= MAX_FIELDS_BEFORE_HASH;
  if (use_hash)
    use_hash= !my_hash_init(&share->name_hash,
                            system_charset_info,
                            share->fields,0,0,
                            (my_hash_get_key) get_field_name,0,0);

  for (i=0 ; i < share->fields; i++, strpos+=field_pack_length, field_ptr++)
  {
    uint pack_flag, interval_nr, unireg_type, recpos, field_length;
    enum_field_types field_type;
    const CHARSET_INFO *charset=NULL;
    Field::geometry_type geom_type= Field::GEOM_GEOMETRY;
    LEX_STRING comment;

    if (new_frm_ver >= 3)
    {
      /* new frm file in 4.1 */
      field_length= uint2korr(strpos+3);
      recpos=	    uint3korr(strpos+5);
      pack_flag=    uint2korr(strpos+8);
      unireg_type=  (uint) strpos[10];
      interval_nr=  (uint) strpos[12];
      uint comment_length=uint2korr(strpos+15);
      field_type=(enum_field_types) (uint) strpos[13];

      /* charset and geometry_type share the same byte in frm */
      if (field_type == MYSQL_TYPE_GEOMETRY)
      {
#ifdef HAVE_SPATIAL
	geom_type= (Field::geometry_type) strpos[14];
	charset= &my_charset_bin;
#else
	error= 4;  // unsupported field type
	goto err;
#endif
      }
      else
      {
        uint csid= strpos[14] + (((uint) strpos[11]) << 8);
        if (!csid)
          charset= &my_charset_bin;
        else if (!(charset= get_charset(csid, MYF(0))))
        {
          error= 5; // Unknown or unavailable charset
          errarg= (int) csid;
          goto err;
        }
      }
      if (!comment_length)
      {
	comment.str= (char*) "";
	comment.length=0;
      }
      else
      {
	comment.str=    (char*) comment_pos;
	comment.length= comment_length;
	comment_pos+=   comment_length;
      }
    }
    else
    {
      field_length= (uint) strpos[3];
      recpos=	    uint2korr(strpos+4),
      pack_flag=    uint2korr(strpos+6);
      pack_flag&=   ~FIELDFLAG_NO_DEFAULT;     // Safety for old files
      unireg_type=  (uint) strpos[8];
      interval_nr=  (uint) strpos[10];

      /* old frm file */
      field_type= (enum_field_types) f_packtype(pack_flag);
      if (f_is_binary(pack_flag))
      {
        /*
          Try to choose the best 4.1 type:
          - for 4.0 "CHAR(N) BINARY" or "VARCHAR(N) BINARY" 
            try to find a binary collation for character set.
          - for other types (e.g. BLOB) just use my_charset_bin. 
        */
        if (!f_is_blob(pack_flag))
        {
          // 3.23 or 4.0 string
          if (!(charset= get_charset_by_csname(share->table_charset->csname,
                                               MY_CS_BINSORT, MYF(0))))
            charset= &my_charset_bin;
        }
        else
          charset= &my_charset_bin;
      }
      else
        charset= share->table_charset;
      memset(&comment, 0, sizeof(comment));
    }

    if (interval_nr && charset->mbminlen > 1)
    {
      /* Unescape UCS2 intervals from HEX notation */
      TYPELIB *interval= share->intervals + interval_nr - 1;
      unhex_type2(interval);
    }
    
#ifndef TO_BE_DELETED_ON_PRODUCTION
    if (field_type == MYSQL_TYPE_NEWDECIMAL && !share->mysql_version)
    {
      /*
        Fix pack length of old decimal values from 5.0.3 -> 5.0.4
        The difference is that in the old version we stored precision
        in the .frm table while we now store the display_length
      */
      uint decimals= f_decimals(pack_flag);
      field_length= my_decimal_precision_to_length(field_length,
                                                   decimals,
                                                   f_is_dec(pack_flag) == 0);
      sql_print_error("Found incompatible DECIMAL field '%s' in %s; "
                      "Please do \"ALTER TABLE `%s` FORCE\" to fix it!",
                      share->fieldnames.type_names[i], share->table_name.str,
                      share->table_name.str);
      push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN,
                          ER_CRASHED_ON_USAGE,
                          "Found incompatible DECIMAL field '%s' in %s; "
                          "Please do \"ALTER TABLE `%s` FORCE\" to fix it!",
                          share->fieldnames.type_names[i],
                          share->table_name.str,
                          share->table_name.str);
      share->crashed= 1;                        // Marker for CHECK TABLE
    }
#endif

    *field_ptr= reg_field=
      make_field(share, record+recpos,
		 (uint32) field_length,
		 null_pos, null_bit_pos,
		 pack_flag,
		 field_type,
		 charset,
		 geom_type,
		 (Field::utype) MTYP_TYPENR(unireg_type),
		 (interval_nr ?
		  share->intervals+interval_nr-1 :
		  (TYPELIB*) 0),
		 share->fieldnames.type_names[i]);
    if (!reg_field)				// Not supported field type
    {
      error= 4;
      goto err;			/* purecov: inspected */
    }

    reg_field->field_index= i;
    reg_field->comment=comment;
    if (field_type == MYSQL_TYPE_BIT && !f_bit_as_char(pack_flag))
    {
      if ((null_bit_pos+= field_length & 7) > 7)
      {
        null_pos++;
        null_bit_pos-= 8;
      }
    }
    if (!(reg_field->flags & NOT_NULL_FLAG))
    {
      if (!(null_bit_pos= (null_bit_pos + 1) & 7))
        null_pos++;
    }
    if (f_no_default(pack_flag))
      reg_field->flags|= NO_DEFAULT_VALUE_FLAG;

    if (reg_field->unireg_check == Field::NEXT_NUMBER)
      share->found_next_number_field= field_ptr;

    if (use_hash)
      if (my_hash_insert(&share->name_hash, (uchar*) field_ptr) )
      {
        /*
          Set return code 8 here to indicate that an error has
          occurred but that the error message already has been
          sent (OOM).
        */
        error= 8; 
        goto err;
      }

    if (format_section_fields)
    {
      const uchar field_flags= format_section_fields[i];
      const uchar field_storage= (field_flags & STORAGE_TYPE_MASK);
      const uchar field_column_format=
        ((field_flags >> COLUMN_FORMAT_SHIFT)& COLUMN_FORMAT_MASK);
      DBUG_PRINT("debug", ("field flags: %u, storage: %u, column_format: %u",
                           field_flags, field_storage, field_column_format));
      reg_field->set_storage_type((ha_storage_media)field_storage);
      reg_field->set_column_format((column_format_type)field_column_format);
    }
  }
  *field_ptr=0;					// End marker

  /* Fix key->name and key_part->field */
  if (key_parts)
  {
    uint primary_key=(uint) (find_type(primary_key_name, &share->keynames,
                                       FIND_TYPE_NO_PREFIX) - 1);
    longlong ha_option= handler_file->ha_table_flags();
    keyinfo= share->key_info;
    key_part= keyinfo->key_part;

    for (uint key=0 ; key < share->keys ; key++,keyinfo++)
    {
      uint usable_parts= 0;
      keyinfo->name=(char*) share->keynames.type_names[key];
      /* Fix fulltext keys for old .frm files */
      if (share->key_info[key].flags & HA_FULLTEXT)
	share->key_info[key].algorithm= HA_KEY_ALG_FULLTEXT;

      if (primary_key >= MAX_KEY && (keyinfo->flags & HA_NOSAME))
      {
	/*
	  If the UNIQUE key doesn't have NULL columns and is not a part key
	  declare this as a primary key.
	*/
	primary_key=key;
	for (i=0 ; i < keyinfo->user_defined_key_parts ;i++)
	{
          DBUG_ASSERT(key_part[i].fieldnr > 0);
          // Table field corresponding to the i'th key part.
          Field *table_field= share->field[key_part[i].fieldnr - 1];

          /*
            If the key column is of NOT NULL BLOB type, then it
            will definitly have key prefix. And if key part prefix size
            is equal to the BLOB column max size, then we can promote
            it to primary key.
          */
          if (!table_field->real_maybe_null() &&
              table_field->type() == MYSQL_TYPE_BLOB &&
              table_field->field_length == key_part[i].length)
            continue;
          /*
            If the key column is of NOT NULL GEOMETRY type, specifically POINT
            type whose length is known internally (which is 25). And key part
            prefix size is equal to the POINT column max size, then we can
            promote it to primary key.
          */
          if (!table_field->real_maybe_null() &&
              table_field->type() == MYSQL_TYPE_GEOMETRY &&
              table_field->get_geometry_type() == Field::GEOM_POINT &&
              key_part[i].length == MAX_LEN_GEOM_POINT_FIELD)
            continue;

	  if (table_field->real_maybe_null() ||
	      table_field->key_length() != key_part[i].length)
 	  {
	    primary_key= MAX_KEY;		// Can't be used
	    break;
	  }
	}
      }

      for (i=0 ; i < keyinfo->user_defined_key_parts ; key_part++,i++)
      {
        Field *field;
	if (new_field_pack_flag <= 1)
	  key_part->fieldnr= (uint16) find_field(share->field,
                                                 share->default_values,
                                                 (uint) key_part->offset,
                                                 (uint) key_part->length);
	if (!key_part->fieldnr)
        {
          error= 4;                             // Wrong file
          goto err;
        }
        field= key_part->field= share->field[key_part->fieldnr-1];
        key_part->type= field->key_type();
        if (field->real_maybe_null())
        {
          key_part->null_offset=field->null_offset(share->default_values);
          key_part->null_bit= field->null_bit;
          key_part->store_length+=HA_KEY_NULL_LENGTH;
          keyinfo->flags|=HA_NULL_PART_KEY;
          keyinfo->key_length+= HA_KEY_NULL_LENGTH;
        }
        if (field->type() == MYSQL_TYPE_BLOB ||
            field->real_type() == MYSQL_TYPE_VARCHAR ||
            field->type() == MYSQL_TYPE_GEOMETRY)
        {
          key_part->store_length+=HA_KEY_BLOB_LENGTH;
          if (i + 1 <= keyinfo->user_defined_key_parts)
            keyinfo->key_length+= HA_KEY_BLOB_LENGTH;
        }
        key_part->init_flags();

        setup_key_part_field(share, handler_file, primary_key,
                             keyinfo, key, i, &usable_parts);

        field->flags|= PART_KEY_FLAG;
        if (key == primary_key)
        {
          field->flags|= PRI_KEY_FLAG;
          /*
            If this field is part of the primary key and all keys contains
            the primary key, then we can use any key to find this column
          */
          if (ha_option & HA_PRIMARY_KEY_IN_READ_INDEX)
          {
            if (field->key_length() == key_part->length &&
                !(field->flags & BLOB_FLAG))
              field->part_of_key= share->keys_in_use;
            if (field->part_of_sortkey.is_set(key))
              field->part_of_sortkey= share->keys_in_use;
          }
        }
        if (field->key_length() != key_part->length)
        {
#ifndef TO_BE_DELETED_ON_PRODUCTION
          if (field->type() == MYSQL_TYPE_NEWDECIMAL)
          {
            /*
              Fix a fatal error in decimal key handling that causes crashes
              on Innodb. We fix it by reducing the key length so that
              InnoDB never gets a too big key when searching.
              This allows the end user to do an ALTER TABLE to fix the
              error.
            */
            keyinfo->key_length-= (key_part->length - field->key_length());
            key_part->store_length-= (uint16)(key_part->length -
                                              field->key_length());
            key_part->length= (uint16)field->key_length();
            sql_print_error("Found wrong key definition in %s; "
                            "Please do \"ALTER TABLE `%s` FORCE \" to fix it!",
                            share->table_name.str,
                            share->table_name.str);
            push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN,
                                ER_CRASHED_ON_USAGE,
                                "Found wrong key definition in %s; "
                                "Please do \"ALTER TABLE `%s` FORCE\" to fix "
                                "it!",
                                share->table_name.str,
                                share->table_name.str);
            share->crashed= 1;                // Marker for CHECK TABLE
            continue;
          }
#endif
          key_part->key_part_flag|= HA_PART_KEY_SEG;
        }
      }


      if (use_extended_sk && primary_key < MAX_KEY &&
          key && !(keyinfo->flags & HA_NOSAME))
        key_part+= add_pk_parts_to_sk(keyinfo, key, share->key_info, primary_key,
                                      share,  handler_file, &usable_parts);

      /* Skip unused key parts if they exist */
      key_part+= keyinfo->unused_key_parts;

      keyinfo->usable_key_parts= usable_parts; // Filesort

      set_if_bigger(share->max_key_length,keyinfo->key_length+
                    keyinfo->user_defined_key_parts);
      share->total_key_length+= keyinfo->key_length;
      /*
        MERGE tables do not have unique indexes. But every key could be
        an unique index on the underlying MyISAM table. (Bug #10400)
      */
      if ((keyinfo->flags & HA_NOSAME) ||
          (ha_option & HA_ANY_INDEX_MAY_BE_UNIQUE))
        set_if_bigger(share->max_unique_length,keyinfo->key_length);
    }
    if (primary_key < MAX_KEY &&
	(share->keys_in_use.is_set(primary_key)))
    {
      share->primary_key= primary_key;
      /*
	If we are using an integer as the primary key then allow the user to
	refer to it as '_rowid'
      */
      if (share->key_info[primary_key].user_defined_key_parts == 1)
      {
	Field *field= share->key_info[primary_key].key_part[0].field;
	if (field && field->result_type() == INT_RESULT)
        {
          /* note that fieldnr here (and rowid_field_offset) starts from 1 */
	  share->rowid_field_offset= (share->key_info[primary_key].key_part[0].
                                      fieldnr);
        }
      }
    }
    else
      share->primary_key = MAX_KEY; // we do not have a primary key
  }
  else
    share->primary_key= MAX_KEY;
  my_free(disk_buff);
  disk_buff=0;
  if (new_field_pack_flag <= 1)
  {
    /* Old file format with default as not null */
    uint null_length= (share->null_fields+7)/8;
    memset(share->default_values + (null_flags - (uchar*) record), 255,
           null_length);
  }

  if (share->found_next_number_field)
  {
    reg_field= *share->found_next_number_field;
    if ((int) (share->next_number_index= (uint)
	       find_ref_key(share->key_info, share->keys,
                            share->default_values, reg_field,
			    &share->next_number_key_offset,
                            &share->next_number_keypart)) < 0)
    {
      /* Wrong field definition */
      error= 4;
      goto err;
    }
    else
      reg_field->flags |= AUTO_INCREMENT_FLAG;
  }

  if (share->blob_fields)
  {
    Field **ptr;
    uint k, *save;

    /* Store offsets to blob fields to find them fast */
    if (!(share->blob_field= save=
	  (uint*) alloc_root(&share->mem_root,
                             (uint) (share->blob_fields* sizeof(uint)))))
      goto err;
    for (k=0, ptr= share->field ; *ptr ; ptr++, k++)
    {
      if ((*ptr)->flags & BLOB_FLAG)
	(*save++)= k;
    }
  }

  /*
    the correct null_bytes can now be set, since bitfields have been taken
    into account
  */
  share->null_bytes= (null_pos - (uchar*) null_flags +
                      (null_bit_pos + 7) / 8);
  share->last_null_bit_pos= null_bit_pos;

  share->db_low_byte_first= handler_file->low_byte_first();
  share->column_bitmap_size= bitmap_buffer_size(share->fields);

  if (!(bitmaps= (my_bitmap_map*) alloc_root(&share->mem_root,
                                             share->column_bitmap_size)))
    goto err;
  bitmap_init(&share->all_set, bitmaps, share->fields, FALSE);
  bitmap_set_all(&share->all_set);

  delete handler_file;
#ifndef DBUG_OFF
  if (use_hash)
    (void) my_hash_check(&share->name_hash);
#endif
  my_free(extra_segment_buff);
  DBUG_RETURN (0);

 err:
  share->error= error;
  share->open_errno= my_errno;
  share->errarg= errarg;
  my_free(disk_buff);
  my_free(extra_segment_buff);
  delete crypted;
  delete handler_file;
  my_hash_free(&share->name_hash);

  open_table_error(share, error, share->open_errno, errarg);
  DBUG_RETURN(error);
} /* open_binary_frm */


/*
  Open a table based on a TABLE_SHARE

  SYNOPSIS
    open_table_from_share()
    thd			Thread handler
    share		Table definition
    alias       	Alias for table
    db_stat		open flags (for example HA_OPEN_KEYFILE|
    			HA_OPEN_RNDFILE..) can be 0 (example in
                        ha_example_table)
    prgflag   		READ_ALL etc..
    ha_open_flags	HA_OPEN_ABORT_IF_LOCKED etc..
    outparam       	result table
    is_create_table     Indicates that table is opened as part
                        of CREATE or ALTER and does not yet exist in SE

  RETURN VALUES
   0	ok
   1	Error (see open_table_error)
   2    Error (see open_table_error)
   3    Wrong data in .frm file
   4    Error (see open_table_error)
   5    Error (see open_table_error: charset unavailable)
   7    Table definition has changed in engine
*/

int open_table_from_share(THD *thd, TABLE_SHARE *share, const char *alias,
                          uint db_stat, uint prgflag, uint ha_open_flags,
                          TABLE *outparam, bool is_create_table)
{
  int error;
  uint records, i, bitmap_size;
  bool error_reported= FALSE;
  uchar *record, *bitmaps;
  Field **field_ptr;
  Field *fts_doc_id_field= NULL;

  DBUG_ENTER("open_table_from_share");
  DBUG_PRINT("enter",("name: '%s.%s'  form: 0x%lx", share->db.str,
                      share->table_name.str, (long) outparam));

  error= 1;
  memset(outparam, 0, sizeof(*outparam));
  outparam->in_use= thd;
  outparam->s= share;
  outparam->db_stat= db_stat;
  outparam->write_row_record= NULL;

  init_sql_alloc(&outparam->mem_root, TABLE_ALLOC_BLOCK_SIZE, 0);

  if (!(outparam->alias= my_strdup(alias, MYF(MY_WME))))
    goto err;
  outparam->quick_keys.init();
  outparam->possible_quick_keys.init();
  outparam->covering_keys.init();
  outparam->merge_keys.init();
  outparam->keys_in_use_for_query.init();

  /* Allocate handler */
  outparam->file= 0;
  if (!(prgflag & OPEN_FRM_FILE_ONLY))
  {
    if (!(outparam->file= get_new_handler(share, &outparam->mem_root,
                                          share->db_type())))
      goto err;
    if (outparam->file->set_ha_share_ref(&share->ha_share))
      goto err;
  }
  else
  {
    DBUG_ASSERT(!db_stat);
  }

  error= 4;
  outparam->reginfo.lock_type= TL_UNLOCK;
  outparam->current_lock= F_UNLCK;
  records=0;
  if ((db_stat & HA_OPEN_KEYFILE) || (prgflag & DELAYED_OPEN))
    records=1;
  if (prgflag & (READ_ALL+EXTRA_RECORD))
    records++;

  if (!(record= (uchar*) alloc_root(&outparam->mem_root,
                                   share->rec_buff_length * records)))
    goto err;                                   /* purecov: inspected */

  if (records == 0)
  {
    /* We are probably in hard repair, and the buffers should not be used */
    outparam->record[0]= outparam->record[1]= share->default_values;
  }
  else
  {
    outparam->record[0]= record;
    if (records > 1)
      outparam->record[1]= record+ share->rec_buff_length;
    else
      outparam->record[1]= outparam->record[0];   // Safety
  }

  if (!(field_ptr = (Field **) alloc_root(&outparam->mem_root,
                                          (uint) ((share->fields+1)*
                                                  sizeof(Field*)))))
    goto err;                                   /* purecov: inspected */

  outparam->field= field_ptr;

  record= (uchar*) outparam->record[0]-1;	/* Fieldstart = 1 */
  if (share->null_field_first)
    outparam->null_flags= (uchar*) record+1;
  else
    outparam->null_flags= (uchar*) (record+ 1+ share->reclength -
                                    share->null_bytes);

  /* Setup copy of fields from share, but use the right alias and record */
  for (i=0 ; i < share->fields; i++, field_ptr++)
  {
    Field *new_field= share->field[i]->clone(&outparam->mem_root);
    *field_ptr= new_field;
    if (new_field == NULL)
      goto err;
    new_field->init(outparam);
    new_field->move_field_offset((my_ptrdiff_t) (outparam->record[0] -
                                                 outparam->s->default_values));
    /* Check if FTS_DOC_ID column is present in the table */
    if (outparam->file &&
        (outparam->file->ha_table_flags() & HA_CAN_FULLTEXT_EXT) &&
        !strcmp(outparam->field[i]->field_name, FTS_DOC_ID_COL_NAME))
      fts_doc_id_field= new_field;
  }
  (*field_ptr)= 0;                              // End marker

  if (share->found_next_number_field)
    outparam->found_next_number_field=
      outparam->field[(uint) (share->found_next_number_field - share->field)];

  /* Fix key->name and key_part->field */
  if (share->key_parts)
  {
    KEY	*key_info, *key_info_end;
    KEY_PART_INFO *key_part;
    uint n_length;
    n_length= share->keys * sizeof(KEY) +
      share->key_parts * sizeof(KEY_PART_INFO);

    if (!(key_info= (KEY*) alloc_root(&outparam->mem_root, n_length)))
      goto err;
    outparam->key_info= key_info;
    key_part= (reinterpret_cast<KEY_PART_INFO*>(key_info+share->keys));

    memcpy(key_info, share->key_info, sizeof(*key_info)*share->keys);
    memcpy(key_part, share->key_info[0].key_part, (sizeof(*key_part) *
                                                   share->key_parts));

    for (key_info_end= key_info + share->keys ;
         key_info < key_info_end ;
         key_info++)
    {
      KEY_PART_INFO *key_part_end;

      key_info->table= outparam;
      key_info->key_part= key_part;

      for (key_part_end= key_part + key_info->actual_key_parts ;
           key_part < key_part_end ;
           key_part++)
      {
        Field *field= key_part->field= outparam->field[key_part->fieldnr-1];

        if (field->key_length() != key_part->length &&
            !(field->flags & BLOB_FLAG))
        {
          /*
            We are using only a prefix of the column as a key:
            Create a new field for the key part that matches the index
          */
          field= key_part->field=field->new_field(&outparam->mem_root,
                                                  outparam, 0);
          field->field_length= key_part->length;
        }
      }
      /* Skip unused key parts if they exist */
      key_part+= key_info->unused_key_parts;

      /* Set TABLE::fts_doc_id_field for tables with FT KEY */
      if ((key_info->flags & HA_FULLTEXT))
        outparam->fts_doc_id_field= fts_doc_id_field;
    }
  }

#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (share->partition_info_str_len && outparam->file)
  {
  /*
    In this execution we must avoid calling thd->change_item_tree since
    we might release memory before statement is completed. We do this
    by changing to a new statement arena. As part of this arena we also
    set the memory root to be the memory root of the table since we
    call the parser and fix_fields which both can allocate memory for
    item objects. We keep the arena to ensure that we can release the
    free_list when closing the table object.
    SEE Bug #21658
  */

    Query_arena *backup_stmt_arena_ptr= thd->stmt_arena;
    Query_arena backup_arena;
    Query_arena part_func_arena(&outparam->mem_root,
                                Query_arena::STMT_INITIALIZED);
    thd->set_n_backup_active_arena(&part_func_arena, &backup_arena);
    thd->stmt_arena= &part_func_arena;
    bool tmp;
    bool work_part_info_used;

    tmp= mysql_unpack_partition(thd, share->partition_info_str,
                                share->partition_info_str_len,
                                outparam, is_create_table,
                                share->default_part_db_type,
                                &work_part_info_used);
    if (tmp)
    {
      thd->stmt_arena= backup_stmt_arena_ptr;
      thd->restore_active_arena(&part_func_arena, &backup_arena);
      goto partititon_err;
    }
    outparam->part_info->is_auto_partitioned= share->auto_partitioned;
    DBUG_PRINT("info", ("autopartitioned: %u", share->auto_partitioned));
    /*
      We should perform the fix_partition_func in either local or
      caller's arena depending on work_part_info_used value.
    */
    if (!work_part_info_used)
      tmp= fix_partition_func(thd, outparam, is_create_table);
    thd->stmt_arena= backup_stmt_arena_ptr;
    thd->restore_active_arena(&part_func_arena, &backup_arena);
    if (!tmp)
    {
      if (work_part_info_used)
        tmp= fix_partition_func(thd, outparam, is_create_table);
    }
    outparam->part_info->item_free_list= part_func_arena.free_list;
partititon_err:
    if (tmp)
    {
      if (is_create_table)
      {
        /*
          During CREATE/ALTER TABLE it is ok to receive errors here.
          It is not ok if it happens during the opening of an frm
          file as part of a normal query.
        */
        error_reported= TRUE;
      }
      goto err;
    }
  }
#endif

  /* Allocate bitmaps */

  bitmap_size= share->column_bitmap_size;
  if (!(bitmaps= (uchar*) alloc_root(&outparam->mem_root, bitmap_size*3)))
    goto err;
  bitmap_init(&outparam->def_read_set,
              (my_bitmap_map*) bitmaps, share->fields, FALSE);
  bitmap_init(&outparam->def_write_set,
              (my_bitmap_map*) (bitmaps+bitmap_size), share->fields, FALSE);
  bitmap_init(&outparam->tmp_set,
              (my_bitmap_map*) (bitmaps+bitmap_size*2), share->fields, FALSE);
  outparam->default_column_bitmaps();

  /* The table struct is now initialized;  Open the table */
  error= 2;
  if (db_stat)
  {
    int ha_err;
    if ((ha_err= (outparam->file->
                  ha_open(outparam, share->normalized_path.str,
                          (db_stat & HA_READ_ONLY ? O_RDONLY : O_RDWR),
                          (db_stat & HA_OPEN_TEMPORARY ? HA_OPEN_TMP_TABLE :
                           ((db_stat & HA_WAIT_IF_LOCKED) ||
                            (specialflag & SPECIAL_WAIT_IF_LOCKED)) ?
                           HA_OPEN_WAIT_IF_LOCKED :
                           (db_stat & (HA_ABORT_IF_LOCKED | HA_GET_INFO)) ?
                          HA_OPEN_ABORT_IF_LOCKED :
                           HA_OPEN_IGNORE_IF_LOCKED) | ha_open_flags))))
    {
      /* Set a flag if the table is crashed and it can be auto. repaired */
      share->crashed= ((ha_err == HA_ERR_CRASHED_ON_USAGE) &&
                       outparam->file->auto_repair() &&
                       !(ha_open_flags & HA_OPEN_FOR_REPAIR));

      switch (ha_err)
      {
        case HA_ERR_NO_SUCH_TABLE:
	  /*
            The table did not exists in storage engine, use same error message
            as if the .frm file didn't exist
          */
	  error= 1;
	  my_errno= ENOENT;
          break;
        case EMFILE:
	  /*
            Too many files opened, use same error message as if the .frm
            file can't open
           */
          DBUG_PRINT("error", ("open file: %s failed, too many files opened (errno: %d)", 
		  share->normalized_path.str, ha_err));
	  error= 1;
	  my_errno= EMFILE;
          break;
        default:
          outparam->file->print_error(ha_err, MYF(0));
          error_reported= TRUE;
          if (ha_err == HA_ERR_TABLE_DEF_CHANGED)
            error= 7;
          break;
      }
      goto err;                                 /* purecov: inspected */
    }
  }

#if defined(HAVE_purify) && !defined(DBUG_OFF)
  memset(bitmaps, 0, bitmap_size*3);
#endif

  if ((share->table_category == TABLE_CATEGORY_LOG) ||
      (share->table_category == TABLE_CATEGORY_RPL_INFO))
  {
    outparam->no_replicate= TRUE;
  }
  else if (outparam->file)
  {
    handler::Table_flags flags= outparam->file->ha_table_flags();
    outparam->no_replicate= ! MY_TEST(flags & (HA_BINLOG_STMT_CAPABLE
                                               | HA_BINLOG_ROW_CAPABLE))
                            || MY_TEST(flags & HA_HAS_OWN_BINLOGGING);
  }
  else
  {
    outparam->no_replicate= FALSE;
  }

  /* Increment the opened_tables counter, only when open flags set. */
  if (db_stat)
    thd->status_var.opened_tables++;

  DBUG_RETURN (0);

 err:
  if (! error_reported)
    open_table_error(share, error, my_errno, 0);
  delete outparam->file;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (outparam->part_info)
    free_items(outparam->part_info->item_free_list);
#endif
  outparam->file= 0;				// For easier error checking
  outparam->db_stat=0;
  free_root(&outparam->mem_root, MYF(0));
  my_free((void *) outparam->alias);
  DBUG_RETURN (error);
}


/*
  Free information allocated by openfrm

  SYNOPSIS
    closefrm()
    table		TABLE object to free
    free_share		Is 1 if we also want to free table_share
*/

int closefrm(register TABLE *table, bool free_share)
{
  int error=0;
  DBUG_ENTER("closefrm");
  DBUG_PRINT("enter", ("table: 0x%lx", (long) table));

  if (table->db_stat)
    error=table->file->ha_close();
  my_free((void *) table->alias);
  table->alias= 0;
  if (table->field)
  {
    for (Field **ptr=table->field ; *ptr ; ptr++)
      delete *ptr;
    table->field= 0;
  }
  delete table->file;
  table->file= 0;				/* For easier errorchecking */
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (table->part_info)
  {
    /* Allocated through table->mem_root, freed below */
    free_items(table->part_info->item_free_list);
    table->part_info->item_free_list= 0;
    table->part_info= 0;
  }
#endif
  if (free_share)
  {
    if (table->s->tmp_table == NO_TMP_TABLE)
      release_table_share(table->s);
    else
      free_table_share(table->s);
  }
  free_root(&table->mem_root, MYF(0));
  DBUG_RETURN(error);
}


/* Deallocate temporary blob storage */

void free_blobs(register TABLE *table)
{
  uint *ptr, *end;
  for (ptr= table->s->blob_field, end=ptr + table->s->blob_fields ;
       ptr != end ;
       ptr++)
  {
    /*
      Reduced TABLE objects which are used by row-based replication for
      type conversion might have some fields missing. Skip freeing BLOB
      buffers for such missing fields.
    */
    if (table->field[*ptr])
      ((Field_blob*) table->field[*ptr])->free();
  }
}


/**
  Reclaim temporary blob storage which is bigger than 
  a threshold.
 
  @param table A handle to the TABLE object containing blob fields
  @param size The threshold value.
 
*/

void free_field_buffers_larger_than(TABLE *table, uint32 size)
{
  uint *ptr, *end;
  for (ptr= table->s->blob_field, end=ptr + table->s->blob_fields ;
       ptr != end ;
       ptr++)
  {
    Field_blob *blob= (Field_blob*) table->field[*ptr];
    if (blob->get_field_buffer_size() > size)
        blob->free();
  }
}

/**
  Find where a form starts.

  @param head The start of the form file.

  @remark If formname is NULL then only formnames is read.

  @retval The form position.
*/

static ulong get_form_pos(File file, uchar *head)
{
  uchar *pos, *buf;
  uint names, length;
  ulong ret_value=0;
  DBUG_ENTER("get_form_pos");

  names= uint2korr(head+8);

  if (!(names= uint2korr(head+8)))
    DBUG_RETURN(0);

  length= uint2korr(head+4);

  mysql_file_seek(file, 64L, MY_SEEK_SET, MYF(0));

  if (!(buf= (uchar*) my_malloc(length+names*4, MYF(MY_WME))))
    DBUG_RETURN(0);

  if (mysql_file_read(file, buf, length+names*4, MYF(MY_NABP)))
  {
    my_free(buf);
    DBUG_RETURN(0);
  }

  pos= buf+length;
  ret_value= uint4korr(pos);

  my_free(buf);

  DBUG_RETURN(ret_value);
}


/*
  Read string from a file with malloc

  NOTES:
    We add an \0 at end of the read string to make reading of C strings easier
*/

int read_string(File file, uchar**to, size_t length)
{
  DBUG_ENTER("read_string");

  my_free(*to);
  if (!(*to= (uchar*) my_malloc(length+1,MYF(MY_WME))) ||
      mysql_file_read(file, *to, length, MYF(MY_NABP)))
  {
     my_free(*to);                            /* purecov: inspected */
    *to= 0;                                   /* purecov: inspected */
    DBUG_RETURN(1);                           /* purecov: inspected */
  }
  *((char*) *to+length)= '\0';
  DBUG_RETURN (0);
} /* read_string */


	/* Add a new form to a form file */

ulong make_new_entry(File file, uchar *fileinfo, TYPELIB *formnames,
		     const char *newname)
{
  uint i,bufflength,maxlength,n_length,length,names;
  ulong endpos,newpos;
  uchar buff[IO_SIZE];
  uchar *pos;
  DBUG_ENTER("make_new_entry");

  length=(uint) strlen(newname)+1;
  n_length=uint2korr(fileinfo+4);
  maxlength=uint2korr(fileinfo+6);
  names=uint2korr(fileinfo+8);
  newpos=uint4korr(fileinfo+10);

  if (64+length+n_length+(names+1)*4 > maxlength)
  {						/* Expand file */
    newpos+=IO_SIZE;
    int4store(fileinfo+10,newpos);
    /* Copy from file-end */
    endpos= (ulong) mysql_file_seek(file, 0L, MY_SEEK_END, MYF(0));
    bufflength= (uint) (endpos & (IO_SIZE-1));	/* IO_SIZE is a power of 2 */

    while (endpos > maxlength)
    {
      mysql_file_seek(file, (ulong) (endpos-bufflength), MY_SEEK_SET, MYF(0));
      if (mysql_file_read(file, buff, bufflength, MYF(MY_NABP+MY_WME)))
	DBUG_RETURN(0L);
      mysql_file_seek(file, (ulong) (endpos-bufflength+IO_SIZE), MY_SEEK_SET,
                      MYF(0));
      if ((mysql_file_write(file, buff, bufflength, MYF(MY_NABP+MY_WME))))
	DBUG_RETURN(0);
      endpos-=bufflength; bufflength=IO_SIZE;
    }
    memset(buff, 0, IO_SIZE);			/* Null new block */
    mysql_file_seek(file, (ulong) maxlength, MY_SEEK_SET, MYF(0));
    if (mysql_file_write(file, buff, bufflength, MYF(MY_NABP+MY_WME)))
	DBUG_RETURN(0L);
    maxlength+=IO_SIZE;				/* Fix old ref */
    int2store(fileinfo+6,maxlength);
    for (i=names, pos= (uchar*) *formnames->type_names+n_length-1; i-- ;
	 pos+=4)
    {
      endpos=uint4korr(pos)+IO_SIZE;
      int4store(pos,endpos);
    }
  }

  if (n_length == 1 )
  {						/* First name */
    length++;
    (void) strxmov((char*) buff,"/",newname,"/",NullS);
  }
  else
    (void) strxmov((char*) buff,newname,"/",NullS); /* purecov: inspected */
  mysql_file_seek(file, 63L+(ulong) n_length, MY_SEEK_SET, MYF(0));
  if (mysql_file_write(file, buff, (size_t) length+1, MYF(MY_NABP+MY_WME)) ||
      (names && mysql_file_write(file,
                                 (uchar*) (*formnames->type_names+n_length-1),
                                 names*4, MYF(MY_NABP+MY_WME))) ||
      mysql_file_write(file, fileinfo+10, 4, MYF(MY_NABP+MY_WME)))
    DBUG_RETURN(0L); /* purecov: inspected */

  int2store(fileinfo+8,names+1);
  int2store(fileinfo+4,n_length+length);
  (void) mysql_file_chsize(file, newpos, 0, MYF(MY_WME));/* Append file with '\0' */
  DBUG_RETURN(newpos);
} /* make_new_entry */


	/* error message when opening a form file */

void open_table_error(TABLE_SHARE *share, int error, int db_errno, int errarg)
{
  int err_no;
  char buff[FN_REFLEN];
  char errbuf[MYSYS_STRERROR_SIZE];
  myf errortype= ME_ERROR+ME_WAITTANG;
  DBUG_ENTER("open_table_error");

  switch (error) {
  case 7:
  case 1:
    if (db_errno == ENOENT)
      my_error(ER_NO_SUCH_TABLE, MYF(0), share->db.str, share->table_name.str);
    else
    {
      strxmov(buff, share->normalized_path.str, reg_ext, NullS);
      my_error((db_errno == EMFILE) ? ER_CANT_OPEN_FILE : ER_FILE_NOT_FOUND,
               errortype, buff,
               db_errno, my_strerror(errbuf, sizeof(errbuf), db_errno));
    }
    break;
  case 2:
  {
    handler *file= 0;
    const char *datext= "";
    
    if (share->db_type() != NULL)
    {
      if ((file= get_new_handler(share, current_thd->mem_root,
                                 share->db_type())))
      {
        if (!(datext= *file->bas_ext()))
          datext= "";
      }
    }
    err_no= (db_errno == ENOENT) ? ER_FILE_NOT_FOUND : (db_errno == EAGAIN) ?
      ER_FILE_USED : ER_CANT_OPEN_FILE;
    strxmov(buff, share->normalized_path.str, datext, NullS);
    my_error(err_no,errortype, buff,
             db_errno, my_strerror(errbuf, sizeof(errbuf), db_errno));
    delete file;
    break;
  }
  case 5:
  {
    const char *csname= get_charset_name((uint) errarg);
    char tmp[10];
    if (!csname || csname[0] =='?')
    {
      my_snprintf(tmp, sizeof(tmp), "#%d", errarg);
      csname= tmp;
    }
    my_printf_error(ER_UNKNOWN_COLLATION,
                    "Unknown collation '%s' in table '%-.64s' definition", 
                    MYF(0), csname, share->table_name.str);
    break;
  }
  case 6:
    strxmov(buff, share->normalized_path.str, reg_ext, NullS);
    my_printf_error(ER_NOT_FORM_FILE,
                    "Table '%-.64s' was created with a different version "
                    "of MySQL and cannot be read", 
                    MYF(0), buff);
    break;
  case 8:
    break;
  case 9:
    /* Unknown FRM type read while preparing File_parser object for view*/
    my_error(ER_FRM_UNKNOWN_TYPE, MYF(0), share->path.str,
             share->view_def->type()->str);
    break;
  default:				/* Better wrong error than none */
  case 4:
    strxmov(buff, share->normalized_path.str, reg_ext, NullS);
    my_error(ER_NOT_FORM_FILE, errortype, buff);
    break;
  }
  DBUG_VOID_RETURN;
} /* open_table_error */


	/*
	** fix a str_type to a array type
	** typeparts separated with some char. differents types are separated
	** with a '\0'
	*/

static void
fix_type_pointers(const char ***array, TYPELIB *point_to_type, uint types,
		  char **names)
{
  char *type_name, *ptr;
  char chr;

  ptr= *names;
  while (types--)
  {
    point_to_type->name=0;
    point_to_type->type_names= *array;

    if ((chr= *ptr))			/* Test if empty type */
    {
      while ((type_name=strchr(ptr+1,chr)) != NullS)
      {
	*((*array)++) = ptr+1;
	*type_name= '\0';		/* End string */
	ptr=type_name;
      }
      ptr+=2;				/* Skip end mark and last 0 */
    }
    else
      ptr++;
    point_to_type->count= (uint) (*array - point_to_type->type_names);
    point_to_type++;
    *((*array)++)= NullS;		/* End of type */
  }
  *names=ptr;				/* Update end */
  return;
} /* fix_type_pointers */


TYPELIB *typelib(MEM_ROOT *mem_root, List<String> &strings)
{
  TYPELIB *result= (TYPELIB*) alloc_root(mem_root, sizeof(TYPELIB));
  if (!result)
    return 0;
  result->count=strings.elements;
  result->name="";
  uint nbytes= (sizeof(char*) + sizeof(uint)) * (result->count + 1);
  if (!(result->type_names= (const char**) alloc_root(mem_root, nbytes)))
    return 0;
  result->type_lengths= (uint*) (result->type_names + result->count + 1);
  List_iterator<String> it(strings);
  String *tmp;
  for (uint i=0; (tmp=it++) ; i++)
  {
    result->type_names[i]= tmp->ptr();
    result->type_lengths[i]= tmp->length();
  }
  result->type_names[result->count]= 0;		// End marker
  result->type_lengths[result->count]= 0;
  return result;
}


/*
 Search after a field with given start & length
 If an exact field isn't found, return longest field with starts
 at right position.
 
 NOTES
   This is needed because in some .frm fields 'fieldnr' was saved wrong

 RETURN
   0  error
   #  field number +1
*/

static uint find_field(Field **fields, uchar *record, uint start, uint length)
{
  Field **field;
  uint i, pos;

  pos= 0;
  for (field= fields, i=1 ; *field ; i++,field++)
  {
    if ((*field)->offset(record) == start)
    {
      if ((*field)->key_length() == length)
	return (i);
      if (!pos || fields[pos-1]->pack_length() <
	  (*field)->pack_length())
	pos= i;
    }
  }
  return (pos);
}


	/* Check that the integer is in the internal */

int set_zone(register int nr, int min_zone, int max_zone)
{
  if (nr<=min_zone)
    return (min_zone);
  if (nr>=max_zone)
    return (max_zone);
  return (nr);
} /* set_zone */

	/* Adjust number to next larger disk buffer */

ulong next_io_size(register ulong pos)
{
  reg2 ulong offset;
  if ((offset= pos & (IO_SIZE-1)))
    return pos-offset+IO_SIZE;
  return pos;
} /* next_io_size */


/*
  Store an SQL quoted string.

  SYNOPSIS  
    append_unescaped()
    res		result String
    pos		string to be quoted
    length	it's length

  NOTE
    This function works correctly with utf8 or single-byte charset strings.
    May fail with some multibyte charsets though.
*/

void append_unescaped(String *res, const char *pos, uint length)
{
  const char *end= pos+length;
  res->append('\'');

  for (; pos != end ; pos++)
  {
#if defined(USE_MB) && MYSQL_VERSION_ID < 40100
    uint mblen;
    if (use_mb(default_charset_info) &&
        (mblen= my_ismbchar(default_charset_info, pos, end)))
    {
      res->append(pos, mblen);
      pos+= mblen;
      continue;
    }
#endif

    switch (*pos) {
    case 0:				/* Must be escaped for 'mysql' */
      res->append('\\');
      res->append('0');
      break;
    case '\n':				/* Must be escaped for logs */
      res->append('\\');
      res->append('n');
      break;
    case '\r':
      res->append('\\');		/* This gives better readability */
      res->append('r');
      break;
    case '\\':
      res->append('\\');		/* Because of the sql syntax */
      res->append('\\');
      break;
    case '\'':
      res->append('\'');		/* Because of the sql syntax */
      res->append('\'');
      break;
    default:
      res->append(*pos);
      break;
    }
  }
  res->append('\'');
}


	/* Create a .frm file */

File create_frm(THD *thd, const char *name, const char *db,
                const char *table, uint reclength, uchar *fileinfo,
  		HA_CREATE_INFO *create_info, uint keys, KEY *key_info)
{
  register File file;
  ulong length;
  uchar fill[IO_SIZE];
  int create_flags= O_RDWR | O_TRUNC;
  ulong key_comment_total_bytes= 0;
  uint i;

  if (create_info->options & HA_LEX_CREATE_TMP_TABLE)
    create_flags|= O_EXCL | O_NOFOLLOW;

  /* Fix this when we have new .frm files;  Current limit is 4G rows (QQ) */
  if (create_info->max_rows > UINT_MAX32)
    create_info->max_rows= UINT_MAX32;
  if (create_info->min_rows > UINT_MAX32)
    create_info->min_rows= UINT_MAX32;

  if ((file= mysql_file_create(key_file_frm,
                               name, CREATE_MODE, create_flags, MYF(0))) >= 0)
  {
    uint key_length, tmp_key_length, tmp, csid;
    memset(fileinfo, 0, 64);
    /* header */
    fileinfo[0]=(uchar) 254;
    fileinfo[1]= 1;
    fileinfo[2]= FRM_VER+3+ MY_TEST(create_info->varchar);

    fileinfo[3]= (uchar) ha_legacy_type(
          ha_checktype(thd,ha_legacy_type(create_info->db_type),0,0));
    fileinfo[4]=1;
    int2store(fileinfo+6,IO_SIZE);		/* Next block starts here */
    /*
      Keep in sync with pack_keys() in unireg.cc
      For each key:
      8 bytes for the key header
      9 bytes for each key-part (MAX_REF_PARTS)
      NAME_LEN bytes for the name
      1 byte for the NAMES_SEP_CHAR (before the name)
      For all keys:
      6 bytes for the header
      1 byte for the NAMES_SEP_CHAR (after the last name)
      9 extra bytes (padding for safety? alignment?)
    */
    for (i= 0; i < keys; i++)
    {
      DBUG_ASSERT(MY_TEST(key_info[i].flags & HA_USES_COMMENT) ==
                 (key_info[i].comment.length > 0));
      if (key_info[i].flags & HA_USES_COMMENT)
        key_comment_total_bytes += 2 + key_info[i].comment.length;
    }

    key_length= keys * (8 + MAX_REF_PARTS * 9 + NAME_LEN + 1) + 16
                + key_comment_total_bytes;

    length= next_io_size((ulong) (IO_SIZE+key_length+reclength+
                                  create_info->extra_size));
    int4store(fileinfo+10,length);
    tmp_key_length= (key_length < 0xffff) ? key_length : 0xffff;
    int2store(fileinfo+14,tmp_key_length);
    int2store(fileinfo+16,reclength);
    int4store(fileinfo+18,create_info->max_rows);
    int4store(fileinfo+22,create_info->min_rows);
    /* fileinfo[26] is set in mysql_create_frm() */
    fileinfo[27]=2;				// Use long pack-fields
    /* fileinfo[28 & 29] is set to key_info_length in mysql_create_frm() */
    create_info->table_options|=HA_OPTION_LONG_BLOB_PTR; // Use portable blob pointers
    int2store(fileinfo+30,create_info->table_options);
    fileinfo[32]=0;				// No filename anymore
    fileinfo[33]=5;                             // Mark for 5.0 frm file
    int4store(fileinfo+34,create_info->avg_row_length);
    csid= (create_info->default_table_charset ?
           create_info->default_table_charset->number : 0);
    fileinfo[38]= (uchar) csid;
    /*
      In future versions, we will store in fileinfo[39] the values of the
      TRANSACTIONAL and PAGE_CHECKSUM clauses of CREATE TABLE.
    */
    fileinfo[39]= 0;
    fileinfo[40]= (uchar) create_info->row_type;
    /* Bytes 41-46 were for RAID support; now reused for other purposes */
    fileinfo[41]= (uchar) (csid >> 8);
    int2store(fileinfo+42, create_info->stats_sample_pages & 0xffff);
    fileinfo[44]= (uchar) create_info->stats_auto_recalc;
    fileinfo[45]= 0;
    fileinfo[46]= 0;
    int4store(fileinfo+47, key_length);
    tmp= MYSQL_VERSION_ID;          // Store to avoid warning from int4store
    int4store(fileinfo+51, tmp);
    int4store(fileinfo+55, create_info->extra_size);
    /*
      59-60 is reserved for extra_rec_buf_length,
      61 for default_part_db_type
    */
    int2store(fileinfo+62, create_info->key_block_size);
    memset(fill, 0, IO_SIZE);
    for (; length > IO_SIZE ; length-= IO_SIZE)
    {
      if (mysql_file_write(file, fill, IO_SIZE, MYF(MY_WME | MY_NABP)))
      {
        (void) mysql_file_close(file, MYF(0));
        (void) mysql_file_delete(key_file_frm, name, MYF(0));
	return(-1);
      }
    }
  }
  else
  {
    if (my_errno == ENOENT)
      my_error(ER_BAD_DB_ERROR,MYF(0),db);
    else
      my_error(ER_CANT_CREATE_TABLE,MYF(0),table,my_errno);
  }
  return (file);
} /* create_frm */


void update_create_info_from_table(HA_CREATE_INFO *create_info, TABLE *table)
{
  TABLE_SHARE *share= table->s;
  DBUG_ENTER("update_create_info_from_table");

  create_info->max_rows= share->max_rows;
  create_info->min_rows= share->min_rows;
  create_info->table_options= share->db_create_options;
  create_info->avg_row_length= share->avg_row_length;
  create_info->row_type= share->row_type;
  create_info->default_table_charset= share->table_charset;
  create_info->table_charset= 0;
  create_info->comment= share->comment;
  create_info->storage_media= share->default_storage_media;
  create_info->tablespace= share->tablespace;

  DBUG_VOID_RETURN;
}

int
rename_file_ext(const char * from,const char * to,const char * ext)
{
  char from_b[FN_REFLEN],to_b[FN_REFLEN];
  (void) strxmov(from_b,from,ext,NullS);
  (void) strxmov(to_b,to,ext,NullS);
  return (mysql_file_rename(key_file_frm, from_b, to_b, MYF(MY_WME)));
}


/*
  Allocate string field in MEM_ROOT and return it as String

  SYNOPSIS
    get_field()
    mem   	MEM_ROOT for allocating
    field 	Field for retrieving of string
    res         result String

  RETURN VALUES
    1   string is empty
    0	all ok
*/

bool get_field(MEM_ROOT *mem, Field *field, String *res)
{
  char buff[MAX_FIELD_WIDTH], *to;
  String str(buff,sizeof(buff),&my_charset_bin);
  uint length;

  field->val_str(&str);
  if (!(length= str.length()))
  {
    res->length(0);
    return 1;
  }
  if (!(to= strmake_root(mem, str.ptr(), length)))
    length= 0;                                  // Safety fix
  res->set(to, length, field->charset());
  return 0;
}


/*
  Allocate string field in MEM_ROOT and return it as NULL-terminated string

  SYNOPSIS
    get_field()
    mem   	MEM_ROOT for allocating
    field 	Field for retrieving of string

  RETURN VALUES
    NullS  string is empty
    #      pointer to NULL-terminated string value of field
*/

char *get_field(MEM_ROOT *mem, Field *field)
{
  char buff[MAX_FIELD_WIDTH], *to;
  String str(buff,sizeof(buff),&my_charset_bin);
  uint length;

  field->val_str(&str);
  length= str.length();
  if (!length || !(to= (char*) alloc_root(mem,length+1)))
    return NullS;
  memcpy(to,str.ptr(),(uint) length);
  to[length]=0;
  return to;
}

/*
  DESCRIPTION
    given a buffer with a key value, and a map of keyparts
    that are present in this value, returns the length of the value
*/
uint calculate_key_len(TABLE *table, uint key, const uchar *buf,
                       key_part_map keypart_map)
{
  /* works only with key prefixes */
  DBUG_ASSERT(((keypart_map + 1) & keypart_map) == 0);

  KEY *key_info= table->key_info + key;
  KEY_PART_INFO *key_part= key_info->key_part;
  KEY_PART_INFO *end_key_part= key_part + actual_key_parts(key_info);
  uint length= 0;

  while (key_part < end_key_part && keypart_map)
  {
    length+= key_part->store_length;
    keypart_map >>= 1;
    key_part++;
  }
  return length;
}

/**
  Check if database name is valid

  @param org_name             Name of database and length
  @param preserve_lettercase  Preserve lettercase if true

  @note If lower_case_table_names is true and preserve_lettercase
  is false then database is converted to lower case

  @retval  IDENT_NAME_OK        Identifier name is Ok (Success)
  @retval  IDENT_NAME_WRONG     Identifier name is Wrong (ER_WRONG_TABLE_NAME)
  @retval  IDENT_NAME_TOO_LONG  Identifier name is too long if it is greater
                                than 64 characters (ER_TOO_LONG_IDENT)

  @note In case of IDENT_NAME_WRONG and IDENT_NAME_TOO_LONG, this
  function reports an error (my_error)
*/

enum_ident_name_check check_and_convert_db_name(LEX_STRING *org_name,
                                                bool preserve_lettercase)
{
  char *name= org_name->str;
  uint name_length= org_name->length;
  bool check_for_path_chars;
  enum_ident_name_check ident_check_status;

  if (!name_length || name_length > NAME_LEN)
  {
    my_error(ER_WRONG_DB_NAME, MYF(0), org_name->str);
    return IDENT_NAME_WRONG;
  }

  if ((check_for_path_chars= check_mysql50_prefix(name)))
  {
    name+= MYSQL50_TABLE_NAME_PREFIX_LENGTH;
    name_length-= MYSQL50_TABLE_NAME_PREFIX_LENGTH;
  }

  if (!preserve_lettercase && lower_case_table_names && name != any_db)
    my_casedn_str(files_charset_info, name);

  ident_check_status= check_table_name(name, name_length, check_for_path_chars);
  if (ident_check_status == IDENT_NAME_WRONG)
    my_error(ER_WRONG_DB_NAME, MYF(0), org_name->str);
  else if (ident_check_status == IDENT_NAME_TOO_LONG)
    my_error(ER_TOO_LONG_IDENT, MYF(0), org_name->str);
  return ident_check_status;
}


/**
  Function to check if table name is valid or not. If it is invalid,
  return appropriate error in each case to the caller.

  @param name                  Table name
  @param length                Length of table name
  @param check_for_path_chars  Check if the table name contains path chars

  @retval  IDENT_NAME_OK        Identifier name is Ok (Success)
  @retval  IDENT_NAME_WRONG     Identifier name is Wrong (ER_WRONG_TABLE_NAME)
  @retval  IDENT_NAME_TOO_LONG  Identifier name is too long if it is greater
                                than 64 characters (ER_TOO_LONG_IDENT)

  @note Reporting error to the user is the responsiblity of the caller.
*/

enum_ident_name_check check_table_name(const char *name, size_t length,
                                       bool check_for_path_chars)
{
  // name length in symbols
  size_t name_length= 0;
  const char *end= name+length;
  if (!length || length > NAME_LEN)
    return IDENT_NAME_WRONG;
#if defined(USE_MB) && defined(USE_MB_IDENT)
  bool last_char_is_space= FALSE;
#else
  if (name[length-1]==' ')
    return IDENT_NAME_WRONG;
#endif

  while (name != end)
  {
#if defined(USE_MB) && defined(USE_MB_IDENT)
    last_char_is_space= my_isspace(system_charset_info, *name);
    if (use_mb(system_charset_info))
    {
      int len=my_ismbchar(system_charset_info, name, end);
      if (len)
      {
        name += len;
        name_length++;
        continue;
      }
    }
#endif
    if (check_for_path_chars &&
        (*name == '/' || *name == '\\' || *name == '~' || *name == FN_EXTCHAR))
      return IDENT_NAME_WRONG;
    name++;
    name_length++;
  }
#if defined(USE_MB) && defined(USE_MB_IDENT)
  if (last_char_is_space)
   return IDENT_NAME_WRONG;
  else if (name_length > NAME_CHAR_LEN)
   return IDENT_NAME_TOO_LONG;
#endif
  return IDENT_NAME_OK;
}


bool check_column_name(const char *name)
{
  // name length in symbols
  size_t name_length= 0;
  bool last_char_is_space= TRUE;

  while (*name)
  {
#if defined(USE_MB) && defined(USE_MB_IDENT)
    last_char_is_space= my_isspace(system_charset_info, *name);
    if (use_mb(system_charset_info))
    {
      int len=my_ismbchar(system_charset_info, name, 
                          name+system_charset_info->mbmaxlen);
      if (len)
      {
        name += len;
        name_length++;
        continue;
      }
    }
#else
    last_char_is_space= *name==' ';
#endif
    if (*name == NAMES_SEP_CHAR)
      return 1;
    name++;
    name_length++;
  }
  /* Error if empty or too long column name */
  return last_char_is_space || (name_length > NAME_CHAR_LEN);
}


/**
  Checks whether a table is intact. Should be done *just* after the table has
  been opened.

  @param[in] table             The table to check
  @param[in] table_f_count     Expected number of columns in the table
  @param[in] table_def         Expected structure of the table (column name
                               and type)

  @retval  FALSE  OK
  @retval  TRUE   There was an error. An error message is output
                  to the error log.  We do not push an error
                  message into the error stack because this
                  function is currently only called at start up,
                  and such errors never reach the user.
*/

bool
Table_check_intact::check(TABLE *table, const TABLE_FIELD_DEF *table_def)
{
  uint i;
  my_bool error= FALSE;
  const TABLE_FIELD_TYPE *field_def= table_def->field;
  DBUG_ENTER("table_check_intact");
  DBUG_PRINT("info",("table: %s  expected_count: %d",
                     table->alias, table_def->count));

  /* Whether the table definition has already been validated. */
  if (table->s->table_field_def_cache == table_def)
    DBUG_RETURN(FALSE);

  if (table->s->fields != table_def->count)
  {
    DBUG_PRINT("info", ("Column count has changed, checking the definition"));

    /* previous MySQL version */
    if (MYSQL_VERSION_ID > table->s->mysql_version)
    {
      report_error(ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE,
                   ER(ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE),
                   table->alias, table_def->count, table->s->fields,
                   static_cast<int>(table->s->mysql_version),
                   MYSQL_VERSION_ID);
      DBUG_RETURN(TRUE);
    }
    else if (MYSQL_VERSION_ID == table->s->mysql_version)
    {
      report_error(ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2,
                   ER(ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2),
                   table->s->db.str, table->s->table_name.str,
                   table_def->count, table->s->fields);
      DBUG_RETURN(TRUE);
    }
    /*
      Something has definitely changed, but we're running an older
      version of MySQL with new system tables.
      Let's check column definitions. If a column was added at
      the end of the table, then we don't care much since such change
      is backward compatible.
    */
  }
  char buffer[STRING_BUFFER_USUAL_SIZE];
  for (i=0 ; i < table_def->count; i++, field_def++)
  {
    String sql_type(buffer, sizeof(buffer), system_charset_info);
    sql_type.length(0);
    if (i < table->s->fields)
    {
      Field *field= table->field[i];

      if (strncmp(field->field_name, field_def->name.str,
                  field_def->name.length))
      {
        /*
          Name changes are not fatal, we use ordinal numbers to access columns.
          Still this can be a sign of a tampered table, output an error
          to the error log.
        */
        report_error(0, "Incorrect definition of table %s.%s: "
                     "expected column '%s' at position %d, found '%s'.",
                     table->s->db.str, table->alias, field_def->name.str, i,
                     field->field_name);
      }
      field->sql_type(sql_type);
      /*
        Generally, if column types don't match, then something is
        wrong.

        However, we only compare column definitions up to the
        length of the original definition, since we consider the
        following definitions compatible:

        1. DATETIME and DATETIM
        2. INT(11) and INT(11
        3. SET('one', 'two') and SET('one', 'two', 'more')

        For SETs or ENUMs, if the same prefix is there it's OK to
        add more elements - they will get higher ordinal numbers and
        the new table definition is backward compatible with the
        original one.
       */
      if (strncmp(sql_type.c_ptr_safe(), field_def->type.str,
                  field_def->type.length - 1))
      {
        report_error(0, "Incorrect definition of table %s.%s: "
                     "expected column '%s' at position %d to have type "
                     "%s, found type %s.", table->s->db.str, table->alias,
                     field_def->name.str, i, field_def->type.str,
                     sql_type.c_ptr_safe());
        error= TRUE;
      }
      else if (field_def->cset.str && !field->has_charset())
      {
        report_error(0, "Incorrect definition of table %s.%s: "
                     "expected the type of column '%s' at position %d "
                     "to have character set '%s' but the type has no "
                     "character set.", table->s->db.str, table->alias,
                     field_def->name.str, i, field_def->cset.str);
        error= TRUE;
      }
      else if (field_def->cset.str &&
               strcmp(field->charset()->csname, field_def->cset.str))
      {
        report_error(0, "Incorrect definition of table %s.%s: "
                     "expected the type of column '%s' at position %d "
                     "to have character set '%s' but found "
                     "character set '%s'.", table->s->db.str, table->alias,
                     field_def->name.str, i, field_def->cset.str,
                     field->charset()->csname);
        error= TRUE;
      }
    }
    else
    {
      report_error(0, "Incorrect definition of table %s.%s: "
                   "expected column '%s' at position %d to have type %s "
                   " but the column is not found.",
                   table->s->db.str, table->alias,
                   field_def->name.str, i, field_def->type.str);
      error= TRUE;
    }
  }

  if (! error)
    table->s->table_field_def_cache= table_def;

  DBUG_RETURN(error);
}


/**
  Traverse portion of wait-for graph which is reachable through edge
  represented by this flush ticket in search for deadlocks.

  @retval TRUE  A deadlock is found. A victim is remembered
                by the visitor.
  @retval FALSE Success, no deadlocks.
*/

bool Wait_for_flush::accept_visitor(MDL_wait_for_graph_visitor *gvisitor)
{
  return m_share->visit_subgraph(this, gvisitor);
}


uint Wait_for_flush::get_deadlock_weight() const
{
  return m_deadlock_weight;
}


/**
  Traverse portion of wait-for graph which is reachable through this
  table share in search for deadlocks.

  @param waiting_ticket  Ticket representing wait for this share.
  @param dvisitor        Deadlock detection visitor.

  @retval TRUE  A deadlock is found. A victim is remembered
                by the visitor.
  @retval FALSE No deadlocks, it's OK to begin wait.
*/

bool TABLE_SHARE::visit_subgraph(Wait_for_flush *wait_for_flush,
                                 MDL_wait_for_graph_visitor *gvisitor)
{
  TABLE *table;
  MDL_context *src_ctx= wait_for_flush->get_ctx();
  bool result= TRUE;
  bool locked= FALSE;

  /*
    To protect used_tables list from being concurrently modified
    while we are iterating through it we acquire LOCK_open.
    This does not introduce deadlocks in the deadlock detector
    because we won't try to acquire LOCK_open while
    holding a write-lock on MDL_lock::m_rwlock.
  */
  if (gvisitor->m_lock_open_count++ == 0)
  {
    locked= TRUE;
    table_cache_manager.lock_all_and_tdc();
  }

  Table_cache_iterator tables_it(this);

  /*
    In case of multiple searches running in parallel, avoid going
    over the same loop twice and shortcut the search.
    Do it after taking the lock to weed out unnecessary races.
  */
  if (src_ctx->m_wait.get_status() != MDL_wait::EMPTY)
  {
    result= FALSE;
    goto end;
  }

  if (gvisitor->enter_node(src_ctx))
    goto end;

  while ((table= tables_it++))
  {
    if (gvisitor->inspect_edge(&table->in_use->mdl_context))
    {
      goto end_leave_node;
    }
  }

  tables_it.rewind();
  while ((table= tables_it++))
  {
    if (table->in_use->mdl_context.visit_subgraph(gvisitor))
    {
      goto end_leave_node;
    }
  }

  result= FALSE;

end_leave_node:
  gvisitor->leave_node(src_ctx);

end:
  gvisitor->m_lock_open_count--;
  if (locked)
  {
    DBUG_ASSERT(gvisitor->m_lock_open_count == 0);
    table_cache_manager.unlock_all_and_tdc();
  }

  return result;
}


/**
  Wait until the subject share is removed from the table
  definition cache and make sure it's destroyed.

  @param mdl_context     MDL context for thread which is going to wait.
  @param abstime         Timeout for waiting as absolute time value.
  @param deadlock_weight Weight of this wait for deadlock detector.

  @pre LOCK_open is write locked, the share is used (has
       non-zero reference count), is marked for flush and
       this connection does not reference the share.
       LOCK_open will be unlocked temporarily during execution.

  @retval FALSE - Success.
  @retval TRUE  - Error (OOM, deadlock, timeout, etc...).
*/

bool TABLE_SHARE::wait_for_old_version(THD *thd, struct timespec *abstime,
                                       uint deadlock_weight)
{
  MDL_context *mdl_context= &thd->mdl_context;
  Wait_for_flush ticket(mdl_context, this, deadlock_weight);
  MDL_wait::enum_wait_status wait_status;

  mysql_mutex_assert_owner(&LOCK_open);
  /*
    We should enter this method only when share's version is not
    up to date and the share is referenced. Otherwise our
    thread will never be woken up from wait.
  */
  DBUG_ASSERT(version != refresh_version && ref_count != 0);

  m_flush_tickets.push_front(&ticket);

  mdl_context->m_wait.reset_status();

  mysql_mutex_unlock(&LOCK_open);

  mdl_context->will_wait_for(&ticket);

  mdl_context->find_deadlock();

  DEBUG_SYNC(thd, "flush_complete");

  wait_status= mdl_context->m_wait.timed_wait(thd, abstime, TRUE,
                                              &stage_waiting_for_table_flush);

  mdl_context->done_waiting_for();

  mysql_mutex_lock(&LOCK_open);

  m_flush_tickets.remove(&ticket);

  if (m_flush_tickets.is_empty() && ref_count == 0)
  {
    /*
      If our thread was the last one using the share,
      we must destroy it here.
    */
    destroy();
  }

  DEBUG_SYNC(thd, "share_destroyed");

  /*
    In cases when our wait was aborted by KILL statement,
    a deadlock or a timeout, the share might still be referenced,
    so we don't delete it. Note, that we can't determine this
    condition by checking wait_status alone, since, for example,
    a timeout can happen after all references to the table share
    were released, but before the share is removed from the
    cache and we receive the notification. This is why
    we first destroy the share, and then look at
    wait_status.
  */
  switch (wait_status)
  {
  case MDL_wait::GRANTED:
    return FALSE;
  case MDL_wait::VICTIM:
    my_error(ER_LOCK_DEADLOCK, MYF(0));
    return TRUE;
  case MDL_wait::TIMEOUT:
    my_error(ER_LOCK_WAIT_TIMEOUT, MYF(0));
    return TRUE;
  case MDL_wait::KILLED:
    return TRUE;
  default:
    DBUG_ASSERT(0);
    return TRUE;
  }
}


/**
  Initialize TABLE instance (newly created, or coming either from table
  cache or THD::temporary_tables list) and prepare it for further use
  during statement execution. Set the 'alias' attribute from the specified
  TABLE_LIST element. Remember the TABLE_LIST element in the
  TABLE::pos_in_table_list member.

  @param thd  Thread context.
  @param tl   TABLE_LIST element.
*/

void TABLE::init(THD *thd, TABLE_LIST *tl)
{
  DBUG_ASSERT(s->ref_count > 0 || s->tmp_table != NO_TMP_TABLE);

  if (thd->lex->need_correct_ident())
    alias_name_used= my_strcasecmp(table_alias_charset,
                                   s->table_name.str,
                                   tl->alias);
  /* Fix alias if table name changes. */
  if (strcmp(alias, tl->alias))
  {
    uint length= (uint) strlen(tl->alias)+1;
    alias= (char*) my_realloc((char*) alias, length, MYF(MY_WME));
    memcpy((char*) alias, tl->alias, length);
  }

  tablenr= thd->current_tablenr++;
  used_fields= 0;
  const_table= 0;
  null_row= 0;
  maybe_null= 0;
  force_index= 0;
  force_index_order= 0;
  force_index_group= 0;
  status= STATUS_GARBAGE | STATUS_NOT_FOUND;
  insert_values= 0;
  fulltext_searched= 0;
  file->ft_handler= 0;
  reginfo.impossible_range= 0;
  reginfo.join_tab= NULL;

  /* Catch wrong handling of the auto_increment_field_not_null. */
  DBUG_ASSERT(!auto_increment_field_not_null);
  auto_increment_field_not_null= FALSE;

  pos_in_table_list= tl;

  clear_column_bitmaps();

  sequence_query= tl->sequence_read;

  DBUG_ASSERT(key_read == 0);

  /* Tables may be reused in a sub statement. */
  DBUG_ASSERT(!file->extra(HA_EXTRA_IS_ATTACHED_CHILDREN));
}


/*
  Create Item_field for each column in the table.

  SYNPOSIS
    TABLE::fill_item_list()
      item_list          a pointer to an empty list used to store items

  DESCRIPTION
    Create Item_field object for each column in the table and
    initialize it with the corresponding Field. New items are
    created in the current THD memory root.

  RETURN VALUE
    0                    success
    1                    out of memory
*/

bool TABLE::fill_item_list(List<Item> *item_list) const
{
  /*
    All Item_field's created using a direct pointer to a field
    are fixed in Item_field constructor.
  */
  for (Field **ptr= field; *ptr; ptr++)
  {
    Item_field *item= new Item_field(*ptr);
    if (!item || item_list->push_back(item))
      return TRUE;
  }
  return FALSE;
}

/*
  Reset an existing list of Item_field items to point to the
  Fields of this table.

  SYNPOSIS
    TABLE::fill_item_list()
      item_list          a non-empty list with Item_fields

  DESCRIPTION
    This is a counterpart of fill_item_list used to redirect
    Item_fields to the fields of a newly created table.
    The caller must ensure that number of items in the item_list
    is the same as the number of columns in the table.
*/

void TABLE::reset_item_list(List<Item> *item_list) const
{
  List_iterator_fast<Item> it(*item_list);
  for (Field **ptr= field; *ptr; ptr++)
  {
    Item_field *item_field= (Item_field*) it++;
    DBUG_ASSERT(item_field != 0);
    item_field->reset_field(*ptr);
  }
}

/**
  Create a TABLE_LIST object representing a nested join

  @param allocator  Mem root allocator that object is created from.
  @param alias      Name of nested join object
  @param embedding  Pointer to embedding join nest (or NULL if top-most)
  @param belongs_to List of tables this nest belongs to (never NULL).
  @param select     The query block that this join nest belongs within.

  @returns Pointer to created join nest object, or NULL if error.
*/

TABLE_LIST *TABLE_LIST::new_nested_join(MEM_ROOT *allocator,
                            const char *alias,
                            TABLE_LIST *embedding,
                            List<TABLE_LIST> *belongs_to,
                            class st_select_lex *select)
{
  DBUG_ASSERT(belongs_to && select);

  TABLE_LIST *const join_nest=
    (TABLE_LIST *) alloc_root(allocator, ALIGN_SIZE(sizeof(TABLE_LIST))+
                                                    sizeof(NESTED_JOIN));
  if (join_nest == NULL)
    return NULL;

  memset(join_nest, 0, ALIGN_SIZE(sizeof(TABLE_LIST)) + sizeof(NESTED_JOIN));
  join_nest->nested_join=
    (NESTED_JOIN *) ((uchar *)join_nest + ALIGN_SIZE(sizeof(TABLE_LIST)));

  join_nest->db= (char *)"";
  join_nest->db_length= 0;
  join_nest->table_name= (char *)"";
  join_nest->table_name_length= 0;
  join_nest->alias= (char *)alias;
  
  join_nest->embedding= embedding;
  join_nest->join_list= belongs_to;
  join_nest->select_lex= select;

  join_nest->nested_join->join_list.empty();

  return join_nest;
}
/*
  calculate md5 of query

  SYNOPSIS
    TABLE_LIST::calc_md5()
    buffer	buffer for md5 writing
*/

void  TABLE_LIST::calc_md5(char *buffer)
{
  uchar digest[MD5_HASH_SIZE];
  compute_md5_hash((char *) digest, (const char *) select_stmt.str,
                   select_stmt.length);
  array_to_hex((char *) buffer, digest, MD5_HASH_SIZE);
}


/**
   @brief Set underlying table for table place holder of view.

   @details

   Replace all views that only use one table with the table itself.  This
   allows us to treat the view as a simple table and even update it (it is a
   kind of optimization).

   @note 

   This optimization is potentially dangerous as it makes views
   masquerade as base tables: Views don't have the pointer TABLE_LIST::table
   set to non-@c NULL.

   We may have the case where a view accesses tables not normally accessible
   in the current Security_context (only in the definer's
   Security_context). According to the table's GRANT_INFO (TABLE::grant),
   access is fulfilled, but this is implicitly meant in the definer's security
   context. Hence we must never look at only a TABLE's GRANT_INFO without
   looking at the one of the referring TABLE_LIST.
*/

void TABLE_LIST::set_underlying_merge()
{
  TABLE_LIST *tbl;

  if ((tbl= merge_underlying_list))
  {
    /* This is a view. Process all tables of view */
    DBUG_ASSERT(view && effective_algorithm == VIEW_ALGORITHM_MERGE);
    do
    {
      if (tbl->merge_underlying_list)          // This is a view
      {
        DBUG_ASSERT(tbl->view &&
                    tbl->effective_algorithm == VIEW_ALGORITHM_MERGE);
        /*
          This is the only case where set_ancestor is called on an object
          that may not be a view (in which case ancestor is 0)
        */
        tbl->merge_underlying_list->set_underlying_merge();
      }
    } while ((tbl= tbl->next_local));

    if (!multitable_view)
    {
      table= merge_underlying_list->table;
      /*
        If underlying view is not updatable and current view
        is a single table view
      */
      if (!merge_underlying_list->updatable)
        updatable= false;
      schema_table= merge_underlying_list->schema_table;
    }
    else
    {
      for (tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
      {
          updatable&= tbl->updatable;
      }
    }
  }
}


/*
  setup fields of placeholder of merged VIEW

  SYNOPSIS
    TABLE_LIST::setup_underlying()
    thd		    - thread handler

  DESCRIPTION
    It is:
    - preparing translation table for view columns
    If there are underlying view(s) procedure first will be called for them.

  RETURN
    FALSE - OK
    TRUE  - error
*/

bool TABLE_LIST::setup_underlying(THD *thd)
{
  DBUG_ENTER("TABLE_LIST::setup_underlying");

  if (!field_translation && merge_underlying_list)
  {
    Field_translator *transl;
    SELECT_LEX *select= &view->select_lex;
    Item *item;
    TABLE_LIST *tbl;
    List_iterator_fast<Item> it(select->item_list);
    uint field_count= 0;

    if (check_stack_overrun(thd, STACK_MIN_SIZE, (uchar*) &field_count))
    {
      DBUG_RETURN(TRUE);
    }

    for (tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
    {
      if (tbl->merge_underlying_list &&
          tbl->setup_underlying(thd))
      {
        DBUG_RETURN(TRUE);
      }
    }

    /* Create view fields translation table */

    if (!(transl=
          (Field_translator*)(thd->stmt_arena->
                              alloc(select->item_list.elements *
                                    sizeof(Field_translator)))))
    {
      DBUG_RETURN(TRUE);
    }

    while ((item= it++))
    {
      transl[field_count].name= item->item_name.ptr();
      transl[field_count++].item= item;
    }
    field_translation= transl;
    field_translation_end= transl + field_count;
    /* TODO: use hash for big number of fields */

    /* full text function moving to current select */
    if (view->select_lex.ftfunc_list->elements)
    {
      Item_func_match *ifm;
      SELECT_LEX *current_select= thd->lex->current_select;
      List_iterator_fast<Item_func_match>
        li(*(view->select_lex.ftfunc_list));
      while ((ifm= li++))
        current_select->ftfunc_list->push_front(ifm);
    }
  }
  DBUG_RETURN(FALSE);
}


/*
  Prepare where expression of view

  SYNOPSIS
    TABLE_LIST::prep_where()
    thd             - thread handler
    conds           - condition of this JOIN
    no_where_clause - do not build WHERE or ON outer qwery do not need it
                      (it is INSERT), we do not need conds if this flag is set

  NOTE: have to be called befor CHECK OPTION preparation, because it makes
  fix_fields for view WHERE clause

  RETURN
    FALSE - OK
    TRUE  - error
*/

bool TABLE_LIST::prep_where(THD *thd, Item **conds,
                               bool no_where_clause)
{
  DBUG_ENTER("TABLE_LIST::prep_where");

  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
  {
    if (tbl->view && tbl->prep_where(thd, conds, no_where_clause))
    {
      DBUG_RETURN(TRUE);
    }
  }

  if (where && !where_processed)
  {

    if (!where->fixed)
    {
      /*
        This WHERE will be included in check_option. If it contains a
        subquery, fix_fields() may convert it to semijoin, making it
        impossible to call val_int() on the Item[...]_subselect, preventing
        evaluation of check_option when we insert/update/delete a row.
        So we must forbid semijoin transformation in fix_fields():
      */
      Switch_resolve_place SRP(&thd->lex->current_select->resolve_place,
                               st_select_lex::RESOLVE_NONE,
                               effective_with_check != VIEW_CHECK_NONE);

      if (where->fix_fields(thd, &where))
        DBUG_RETURN(TRUE);
    }

    /*
      check that it is not VIEW in which we insert with INSERT SELECT
      (in this case we can't add view WHERE condition to main SELECT_LEX)
    */
    if (!no_where_clause)
    {
      TABLE_LIST *tbl= this;

      Prepared_stmt_arena_holder ps_arena_holder(thd);

      /* Go up to join tree and try to find left join */
      for (; tbl; tbl= tbl->embedding)
      {
        if (tbl->outer_join)
        {
          /*
            Store WHERE condition to ON expression for outer join, because
            we can't use WHERE to correctly execute left joins on VIEWs and
            this expression will not be moved to WHERE condition (i.e. will
            be clean correctly for PS/SP)
          */
          tbl->set_join_cond(and_conds(tbl->join_cond(),
                                       where->copy_andor_structure(thd)));
          break;
        }
      }
      if (tbl == 0)
        *conds= and_conds(*conds, where->copy_andor_structure(thd));
      where_processed= TRUE;
    }
  }

  DBUG_RETURN(FALSE);
}


/*
  Merge ON expressions for a view

  SYNOPSIS
    merge_on_conds()
    thd             thread handle
    table           table for the VIEW
    is_cascaded     TRUE <=> merge ON expressions from underlying views

  DESCRIPTION
    This function returns the result of ANDing the ON expressions
    of the given view and all underlying views. The ON expressions
    of the underlying views are added only if is_cascaded is TRUE.

  RETURN
    Pointer to the built expression if there is any.
    Otherwise and in the case of a failure NULL is returned.
*/

static Item *
merge_on_conds(THD *thd, TABLE_LIST *table, bool is_cascaded)
{
  DBUG_ENTER("merge_on_conds");

  Item *cond= NULL;
  DBUG_PRINT("info", ("alias: %s", table->alias));
  if (table->join_cond())
    cond= table->join_cond()->copy_andor_structure(thd);
  if (!table->nested_join)
    DBUG_RETURN(cond);
  List_iterator<TABLE_LIST> li(table->nested_join->join_list);
  while (TABLE_LIST *tbl= li++)
  {
    if (tbl->view && !is_cascaded)
      continue;
    cond= and_conds(cond, merge_on_conds(thd, tbl, is_cascaded));
  }
  DBUG_RETURN(cond);
}


/*
  Prepare check option expression of table

  SYNOPSIS
    TABLE_LIST::prep_check_option()
    thd             - thread handler
    check_opt_type  - WITH CHECK OPTION type (VIEW_CHECK_NONE,
                      VIEW_CHECK_LOCAL, VIEW_CHECK_CASCADED)
                      we use this parameter instead of direct check of
                      effective_with_check to change type of underlying
                      views to VIEW_CHECK_CASCADED if outer view have
                      such option and prevent processing of underlying
                      view check options if outer view have just
                      VIEW_CHECK_LOCAL option.

  NOTE
    This method builds check option condition to use it later on
    every call (usual execution or every SP/PS call).
    This method have to be called after WHERE preparation
    (TABLE_LIST::prep_where)

  RETURN
    FALSE - OK
    TRUE  - error
*/

bool TABLE_LIST::prep_check_option(THD *thd, uint8 check_opt_type)
{
  DBUG_ENTER("TABLE_LIST::prep_check_option");
  bool is_cascaded= check_opt_type == VIEW_CHECK_CASCADED;

  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
  {
    /* see comment of check_opt_type parameter */
    if (tbl->view && tbl->prep_check_option(thd, (is_cascaded ?
                                                  VIEW_CHECK_CASCADED :
                                                  VIEW_CHECK_NONE)))
      DBUG_RETURN(TRUE);
  }

  if (check_opt_type && !check_option_processed)
  {
    Prepared_stmt_arena_holder ps_arena_holder(thd);

    if (where)
    {
      DBUG_ASSERT(where->fixed);
      check_option= where->copy_andor_structure(thd);
    }
    if (is_cascaded)
    {
      for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
      {
        if (tbl->check_option)
          check_option= and_conds(check_option, tbl->check_option);
      }
    }
    check_option= and_conds(check_option,
                            merge_on_conds(thd, this, is_cascaded));

    check_option_processed= TRUE;
  }

  if (check_option)
  {
    const char *save_where= thd->where;
    thd->where= "check option";
    if ((!check_option->fixed &&
        check_option->fix_fields(thd, &check_option)) ||
        check_option->check_cols(1))
    {
      DBUG_RETURN(TRUE);
    }
    thd->where= save_where;
  }
  DBUG_RETURN(FALSE);
}


/**
  Hide errors which show view underlying table information. 
  There are currently two mechanisms at work that handle errors for views,
  this one and a more general mechanism based on an Internal_error_handler,
  see Show_create_error_handler. The latter handles errors encountered during
  execution of SHOW CREATE VIEW, while the machanism using this method is
  handles SELECT from views. The two methods should not clash.

  @param[in,out]  thd     thread handler

  @pre This method can be called only if there is an error.
*/

void TABLE_LIST::hide_view_error(THD *thd)
{
  if (thd->killed || thd->get_internal_handler())
    return;
  /* Hide "Unknown column" or "Unknown function" error */
  DBUG_ASSERT(thd->is_error());

  switch (thd->get_stmt_da()->sql_errno()) {
    case ER_BAD_FIELD_ERROR:
    case ER_SP_DOES_NOT_EXIST:
    case ER_FUNC_INEXISTENT_NAME_COLLISION:
    case ER_PROCACCESS_DENIED_ERROR:
    case ER_COLUMNACCESS_DENIED_ERROR:
    case ER_TABLEACCESS_DENIED_ERROR:
    case ER_TABLE_NOT_LOCKED:
    case ER_NO_SUCH_TABLE:
    {
      TABLE_LIST *top= top_table();
      thd->clear_error();
      my_error(ER_VIEW_INVALID, MYF(0),
               top->view_db.str, top->view_name.str);
      break;
    }

    case ER_NO_DEFAULT_FOR_FIELD:
    {
      TABLE_LIST *top= top_table();
      thd->clear_error();
      // TODO: make correct error message
      my_error(ER_NO_DEFAULT_FOR_VIEW_FIELD, MYF(0),
               top->view_db.str, top->view_name.str);
      break;
    }
  }
}


/*
  Find underlying base tables (TABLE_LIST) which represent given
  table_to_find (TABLE)

  SYNOPSIS
    TABLE_LIST::find_underlying_table()
    table_to_find table to find

  RETURN
    0  table is not found
    found table reference
*/

TABLE_LIST *TABLE_LIST::find_underlying_table(TABLE *table_to_find)
{
  /* is this real table and table which we are looking for? */
  if (table == table_to_find && merge_underlying_list == 0)
    return this;

  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
  {
    TABLE_LIST *result;
    if ((result= tbl->find_underlying_table(table_to_find)))
      return result;
  }
  return 0;
}

/*
  cleanup items belonged to view fields translation table

  SYNOPSIS
    TABLE_LIST::cleanup_items()
*/

void TABLE_LIST::cleanup_items()
{
  if (!field_translation)
    return;

  for (Field_translator *transl= field_translation;
       transl < field_translation_end;
       transl++)
    transl->item->walk(&Item::cleanup_processor, 0, 0);
}


/*
  check CHECK OPTION condition

  SYNOPSIS
    TABLE_LIST::view_check_option()
    ignore_failure ignore check option fail

  RETURN
    VIEW_CHECK_OK     OK
    VIEW_CHECK_ERROR  FAILED
    VIEW_CHECK_SKIP   FAILED, but continue
*/

int TABLE_LIST::view_check_option(THD *thd, bool ignore_failure) const
{
  if (check_option && check_option->val_int() == 0)
  {
    const TABLE_LIST *main_view= top_table();
    if (ignore_failure)
    {
      push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN,
                          ER_VIEW_CHECK_FAILED, ER(ER_VIEW_CHECK_FAILED),
                          main_view->view_db.str, main_view->view_name.str);
      return(VIEW_CHECK_SKIP);
    }
    my_error(ER_VIEW_CHECK_FAILED, MYF(0), main_view->view_db.str,
             main_view->view_name.str);
    return(VIEW_CHECK_ERROR);
  }
  return(VIEW_CHECK_OK);
}


/*
  Find table in underlying tables by mask and check that only this
  table belong to given mask

  SYNOPSIS
    TABLE_LIST::check_single_table()
    table_arg	reference on variable where to store found table
		(should be 0 on call, to find table, or point to table for
		unique test)
    map         bit mask of tables
    view_arg    view for which we are looking table

  RETURN
    FALSE table not found or found only one
    TRUE  found several tables
*/

bool TABLE_LIST::check_single_table(TABLE_LIST **table_arg,
                                       table_map map,
                                       TABLE_LIST *view_arg)
{
  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
  {
    if (tbl->table)
    {
      if (tbl->table->map & map)
      {
	if (*table_arg)
	  return TRUE;
        *table_arg= tbl;
        tbl->check_option= view_arg->check_option;
      }
    }
    else if (tbl->check_single_table(table_arg, map, view_arg))
      return TRUE;
  }
  return FALSE;
}


/*
  Set insert_values buffer

  SYNOPSIS
    set_insert_values()
    mem_root   memory pool for allocating

  RETURN
    FALSE - OK
    TRUE  - out of memory
*/

bool TABLE_LIST::set_insert_values(MEM_ROOT *mem_root)
{
  if (table)
  {
    if (!table->insert_values &&
        !(table->insert_values= (uchar *)alloc_root(mem_root,
                                                   table->s->rec_buff_length)))
      return TRUE;
  }
  else
  {
    DBUG_ASSERT(view && merge_underlying_list);
    for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
      if (tbl->set_insert_values(mem_root))
        return TRUE;
  }
  return FALSE;
}


/*
  Test if this is a leaf with respect to name resolution.

  SYNOPSIS
    TABLE_LIST::is_leaf_for_name_resolution()

  DESCRIPTION
    A table reference is a leaf with respect to name resolution if
    it is either a leaf node in a nested join tree (table, view,
    schema table, subquery), or an inner node that represents a
    NATURAL/USING join, or a nested join with materialized join
    columns.

  RETURN
    TRUE if a leaf, FALSE otherwise.
*/
bool TABLE_LIST::is_leaf_for_name_resolution()
{
  return (view || is_natural_join || is_join_columns_complete ||
          !nested_join);
}


/*
  Retrieve the first (left-most) leaf in a nested join tree with
  respect to name resolution.

  SYNOPSIS
    TABLE_LIST::first_leaf_for_name_resolution()

  DESCRIPTION
    Given that 'this' is a nested table reference, recursively walk
    down the left-most children of 'this' until we reach a leaf
    table reference with respect to name resolution.

  IMPLEMENTATION
    The left-most child of a nested table reference is the last element
    in the list of children because the children are inserted in
    reverse order.

  RETURN
    If 'this' is a nested table reference - the left-most child of
      the tree rooted in 'this',
    else return 'this'
*/

TABLE_LIST *TABLE_LIST::first_leaf_for_name_resolution()
{
  TABLE_LIST *cur_table_ref;
  NESTED_JOIN *cur_nested_join;
  LINT_INIT(cur_table_ref);

  if (is_leaf_for_name_resolution())
    return this;
  DBUG_ASSERT(nested_join);

  for (cur_nested_join= nested_join;
       cur_nested_join;
       cur_nested_join= cur_table_ref->nested_join)
  {
    List_iterator_fast<TABLE_LIST> it(cur_nested_join->join_list);
    cur_table_ref= it++;
    /*
      If the current nested join is a RIGHT JOIN, the operands in
      'join_list' are in reverse order, thus the first operand is
      already at the front of the list. Otherwise the first operand
      is in the end of the list of join operands.
    */
    if (!(cur_table_ref->outer_join & JOIN_TYPE_RIGHT))
    {
      TABLE_LIST *next;
      while ((next= it++))
        cur_table_ref= next;
    }
    if (cur_table_ref->is_leaf_for_name_resolution())
      break;
  }
  return cur_table_ref;
}


/*
  Retrieve the last (right-most) leaf in a nested join tree with
  respect to name resolution.

  SYNOPSIS
    TABLE_LIST::last_leaf_for_name_resolution()

  DESCRIPTION
    Given that 'this' is a nested table reference, recursively walk
    down the right-most children of 'this' until we reach a leaf
    table reference with respect to name resolution.

  IMPLEMENTATION
    The right-most child of a nested table reference is the first
    element in the list of children because the children are inserted
    in reverse order.

  RETURN
    - If 'this' is a nested table reference - the right-most child of
      the tree rooted in 'this',
    - else - 'this'
*/

TABLE_LIST *TABLE_LIST::last_leaf_for_name_resolution()
{
  TABLE_LIST *cur_table_ref= this;
  NESTED_JOIN *cur_nested_join;

  if (is_leaf_for_name_resolution())
    return this;
  DBUG_ASSERT(nested_join);

  for (cur_nested_join= nested_join;
       cur_nested_join;
       cur_nested_join= cur_table_ref->nested_join)
  {
    cur_table_ref= cur_nested_join->join_list.head();
    /*
      If the current nested is a RIGHT JOIN, the operands in
      'join_list' are in reverse order, thus the last operand is in the
      end of the list.
    */
    if ((cur_table_ref->outer_join & JOIN_TYPE_RIGHT))
    {
      List_iterator_fast<TABLE_LIST> it(cur_nested_join->join_list);
      TABLE_LIST *next;
      cur_table_ref= it++;
      while ((next= it++))
        cur_table_ref= next;
    }
    if (cur_table_ref->is_leaf_for_name_resolution())
      break;
  }
  return cur_table_ref;
}


/*
  Register access mode which we need for underlying tables

  SYNOPSIS
    register_want_access()
    want_access          Acess which we require
*/

void TABLE_LIST::register_want_access(ulong want_access)
{
  /* Remove SHOW_VIEW_ACL, because it will be checked during making view */
  want_access&= ~SHOW_VIEW_ACL;
  if (belong_to_view)
  {
    grant.want_privilege= want_access;
    if (table)
      table->grant.want_privilege= want_access;
  }
  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
    tbl->register_want_access(want_access);
}


/*
  Load security context information for this view

  SYNOPSIS
    TABLE_LIST::prepare_view_securety_context()
    thd                  [in] thread handler

  RETURN
    FALSE  OK
    TRUE   Error
*/

#ifndef NO_EMBEDDED_ACCESS_CHECKS
bool TABLE_LIST::prepare_view_securety_context(THD *thd)
{
  DBUG_ENTER("TABLE_LIST::prepare_view_securety_context");
  DBUG_PRINT("enter", ("table: %s", alias));

  DBUG_ASSERT(!prelocking_placeholder && view);
  if (view_suid)
  {
    DBUG_PRINT("info", ("This table is suid view => load contest"));
    DBUG_ASSERT(view && view_sctx);
    if (acl_getroot(view_sctx, definer.user.str, definer.host.str,
                                definer.host.str, thd->db))
    {
      if ((thd->lex->sql_command == SQLCOM_SHOW_CREATE) ||
          (thd->lex->sql_command == SQLCOM_SHOW_FIELDS))
      {
        push_warning_printf(thd, Sql_condition::WARN_LEVEL_NOTE, 
                            ER_NO_SUCH_USER, 
                            ER(ER_NO_SUCH_USER),
                            definer.user.str, definer.host.str);
      }
      else
      {
        if (thd->security_ctx->master_access & SUPER_ACL)
        {
          my_error(ER_NO_SUCH_USER, MYF(0), definer.user.str, definer.host.str);

        }
        else
        {
          if (thd->password == 2)
            my_error(ER_ACCESS_DENIED_NO_PASSWORD_ERROR, MYF(0),
                     thd->security_ctx->priv_user,
                     thd->security_ctx->priv_host);
          else
            my_error(ER_ACCESS_DENIED_ERROR, MYF(0),
                     thd->security_ctx->priv_user,
                     thd->security_ctx->priv_host,
                     (thd->password ?  ER(ER_YES) : ER(ER_NO)));
        }
        DBUG_RETURN(TRUE);
      }
    }
  }
  DBUG_RETURN(FALSE);
}
#endif


/*
  Find security context of current view

  SYNOPSIS
    TABLE_LIST::find_view_security_context()
    thd                  [in] thread handler

*/

#ifndef NO_EMBEDDED_ACCESS_CHECKS
Security_context *TABLE_LIST::find_view_security_context(THD *thd)
{
  Security_context *sctx;
  TABLE_LIST *upper_view= this;
  DBUG_ENTER("TABLE_LIST::find_view_security_context");

  DBUG_ASSERT(view);
  while (upper_view && !upper_view->view_suid)
  {
    DBUG_ASSERT(!upper_view->prelocking_placeholder);
    upper_view= upper_view->referencing_view;
  }
  if (upper_view)
  {
    DBUG_PRINT("info", ("Securety context of view %s will be used",
                        upper_view->alias));
    sctx= upper_view->view_sctx;
    DBUG_ASSERT(sctx);
  }
  else
  {
    DBUG_PRINT("info", ("Current global context will be used"));
    sctx= thd->security_ctx;
  }
  DBUG_RETURN(sctx);
}
#endif


/*
  Prepare security context and load underlying tables priveleges for view

  SYNOPSIS
    TABLE_LIST::prepare_security()
    thd                  [in] thread handler

  RETURN
    FALSE  OK
    TRUE   Error
*/

bool TABLE_LIST::prepare_security(THD *thd)
{
  List_iterator_fast<TABLE_LIST> tb(*view_tables);
  TABLE_LIST *tbl;
  DBUG_ENTER("TABLE_LIST::prepare_security");
#ifndef NO_EMBEDDED_ACCESS_CHECKS
  Security_context *save_security_ctx= thd->security_ctx;

  DBUG_ASSERT(!prelocking_placeholder);
  if (prepare_view_securety_context(thd))
    DBUG_RETURN(TRUE);
  thd->security_ctx= find_view_security_context(thd);
  opt_trace_disable_if_no_security_context_access(thd);
  while ((tbl= tb++))
  {
    DBUG_ASSERT(tbl->referencing_view);
    char *local_db, *local_table_name;
    if (tbl->view)
    {
      local_db= tbl->view_db.str;
      local_table_name= tbl->view_name.str;
    }
    else
    {
      local_db= tbl->db;
      local_table_name= tbl->table_name;
    }
    fill_effective_table_privileges(thd, &tbl->grant, local_db,
                                    local_table_name);
    if (tbl->table)
      tbl->table->grant= grant;
  }
  thd->security_ctx= save_security_ctx;
#else
  while ((tbl= tb++))
    tbl->grant.privilege= ~NO_ACCESS;
#endif
  DBUG_RETURN(FALSE);
}


Natural_join_column::Natural_join_column(Field_translator *field_param,
                                         TABLE_LIST *tab)
{
  DBUG_ASSERT(tab->field_translation);
  view_field= field_param;
  table_field= NULL;
  table_ref= tab;
  is_common= FALSE;
}


Natural_join_column::Natural_join_column(Item_field *field_param,
                                         TABLE_LIST *tab)
{
  DBUG_ASSERT(tab->table == field_param->field->table);
  table_field= field_param;
  view_field= NULL;
  table_ref= tab;
  is_common= FALSE;
}


const char *Natural_join_column::name()
{
  if (view_field)
  {
    DBUG_ASSERT(table_field == NULL);
    return view_field->name;
  }

  return table_field->field_name;
}


Item *Natural_join_column::create_item(THD *thd)
{
  if (view_field)
  {
    DBUG_ASSERT(table_field == NULL);
    SELECT_LEX *select= thd->lex->current_select;
    return create_view_field(thd, table_ref, &view_field->item,
                             view_field->name, &select->context);
  }
  return table_field;
}


Field *Natural_join_column::field()
{
  if (view_field)
  {
    DBUG_ASSERT(table_field == NULL);
    return NULL;
  }
  return table_field->field;
}


const char *Natural_join_column::table_name()
{
  DBUG_ASSERT(table_ref);
  return table_ref->alias;
}


const char *Natural_join_column::db_name()
{
  if (view_field)
    return table_ref->view_db.str;

  /*
    Test that TABLE_LIST::db is the same as TABLE_SHARE::db to
    ensure consistency. An exception are I_S schema tables, which
    are inconsistent in this respect.
  */
  DBUG_ASSERT(!strcmp(table_ref->db,
                      table_ref->table->s->db.str) ||
              (table_ref->schema_table &&
               is_infoschema_db(table_ref->table->s->db.str,
                                table_ref->table->s->db.length)));
  return table_ref->db;
}


GRANT_INFO *Natural_join_column::grant()
{
  if (view_field)
    return &(table_ref->grant);
  return &(table_ref->table->grant);
}


void Field_iterator_view::set(TABLE_LIST *table)
{
  DBUG_ASSERT(table->field_translation);
  view= table;
  ptr= table->field_translation;
  array_end= table->field_translation_end;
}


const char *Field_iterator_table::name()
{
  return (*ptr)->field_name;
}


Item *Field_iterator_table::create_item(THD *thd)
{
  SELECT_LEX *select= thd->lex->current_select;

  Item_field *item= new Item_field(thd, &select->context, *ptr);
  /*
    This function creates Item-s which don't go through fix_fields(); see same
    code in Item_field::fix_fields().
    */
  if (item && !thd->lex->in_sum_func &&
      select->cur_pos_in_all_fields != SELECT_LEX::ALL_FIELDS_UNDEF_POS)
  {
    if (thd->variables.sql_mode & MODE_ONLY_FULL_GROUP_BY)
    {
      item->push_to_non_agg_fields(select);
      select->set_non_agg_field_used(true);
    }
    if (thd->lex->current_select->with_sum_func &&
        !thd->lex->current_select->group_list.elements)
      item->maybe_null= true;
  }
  return item;
}


const char *Field_iterator_view::name()
{
  return ptr->name;
}


Item *Field_iterator_view::create_item(THD *thd)
{
  SELECT_LEX *select= thd->lex->current_select;
  return create_view_field(thd, view, &ptr->item, ptr->name,
                           &select->context);
}

static Item *create_view_field(THD *thd, TABLE_LIST *view, Item **field_ref,
                               const char *name,
                               Name_resolution_context *context)
{
  bool save_wrapper= thd->lex->select_lex.no_wrap_view_item;
  Item *field= *field_ref;
  DBUG_ENTER("create_view_field");

  if (view->schema_table_reformed)
  {
    /*
      Translation table items are always Item_fields and already fixed
      ('mysql_schema_table' function). So we can return directly the
      field. This case happens only for 'show & where' commands.
    */
    DBUG_ASSERT(field && field->fixed);
    DBUG_RETURN(field);
  }

  DBUG_ASSERT(field);
  thd->lex->current_select->no_wrap_view_item= TRUE;
  if (!field->fixed)
  {
    if (field->fix_fields(thd, field_ref))
    {
      thd->lex->current_select->no_wrap_view_item= save_wrapper;
      DBUG_RETURN(0);
    }
    field= *field_ref;
  }
  thd->lex->current_select->no_wrap_view_item= save_wrapper;
  if (save_wrapper)
  {
    DBUG_RETURN(field);
  }
  Item *item= new Item_direct_view_ref(context, field_ref,
                                       view->alias, view->table_name, name);
  DBUG_RETURN(item);
}


void Field_iterator_natural_join::set(TABLE_LIST *table_ref)
{
  DBUG_ASSERT(table_ref->join_columns);
  column_ref_it.init(*(table_ref->join_columns));
  cur_column_ref= column_ref_it++;
}


void Field_iterator_natural_join::next()
{
  cur_column_ref= column_ref_it++;
  DBUG_ASSERT(!cur_column_ref || ! cur_column_ref->table_field ||
              cur_column_ref->table_ref->table ==
              cur_column_ref->table_field->field->table);
}


void Field_iterator_table_ref::set_field_iterator()
{
  DBUG_ENTER("Field_iterator_table_ref::set_field_iterator");
  /*
    If the table reference we are iterating over is a natural join, or it is
    an operand of a natural join, and TABLE_LIST::join_columns contains all
    the columns of the join operand, then we pick the columns from
    TABLE_LIST::join_columns, instead of the  orginial container of the
    columns of the join operator.
  */
  if (table_ref->is_join_columns_complete)
  {
    /* Necesary, but insufficient conditions. */
    DBUG_ASSERT(table_ref->is_natural_join ||
                table_ref->nested_join ||
                (table_ref->join_columns &&
                /* This is a merge view. */
                ((table_ref->field_translation &&
                  table_ref->join_columns->elements ==
                  (ulong)(table_ref->field_translation_end -
                          table_ref->field_translation)) ||
                 /* This is stored table or a tmptable view. */
                 (!table_ref->field_translation &&
                  table_ref->join_columns->elements ==
                  table_ref->table->s->fields))));
    field_it= &natural_join_it;
    DBUG_PRINT("info",("field_it for '%s' is Field_iterator_natural_join",
                       table_ref->alias));
  }
  /* This is a merge view, so use field_translation. */
  else if (table_ref->field_translation)
  {
    DBUG_ASSERT(table_ref->view &&
                table_ref->effective_algorithm == VIEW_ALGORITHM_MERGE);
    field_it= &view_field_it;
    DBUG_PRINT("info", ("field_it for '%s' is Field_iterator_view",
                        table_ref->alias));
  }
  /* This is a base table or stored view. */
  else
  {
    DBUG_ASSERT(table_ref->table || table_ref->view);
    field_it= &table_field_it;
    DBUG_PRINT("info", ("field_it for '%s' is Field_iterator_table",
                        table_ref->alias));
  }
  field_it->set(table_ref);
  DBUG_VOID_RETURN;
}


void Field_iterator_table_ref::set(TABLE_LIST *table)
{
  DBUG_ASSERT(table);
  first_leaf= table->first_leaf_for_name_resolution();
  last_leaf=  table->last_leaf_for_name_resolution();
  DBUG_ASSERT(first_leaf && last_leaf);
  table_ref= first_leaf;
  set_field_iterator();
}


void Field_iterator_table_ref::next()
{
  /* Move to the next field in the current table reference. */
  field_it->next();
  /*
    If all fields of the current table reference are exhausted, move to
    the next leaf table reference.
  */
  if (field_it->end_of_fields() && table_ref != last_leaf)
  {
    table_ref= table_ref->next_name_resolution_table;
    DBUG_ASSERT(table_ref);
    set_field_iterator();
  }
}


const char *Field_iterator_table_ref::get_table_name()
{
  if (table_ref->view)
    return table_ref->view_name.str;
  else if (table_ref->is_natural_join)
    return natural_join_it.column_ref()->table_name();

  DBUG_ASSERT(!strcmp(table_ref->table_name,
                      table_ref->table->s->table_name.str));
  return table_ref->table_name;
}


const char *Field_iterator_table_ref::get_db_name()
{
  if (table_ref->view)
    return table_ref->view_db.str;
  else if (table_ref->is_natural_join)
    return natural_join_it.column_ref()->db_name();

  /*
    Test that TABLE_LIST::db is the same as TABLE_SHARE::db to
    ensure consistency. An exception are I_S schema tables, which
    are inconsistent in this respect.
  */
  DBUG_ASSERT(!strcmp(table_ref->db, table_ref->table->s->db.str) ||
              (table_ref->schema_table &&
               is_infoschema_db(table_ref->table->s->db.str,
                                table_ref->table->s->db.length)));

  return table_ref->db;
}


GRANT_INFO *Field_iterator_table_ref::grant()
{
  if (table_ref->view)
    return &(table_ref->grant);
  else if (table_ref->is_natural_join)
    return natural_join_it.column_ref()->grant();
  return &(table_ref->table->grant);
}


/*
  Create new or return existing column reference to a column of a
  natural/using join.

  SYNOPSIS
    Field_iterator_table_ref::get_or_create_column_ref()
    parent_table_ref  the parent table reference over which the
                      iterator is iterating

  DESCRIPTION
    Create a new natural join column for the current field of the
    iterator if no such column was created, or return an already
    created natural join column. The former happens for base tables or
    views, and the latter for natural/using joins. If a new field is
    created, then the field is added to 'parent_table_ref' if it is
    given, or to the original table referene of the field if
    parent_table_ref == NULL.

  NOTES
    This method is designed so that when a Field_iterator_table_ref
    walks through the fields of a table reference, all its fields
    are created and stored as follows:
    - If the table reference being iterated is a stored table, view or
      natural/using join, store all natural join columns in a list
      attached to that table reference.
    - If the table reference being iterated is a nested join that is
      not natural/using join, then do not materialize its result
      fields. This is OK because for such table references
      Field_iterator_table_ref iterates over the fields of the nested
      table references (recursively). In this way we avoid the storage
      of unnecessay copies of result columns of nested joins.

  RETURN
    #     Pointer to a column of a natural join (or its operand)
    NULL  No memory to allocate the column
*/

Natural_join_column *
Field_iterator_table_ref::get_or_create_column_ref(THD *thd, TABLE_LIST *parent_table_ref)
{
  Natural_join_column *nj_col;
  bool is_created= TRUE;
  uint field_count;
  TABLE_LIST *add_table_ref= parent_table_ref ?
                             parent_table_ref : table_ref;
  LINT_INIT(field_count);

  if (field_it == &table_field_it)
  {
    /* The field belongs to a stored table. */
    Field *tmp_field= table_field_it.field();
    Item_field *tmp_item=
      new Item_field(thd, &thd->lex->current_select->context, tmp_field);
    if (!tmp_item)
      return NULL;
    nj_col= new Natural_join_column(tmp_item, table_ref);
    field_count= table_ref->table->s->fields;
  }
  else if (field_it == &view_field_it)
  {
    /* The field belongs to a merge view or information schema table. */
    Field_translator *translated_field= view_field_it.field_translator();
    nj_col= new Natural_join_column(translated_field, table_ref);
    field_count= table_ref->field_translation_end -
                 table_ref->field_translation;
  }
  else
  {
    /*
      The field belongs to a NATURAL join, therefore the column reference was
      already created via one of the two constructor calls above. In this case
      we just return the already created column reference.
    */
    DBUG_ASSERT(table_ref->is_join_columns_complete);
    is_created= FALSE;
    nj_col= natural_join_it.column_ref();
    DBUG_ASSERT(nj_col);
  }
  DBUG_ASSERT(!nj_col->table_field ||
              nj_col->table_ref->table == nj_col->table_field->field->table);

  /*
    If the natural join column was just created add it to the list of
    natural join columns of either 'parent_table_ref' or to the table
    reference that directly contains the original field.
  */
  if (is_created)
  {
    /* Make sure not all columns were materialized. */
    DBUG_ASSERT(!add_table_ref->is_join_columns_complete);
    if (!add_table_ref->join_columns)
    {
      /* Create a list of natural join columns on demand. */
      if (!(add_table_ref->join_columns= new List<Natural_join_column>))
        return NULL;
      add_table_ref->is_join_columns_complete= FALSE;
    }
    add_table_ref->join_columns->push_back(nj_col);
    /*
      If new fields are added to their original table reference, mark if
      all fields were added. We do it here as the caller has no easy way
      of knowing when to do it.
      If the fields are being added to parent_table_ref, then the caller
      must take care to mark when all fields are created/added.
    */
    if (!parent_table_ref &&
        add_table_ref->join_columns->elements == field_count)
      add_table_ref->is_join_columns_complete= TRUE;
  }

  return nj_col;
}


/*
  Return an existing reference to a column of a natural/using join.

  SYNOPSIS
    Field_iterator_table_ref::get_natural_column_ref()

  DESCRIPTION
    The method should be called in contexts where it is expected that
    all natural join columns are already created, and that the column
    being retrieved is a Natural_join_column.

  RETURN
    #     Pointer to a column of a natural join (or its operand)
    NULL  No memory to allocate the column
*/

Natural_join_column *
Field_iterator_table_ref::get_natural_column_ref()
{
  Natural_join_column *nj_col;

  DBUG_ASSERT(field_it == &natural_join_it);
  /*
    The field belongs to a NATURAL join, therefore the column reference was
    already created via one of the two constructor calls above. In this case
    we just return the already created column reference.
  */
  nj_col= natural_join_it.column_ref();
  DBUG_ASSERT(nj_col &&
              (!nj_col->table_field ||
               nj_col->table_ref->table == nj_col->table_field->field->table));
  return nj_col;
}

/*****************************************************************************
  Functions to handle column usage bitmaps (read_set, write_set etc...)
*****************************************************************************/

/* Reset all columns bitmaps */

void TABLE::clear_column_bitmaps()
{
  /*
    Reset column read/write usage. It's identical to:
    bitmap_clear_all(&table->def_read_set);
    bitmap_clear_all(&table->def_write_set);
  */
  memset(def_read_set.bitmap, 0, s->column_bitmap_size*2);
  column_bitmaps_set(&def_read_set, &def_write_set);
}


/**
  Tell handler we are going to call position() and rnd_pos() later.
  
  This is needed for handlers that uses the primary key to find the
  row. In this case we have to extend the read bitmap with the primary
  key fields.

  @note: Calling this function does not initialize the table for
  reading using rnd_pos(). rnd_init() still has to be called before
  rnd_pos().
*/

void TABLE::prepare_for_position()
{
  DBUG_ENTER("TABLE::prepare_for_position");

  if ((file->ha_table_flags() & HA_PRIMARY_KEY_REQUIRED_FOR_POSITION) &&
      s->primary_key < MAX_KEY)
  {
    mark_columns_used_by_index_no_reset(s->primary_key, read_set);
    /* signal change */
    file->column_bitmaps_signal();
  }
  DBUG_VOID_RETURN;
}


/*
  Mark that only fields from one key is used

  NOTE:
    This changes the bitmap to use the tmp bitmap
    After this, you can't access any other columns in the table until
    bitmaps are reset, for example with TABLE::clear_column_bitmaps().
*/

void TABLE::mark_columns_used_by_index(uint index)
{
  MY_BITMAP *bitmap= &tmp_set;
  DBUG_ENTER("TABLE::mark_columns_used_by_index");

  set_keyread(TRUE);
  bitmap_clear_all(bitmap);
  mark_columns_used_by_index_no_reset(index, bitmap);
  column_bitmaps_set(bitmap, bitmap);
  DBUG_VOID_RETURN;
}


/*
  mark columns used by key, but don't reset other fields
*/

void TABLE::mark_columns_used_by_index_no_reset(uint index,
                                                   MY_BITMAP *bitmap)
{
  KEY_PART_INFO *key_part= key_info[index].key_part;
  KEY_PART_INFO *key_part_end= (key_part +
                                key_info[index].user_defined_key_parts);
  for (;key_part != key_part_end; key_part++)
    bitmap_set_bit(bitmap, key_part->fieldnr-1);
}


/*
  Mark auto-increment fields as used fields in both read and write maps

  NOTES
    This is needed in insert & update as the auto-increment field is
    always set and sometimes read.
*/

void TABLE::mark_auto_increment_column()
{
  DBUG_ASSERT(found_next_number_field);
  /*
    We must set bit in read set as update_auto_increment() is using the
    store() to check overflow of auto_increment values
  */
  bitmap_set_bit(read_set, found_next_number_field->field_index);
  bitmap_set_bit(write_set, found_next_number_field->field_index);
  if (s->next_number_keypart)
    mark_columns_used_by_index_no_reset(s->next_number_index, read_set);
  file->column_bitmaps_signal();
}


/*
  Mark columns needed for doing an delete of a row

  DESCRIPTON
    Some table engines don't have a cursor on the retrieve rows
    so they need either to use the primary key or all columns to
    be able to delete a row.

    If the engine needs this, the function works as follows:
    - If primary key exits, mark the primary key columns to be read.
    - If not, mark all columns to be read

    If the engine has HA_REQUIRES_KEY_COLUMNS_FOR_DELETE, we will
    mark all key columns as 'to-be-read'. This allows the engine to
    loop over the given record to find all keys and doesn't have to
    retrieve the row again.
*/

void TABLE::mark_columns_needed_for_delete()
{
  mark_columns_per_binlog_row_image();

  if (triggers)
    triggers->mark_fields_used(TRG_EVENT_DELETE);
  if (file->ha_table_flags() & HA_REQUIRES_KEY_COLUMNS_FOR_DELETE)
  {
    Field **reg_field;
    for (reg_field= field ; *reg_field ; reg_field++)
    {
      if ((*reg_field)->flags & PART_KEY_FLAG)
        bitmap_set_bit(read_set, (*reg_field)->field_index);
    }
    file->column_bitmaps_signal();
  }
  if (file->ha_table_flags() & HA_PRIMARY_KEY_REQUIRED_FOR_DELETE)
  {

    /*
      If the handler has no cursor capabilites we have to read
      either the primary key, the hidden primary key or all columns to
      be able to do an delete
    */
    if (s->primary_key == MAX_KEY)
    {
      /*
        If in RBR, we have alreay marked the full before image
        in mark_columns_per_binlog_row_image, if not, then use
        the hidden primary key
      */
      if (!(mysql_bin_log.is_open() && in_use &&
          in_use->is_current_stmt_binlog_format_row()))
        file->use_hidden_primary_key();
    }
    else
      mark_columns_used_by_index_no_reset(s->primary_key, read_set);

    file->column_bitmaps_signal();
  }
}


/**
  @brief
  Mark columns needed for doing an update of a row

  @details
    Some engines needs to have all columns in an update (to be able to
    build a complete row). If this is the case, we mark all not
    updated columns to be read.

    If this is no the case, we do like in the delete case and mark
    if neeed, either the primary key column or all columns to be read.
    (see mark_columns_needed_for_delete() for details)

    If the engine has HA_REQUIRES_KEY_COLUMNS_FOR_DELETE, we will
    mark all USED key columns as 'to-be-read'. This allows the engine to
    loop over the given record to find all changed keys and doesn't have to
    retrieve the row again.
    
    Unlike other similar methods, it doesn't mark fields used by triggers,
    that is the responsibility of the caller to do, by using
    Table_triggers_list::mark_used_fields(TRG_EVENT_UPDATE)!
*/

void TABLE::mark_columns_needed_for_update()
{

  DBUG_ENTER("mark_columns_needed_for_update");
  mark_columns_per_binlog_row_image();
  if (file->ha_table_flags() & HA_REQUIRES_KEY_COLUMNS_FOR_DELETE)
  {
    /* Mark all used key columns for read */
    Field **reg_field;
    for (reg_field= field ; *reg_field ; reg_field++)
    {
      /* Merge keys is all keys that had a column refered to in the query */
      if (merge_keys.is_overlapping((*reg_field)->part_of_key))
        bitmap_set_bit(read_set, (*reg_field)->field_index);
    }
    file->column_bitmaps_signal();
  }

  if (file->ha_table_flags() & HA_PRIMARY_KEY_REQUIRED_FOR_DELETE)
  {
    /*
      If the handler has no cursor capabilites we have to read either
      the primary key, the hidden primary key or all columns to be
      able to do an update
    */
    if (s->primary_key == MAX_KEY)
    {
      /*
        If in RBR, we have alreay marked the full before image
        in mark_columns_per_binlog_row_image, if not, then use
        the hidden primary key
      */
      if (!(mysql_bin_log.is_open() && in_use &&
          in_use->is_current_stmt_binlog_format_row()))
        file->use_hidden_primary_key();
    }
    else
      mark_columns_used_by_index_no_reset(s->primary_key, read_set);

    file->column_bitmaps_signal();
  }
  DBUG_VOID_RETURN;
}

/*
  Mark columns according the binlog row image option.

  When logging in RBR, the user can select whether to
  log partial or full rows, depending on the table
  definition, and the value of binlog_row_image.

  Semantics of the binlog_row_image are the following 
  (PKE - primary key equivalent, ie, PK fields if PK 
  exists, all fields otherwise):

  binlog_row_image= MINIMAL
    - This marks the PKE fields in the read_set
    - This marks all fields where a value was specified
      in the write_set

  binlog_row_image= NOBLOB
    - This marks PKE + all non-blob fields in the read_set
    - This marks all fields where a value was specified
      and all non-blob fields in the write_set

  binlog_row_image= FULL
    - all columns in the read_set
    - all columns in the write_set
    
  This marking is done without resetting the original 
  bitmaps. This means that we will strip extra fields in
  the read_set at binlogging time (for those cases that 
  we only want to log a PK and we needed other fields for
  execution).
 */
void TABLE::mark_columns_per_binlog_row_image()
{
  DBUG_ENTER("mark_columns_per_binlog_row_image");
  DBUG_ASSERT(read_set->bitmap);
  DBUG_ASSERT(write_set->bitmap);

  /**
    If in RBR we may need to mark some extra columns,
    depending on the binlog-row-image command line argument.
   */
  if ((mysql_bin_log.is_open() && in_use &&
       in_use->is_current_stmt_binlog_format_row() &&
       !ha_check_storage_engine_flag(s->db_type(), HTON_NO_BINLOG_ROW_OPT)))
  {

    THD *thd= current_thd;

    /* if there is no PK, then mark all columns for the BI. */
    if (s->primary_key >= MAX_KEY)
      bitmap_set_all(read_set);

    switch (thd->variables.binlog_row_image)
    {
      case BINLOG_ROW_IMAGE_FULL:
        if (s->primary_key < MAX_KEY)
          bitmap_set_all(read_set);
        bitmap_set_all(write_set);
        break;
      case BINLOG_ROW_IMAGE_NOBLOB:
        /* for every field that is not set, mark it unless it is a blob */
        for (Field **ptr=field ; *ptr ; ptr++)
        {
          Field *field= *ptr;
          /* 
            bypass blob fields. These can be set or not set, we don't care.
            Later, at binlogging time, if we don't need them in the before 
            image, we will discard them.

            If set in the AI, then the blob is really needed, there is 
            nothing we can do about it.
           */
          if ((s->primary_key < MAX_KEY) && 
              ((field->flags & PRI_KEY_FLAG) || 
              (field->type() != MYSQL_TYPE_BLOB)))
            bitmap_set_bit(read_set, field->field_index);

          if (field->type() != MYSQL_TYPE_BLOB)
            bitmap_set_bit(write_set, field->field_index);
        }
        break;
      case BINLOG_ROW_IMAGE_MINIMAL:
        /* mark the primary key if available in the read_set */
        if (s->primary_key < MAX_KEY)
          mark_columns_used_by_index_no_reset(s->primary_key, read_set);
        break;

      default: 
        DBUG_ASSERT(FALSE);
    }
    file->column_bitmaps_signal();
  }

  DBUG_VOID_RETURN;
}



/**
  @brief
  Allocate space for keys

  @param key_count  number of keys to allocate.

  @details
  Allocate space enough to fit 'key_count' keys for this table.

  @return FALSE space was successfully allocated.
  @return TRUE OOM error occur.
*/

bool TABLE::alloc_keys(uint key_count)
{
  DBUG_ASSERT(!s->keys);
  max_keys= key_count;
  if (!(key_info= s->key_info=
        (KEY*) alloc_root(&mem_root, sizeof(KEY)*max_keys)))
    return TRUE;

  memset(key_info, 0, sizeof(KEY)*max_keys);
  return FALSE;
}


/**
  @brief Add one key to a temporary table.

  @param key_parts      bitmap of fields that take a part in the key.
  @param key_name       name of the key

  @details
  Creates a key for this table from fields which corresponds the bits set to 1
  in the 'key_parts' bitmap. The 'key_name' name is given to the newly created
  key.
  @see add_derived_key

  @TODO somehow manage to create keys in tmp_table_param for unification
        purposes

  @return TRUE OOM error.
  @return FALSE the key was created or ignored (too long key).
*/

bool TABLE::add_tmp_key(Field_map *key_parts, char *key_name)
{
  DBUG_ASSERT(!created && s->keys < max_keys && key_parts);

  KEY* cur_key= key_info + s->keys;
  Field **reg_field;
  uint i;
  bool key_start= TRUE;
  uint field_count= 0;
  uchar *key_buf;
  KEY_PART_INFO* key_part_info;
  uint key_len= 0;

  for (i= 0, reg_field=field ; *reg_field; i++, reg_field++)
  {
    if (key_parts->is_set(i))
    {
      KEY_PART_INFO tkp;
      // Ensure that we're not creating a key over a blob field.
      DBUG_ASSERT(!((*reg_field)->flags & BLOB_FLAG));
      /*
        Check if possible key is too long, ignore it if so.
        The reason to use MI_MAX_KEY_LENGTH (myisam's default) is that it is
        smaller than MAX_KEY_LENGTH (heap's default) and it's unknown whether
        myisam or heap will be used for tmp table.
      */
      tkp.init_from_field(*reg_field);
      key_len+= tkp.store_length;
      if (key_len > MI_MAX_KEY_LENGTH)
      {
        max_keys--;
        return FALSE;
      }
    }
    field_count++;
  }
  const uint key_part_count= key_parts->bits_set();

  /* Allocate key parts in the tables' mem_root. */
  size_t key_buf_size= sizeof(KEY_PART_INFO) * key_part_count +
                       sizeof(ulong) * key_part_count;
  key_buf= (uchar*) alloc_root(&mem_root, key_buf_size);

  if (!key_buf)
    return TRUE;
  memset(key_buf, 0, key_buf_size);
  cur_key->key_part= key_part_info= (KEY_PART_INFO*) key_buf;
  cur_key->usable_key_parts= cur_key->user_defined_key_parts= key_part_count;
  cur_key->actual_key_parts= cur_key->user_defined_key_parts;
  s->key_parts+= key_part_count;
  cur_key->key_length= key_len;
  cur_key->algorithm= HA_KEY_ALG_BTREE;
  cur_key->name= key_name;
  cur_key->actual_flags= cur_key->flags= HA_GENERATED_KEY;
  cur_key->rec_per_key= (ulong*) (key_buf + sizeof(KEY_PART_INFO) * key_part_count);
  cur_key->table= this;

  if (field_count == key_part_count)
    covering_keys.set_bit(s->keys);

  keys_in_use_for_group_by.set_bit(s->keys);
  keys_in_use_for_order_by.set_bit(s->keys);
  for (i= 0, reg_field=field ; *reg_field; i++, reg_field++)
  {
    if (!(key_parts->is_set(i)))
      continue;

    if (key_start)
      (*reg_field)->key_start.set_bit(s->keys);
    key_start= FALSE;
    (*reg_field)->part_of_key.set_bit(s->keys);
    (*reg_field)->part_of_sortkey.set_bit(s->keys);
    (*reg_field)->flags|= PART_KEY_FLAG;
    key_part_info->init_from_field(*reg_field);
    key_part_info++;
  }
  set_if_bigger(s->max_key_length, cur_key->key_length);
  s->keys++;
  return FALSE;
}

/*
  @brief
  Save the specified index for later use for ref access.

  @param key_to_save the key to save

  @details
  Save given index as index #0. Table is configured to ignore other indexes.
  Memory occupied by other indexes and index parts will be freed along with
  the table. If the 'key_to_save' is negative then all indexes are freed.
  After keys info being changed, info in fields regarding taking part in keys
  becomes outdated. This function fixes this also.
  @see add_derived_key
*/

void TABLE::use_index(int key_to_save)
{
  DBUG_ASSERT(!created && s->keys && key_to_save < (int)s->keys);

  Field **reg_field;
  /*
    Reset the flags and maps associated with the fields. They are set
    only for the key chosen by the optimizer later.
   */
  for (reg_field=field ; *reg_field; reg_field++)
  {
    if(!(*reg_field)->part_of_key.is_set(key_to_save))
      (*reg_field)->key_start.clear_all();
    (*reg_field)->part_of_key.clear_all();
    (*reg_field)->part_of_sortkey.clear_all();
    (*reg_field)->flags&= ~PART_KEY_FLAG;
  }

  /* Drop all keys if none of them were chosen */
  if (key_to_save < 0)
  {
    key_info= s->key_info= 0;
    s->key_parts= 0;
    s->keys= 0;
    covering_keys.clear_all();
    keys_in_use_for_group_by.clear_all();
    keys_in_use_for_order_by.clear_all();
  }
  else
  {
    /* Set the flags and maps for the key chosen by the optimizer */
    uint i;
    KEY_PART_INFO *kp;
    for (kp= key_info[key_to_save].key_part, i= 0;
         i < key_info[key_to_save].user_defined_key_parts;
         i++, kp++)
    {
      if (kp->field->key_start.is_set(key_to_save))
        kp->field->key_start.set_prefix(1);
      kp->field->part_of_key.set_prefix(1);
      kp->field->part_of_sortkey.set_prefix(1);
      kp->field->flags|= PART_KEY_FLAG;
    }

    /* Save the given key. No need to copy key#0. */
    if (key_to_save > 0)
      key_info[0]= key_info[key_to_save];
    s->keys= 1;
    s->key_parts= key_info[0].user_defined_key_parts;
    if (covering_keys.is_set(key_to_save))
      covering_keys.set_prefix(1);
    else
      covering_keys.clear_all();
    keys_in_use_for_group_by.set_prefix(1);
    keys_in_use_for_order_by.set_prefix(1);
  }
}



/*
  Mark columns the handler needs for doing an insert

  For now, this is used to mark fields used by the trigger
  as changed.
*/

void TABLE::mark_columns_needed_for_insert()
{
  mark_columns_per_binlog_row_image();
  if (triggers)
  {
    /*
      We don't need to mark columns which are used by ON DELETE and
      ON UPDATE triggers, which may be invoked in case of REPLACE or
      INSERT ... ON DUPLICATE KEY UPDATE, since before doing actual
      row replacement or update write_record() will mark all table
      fields as used.
    */
    triggers->mark_fields_used(TRG_EVENT_INSERT);
  }
  if (found_next_number_field)
    mark_auto_increment_column();
}


/*
  Cleanup this table for re-execution.

  SYNOPSIS
    TABLE_LIST::reinit_before_use()
*/

void TABLE_LIST::reinit_before_use(THD *thd)
{
  /*
    Reset old pointers to TABLEs: they are not valid since the tables
    were closed in the end of previous prepare or execute call.
  */
  table= 0;

 /*
   Reset table_name and table_name_length,if it is a anonymous derived table
   or schema table. They are not valid as TABLEs were closed in the end of
   previous prepare or execute call. For derived table of view, restore view's
   name and database wiped out by derived table processing.
 */
  if (derived != NULL)
  {
    if (view != NULL)
    {
      db= view_db.str;
      db_length= view_db.length;
      table_name= view_name.str;
      table_name_length= view_name.length;
    }
    else
    {
      table_name= NULL;
      table_name_length= 0;
    }
  }
  else if (schema_table_name)
  {
    table_name= schema_table_name;
    table_name_length= strlen(schema_table_name);
  }

  /* Reset is_schema_table_processed value(needed for I_S tables */
  schema_table_state= NOT_PROCESSED;

  TABLE_LIST *embedded; /* The table at the current level of nesting. */
  TABLE_LIST *parent_embedding= this; /* The parent nested table reference. */
  do
  {
    embedded= parent_embedding;
    if (embedded->prep_join_cond)
      embedded->
        set_join_cond(embedded->prep_join_cond->copy_andor_structure(thd));
    parent_embedding= embedded->embedding;
  }
  while (parent_embedding &&
         parent_embedding->nested_join->join_list.head() == embedded);

  mdl_request.ticket= NULL;
}

/*
  Return subselect that contains the FROM list this table is taken from

  SYNOPSIS
    TABLE_LIST::containing_subselect()
 
  RETURN
    Subselect item for the subquery that contains the FROM list
    this table is taken from if there is any
    0 - otherwise

*/

Item_subselect *TABLE_LIST::containing_subselect()
{    
  return (select_lex ? select_lex->master_unit()->item : 0);
}

uint TABLE_LIST::query_block_id() const
{
  return derived ? derived->first_select()->select_number : 0;
}

/*
  Compiles the tagged hints list and fills up the bitmasks.

  SYNOPSIS
    process_index_hints()
      table         the TABLE to operate on.

  DESCRIPTION
    The parser collects the index hints for each table in a "tagged list" 
    (TABLE_LIST::index_hints). Using the information in this tagged list
    this function sets the members st_table::keys_in_use_for_query,
    st_table::keys_in_use_for_group_by, st_table::keys_in_use_for_order_by,
    st_table::force_index, st_table::force_index_order,
    st_table::force_index_group and st_table::covering_keys.

    Current implementation of the runtime does not allow mixing FORCE INDEX
    and USE INDEX, so this is checked here. Then the FORCE INDEX list 
    (if non-empty) is appended to the USE INDEX list and a flag is set.

    Multiple hints of the same kind are processed so that each clause 
    is applied to what is computed in the previous clause.
    For example:
        USE INDEX (i1) USE INDEX (i2)
    is equivalent to
        USE INDEX (i1,i2)
    and means "consider only i1 and i2".
        
    Similarly
        USE INDEX () USE INDEX (i1)
    is equivalent to
        USE INDEX (i1)
    and means "consider only the index i1"

    It is OK to have the same index several times, e.g. "USE INDEX (i1,i1)" is
    not an error.
        
    Different kind of hints (USE/FORCE/IGNORE) are processed in the following
    order:
      1. All indexes in USE (or FORCE) INDEX are added to the mask.
      2. All IGNORE INDEX

    e.g. "USE INDEX i1, IGNORE INDEX i1, USE INDEX i1" will not use i1 at all
    as if we had "USE INDEX i1, USE INDEX i1, IGNORE INDEX i1".

    As an optimization if there is a covering index, and we have 
    IGNORE INDEX FOR GROUP/ORDER, and this index is used for the JOIN part, 
    then we have to ignore the IGNORE INDEX FROM GROUP/ORDER.

  RETURN VALUE
    FALSE                no errors found
    TRUE                 found and reported an error.
*/
bool TABLE_LIST::process_index_hints(TABLE *tbl)
{
  /* initialize the result variables */
  tbl->keys_in_use_for_query= tbl->keys_in_use_for_group_by= 
    tbl->keys_in_use_for_order_by= tbl->s->usable_indexes();

  /* index hint list processing */
  if (index_hints)
  {
    /* Temporary variables used to collect hints of each kind. */
    key_map index_join[INDEX_HINT_FORCE + 1];
    key_map index_order[INDEX_HINT_FORCE + 1];
    key_map index_group[INDEX_HINT_FORCE + 1];
    Index_hint *hint;
    bool have_empty_use_join= FALSE, have_empty_use_order= FALSE, 
         have_empty_use_group= FALSE;
    List_iterator <Index_hint> iter(*index_hints);

    /* iterate over the hints list */
    while ((hint= iter++))
    {
      uint pos;

      /* process empty USE INDEX () */
      if (hint->type == INDEX_HINT_USE && !hint->key_name.str)
      {
        if (hint->clause & INDEX_HINT_MASK_JOIN)
        {
          index_join[hint->type].clear_all();
          have_empty_use_join= TRUE;
        }
        if (hint->clause & INDEX_HINT_MASK_ORDER)
        {
          index_order[hint->type].clear_all();
          have_empty_use_order= TRUE;
        }
        if (hint->clause & INDEX_HINT_MASK_GROUP)
        {
          index_group[hint->type].clear_all();
          have_empty_use_group= TRUE;
        }
        continue;
      }

      /* 
        Check if an index with the given name exists and get his offset in 
        the keys bitmask for the table 
      */
      if (tbl->s->keynames.type_names == 0 ||
          (pos= find_type(&tbl->s->keynames, hint->key_name.str,
                          hint->key_name.length, 1)) <= 0 ||
          !tbl->s->key_info[pos - 1].is_visible)
      {
        my_error(ER_KEY_DOES_NOT_EXITS, MYF(0), hint->key_name.str, alias);
        return 1;
      }

      pos--;

      /* add to the appropriate clause mask */
      if (hint->clause & INDEX_HINT_MASK_JOIN)
        index_join[hint->type].set_bit (pos);
      if (hint->clause & INDEX_HINT_MASK_ORDER)
        index_order[hint->type].set_bit (pos);
      if (hint->clause & INDEX_HINT_MASK_GROUP)
        index_group[hint->type].set_bit (pos);
    }

    /* cannot mix USE INDEX and FORCE INDEX */
    if ((!index_join[INDEX_HINT_FORCE].is_clear_all() ||
         !index_order[INDEX_HINT_FORCE].is_clear_all() ||
         !index_group[INDEX_HINT_FORCE].is_clear_all()) &&
        (!index_join[INDEX_HINT_USE].is_clear_all() ||  have_empty_use_join ||
         !index_order[INDEX_HINT_USE].is_clear_all() || have_empty_use_order ||
         !index_group[INDEX_HINT_USE].is_clear_all() || have_empty_use_group))
    {
      my_error(ER_WRONG_USAGE, MYF(0), index_hint_type_name[INDEX_HINT_USE],
               index_hint_type_name[INDEX_HINT_FORCE]);
      return 1;
    }

    /* process FORCE INDEX as USE INDEX with a flag */
    if (!index_order[INDEX_HINT_FORCE].is_clear_all())
    {
      tbl->force_index_order= TRUE;
      index_order[INDEX_HINT_USE].merge(index_order[INDEX_HINT_FORCE]);
    }

    if (!index_group[INDEX_HINT_FORCE].is_clear_all())
    {
      tbl->force_index_group= TRUE;
      index_group[INDEX_HINT_USE].merge(index_group[INDEX_HINT_FORCE]);
    }

    /*
      TODO: get rid of tbl->force_index (on if any FORCE INDEX is specified) and
      create tbl->force_index_join instead.
      Then use the correct force_index_XX instead of the global one.
    */
    if (!index_join[INDEX_HINT_FORCE].is_clear_all() ||
        tbl->force_index_group || tbl->force_index_order)
    {
      tbl->force_index= TRUE;
      index_join[INDEX_HINT_USE].merge(index_join[INDEX_HINT_FORCE]);
    }

    /* apply USE INDEX */
    if (!index_join[INDEX_HINT_USE].is_clear_all() || have_empty_use_join)
      tbl->keys_in_use_for_query.intersect(index_join[INDEX_HINT_USE]);
    if (!index_order[INDEX_HINT_USE].is_clear_all() || have_empty_use_order)
      tbl->keys_in_use_for_order_by.intersect (index_order[INDEX_HINT_USE]);
    if (!index_group[INDEX_HINT_USE].is_clear_all() || have_empty_use_group)
      tbl->keys_in_use_for_group_by.intersect (index_group[INDEX_HINT_USE]);

    /* apply IGNORE INDEX */
    tbl->keys_in_use_for_query.subtract (index_join[INDEX_HINT_IGNORE]);
    tbl->keys_in_use_for_order_by.subtract (index_order[INDEX_HINT_IGNORE]);
    tbl->keys_in_use_for_group_by.subtract (index_group[INDEX_HINT_IGNORE]);
  }

  /* make sure covering_keys don't include indexes disabled with a hint */
  tbl->covering_keys.intersect(tbl->keys_in_use_for_query);
  return 0;
}


size_t max_row_length(TABLE *table, const uchar *data)
{
  TABLE_SHARE *table_s= table->s;
  size_t length= table_s->reclength + 2 * table_s->fields;
  uint *const beg= table_s->blob_field;
  uint *const end= beg + table_s->blob_fields;

  for (uint *ptr= beg ; ptr != end ; ++ptr)
  {
    Field_blob* const blob= (Field_blob*) table->field[*ptr];
    length+= blob->get_length((const uchar*)
                              (data + blob->offset(table->record[0]))) +
      HA_KEY_BLOB_LENGTH;
  }
  return length;
}


/**
   Helper function which allows to allocate metadata lock request
   objects for all elements of table list.
*/

void init_mdl_requests(TABLE_LIST *table_list)
{
  for ( ; table_list ; table_list= table_list->next_global)
    table_list->mdl_request.init(MDL_key::TABLE,
                                 table_list->db, table_list->table_name,
                                 table_list->lock_type >= TL_WRITE_ALLOW_WRITE ?
                                 MDL_SHARED_WRITE : MDL_SHARED_READ,
                                 MDL_TRANSACTION);
}



///  @returns true if materializable table contains one or zero rows
bool TABLE_LIST::materializable_is_const() const
{
  DBUG_ASSERT(uses_materialization());
  return get_unit()->get_result()->estimated_rowcount <= 1;
}

/**
  @brief
  Retrieve number of rows in the table

  @details
  Retrieve number of rows in the table referred by this TABLE_LIST and
  store it in the table's stats.records variable. If this TABLE_LIST refers
  to a materialized derived table/view, then the estimated number of rows of
  the derived table/view is used instead.

  @return 0          ok
  @return non zero   error
*/

int TABLE_LIST::fetch_number_of_rows()
{
  int error= 0;
  if (uses_materialization())
    table->file->stats.records= derived->get_result()->estimated_rowcount;
  else
    error= table->file->info(HA_STATUS_VARIABLE | HA_STATUS_NO_LOCK);
  return error;
}


/**
  A helper function to add a derived key to the list of possible keys

  @param derived_key_list  list of all possible derived keys
  @param field             referenced field
  @param ref_by_tbl        the table that refers to given field

  @details The possible key to be used for join with table with ref_by_tbl
  table map is extended to include 'field'. If ref_by_tbl == 0 then the key
  that includes all referred fields is extended.

  @note
  Procedure of keys generation for result tables of materialized derived
  tables/views for allowing ref access to them.

  A key is generated for each equi-join pair (derived table, another table).
  Each generated key consists of fields of derived table used in equi-join.
  Example:

    SELECT * FROM (SELECT f1, f2, count(*) FROM t1 GROUP BY f1) tt JOIN
                  t1 ON tt.f1=t1.f3 and tt.f2=t1.f4;

  In this case for the derived table tt one key will be generated. It will
  consist of two parts f1 and f2.
  Example:

    SELECT * FROM (SELECT f1, f2, count(*) FROM t1 GROUP BY f1) tt JOIN
                  t1 ON tt.f1=t1.f3 JOIN
                  t2 ON tt.f2=t2.f4;

  In this case for the derived table tt two keys will be generated.
  One key over f1 field, and another key over f2 field.
  Currently optimizer may choose to use only one such key, thus the second
  one will be dropped after the range optimizer is finished.
  See also JOIN::drop_unused_derived_keys function.
  Example:

    SELECT * FROM (SELECT f1, f2, count(*) FROM t1 GROUP BY f1) tt JOIN
                  t1 ON tt.f1=a_function(t1.f3);

  In this case for the derived table tt one key will be generated. It will
  consist of one field - f1.
  In all cases beside one-per-table keys one additional key is generated.
  It includes all fields referenced by other tables.

  Implementation is split in two steps:
    gather information on all used fields of derived tables/view and
      store it in lists of possible keys, one per a derived table/view.
    add keys to result tables of derived tables/view using info from above
      lists.

  The above procedure is implemented in 4 functions:
    TABLE_LIST::update_derived_keys
                          Create/extend list of possible keys for one derived
                          table/view based on given field/used tables info.
                          (Step one)
    JOIN::generate_derived_keys
                          This function is called from update_ref_and_keys
                          when all possible info on keys is gathered and it's
                          safe to add keys - no keys or key parts would be
                          missed.  Walk over list of derived tables/views and
                          call to TABLE_LIST::generate_keys to actually
                          generate keys. (Step two)
    TABLE_LIST::generate_keys
                          Walks over list of possible keys for this derived
                          table/view to add keys to the result table.
                          Calls to TABLE::add_tmp_key to actually add
                          keys. (Step two)
    TABLE::add_tmp_key    Creates one index description according to given
                          bitmap of used fields. (Step two)
  There is also the fifth function called TABLE::use_index. It saves used
  key and frees others. It is called when the optimizer has chosen which key
  it will use, thus we don't need other keys anymore.

  @return TRUE  OOM
  @return FALSE otherwise
*/

static bool add_derived_key(List<Derived_key> &derived_key_list, Field *field,
                             table_map ref_by_tbl)
{
  uint key= 0;
  Derived_key *entry= 0;
  List_iterator<Derived_key> ki(derived_key_list);

  /* Search for already existing possible key. */
  while ((entry= ki++))
  {
    key++;
    if (ref_by_tbl)
    {
      /* Search for the entry for the specified table.*/
      if (entry->referenced_by & ref_by_tbl)
        break;
    }
    else
    {
      /*
        Search for the special entry that should contain fields referred
        from any table.
      */
      if (!entry->referenced_by)
        break;
    }
  }
  /* Add new possible key if nothing is found. */
  if (!entry)
  {
    THD *thd= field->table->in_use;
    key++;
    entry= new (thd->mem_root) Derived_key();
    if (!entry)
      return TRUE;
    entry->referenced_by= ref_by_tbl;
    entry->used_fields.clear_all();
    if (derived_key_list.push_back(entry, thd->mem_root))
      return TRUE;
    field->table->max_keys++;
  }
  /* Don't create keys longer than REF access can use. */
  if (entry->used_fields.bits_set() < MAX_REF_PARTS)
  {
    field->part_of_key.set_bit(key - 1);
    field->flags|= PART_KEY_FLAG;
    entry->used_fields.set_bit(field->field_index);
  }
  return FALSE;
}

/*
  @brief
  Update derived table's list of possible keys

  @param field      derived table's field to take part in a key
  @param values     array of values that a part of equality predicate with the
                    field above
  @param num_values number of elements in the array values

  @details
  This function creates/extends a list of possible keys for this derived
  table/view. For each table used by a value from the 'values' array the
  corresponding possible key is extended to include the 'field'.
  If there is no such possible key, then it is created. field's
  part_of_key bitmaps are updated accordingly.
  @see add_derived_key

  @return TRUE  new possible key can't be allocated.
  @return FALSE list of possible keys successfully updated.
*/

bool TABLE_LIST::update_derived_keys(Field *field, Item **values,
                                     uint num_values)
{
  /* Don't bother with keys for CREATE VIEW and for BLOB fields. */
  if (field->table->in_use->lex->is_ps_or_view_context_analysis() ||
      field->flags & BLOB_FLAG)
    return FALSE;

  /* Allow all keys to be used. */
  if (derived_key_list.elements == 0)
  {
    table->keys_in_use_for_query.set_all();
    table->s->uniques= 0;
  }

  for (uint i= 0; i < num_values; i++)
  {
    table_map tables= values[i]->used_tables() & ~PSEUDO_TABLE_BITS;
    if (!tables || values[i]->real_item()->type() != Item::FIELD_ITEM)
      continue;
    for (table_map tbl= 1; tables >= tbl; tbl<<= 1)
    {
      if (! (tables & tbl))
        continue;
      if (add_derived_key(derived_key_list, field, tbl))
        return TRUE;
    }
  }
  /* Extend key which includes all referenced fields. */
  if (add_derived_key(derived_key_list, field, (table_map)0))
    return TRUE;
  return FALSE;
}


/*
  Comparison function for Derived_key entries.
  See TABLE_LIST::generate_keys.
*/

static int Derived_key_comp(Derived_key *e1, Derived_key *e2, void *arg)
{
  /* Move entries for tables with greater table bit to the end. */
  return ((e1->referenced_by < e2->referenced_by) ? -1 :
          ((e1->referenced_by > e2->referenced_by) ? 1 : 0));
}


/**
  @brief
  Generate keys for a materialized derived table/view

  @details
  This function adds keys to the result table by walking over the list of
  possible keys for this derived table/view and calling the
  TABLE::add_tmp_key to actually add keys. A name <auto_keyN>, where N is a
  sequential number, is given to each key to ease debugging.
  @see add_derived_key

  @return TRUE  an error occur.
  @return FALSE all keys were successfully added.
*/

bool TABLE_LIST::generate_keys()
{
  List_iterator<Derived_key> it(derived_key_list);
  Derived_key *entry;
  uint key= 0;
  char buf[NAME_CHAR_LEN];
  DBUG_ASSERT(uses_materialization());

  if (!derived_key_list.elements)
    return FALSE;

  if (table->alloc_keys(derived_key_list.elements))
    return TRUE;

  /* Sort entries to make key numbers sequence deterministic. */
  derived_key_list.sort((Node_cmp_func)Derived_key_comp, 0);
  while ((entry= it++))
  {
    sprintf(buf, "<auto_key%i>", key++);
    if (table->add_tmp_key(&entry->used_fields,
                           table->in_use->strdup(buf)))
      return TRUE;
  }
  return FALSE;
}


/*
  @brief Run derived tables/view handling phases on underlying select_lex.

  @param lex    LEX for this thread
  @param phases derived tables/views handling phases to run
                (set of DT_XXX constants)
  @details
  This function runs this derived table through specified 'phases' and used for
  handling materialized derived tables on all stages except preparation.
  The reason that on all stages except prepare derived tables of different
  type needs different handling. Materializable derived tables needs
  processor to be called directly on them. Mergeable derived tables doesn't
  need such call, but require diving into them to process underlying derived
  tables. This differs from the mysql_handle_derived which runs preparation
  processor on all derived tables without exception. 'lex' is passed as
  an argument to called functions.

  @see mysql_handle_derived.

  @return TRUE on error
  @return FALSE ok
*/

bool TABLE_LIST::handle_derived(LEX *lex,
                                bool (*processor)(THD*, LEX*, TABLE_LIST*))
{
  SELECT_LEX_UNIT *unit= get_unit();
  DBUG_ASSERT(unit);

  /* Dive into a merged derived table or materialize as is otherwise. */
  if (!uses_materialization())
  {
    for (SELECT_LEX *sl= unit->first_select(); sl; sl= sl->next_select())
      if (sl->handle_derived(lex, processor))
        return TRUE;
  }
  else
    return mysql_handle_single_derived(lex, this, processor);

  return FALSE;
}


/**
  @brief
  Return unit of this derived table/view

  @return reference to a unit  if it's a derived table/view.
  @return 0                    when it's not a derived table/view.
*/

st_select_lex_unit *TABLE_LIST::get_unit() const
{
  return (view ? &view->unit : derived);
}


/**
  Update TABLE::const_key_parts for single table UPDATE/DELETE query

  @param conds               WHERE clause expression

  @retval TRUE   error (OOM)
  @retval FALSE  success

  @note
    Set const_key_parts bits if key fields are equal to constants in
    the WHERE expression.
*/

bool TABLE::update_const_key_parts(Item *conds)
{
  memset(const_key_parts, 0, sizeof(key_part_map) * s->keys);

  if (conds == NULL)
    return FALSE;

  for (uint index= 0; index < s->keys; index++)
  {
    KEY_PART_INFO *keyinfo= key_info[index].key_part;
    KEY_PART_INFO *keyinfo_end= keyinfo + key_info[index].user_defined_key_parts;

    for (key_part_map part_map= (key_part_map)1; 
        keyinfo < keyinfo_end;
        keyinfo++, part_map<<= 1)
    {
      if (const_expression_in_where(conds, NULL, keyinfo->field))
        const_key_parts[index]|= part_map;
    }
  }
  return FALSE;
}


/**
  Read removal is possible if the selected quick read
  method is using full unique index

  @see HA_READ_BEFORE_WRITE_REMOVAL

  @param index              Number of the index used for read

  @retval true   success, read removal started
  @retval false  read removal not started
*/

bool TABLE::check_read_removal(uint index)
{
  DBUG_ENTER("check_read_removal");
  DBUG_ASSERT(file->ha_table_flags() & HA_READ_BEFORE_WRITE_REMOVAL);
  DBUG_ASSERT(index != MAX_KEY);

  // Index must be unique
  if ((key_info[index].flags & HA_NOSAME) == 0)
    DBUG_RETURN(false);

  // Full index must be used
  bitmap_clear_all(&tmp_set);
  mark_columns_used_by_index_no_reset(index, &tmp_set);
  if (!bitmap_cmp(&tmp_set, read_set))
    DBUG_RETURN(false);

  // Start read removal in handler
  DBUG_RETURN(file->start_read_removal());
}


/**
  Test if the order list consists of simple field expressions

  @param order                Linked list of ORDER BY arguments

  @return TRUE if @a order is empty or consist of simple field expressions
*/

bool is_simple_order(ORDER *order)
{
  for (ORDER *ord= order; ord; ord= ord->next)
  {
    if (ord->item[0]->real_item()->type() != Item::FIELD_ITEM)
      return FALSE;
  }
  return TRUE;
}
