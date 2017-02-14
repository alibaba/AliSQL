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
#include "sql_parse.h"
#include "sql_table.h"
#include "sql_base.h"
#include "transaction.h"
#include "sql_class.h"

/* Sequence fields pre_definition */
ST_SEQ_FIELD_INFO seq_fields[]=
{
  {"currval", "21", FIELD_NUM_CURRVAL, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("current value")}},
  {"nextval", "21", FIELD_NUM_NEXTVAL, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("next value")}},
  {"minvalue", "21", FIELD_NUM_MINVALUE, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("min value")}},
  {"maxvalue", "21", FIELD_NUM_MAXVALUE, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("max value")}},
  {"start", "21", FIELD_NUM_START, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("start value")}},
  {"increment", "21", FIELD_NUM_INCREMENT, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("increment value")}},
  {"cache", "21", FIELD_NUM_CACHE, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("cache size")}},
  {"cycle", "21", FIELD_NUM_CYCLE, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("cycle state")}},
  {"round", "21", FIELD_NUM_ROUND, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("already how many round")}},
  {NULL, NULL, FIELD_NUM_END, MYSQL_TYPE_LONGLONG,
    {C_STRING_WITH_LEN("")}}
};

/*
  Sequence field default values setting.
*/
void Sequence_create_info::init_default()
{
  DBUG_ENTER("Sequence_create_info::init_default");
  values[FIELD_NUM_CURRVAL]= 0;
  values[FIELD_NUM_NEXTVAL]= 0;
  values[FIELD_NUM_MINVALUE]= 1;
  values[FIELD_NUM_MAXVALUE]= ULONGLONG_MAX;
  values[FIELD_NUM_START]= 1;
  values[FIELD_NUM_INCREMENT]= 1;
  values[FIELD_NUM_CACHE]= 10000;
  values[FIELD_NUM_CYCLE]= 0;
  values[FIELD_NUM_ROUND]= 0;

  base_db_type= NULL;
  db= NULL;
  name= NULL;

  DBUG_VOID_RETURN;
};
Sequence_create_info::Sequence_create_info()
{
  init_default();
};
/*
  Sequence field value set function.
*/
void Sequence_create_info::init_value(enum enum_sequence_field field_num,
                                      ulonglong value)
{
  DBUG_ENTER("Sequence_create_info::init_value");
  values[field_num]= value;
  DBUG_VOID_RETURN;
};

/*
  Adjust the sequence table engine.

  RETURN VALUES
    false       Success
    true        Failure
*/
bool adjust_sequence_engine(THD *thd, LEX *lex)
{
  handlerton *engine;
  DBUG_ENTER("adjust_sequence_engine");
  DBUG_ASSERT(lex->seq_create_info);
  DBUG_ASSERT(!(lex->create_info.used_fields & HA_CREATE_USED_ENGINE));

  /* TODO: support more storage engine.
     Default: InnoDB storage engine.
  */
  LEX_STRING engine_name= {C_STRING_WITH_LEN("InnoDB")};
  plugin_ref plugin= ha_resolve_by_name(thd, &engine_name, false);

  DBUG_EXECUTE_IF("sequence_engine_error",
                  {
                    plugin= NULL;
                  });

  if (plugin)
  {
    engine= plugin_data(plugin, handlerton*);
    lex->create_info.db_type= engine;
  }
  else
  {
    my_error(ER_UNKNOWN_STORAGE_ENGINE, MYF(0), engine_name.str);
    DBUG_RETURN(TRUE);
  }
  lex->create_info.used_fields|= HA_CREATE_USED_ENGINE;
  lex->seq_create_info->base_db_type= engine;
  DBUG_RETURN(FALSE);
}
/*
  Check whether sequence values are valid.

  Rules:
    1. maxvalue > start
    2. maxvalue >= minvalue
    3. start >= minvalue
    4. increment >= 1

  RETUREN VALUES
     true       invalid
     false      valid
*/
bool check_sequence_values_valid(ulonglong *items)
{
  DBUG_ENTER("check_sequence_values_valid");
  if (items[FIELD_NUM_MAXVALUE] >= items[FIELD_NUM_MINVALUE]
      && items[FIELD_NUM_START] >= items[FIELD_NUM_MINVALUE]
      && items[FIELD_NUM_INCREMENT] >= 1
      && items[FIELD_NUM_MAXVALUE] > items[FIELD_NUM_START])
    DBUG_RETURN(FALSE);

  DBUG_RETURN(TRUE);
}
/*
  Check whether inited values are valid through
    syntax: 'create sequence ...'

  RETUREN VALUES
     true       invalid
     false      valid
*/
bool Sequence_create_info::check_valid()
{
  DBUG_ENTER("Sequence_create_info::check_valid");

  if (check_sequence_values_valid(values))
  {
    my_error(ER_SEQUENCE_INVALID, MYF(0), db, name);
    DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}
/* Sequence field values get method
*/
ulonglong Sequence_create_info::get_value(enum enum_sequence_field field_num)
{
  DBUG_ENTER("Sequence_create_info::get_value");
  DBUG_ASSERT(field_num < FIELD_NUM_END);
  DBUG_RETURN(values[field_num]);
}
/*
  Check the sequence fields through seq_fields when create sequence.

  RETURN VALUES
    false       Success
    true        Failure
*/
bool check_sequence_fields(THD *thd, LEX *lex)
{
  Create_field *field;
  List_iterator<Create_field> it(lex->alter_info.create_list);
  uint field_count;
  uint field_no;
  Sequence_create_info *seq_create_info;
  DBUG_ENTER("check_sequence_fields");
  DBUG_ASSERT(!lex->native_create_sequence);

  seq_create_info= lex->seq_create_info;
  field_count= lex->alter_info.create_list.elements;
  field_no= 0;
  if (field_count != FIELD_NUM_END
      || lex->alter_info.key_list.elements > 0)
    goto err;

  while ((field= it++))
  {
    if (my_strcasecmp(system_charset_info,
                      seq_fields[field_no].field_name,
                      field->field_name)
        || (field->flags != (NOT_NULL_FLAG | NO_DEFAULT_VALUE_FLAG))
        || (field->sql_type != seq_fields[field_no].field_type))
      goto err;

    field_no++;
  }
  DBUG_RETURN(FALSE);

err:
  my_error(ER_SEQUENCE_INVALID, MYF(0),
           seq_create_info->db, seq_create_info->name);
  DBUG_RETURN(TRUE);
}
/*
  Prepare the sequence fields through seq_fields when create sequence.

  RETURN VALUES
    false       Success
    true        Failure
*/
bool prepare_sequence_fields(THD *thd, LEX *lex)
{
  ST_SEQ_FIELD_INFO *field_info;
  Create_field *new_field;
  char *decimal;
  uint fld_mod;
  Item *fld_default_value;
  Item *fld_on_update_value;
  char *fld_change;
  List<String> *fld_interval_list;
  CHARSET_INFO *fld_charset;
  uint fld_geom_type;
  DBUG_ENTER("prepare_sequence_fields");

  DBUG_ASSERT(lex->seq_create_info);
  decimal= NULL;
  fld_mod= NOT_NULL_FLAG;
  fld_default_value= NULL;
  fld_on_update_value= NULL;
  fld_change= NULL;
  fld_interval_list= NULL;
  fld_charset= NULL;
  fld_geom_type= 0;

  /* Generate all sequence columns */
  field_info= seq_fields;
  while (field_info->field_name)
  {
    if (!(new_field= new Create_field()) ||
        new_field->init(thd, (char*)field_info->field_name,
                        field_info->field_type,
                        (char *)(field_info->field_length),
                        decimal, fld_mod,
                        fld_default_value, fld_on_update_value,
                        &field_info->comment,
                        fld_change,
                        fld_interval_list, fld_charset, fld_geom_type))
      DBUG_RETURN(TRUE); //no cover line.

    lex->alter_info.create_list.push_back(new_field);
    lex->last_field= new_field;
    field_info++;
  }
  DBUG_ASSERT(field_info->field_num == FIELD_NUM_END);
  DBUG_RETURN(FALSE);
}
/*
  Prepare engine and fields before create sequence.

  RETURN VALUES
    false       Success
    true        Failure
*/
bool prepare_create_sequence(THD *thd, LEX *lex, TABLE_LIST * create_table)
{
  DBUG_ENTER("prepare_create_sequence");
  DBUG_ASSERT(lex->seq_create_info);

  lex->seq_create_info->db= create_table->db;
  lex->seq_create_info->name= create_table->table_name;

  /* Step 1: adjust sequence engine */
  if (adjust_sequence_engine(thd, lex))
    DBUG_RETURN(TRUE);

  /* Step 2: check sequence field values */
  if (lex->seq_create_info->check_valid())
    DBUG_RETURN(TRUE);

  /* Step 3: prepare or check sequence table fields */
  if (lex->native_create_sequence)
  {
    if (prepare_sequence_fields(thd, lex))
      DBUG_RETURN(TRUE);//no cover line.
  }
  else
  {
    if (check_sequence_fields(thd, lex))
      DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}
/*
  Initialize the sequence table record. only save one row.

  RETURN VALUES
    false       Success
    true        Failure
*/
bool sequence_insert(THD *thd, LEX *lex, TABLE_LIST *table_list)
{
  ST_SEQ_FIELD_INFO *field_info;
  Sequence_create_info *seq_create_info;
  ulonglong field_value;
  enum enum_sequence_field field_num;
  bool save_binlog_row_based;
  int error= 0;
  TABLE *table;
  DBUG_ENTER("sequence_insert");

  seq_create_info= lex->seq_create_info;
  DBUG_ASSERT(seq_create_info);
  /* Sequence values will be replicated as a statement
     like 'create sequence'. So clear row binlog format temporaryly.
  */
  if ((save_binlog_row_based= thd->is_current_stmt_binlog_format_row()))
    thd->clear_current_stmt_binlog_format_row();

  /* The sequence creation will cause implicit commit. */
  close_thread_tables(thd);
  thd->mdl_context.release_transactional_locks();
  table_list->mdl_request.init(MDL_key::TABLE,
                               table_list->db,
                               table_list->table_name,
                               MDL_SHARED_WRITE, MDL_TRANSACTION);
  if (open_and_lock_tables(thd, table_list, FALSE,
                           MYSQL_LOCK_IGNORE_TIMEOUT))
  {
    if (save_binlog_row_based) //no cover begin.
      thd->set_current_stmt_binlog_format_row();

    DBUG_RETURN(TRUE); //no cover end.
  }
  table= table_list->table;
  DBUG_ASSERT(table->s->db_type() == sequence_hton);
  DBUG_ASSERT(table->in_use == thd);
  table->use_all_columns();

  /* Generate all fields and values for sequence table row */
  field_info= seq_fields;
  while (field_info->field_name)
  {
    field_num= field_info->field_num;
    field_value= seq_create_info->get_value(field_num);
    table->field[field_num]->store((longlong)(field_value), true);
    field_info++;
  }
  /* Write into the sequence inited row. */
  if ((error= table->file->ha_write_row(table->record[0])))
  {
    table->file->print_error(error, MYF(0));
    goto end;
  }

  trans_commit_stmt(thd);
  trans_commit_implicit(thd);
  close_thread_tables(thd);
  thd->mdl_context.release_transactional_locks();

end:
 if (save_binlog_row_based)
   thd->set_current_stmt_binlog_format_row();
 DBUG_RETURN(error);
}
