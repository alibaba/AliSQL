#ifndef SQL_SEQUENCE_INCLUDED
#define SQL_SEQUENCE_INCLUDED
/* Copyright (c) 2006, 2015, Aliyun and/or its affiliates. All rights reserved.

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

#include "my_global.h"
#include "mysql_com.h"
#include "sql_table.h"
#include "m_ctype.h"
#include "m_string.h"
#include "sql_alloc.h"
#include "handler.h"

struct LEX;
class THD;

/* Sequence table fields. */
enum enum_sequence_field
{
  FIELD_NUM_CURRVAL= 0,
  FIELD_NUM_NEXTVAL,
  FIELD_NUM_MINVALUE,
  FIELD_NUM_MAXVALUE,
  FIELD_NUM_START,
  FIELD_NUM_INCREMENT,
  FIELD_NUM_CACHE,
  FIELD_NUM_CYCLE,
  FIELD_NUM_ROUND,
  /* This must be last! */
  FIELD_NUM_END
};

typedef struct st_seq_cache
{
  ulonglong currval;
  ulonglong nextval;
  ulonglong minvalue;
  ulonglong maxvalue;
  ulonglong start;
  ulonglong increment;
  ulonglong cache;
  ulonglong cycle;
  ulonglong round;
}ST_SEQ_CACHE;

/* Sequence table fields definition. */
typedef struct st_sequence_field_info
{
  const char* field_name;
  const char* field_length;
  enum enum_sequence_field field_num;
  enum enum_field_types field_type;
  LEX_STRING comment;
}ST_SEQ_FIELD_INFO;

extern ST_SEQ_FIELD_INFO seq_fields[];

/*
  Sequence create information.
*/
class Sequence_create_info : public Sql_alloc
{
private:
  ulonglong values[FIELD_NUM_END];
  void init_default();

public:
  handlerton *base_db_type;
  char *db;
  char *name;

  Sequence_create_info();
  ~Sequence_create_info();

  void init_value(enum enum_sequence_field field_num,
                  ulonglong value);

  ulonglong get_value(enum enum_sequence_field field_num);

  bool check_valid();
};

handler *get_ha_sequence(Sequence_create_info *info);

/* Public method called when create sequence */
bool prepare_create_sequence(THD *thd, LEX *lex, TABLE_LIST *create_table);
bool sequence_insert(THD *thd, LEX *lex, TABLE_LIST *table_list);
bool check_sequence_values_valid(ulonglong *items);


#endif
