#include "duckdb/duckdb_table.h"
#include "derror.h"
#include "handler.h"
#include "sql_class.h"
#include "sql_table.h"
#include "sql_lex.h"
#include "table.h"

namespace myduck {

/** Process create fields.
  @param[in]       thd         Thread context
  @param[in, out]  alter_info  Lists of fields, keys to be changed, added
                               or dropped.
*/
static void process_fields(THD *thd, Alter_info *alter_info) {
  /** Duckdb don't support auto-increment */
  List_iterator<Create_field> it(alter_info->create_list);

  Create_field *sql_field;
  for (; (sql_field = it++);) {
    /* Unmark auto_increment. */
    if (MTYP_TYPENR(sql_field->unireg_check) == Field::NEXT_NUMBER) {
      sql_field->unireg_check = Field::NONE;
      // push_warning_printf(thd, Sql_condition::SL_WARNING,
      //                     ER_DUCKDB_TABLE_AUTO_INCREMENT_REMOVED,
      //                     ER_THD(thd,
      //                     ER_DUCKDB_TABLE_AUTO_INCREMENT_REMOVED),
      //                     sql_field->field_name);
    }

    /* Unmark ON_UPDATE_NOW. */
    if (sql_field->unireg_check == Field::TIMESTAMP_UN_FIELD) {
      sql_field->unireg_check = Field::NONE;
      push_warning_printf(thd, Sql_condition::SL_WARNING,
                          ER_DUCKDB_TABLE_ON_UPDATE_NOW_REMOVED,
                          ER_THD(thd, ER_DUCKDB_TABLE_ON_UPDATE_NOW_REMOVED),
                          sql_field->field_name);
    }
  }
}

static bool is_not_nullable_uk(Key *key, Alter_info *alter_info)
{
  if (key->type != KEYTYPE_UNIQUE)
  {
    return false;
  }

  Key_part_spec *col;
  List_iterator<Key_part_spec> key_iterator(key->columns);
  while ((col = key_iterator++))
  {
    List_iterator<Create_field> it(alter_info->create_list);
    Create_field *sql_field = nullptr;
    while ((sql_field = it++))
    {
      if (!my_strcasecmp(system_charset_info, col->field_name.str,
        sql_field->field_name))
      {
        if ((sql_field->flags & NOT_NULL_FLAG) == 0)
          return false;
      }
    }
  }

  return true;
}

/** Select primary key from all keys.
  The selected key will be new primary key. The alternative order is: primary
  key, RDS implicit primary key, candidate unique key.
  @param[in]   thd            Thread context
  @param[in]   alter_info     Lists of fields, keys to be changed, added
                              or dropped.
  @param[out]  selected_key   Selected primary key.
  @return false if successful, true if error happens.
*/
static bool select_primary_key(THD *thd, Alter_info *alter_info,
                               Key **selected_key) {
  Key *pk = nullptr;
  Key *candidate_uk = nullptr;

  Key *key;
  List_iterator<Key> key_iterator(alter_info->key_list);
  while ((key = key_iterator++))
  {
    if (key->type == KEYTYPE_PRIMARY)
    {
      if (pk != nullptr)
      {
        if (!thd->slave_thread)
        {
          my_error(ER_MULTIPLE_PRI_KEY, MYF(0));
          return true;
        }
      }
      pk = key;
    }
    else if (is_not_nullable_uk(key, alter_info) && !candidate_uk)
    {
      candidate_uk = key;
    }
  }

  if (pk)
    *selected_key = pk;
  else if (candidate_uk)
    *selected_key = candidate_uk;
  else
    *selected_key = nullptr;

  return false;
}

/** Construct a new primary key based on the alternative primary key.

  If there is a primary key, only the primary key is retained. If there is no
  primary key but candidate UK exists, candidate UK is upgraded to the primary 
  key.

  @param[in]  thd           Thread context
  @param[in]  alter_info    Lists of fields, keys to be changed, added
                            or dropped.
  @param[in]  selected_key  Selected primary key.
  @return false if success, true if error happens.
*/
static bool create_new_primary_key(THD *thd, Alter_info *alter_info,
                                   Key *selected_key) {
  List<Key> new_key_list;
  if (selected_key == nullptr) {
    goto finish;
  }

  if (selected_key->type == KEYTYPE_PRIMARY) {
    new_key_list.push_back(selected_key);
  } else {
    List<Key_part_spec> key_col_list;
    Key_part_spec *col;
    List_iterator<Key_part_spec> key_iterator(selected_key->columns);
    while ((col = key_iterator++))
    {
      key_col_list.push_back(col);
    }

    Key *new_pk = new Key(KEYTYPE_PRIMARY, null_lex_str,
                 &default_key_create_info, false, key_col_list);
    if (new_pk == nullptr) {
      my_error(ER_OUTOFMEMORY, MYF(0));
      return true;
    }

    push_warning_printf(
        thd, Sql_condition::SL_WARNING, ER_DUCKDB_TABLE_INDEX_UPGRADED,
        ER_THD(thd, ER_DUCKDB_TABLE_INDEX_UPGRADED), selected_key->name.str);
    new_key_list.push_back(new_pk);
  }

finish:
  alter_info->key_list.swap(new_key_list);
  return false;
}

/** Remove flag bit in alter_info if exists.
    If removed, warning will be reported.
  @param[in]       thd               Thread context
  @param[in]       flag              Flag  to be removed
  @param[in]       operation         Operation name
  @param[in, out]  alter_info        Lists of fields, keys to be changed, added
                                     or dropped.
*/
static void remove_flag_if_exists(THD *thd, ulonglong flag,
                                  const char *operation,
                                  Alter_info *alter_info) {
  if (alter_info->flags & flag) {
    alter_info->flags &= ~flag;
    push_warning_printf(thd, Sql_condition::SL_WARNING,
                        ER_DUCKDB_ALTER_FLAG_REMOVED,
                        ER_THD(thd, ER_DUCKDB_ALTER_FLAG_REMOVED), operation);
  }
}

/** Remove flags of add index and constraint operations if need.

  @param[in]       thd               Thread context
  @param[in, out]  alter_info        Lists of fields, keys to be changed, added
                                     or dropped.
  @param[in]       has_selected_key  If there is a selected primary key.
*/
static void remove_add_key_and_constraint_flags(THD *thd,
                                                Alter_info *alter_info,
                                                bool has_selected_key) {
  /* MAY_IGNORED_ALTER_FLAGS */
  if (!has_selected_key) {
    remove_flag_if_exists(thd, Alter_info::ALTER_ADD_INDEX, "ADD INDEX",
                          alter_info);
  }

  /* IGNORED_ALTER_FLAGS */
  remove_flag_if_exists(thd, Alter_info::ADD_FOREIGN_KEY, "ADD FOREIGN KEY",
                        alter_info);
}

/** Called by mysql_prepare_create_table */
bool prepare_create_duckdb_table(THD *thd, HA_CREATE_INFO *create_info,
                                 Alter_info *alter_info) {
  if (create_info->db_type->db_type != DB_TYPE_DUCKDB) {
    return false;
  }

  /* Rename auto_increment. */
  process_fields(thd, alter_info);

  /* Determine one key as primary key. */
  Key *selected_key = nullptr;
  if (select_primary_key(thd, alter_info, &selected_key)) {
    return true;
  }

  /* We need to remove all non-primary key indexes and constraints. */
  Key *key;
  List_iterator<Key> key_iterator(alter_info->key_list);
  while ((key = key_iterator++))
  {
    if (key != selected_key)
    {
      push_warning_printf(
          thd, Sql_condition::SL_WARNING, ER_DUCKDB_TABLE_INDEX_REMOVED,
          ER_THD(thd, ER_DUCKDB_TABLE_INDEX_REMOVED), key->name.str);
    }
  }

  if (create_new_primary_key(thd, alter_info, selected_key)) {
    return true;
  }

  /* Remove flags of add key and constraint operations. */
  remove_add_key_and_constraint_flags(thd, alter_info,
                                      (selected_key != nullptr));

  return false;
}

bool is_duckdb_table(const TABLE *table) {
  if (table == nullptr || table->file == nullptr ||
      table->file->ht == nullptr) {
    return false;
  }

  bool res = (table->file->ht->db_type == DB_TYPE_DUCKDB);

  return res;
}

bool is_supported_ddl(Alter_info *alter_info) {
  ulonglong flags = alter_info->flags;

  /* Do nothing. */
  if (flags == 0) {
    return true;
  }

  if (flags & Alter_info::ALTER_ADD_COLUMN) {
    List_iterator<Create_field> new_field_it(alter_info->create_list);
    Create_field *new_field;

    while ((new_field = new_field_it++)) {
      /* Skip modify/change column */
      if (new_field->change != nullptr) {
        continue;
      }

      if (MTYP_TYPENR(new_field->unireg_check) == Field::NEXT_NUMBER) {
        my_error(ER_DUCKDB_ALTER_OPERATION_NOT_SUPPORTED, MYF(0),
                 "ADD AUTO_INCREMENT COLUMN");
        return false;
      }
    }
  }

  /*
    We do not to check Alter_info::ALTER_DISCARD_TABLESPACE,
    Alter_info::ALTER_IMPORT_TABLESPACE and Alter_info::ANY_ENGINE_ATTRIBUTE
    because ENGINE  'DuckDB' do not support it now.
  */

  return true;
}

bool report_duckdb_table_struct_error(std::string const &err_msg)
{
  my_error(ER_DUCKDB_TABLE_STRUCT_INVALID, MYF(0), err_msg.c_str());
  return true;
}

void prepare_alter_duckdb_table(THD *thd, HA_CREATE_INFO *create_info,
                                Alter_info *alter_info) {
  if (create_info->db_type->db_type != DB_TYPE_DUCKDB) {
    return;
  }

  /*
    If drop primary key, we need to try to drop it because there is existed
    primary key on duckdb table.
  */
  bool drop_primary_key = false;
  List_iterator<Alter_drop> drop_it(alter_info->drop_list);
  Alter_drop *drop;
  drop_it.rewind();
  while ((drop = drop_it++)) {
    Alter_drop::drop_type type = drop->type;
    switch (type) {
      case Alter_drop::COLUMN:
        continue;
        break;

      case Alter_drop::FOREIGN_KEY:
        drop_it.remove();
        break;

      case Alter_drop::KEY:
        if (my_strcasecmp(system_charset_info, drop->name, "PRIMARY") == 0) {
          drop_primary_key = true;
          continue;
        } else {
          drop_it.remove();
        }
        break;

      default:
        break;
    }
  }
  /* MAY_IGNORED_ALTER_FLAGS */
  if (!drop_primary_key) {
    remove_flag_if_exists(thd, Alter_info::ALTER_DROP_INDEX, "DROP INDEX",
                          alter_info);
  }

  /* IGNORED_ALTER_FLAGS */
  remove_flag_if_exists(thd, Alter_info::DROP_FOREIGN_KEY, "DROP FOREIGN KEY",
                        alter_info);

  remove_flag_if_exists(thd, Alter_info::ALTER_RENAME_INDEX, "RENAME INDEX",
                        alter_info);
  List<Alter_rename_key> rename_empty_list;
  alter_info->alter_rename_key_list.swap(rename_empty_list);

  remove_flag_if_exists(thd, Alter_info::ALTER_ORDER, "ALTER ORDER",
                        alter_info);
}

bool precheck_convert_to_duckdb(const TABLE_SHARE *table)
{
  /* Table level */
  if (table->auto_partitioned || table->partition_info_str != NULL) {
    return report_duckdb_table_struct_error("partition table is not support");
  }

  /* Index level*/
  bool has_candidate_key = false;
  for (int i = 0; i < table->keys; i++) {
    KEY *key = &(table->key_info[i]);
    if ((my_strcasecmp(system_charset_info, key->name, "PRIMARY") == 0) ||
        ((key->flags & HA_NOSAME) && !(key->flags & HA_NULL_PART_KEY)))
    {
      /*
        There are currently no indexes in DuckDB, we don't care whether they are
        prefix/partial indexes or not.
      */
      has_candidate_key |= true;
    }
  }

  if (!has_candidate_key && duckdb_require_primary_key) {
    my_error(ER_REQUIRES_PRIMARY_KEY, MYF(0));
    return true;
  }

  /* Column level */
  for (int i = 0; i < table->fields; i++) {
    Field *field = table->field[i];
    enum_field_types field_type = field->real_type();
    if (field_type == MYSQL_TYPE_GEOMETRY)
    {
      return report_duckdb_table_struct_error(
          "geometry column is not supported");
    }

    if (field->gcol_info) {
      return report_duckdb_table_struct_error(
          "generation expression is not supported");
    }
  }

  return false;
}

}  // namespace myduck
