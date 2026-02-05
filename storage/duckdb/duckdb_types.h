#pragma once

#include <string>

class DatabaseTableNames {
 public:
  DatabaseTableNames(const char *name);
  DatabaseTableNames(std::string db, std::string tb)
      : db_name(db), table_name(tb) {}
  std::string full_name() { return db_name + "." + table_name; }

  std::string db_name;
  std::string table_name;
};

class Databasename {
 public:
  /** Set a given location from full pathname to database name.
  refer to ndb_set_dbname
  @param[in]  path_name full path name */
  Databasename(const char *path_name);

  std::string name;
};
