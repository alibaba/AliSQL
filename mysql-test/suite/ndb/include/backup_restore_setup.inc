# This include file should be included at the beginning of all tests that uses
# NDB backup, it will define the path for backup directory.

if (!$NDB_BACKUP_DIR)
{
  --die Test requires NDB_BACKUP_DIR variable
  # The variable is normally specified in my.cnf returning
  # the BackupDataDir value of the data nodes
}

# Directory where NDB will create backups
--let NDB_BACKUPS=$NDB_BACKUP_DIR/BACKUP/BACKUP
#echo NDB_BACKUPS: $NDB_BACKUPS;

# Directory containing backups from earlier versions of NDB
--let $NDB_SAVED_BACKUPS=$MYSQL_TEST_DIR/suite/ndb/backups
#echo NDB_SAVED_BACKUPS: $NDB_SAVED_BACKUPS;

