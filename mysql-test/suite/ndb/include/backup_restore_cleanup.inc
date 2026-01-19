# This include file should be included at the end of all tests that uses
# NDB backup, it will delete the backup directory which mtr will
# otherwise report an error for.

if (!$NDB_BACKUP_DIR)
{
  --die Need NDB_BACKUP_DIR variable
}
--force-rmdir $NDB_BACKUP_DIR/BACKUP
