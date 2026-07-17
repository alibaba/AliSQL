# ==== Purpose ====
#
# Ensure that the server is running with persist_binlog_to_redo off.
#
# ==== Usage ====
#
# --source include/not_persist_binlog_to_redo.inc

if (`SELECT @@GLOBAL.persist_binlog_to_redo`)
{
  --skip Doesn't support --persist_binlog_to_redo
}
