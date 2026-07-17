# ==== Purpose ====
#
# Ensure the server supports persist_binlog_to_redo, i.e. the "Persist Binlog
# Into Redo" feature is compiled in and the binary log is enabled.
#
# ==== Usage ====
#
# --source include/have_persist_binlog_to_redo.inc

--disable_query_log
if (`SELECT COUNT(*) = 0 FROM performance_schema.global_variables
     WHERE variable_name = 'persist_binlog_to_redo'`)
{
  --skip The server does not support persist_binlog_to_redo
}

if (`SELECT @@GLOBAL.log_bin = 0`)
{
  --skip persist_binlog_to_redo requires the binary log to be enabled
}
--enable_query_log
