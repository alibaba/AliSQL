# ==== Purpose ====
# Waiting for status replica_brr_cache_count change to $wait_for_replica_brr_cache_count_value;
# check relay log events if $relay_log_file.
#
# ==== Usage ====
# --let $relay_log_file=slave-relay-bin.000004
# --let $wait_for_replica_brr_cache_count_value=1
# --source suite/rpl/include/binlog_realtime_transmit_wait_and_test_replica.inc
#
# Parameters:
#
#   $wait_for_replica_brr_cache_count_value
#     wait for replica_brr_cache_count change to this value on slave.
#
#   $skip_connection_slave
#     If true, skip connect to slave by rpl_connection_slave.
#
#   $wait_and_test_after_commit
#     If true, current wait and test is happened after trx commit.
#
#   $relay_log_file
#     The relay log file name which need to be check.
#
#   $wait_sql
#     The sql which need to wait in wait_and_test_after_commit mode.

if (!$skip_connection_slave)
{
  --source include/rpl_connection_slave.inc
}

if ($wait_and_test_after_commit)
{
  --let $wait_condition= $wait_sql
  --source include/wait_condition.inc

  --let $binlog_file= $relay_log_file
  --source include/show_relaylog_events.inc
}

if (!$wait_and_test_after_commit)
{
  --echo # Wait for replica_brr_cache_count, expect $wait_for_replica_brr_cache_count_value
  --let $show_statement= SHOW STATUS LIKE 'replica_brr_cache_count'
  --let $field= Value
  --let $condition= = '$wait_for_replica_brr_cache_count_value'
  --source include/wait_show_condition.inc

  --echo # Check tmp files in binlog cache tmp file dir.
  --let $MYSQLD_DATADIR= `select @@datadir`
  --list_files $MYSQLD_DATADIR/#alisql_binlog_cache_temp_file
}
