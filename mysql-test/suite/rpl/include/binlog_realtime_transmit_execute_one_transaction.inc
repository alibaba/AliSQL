# ==== Purpose ====
# Start one transaction, waiting for realtime transmit if $wait_for_replica_brr_cache_count;
# commit and check data if $include_commit_transaction; check relay log events if $relay_log_file.
#
# ==== Usage ====
#--let $wait_for_replica_brr_cache_count=1
#--let $include_commit_transaction=1
#--let $relay_log_file=slave-relay-bin.000004
# --source suite/rpl/include/binlog_realtime_transmit_execute_one_transaction.inc
#
# Parameters:
#
#   $wait_for_replica_brr_cache_count
#     Whether wait for replica_brr_cache_count change to 1 on slave.
#
#   $include_commit_transaction
#     Whether commit the transaction and check data on slave.
#
#   $relay_log_file
#     The relay log file name which need to be check.
#

--source include/rpl_connection_master.inc

BEGIN;
INSERT INTO rpl_test.t2 SELECT * FROM rpl_test.t1;

if ($wait_for_replica_brr_cache_count)
{
  --let $wait_for_replica_brr_cache_count_value=1
  --source suite/rpl/include/binlog_realtime_transmit_wait_and_test_replica.inc

  # Check brr trx will not affect rpl_sync before it commit.
  --source include/rpl_sync.inc
}

if ($include_commit_transaction)
{
  --source include/rpl_connection_master.inc
  COMMIT;

  --source include/rpl_connection_slave.inc
  --source include/rpl_sync.inc
  SELECT COUNT(*) FROM rpl_test.t2;

  if ($relay_log_file)
  {
    --let $binlog_file=$relay_log_file
    --source include/show_relaylog_events.inc
  }
}
