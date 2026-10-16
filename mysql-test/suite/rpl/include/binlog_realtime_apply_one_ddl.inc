# ==== Purpose ====
# Execute $ddl_statement and waiting for replica realtime apply it. 
# Determine whether the ddl is realtime applied by check $expect_assigned_to_brr_worker_value
# and $expect_commit_by_brr_worker_value.
#
# ==== Usage ====
# --let $sql_statement= "ALTER TABLE rpl_test.t1 ADD COLUMN id1 INT"
# --let $expect_assigned_to_brr_worker_value=1
# --let $expect_commit_by_brr_worker_value=1
# --let $algorithm= 'INPLACE'
# --source suite/rpl/include/binlog_realtime_apply_one_transaction.inc
#
# Parameters:
#
#   $ddl_statement
#    The SQL statement to be executed.
#
#   $expect_assigned_to_brr_worker_value
#     Wait for replica_brr_trx_assigned_to_brr_worker to be equal to this value.
#
#   $expect_commit_by_brr_worker_value
#     Wait for replica_brr_ddl_commit_by_brr_worker to be equal to this value.
#   
#   $algorithm 
#     Execute DDL algorithm
#  
#   $wait_timeout
#     The default timeout is 1 minute. You can change the timeout by setting $wait_timeout.
#     The unit is tenths of seconds.
#   
#   $is_optimize
#     Optimize table operation


if (!$wait_timeout)
{
  let $wait_timeout= 600;
}

let $set_debug_val= '+d,brr_wait_copy_alter_table';
let $clear_debug_val= '-d,brr_wait_copy_alter_table';
if ($algorithm == 'INPLACE') {
    let $set_debug_val='+d,brr_wait_inplace_alter_table';
    let $clear_debug_val='-d,brr_wait_inplace_alter_table';
}

--let $ddl_stmt = $ddl_statement

if (!$is_optimize) {
  --let $ddl_stmt = $ddl_statement, ALGORITHM=$algorithm
}

--echo
--echo ## Let brr worker wait, so we can see normal worker is waiting for brr worker finish 
--echo ## transaction.
connection slave;
--disable_query_log
SET DEBUG_SYNC = 'RESET';
--enable_query_log
--eval SET GLOBAL debug = $set_debug_val

--echo
--echo ## Wait after sending SA to simulate the long-running DDL and ensure that the DDL can 
--echo ## be applied by the brr worker.
connection master;
--disable_query_log
SET DEBUG_SYNC = 'RESET';
--enable_query_log
--eval SET SESSION debug = $set_debug_val
--send_eval $ddl_stmt

connection slave;
--let $status_var= replica_brr_trx_assigned_to_brr_worker
--let $status_var_value= $expect_assigned_to_brr_worker_value
--let $status_timeout= $wait_timeout
--source include/wait_for_status_var.inc

# Assignment precedes the debug pause. Wait for the worker before releasing
# the source or clearing the debug flag.
--disable_query_log
SET DEBUG_SYNC = 'now WAIT_FOR brr_ddl_paused';
--enable_query_log

--echo
--echo ## relay log cache temp file is created.
--let $MYSQLD_DATADIR= `select @@datadir`
--let $RELAY_LOG_CACHE_TEMP_FILE_DIR = $MYSQLD_DATADIR/#alisql_binlog_cache_temp_file
--exec ls $RELAY_LOG_CACHE_TEMP_FILE_DIR|grep "RL"|wc -l

connection master1;
--disable_query_log
SET DEBUG_SYNC = 'now WAIT_FOR brr_ddl_paused';
--enable_query_log
SET DEBUG_SYNC = 'now SIGNAL continue';

--echo
--echo ## Worker thread should be waiting for the transaction to be committed by brr worker
connection slave;
--let $show_statement= SHOW PROCESSLIST
--let $field= State
--let $condition= = 'Waiting for GTID to be committed'
--source include/wait_show_condition.inc

connection slave1;
--eval SET GLOBAL debug = $clear_debug_val
SET DEBUG_SYNC = 'now SIGNAL continue';

--let $status_var= replica_brr_ddl_commit_by_brr_worker
--let $status_var_value= $expect_commit_by_brr_worker_value
--source include/wait_for_status_var.inc
--source include/rpl_sync.inc
SHOW CREATE TABLE rpl_test.t1;

--echo
--echo ## After brr transaction is committed, the brr cache will be removed, and relay log 
--echo ## will be removed.
--exec ls $RELAY_LOG_CACHE_TEMP_FILE_DIR|grep "RL"|wc -l

connection master;
--reap
--eval SET SESSION debug = $clear_debug_val
