# ==== Purpose ====
# Execute $ddl_statement failed and waiting for replica realtime rollback it. 
# Determine whether the ddl is realtime rolled back by check $expect_assigned_to_brr_worker_value
# and $expect_rollback_by_brr_worker_value.
#
# ==== Usage ====
# --let $sql_statement= "ALTER TABLE rpl_test.t1 ADD COLUMN id1 INT"
# --let $expect_assigned_to_brr_worker_value=1
# --let $expect_rollback_by_brr_worker_value=1
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
#   $expect_rollback_by_brr_worker_value
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

let $set_debug_val= '+d,brr_wait_inplace_alter_table';
let $clear_debug_val= '-d,brr_wait_inplace_alter_table';
if ($algorithm == 'COPY') {
    let $set_debug_val='+d,brr_wait_copy_alter_table';
    let $clear_debug_val='-d,brr_wait_copy_alter_table';
}

--let $ddl_stmt = $ddl_statement

if (!$is_optimize) {
  --let $ddl_stmt = $ddl_statement, ALGORITHM=$algorithm
}

--echo
connection master;
--eval SET SESSION debug = $set_debug_val
--send_eval $ddl_stmt

connection slave;
--let $status_var= replica_brr_trx_assigned_to_brr_worker
--let $status_var_value= $expect_assigned_to_brr_worker_value
--let $status_timeout= $wait_timeout
--source include/wait_for_status_var.inc

--echo
--echo ## relay log cache temp file is created.
--let $MYSQLD_DATADIR= `select @@datadir`
--let $RELAY_LOG_CACHE_TEMP_FILE_DIR = $MYSQLD_DATADIR/#alisql_binlog_cache_temp_file
--exec ls $RELAY_LOG_CACHE_TEMP_FILE_DIR|grep "RL"|wc -l

--echo
--echo ## Kill the thd, so fail to execute DDL.
connection master1;
SELECT id INTO @id FROM information_schema.processlist WHERE INFO like '%rpl_test%' LIMIT 1;
KILL QUERY @id;

connection slave;
--let $status_var= replica_brr_ddl_rollback_by_brr_worker
--let $status_var_value= $expect_rollback_by_brr_worker_value
--let $status_timeout= $wait_timeout
--source include/wait_for_status_var.inc

--echo
--echo ## relay log cache temp file is removed.
--let $MYSQLD_DATADIR= `select @@datadir`
--let $RELAY_LOG_CACHE_TEMP_FILE_DIR = $MYSQLD_DATADIR/#alisql_binlog_cache_temp_file
--exec ls $RELAY_LOG_CACHE_TEMP_FILE_DIR|grep "RL"|wc -l

--source include/rpl_sync.inc
SHOW CREATE TABLE rpl_test.t1;

connection master;
if (!$is_optimize) {
--error ER_QUERY_INTERRUPTED
}
--reap
--eval SET SESSION debug = $clear_debug_val