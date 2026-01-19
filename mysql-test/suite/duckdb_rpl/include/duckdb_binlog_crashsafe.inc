#
# Auxaliary file to repeat crashsafe tests
# Usage:
# --let $dbug_point = crash_before_duckdb_engine_commit
# --let $crash_stmt = COMMIT
# --source include/duckdb_binlog_crashsafe.inc
#
--let $binlog_file = query_get_value(SHOW MASTER STATUS, File, 1)

eval SET debug = "d,$dbug_point";

--source include/expect_crash.inc
--error 2013
--eval $crash_stmt

--let $rpl_server_number = 1
--source include/rpl_start_server.inc
--let $keep_gtid_events = 1
--source include/show_binlog_events.inc
SELECT * FROM t1;

--source include/rpl_connection_slave.inc
--disable_warnings
--let $rpl_allow_error = 1
--source include/start_slave_io.inc
--enable_warnings

--let $rpl_diff_statement= SELECT * FROM t1 ORDER BY c1
--source include/rpl_connection_master.inc
--source include/rpl_diff.inc

