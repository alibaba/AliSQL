--source include/rpl_connection_slave.inc
SET GLOBAL duckdb_log_options = '';
STOP REPLICA SQL_THREAD;

--source include/rpl_connection_master.inc
INSERT INTO t1 (c0) VALUES('duck');
INSERT INTO t1 (c0) VALUES('mysql');

--let $rpl_only_running_threads=1
--source include/rpl_sync.inc

--source include/rpl_connection_slave.inc
START REPLICA SQL_THREAD;
--source include/rpl_sync.inc
--let $assert_file = $MYSQLTEST_VARDIR/log/mysqld.2.err
--let $assert_count = 0
--source include/assert_grep.inc

--source include/rpl_connection_slave.inc
eval SET GLOBAL duckdb_log_options = $diagnose_option;
STOP REPLICA SQL_THREAD;

--source include/rpl_connection_master.inc
INSERT INTO t1 (c0) VALUES('duck');
INSERT INTO t1 (c0) VALUES('mysql');

--let $rpl_only_running_threads=1
--source include/rpl_sync.inc

--source include/rpl_connection_slave.inc
START REPLICA SQL_THREAD;
--source include/rpl_sync.inc
--let $assert_file = $MYSQLTEST_VARDIR/log/mysqld.2.err
--let $assert_count = $assert_count_on
--source include/assert_grep.inc

SET GLOBAL duckdb_log_options = DEFAULT;
