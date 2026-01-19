--source include/rpl_connection_slave.inc
STOP REPLICA;

--source include/rpl_connection_master.inc
eval $dml_query;

--source include/rpl_connection_slave.inc
eval SET GLOBAL debug=$crash_point;
--source include/expect_crash.inc
START REPLICA;

--enable_reconnect
--source include/wait_until_disconnected.inc

--let $rpl_server_number= 2
--source include/rpl_start_server.inc
--disable_reconnect
START REPLICA;

--source include/rpl_sync.inc
eval $result_query;
