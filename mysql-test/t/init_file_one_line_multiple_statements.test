--echo #
--echo # Bug# 37559598 - Unexpected incorrect syntax reported for statement in init file
--echo #

--source include/linux.inc

let BOOTSTRAP_SQL= $MYSQL_TMP_DIR/tiny_bootstrap.sql;

let $MYSQLD_LOG= $MYSQLTEST_VARDIR/log/init_file_bootstrap.log;
let ENV_MYSQLD_LOG= $MYSQLD_LOG;

--echo # Create the bootstrap file.
write_file $BOOTSTRAP_SQL;
  SET character_set_client = 'cp850';
  SET sql_log_bin = OFF; ALTER USER 'test'@'localhost' IDENTIFIED BY 'FAArxtw*e8sDnSl:V)u>Z/QLY;IvfC)o'; CREATE DATABASE foo;
EOF

--echo # Create a test user.
CREATE USER 'test'@'localhost';

--let $restart_parameters=restart:--log_error_verbosity=3 --init-file=$BOOTSTRAP_SQL --log-error=$MYSQLD_LOG
--let $do_not_echo_parameters = 1
--source include/restart_mysqld.inc

--echo # There should be no syntax error during the execution of init-file
perl;
  use strict;
  my $log= $ENV{'ENV_MYSQLD_LOG'} or die;
  open(FILE, "$log") or die;
  my $c_w= grep(/You have an error in your SQL syntax/gi,<FILE>);
  print "#     You have an error in your SQL syntax - found $c_w times.\n";
  close(FILE);
EOF

--echo # 'foo' schema must exist.
SHOW DATABASES;

--echo # 'test'@'localhost' user should be able to log in using new password
--connect(changed_pass_conn, localhost, test, 'FAArxtw*e8sDnSl:V)u>Z/QLY;IvfC)o')
SELECT USER();
connection default;

--echo # Clean up
DROP DATABASE foo;
DROP USER 'test'@'localhost';
remove_file $BOOTSTRAP_SQL;
remove_file $MYSQLD_LOG;
