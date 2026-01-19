--source include/add_mtr_suppression_for_innodb_readonly.inc

# Force a change buffer merge, so that --innodb-read-only will not
# refuse startup, in case there exist buffered changes from earlier tests.
--source include/shutdown_innodb_clean.inc


--source include/start_innodb_readonly.inc
