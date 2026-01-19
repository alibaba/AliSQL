let $restart_parameters = restart: --innodb-read-only;
if ($extra_restart_parameters) {
  let $restart_parameters=$restart_parameters $extra_restart_parameters;
  # Clear for future usages:
  let $extra_restart_parameters=;
}

--source include/start_mysqld.inc
let $restart_parameters=;
