#!/usr/bin/env bash
set -euo pipefail

basedir=$(realpath "$1")
expected_version=$2
source_dir=$(realpath "$3")
smoke_dir=$(mktemp -d /tmp/alisql-release-smoke.XXXXXX)
server_pid=
user_options=()
if [[ $(id -u) == 0 ]]; then
  user_options+=(--user=root)
fi

cleanup() {
  smoke_status=$?
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    timeout 20 "$basedir/bin/mysqladmin" --no-defaults --protocol=socket \
      --socket="$smoke_dir/mysql.sock" -uroot shutdown >/dev/null 2>&1 || \
      kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" || true
  fi
  if [[ "$smoke_status" != 0 ]]; then
    for log in "$smoke_dir/initialize.log" "$smoke_dir/mysqld.log"; do
      if [[ -f "$log" ]]; then tail -n 120 "$log"; fi
    done
  fi
  exit "$smoke_status"
}
trap cleanup EXIT

"$basedir/bin/mysqld" --no-defaults --version
"$basedir/bin/mysql" --no-defaults --version
server_options=()
if [[ -d "$source_dir/storage/duckdb" ]]; then
  server_options+=(--duckdb_mode=ON --duckdb_threads=2 --duckdb_memory_limit=1073741824)
fi

"$basedir/bin/mysqld" --no-defaults --initialize-insecure "${user_options[@]}" \
  --basedir="$basedir" --datadir="$smoke_dir/data" >"$smoke_dir/initialize.log" 2>&1
"$basedir/bin/mysqld" --no-defaults "${user_options[@]}" \
  --basedir="$basedir" --datadir="$smoke_dir/data" \
  --socket="$smoke_dir/mysql.sock" --pid-file="$smoke_dir/mysqld.pid" \
  --log-error="$smoke_dir/mysqld.log" --skip-networking --loose-mysqlx=OFF \
  "${server_options[@]}" &
server_pid=$!

ready=false
for attempt in {1..120}; do
  if "$basedir/bin/mysqladmin" --no-defaults --protocol=socket \
      --socket="$smoke_dir/mysql.sock" -uroot ping >/dev/null 2>&1; then
    ready=true
    break
  fi
  kill -0 "$server_pid"
  sleep 1
done
[[ "$ready" == true ]]

query() {
  "$basedir/bin/mysql" --no-defaults --protocol=socket \
    --socket="$smoke_dir/mysql.sock" -uroot --batch --skip-column-names -e "$1"
}

actual_version=$(query 'SELECT VERSION()')
[[ "$actual_version" == "$expected_version"* ]]
echo "Version verified: $actual_version"
result=$(query 'CREATE DATABASE release_smoke; CREATE TABLE release_smoke.i (id INT PRIMARY KEY, v INT) ENGINE=InnoDB; INSERT INTO release_smoke.i VALUES (1,10),(2,20); UPDATE release_smoke.i SET v=30 WHERE id=2; SELECT SUM(v) FROM release_smoke.i;')
[[ "$result" == 40 ]]
echo 'InnoDB read/write check passed'

if [[ -d "$source_dir/storage/duckdb" ]]; then
  result=$(query 'CREATE TABLE release_smoke.d (id INT PRIMARY KEY, v INT) ENGINE=DuckDB; INSERT INTO release_smoke.d VALUES (1,10),(2,20); SELECT SUM(v) FROM release_smoke.d;')
  [[ "$result" == 30 ]]
  echo 'DuckDB read/write check passed'
fi

if [[ -f "$source_dir/sql/rpl_replica_ext.h" ]]; then
  result=$(query "SELECT CONCAT(@@global.binlog_realtime_transmit_source_enabled, ',', @@global.binlog_realtime_transmit_replica_enabled, ',', @@global.binlog_realtime_apply_workers)")
  [[ "$result" == 0,0,0 ]]
  echo 'BRR defaults verified'
fi

if grep -q 'Sys_duckdb_support_auto_increment' "$source_dir/sql/sys_vars_ext.cc" 2>/dev/null; then
  result=$(query "SET GLOBAL duckdb_support_auto_increment=ON; CREATE TABLE release_smoke.a (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT) ENGINE=DuckDB; INSERT INTO release_smoke.a (v) VALUES (10),(20); SELECT GROUP_CONCAT(id ORDER BY id) FROM release_smoke.a;")
  [[ "$result" == 1,2 ]]
  query 'SET GLOBAL duckdb_enable_background_checkpoint=ON; SET GLOBAL duckdb_background_checkpoint_full_interval=0; SET GLOBAL duckdb_enable_background_checkpoint=OFF;'
  echo 'DuckDB AUTO_INCREMENT and background-checkpoint controls verified'
fi
query 'DROP DATABASE release_smoke'
"$basedir/bin/mysqladmin" --no-defaults --protocol=socket \
  --socket="$smoke_dir/mysql.sock" -uroot shutdown
wait "$server_pid"
server_pid=
echo 'Relocated release package smoke checks passed'
