--source include/not_hypergraph.inc

--echo # Bug#30875669 CORE CLIENT CANNOT SEND QUERY WITH NUMBER SIGN OR DOUBLE DASH IN HINT COMMENT

CREATE USER myuser;

if (`SELECT CONVERT(@@VERSION_COMPILE_OS USING latin1) IN ('Win32', 'Win64', 'Windows')`) {
  --exec $MYSQL --user=myuser --skip-comments -e "explain select /*+ QB_NAME(`select#1`) */ 1;"
  --exec $MYSQL --user=myuser --skip-comments -e "explain select /*+ QB_NAME(`select-- `) */ 1;"
}
if (`SELECT CONVERT(@@VERSION_COMPILE_OS USING latin1) NOT IN ('Win32', 'Win64', 'Windows')`) {
  --exec $MYSQL --user=myuser --skip-comments -e "explain select /*+ QB_NAME(\`select#1\`) */ 1;"
  --exec $MYSQL --user=myuser --skip-comments -e "explain select /*+ QB_NAME(\`select-- \`) */ 1;"
}

DROP USER myuser;