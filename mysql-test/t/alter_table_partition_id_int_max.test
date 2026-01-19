########################################################################
# ==== Purpose ====
#
# This test ensures that ALTER TABLE TRUNCATE PARTITION is
# successful when the `partition_id` in the `mysql.table_partitions`
# DD table exceeds the INT_MAX value
#
# ==== Implementation ====
#
# 1. In Set the AUTO_INCREMENT value in the mysql.table_partitions to
#    INT_MAX - 1.
# 2. Create a partitioned table with two partitions.
# 3. Add a third partition in the table, this should make the
#    partition_id exceed INT_MAX
# 4. Ensure that TRUNCATE PARTITION operation is successful without
#    any errors.
# 5. Cleanup
#
# ==== References ====
# Bug#35912852: cant truncate partition when partition_id exceed INT_MAX
########################################################################

# This test needs debug binaries
--source include/have_debug.inc
# This test alters the system table so restart is needed
--source include/force_restart.inc

# Enable the skip_dd_table_access_check debug_point
--let $debug_point= skip_dd_table_access_check
--let $debug_type= SESSION
--source include/add_debug_point.inc

# Before the test ensure that there are no entries in the
# mysql.table_partitions table
--assert(`SELECT COUNT(*) = 0 FROM mysql.table_partitions`)

# 1. Set the AUTO_INCREMENT value in the mysql.table_partitions to
#    INT_MAX - 1.
ALTER TABLE mysql.table_partitions AUTO_INCREMENT=2147483646;

# 2. Create a partitioned table with two partitions.
CREATE TABLE `t1` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `dtstat` bigint NOT NULL,
    PRIMARY KEY (`id`,`dtstat`)
    ) ENGINE=InnoDB AUTO_INCREMENT=5808007 PARTITION BY LIST (`dtstat`)
        (PARTITION p_20230601 VALUES IN (20230601) ENGINE = InnoDB,
         PARTITION P_20230201 VALUES IN (20230201) ENGINE = InnoDB);

# Assert that there are two partitions and the partition_id has reached INT_MAX
--assert(`SELECT COUNT(*) = 2 FROM mysql.table_partitions`)
--assert(`SELECT id = 2147483646 FROM mysql.table_partitions WHERE name = "p_20230601"`)
--assert(`SELECT id = 2147483647 FROM mysql.table_partitions WHERE name = "p_20230201"`)

# 3. Add a third partition in the table, this should make the
#    partition_id exceed INT_MAX
ALTER TABLE t1 ADD PARTITION
    (PARTITION P_20240101 VALUES IN (20240101) ENGINE = InnoDB);
--assert(`SELECT id = 2147483648 FROM mysql.table_partitions WHERE name = "p_20230601"`)
--assert(`SELECT id = 2147483649 FROM mysql.table_partitions WHERE name = "p_20230201"`)
--assert(`SELECT id = 2147483650 FROM mysql.table_partitions WHERE name = "p_20240101"`)

# 4. Ensure that TRUNCATE PARTITION operation is successful without
#    any errors.
ALTER TABLE t1 TRUNCATE PARTITION P_20240101;

# 5. Cleanup
DROP TABLE t1;
--assert(`SELECT COUNT(*) = 0 FROM mysql.table_partitions`)
--source include/remove_debug_point.inc
