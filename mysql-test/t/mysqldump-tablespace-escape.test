--source include/mysql_have_debug.inc

--echo #
--echo # Bug#36816986 - MySQL Shell command injection
--echo #

let $grep_file= $MYSQLTEST_VARDIR/tmp/bug36816986.sql;
let $grep_output=boolean;

CREATE DATABASE bug36816986;
USE bug36816986;

--echo -- Run mysqldump with tablespace_injection_test.
--exec $MYSQL_DUMP --debug="d,tablespace_injection_test" --result-file=$grep_file bug36816986 --all-tablespaces 2>&1

--echo The test injected string must be found:
let $grep_pattern=qr|  ENGINE=\*/\nsystem touch foo|;
--source include/grep_pattern.inc

--echo The ` must be escaped:
let $grep_pattern=qr|CREATE TABLESPACE `T``N; /*`|;
--source include/grep_pattern.inc

--remove_file $grep_file
DROP DATABASE bug36816986;

--echo
--echo #######################################
--echo

--echo #
--echo # Bug#37607195 - fprintf_string not using the actual quote parameter
--echo #

CREATE DATABASE bug37607195;
USE bug37607195;

let $grep_file= $MYSQLTEST_VARDIR/tmp/bug37607195.sql;
let $grep_output=boolean;

--echo Create a bunch of tables with numerous ` ' " \n etc.

SET @@sql_mode='ANSI_QUOTES,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

CREATE TABLE "custo`mers" (
    "customer'_id" INT AUTO_INCREMENT PRIMARY KEY,
    "fir`st_`na`me" VARCHAR(50) NOT NULL,
    "last_'name" VARCHAR(50) NOT NULL,
    "em`ail" VARCHAR(100) UNIQUE NOT NULL,
    `pho"\ne` VARCHAR(15),
    "created'_'at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "updated'_'at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE "prod'ucts" (
    "product`_`id" INT AUTO_INCREMENT PRIMARY KEY,
    "product'_`name" VARCHAR(100) NOT NULL,
    "descri`p`t`i`o`n" TEXT,
    "pr'i'ce" DECIMAL(10, 2) NOT NULL CHECK ("pr'i'ce" >= 0),
    `stock"_"qua\ntity` INT DEFAULT 0,
    `created'_'at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `updated"_'at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX ("product'_`name")
);

CREATE TABLE "orders" (
    "order_id" INT AUTO_INCREMENT PRIMARY KEY,
    "customer_id" INT NOT NULL,
    "order_date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "status" ENUM('Pending', 'Completed', 'Cancelled') NOT NULL,
    "total\n" DECIMAL(10, 2) NOT NULL CHECK ("total\n" >= 0),
    FOREIGN KEY (customer_id) REFERENCES "custo`mers"("customer'_id") ON DELETE CASCADE,
    INDEX (order_date)
);

CREATE TABLE `'order'_'items'` (
    `order'_'item_id` INT AUTO_INCREMENT PRIMARY KEY,
    `'order'_'id'` INT NOT NULL,
    `product'_'id` INT NOT NULL,
    `qua\ntity` INT NOT NULL CHECK (`qua\ntity` > 0),
    `p'rice` DECIMAL(10,2) NOT NULL CHECK (`p'rice` >= 0),
    FOREIGN KEY (`'order'_'id'`) REFERENCES "orders"(order_id) ON DELETE CASCADE,
    FOREIGN KEY (`product'_'id`) REFERENCES "prod'ucts"("product`_`id") ON DELETE CASCADE,
    UNIQUE KEY (`'order'_'id'`, `product'_'id`)
);

--exec $MYSQL_DUMP bug37607195 --init-command="SET @@sql_mode='ANSI_QUOTES,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'" --result-file=$grep_file 2>&1

--echo # Table 1: `'order'_'items'`
--echo # `qua\ntity` must be escaped
let $grep_pattern=qr|    `qua\ntity` INT NOT NULL CHECK (`qua\ntity` > 0)|;
--source include/grep_pattern.inc

--echo # Table 2: "custo`mers"
--echo # "custo`mers" must be escaped
let $grep_pattern=qr|CREATE TABLE `custo``mers`|;
--source include/grep_pattern.inc

--echo # `pho"\ne` must be escaped
let $grep_pattern=qr|`pho"\ne` varchar(15) DEFAULT NULL|;
--source include/grep_pattern.inc

--echo # Table 3: "orders"
--echo # `total\n` must be escaped
let $grep_pattern=qr|`total\n` decimal(10,2) NOT NULL|;
--source include/grep_pattern.inc

--echo # FOREIGN KEY (`customer_id`) REFERENCES must be escaped
let $grep_pattern=qr|REFERENCES `custo``mers`|;
--source include/grep_pattern.inc

--echo # Table 4: `prod'ucts`
--echo # "descri`p`t`i`o`n" TEXT must be escaped
let $grep_pattern=qr|`descri``p``t``i``o``n` text|;
--source include/grep_pattern.inc

--echo # `stock"_"qua\ntity` must be escaped
let $grep_pattern=qr|`stock"_"qua\ntity` int DEFAULT '0'|;
--source include/grep_pattern.inc

SET @@sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

# Cleanup
--remove_file $grep_file
DROP DATABASE bug37607195;
