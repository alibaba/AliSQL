#pragma once
#include "duckdb/main/connection.hpp"
namespace myduck {
void register_mysql_udf(duckdb::Connection *con);
}