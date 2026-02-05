
/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#include <algorithm>  // std::transform
#include <cctype>     // std::tolower
#include "ddl_convertor.h"
#include "delta_appender.h"
#include "dml_convertor.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "row_mysql.h"
#include "duckdb/duckdb_context.h"
#include "duckdb/duckdb_query.h"

#include "my_time.h"
#include "duckdb/duckdb_config.h"
#include "duckdb/duckdb_table.h"
#include "rpl_rli.h"
#include "sql_class.h"
#include "sql_plugin.h"
#include "sql_show.h"
#include "sql_table.h"
#include "sql_time.h"
#include "tztime.h"
#include "typelib.h"

#include "duckdb_select.h"

/**
  Store a duckdb value in a field of mysql format.
  @param field            the field to store value
  @param value            the value to be stored
  @param thd              the thread handle
*/
void store_duckdb_field_in_mysql_format(Field *field, duckdb::Value &value,
                                        THD *thd) {
  if (value.IsNull()) {
    assert(field->real_maybe_null());
    field->set_default();
    field->set_null();
  } else {
    field->set_notnull();
    switch (field->type()) {
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_BIT: {
        auto str = value.GetValueUnsafe<duckdb::string>();
        auto varchar = str.c_str();
        auto varchar_len = str.size();
        field->store(varchar, varchar_len, &my_charset_bin);
        break;
      }
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VAR_STRING: {
        if (field->has_charset()) {
          assert(field->charset() != &my_charset_bin);
          auto str = value.GetValue<duckdb::string>();
          auto varchar = str.c_str();
          auto varchar_len = str.size();
          field->store(varchar, varchar_len, field->charset());
          break;
        } else {
          auto str = value.GetValueUnsafe<duckdb::string>();
          auto varchar = str.c_str();
          auto varchar_len = str.size();
          field->store(varchar, varchar_len, &my_charset_bin);
          break;
        }
      }
      /* json field should use system_charset_info to store */
      case MYSQL_TYPE_JSON:
      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_NEWDECIMAL: {
        auto str = value.GetValue<duckdb::string>();
        auto varchar = str.c_str();
        auto varchar_len = str.size();
        field->store(varchar, varchar_len, system_charset_info);
        break;
      }
      case MYSQL_TYPE_TINY: {
        int64_t v = value.GetValue<int64_t>();
        field->store(v, field->is_unsigned());
        break;
      }
      case MYSQL_TYPE_YEAR:
      case MYSQL_TYPE_SHORT: {
        int64_t v = value.GetValue<int64_t>();
        field->store(v, field->is_unsigned());
        break;
      }
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG: {
        int64_t v = value.GetValue<int64_t>();
        field->store(v, field->is_unsigned());
        break;
      }
      case MYSQL_TYPE_LONGLONG: {
        int64_t v;
        if (field->is_unsigned()) {
          v = value.GetValue<uint64_t>();
        } else {
          v = value.GetValue<int64_t>();
        }
        field->store(v, field->is_unsigned());
        break;
      }
      case MYSQL_TYPE_FLOAT: {
        float v = value.GetValue<float>();
        field->store(v);
        break;
      }
      case MYSQL_TYPE_DOUBLE: {
        double v = value.GetValue<double>();
        field->store(v);
        break;
      }
      case MYSQL_TYPE_DATE: {
        MYSQL_TIME tm;
        memset(&tm, 0, sizeof(tm));
        tm.time_type = MYSQL_TIMESTAMP_DATE;
        duckdb::date_t v =
            value.GetValue<duckdb::date_t>() + myduck::days_at_timestart;
        get_date_from_daynr(v.days, &tm.year, &tm.month, &tm.day);
        // store_field_temporal_value(field, &tm);
        field->store_time(&tm);
        break;
      }
      case MYSQL_TYPE_DATETIME: {
        Time_zone *time_zone =
            (value.type().id() == duckdb::LogicalTypeId::TIMESTAMP_TZ)
                ? thd->time_zone()
                : my_tz_UTC;

        int64_t v = value.GetValue<int64_t>();
        MYSQL_TIME ltime;
        struct timeval tm;
        if (v < 0) {
          my_micro_time_to_timeval(0, &tm);
          time_zone->gmt_sec_to_TIME(&ltime, tm);

          Interval interval;
          memset(&interval, 0, sizeof(interval));
          interval.neg = true;
          interval.second_part = -v;
          date_add_interval(&ltime, INTERVAL_MICROSECOND,interval);
        } else {
          my_micro_time_to_timeval(v, &tm);
          time_zone->gmt_sec_to_TIME(&ltime, tm);
        }
        // store_field_temporal_value(field, &ltime);
        field->store_time(&ltime);
        break;
      }
      case MYSQL_TYPE_TIMESTAMP: {
        int64_t v = value.GetValue<int64_t>();
        MYSQL_TIME ltime;
        struct timeval tm;
        my_micro_time_to_timeval(v, &tm);
        thd->time_zone()->gmt_sec_to_TIME(&ltime, tm);
        // store_field_temporal_value(field, &ltime);
        field->store_time(&ltime);
        break;
      }
      case MYSQL_TYPE_TIME: {
        int64_t v = value.GetValue<int64_t>();
        MYSQL_TIME ltime;
        ltime.second_part = v % 1000000;
        v /= 1000000;
        ltime.second = v % 60;
        v /= 60;
        ltime.minute = v % 60;
        v /= 60;
        ltime.hour = v;
        ltime.year = ltime.month = ltime.day = 0;
        ltime.neg = false;
        ltime.time_type = MYSQL_TIMESTAMP_TIME;
        // store_field_temporal_value(field, &ltime);
        field->store_time(&ltime);
        break;
      }
      default:
        /* TODO: no support */
        assert(0);
        break;
    }
  }
}