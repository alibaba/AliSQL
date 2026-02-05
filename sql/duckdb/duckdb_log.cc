/*****************************************************************************

Copyright (c) 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

#include "duckdb_log.h"
#include "sql_class.h"
#include "mysqld_error.h"
#include "log.h"

#define LOG_BUFF_MAX 1024

namespace myduck {

ulonglong duckdb_log_options = 0;

const char *duckdb_log_types[] = {
  "DUCKDB_MULTI_TRX_BATCH_COMMIT",
  "DUCKDB_MULTI_TRX_BATCH_DETAIL",
  "DUCKDB_QUERY",
  "DUCKDB_QUERY_RESULT",
  nullptr};

bool diagnose_log(THD *thd, int level, const char *type_specific_message) {
  assert(thd);
  const char *db = 
    thd->db().str && thd->db().length > 0 ? thd->db().str : "NULL";
  const char *query_str =
    thd->query().str && thd->query().length > 0 ? thd->query().str : "NULL";

  Security_context *sctx = thd->security_context();
  const char *ip =
    sctx->ip().str && sctx->ip().length > 0 ? sctx->ip().str : "NULL";
  const char *user =
    sctx->user().str && sctx->user().length > 0 ? sctx->user().str : "NULL";

  char local_start_time_buff[iso8601_size];
  ulonglong sql_start_time = thd->start_utime;
 
  make_iso8601_timestamp(local_start_time_buff, sql_start_time);
 
  // The maximum length of this message is MAX_LOG_BUFFER_SIZE, and the part exceeding
  // this length will be truncated
  char total_message[LOG_BUFF_MAX];
  if (snprintf(
          total_message, LOG_BUFF_MAX,
          "%s, sql_begin_time = %s, ip = %s, user = %s, db = %s, query = %s",
          type_specific_message, local_start_time_buff, ip, user, db,
          query_str) < 0)
    return true;

  switch (level) {
    case ERROR_LEVEL:
      sql_print_error(ER_DEFAULT(ER_RDS_DIAGNOSE), total_message);
      break;
    case WARNING_LEVEL:
      sql_print_warning(ER_DEFAULT(ER_RDS_DIAGNOSE), total_message);
      break;
    case INFORMATION_LEVEL:
      sql_print_information(ER_DEFAULT(ER_RDS_DIAGNOSE), total_message);
      break;
  }
  return false;
}

bool log_duckdb_multi_trx_batch_commit(THD *thd, const char *reason) {
  const char *format = "commit duckdb batch due to %s";
  char type_specific_message[LOG_BUFF_MAX];
  int specific_message_length =
      snprintf(type_specific_message, LOG_BUFF_MAX, format, reason);

  if (specific_message_length < 0) return true;

  diagnose_log(thd, INFORMATION_LEVEL, type_specific_message);

  return false;
}

bool log_duckdb_apply_event_type(THD *thd, const char *type) {
  const char *format = "apply event, type = %s";
  char type_specific_message[LOG_BUFF_MAX];
  int specific_message_length =
      snprintf(type_specific_message, LOG_BUFF_MAX, format, type);

  if (specific_message_length < 0) return true;

  diagnose_log(thd, INFORMATION_LEVEL, type_specific_message);

  return false;
}

bool log_duckdb_gtid(THD *thd, const char *prefix, int type, int sidno,
                            int64_t gno) {
  const char *format = "%s, type = %d, sidno = %d, gno = %lld";
  char type_specific_message[LOG_BUFF_MAX];
  int specific_message_length = snprintf(type_specific_message, LOG_BUFF_MAX,
                                         format, prefix, type, sidno, gno);

  if (specific_message_length < 0) return true;

  diagnose_log(thd, INFORMATION_LEVEL, type_specific_message);

  return false;
}
}
