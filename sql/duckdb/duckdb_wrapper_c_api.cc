#include "duckdb_wrapper_c_api.h"
#include "duckdb_manager.h"
#include "duckdb_query.h"
#include "duckdb_config.h"
#include "duckdb_context.h"
#include "sql_class.h"
#include "log_event.h"

bool duckdb_manager_create_ins()
{
    return myduck::DuckdbManager::CreateInstance();
}

void duckdb_manager_cleanup()
{
    myduck::DuckdbManager::Cleanup();
}

void duckdb_create_test_schema(THD *thd)
{
    if (myduck::global_mode != myduck::enum_modes::DUCKDB_ON)  return;

    std::string query = "CREATE SCHEMA IF NOT EXISTS `test`";
    auto query_result = myduck::duckdb_query(thd, query, false);
    assert(!query_result->HasError());
}

bool myduck_is_duckdb_table(const TABLE *table)
{
    return myduck::is_duckdb_table(table);
}

bool myduck_duckdb_query_and_send(THD *thd, const std::string &query, bool send_result, bool push_error)
{
    return myduck::duckdb_query_and_send(thd, query, send_result, push_error);
}

bool myduck_duckdb_query_update(THD *thd, std::string &query, ha_rows &duckdb_update_rows)
{
    bool rval = false;
    auto res = myduck::duckdb_query(thd, query);
    if (res->HasError())
    {
        rval = true;
        my_error(ER_DUCKDB_CLIENT, MYF(0), res->GetError().c_str());
    }
    else
    {
      auto chunk = res->Fetch();
      duckdb_update_rows = chunk->GetValue(0, 0).GetValue<int64_t>();
    }
    return rval;
}

bool duckdb_global_mode_on() { return myduck::global_mode == myduck::enum_modes::DUCKDB_ON; }

void thd_duckdb_context_update_on_commit(THD *thd)
{
    thd->get_duckdb_context()->update_on_commit();
}

int thd_duckdb_context_save_batch_gtid_set(THD *thd)
{
    return thd->get_duckdb_context()->save_batch_gtid_set();
}

bool thd_duckdb_context_need_implicit_commit_batch(THD *thd, Log_event *ev)
{
    return thd->get_duckdb_context()->need_implicit_commit_batch(ev);
}

int thd_duckdb_context_implicit_commit_batch(THD *thd)
{
    return thd->get_duckdb_context()->implicit_commit_batch();
}

bool thd_duckdb_context_duckdb_delay_commit(THD *thd)
{
    return thd->get_duckdb_context()->duckdb_delay_commit();
}

int thd_duckdb_context_commit_partial_batch(THD *thd)
{
    return thd->get_duckdb_context()->commit_partial_batch();
}

int thd_duckdb_context_flush_appenders(THD *thd, std::string &error_msg)
{
    return thd->get_duckdb_context()->flush_appenders(error_msg);
}

void rli_info_thd_duckdb_context_add_gtid_set(THD *thd, Gtid_set *s)
{
    thd->get_duckdb_context()->add_gtid_set(s);
}

void duckdb_context_prepare_gtids_for_binlog_commit(THD *thd)
{
    thd->get_duckdb_context()->prepare_gtids_for_binlog_commit();
}