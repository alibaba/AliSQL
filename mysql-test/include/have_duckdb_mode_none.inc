let $have_duckdb_mode_none= `SELECT @@duckdb_mode LIKE 'NONE'`;
if (!$have_duckdb_mode_none)
{
  --skip Test requires duckdb_mode = NONE
}

