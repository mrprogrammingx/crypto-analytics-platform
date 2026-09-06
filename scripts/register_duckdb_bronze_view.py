#!/usr/bin/env python3
"""Register the Spark Bronze output as the btc_trades view in DuckDB."""
import os
import duckdb
from config import load_config


def main():
    cfg = load_config()
    conn = duckdb.connect(cfg.DUCKDB_DATABASE or "warehouse/analytics.duckdb")
    # Use an absolute path: the view's read_parquet() glob is re-resolved at
    # query time relative to whatever process/cwd later queries the view
    # (e.g. dbt, which cd's into crypto_analytics_dbt/), not to this script's cwd.
    bronze_path = os.path.abspath(cfg.SPARK_BRONZE_LOCAL_PATH)
    glob_path = f"{bronze_path}/*/*.parquet"
    table_id = cfg.BIGQUERY_TABLE_ID or "btc_trades"

    if "." in table_id:
        schema_name, view_name = table_id.rsplit(".", 1)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        qualified = f"{schema_name}.{view_name}"
    else:
        qualified = table_id

    conn.execute(f"CREATE OR REPLACE VIEW {qualified} AS SELECT * FROM read_parquet('{glob_path}', hive_partitioning=1)")
    print(f"Registered view {qualified} over {glob_path}")


if __name__ == "__main__":
    main()