select
    timestamp,
    symbol,
    price,
    quantity,
    -- Handle different adapters and input types for timestamp:
    -- - If the source already has a TIMESTAMP, keep it.
    -- - If the source has epoch milliseconds, convert to timestamp.
    -- Use adapter-aware logic so this model works both with DuckDB and BigQuery.
{% if target.type == 'duckdb' %}
    case
        when typeof(timestamp) = 'TIMESTAMP' then timestamp
        when typeof(timestamp) in ('BIGINT', 'INTEGER', 'DOUBLE') then to_timestamp(CAST(timestamp AS DOUBLE)/1000)
        else to_timestamp(CAST(timestamp AS DOUBLE)/1000)
    end as event_time
{% elif target.type == 'bigquery' %}
    case
        when SAFE_CAST(timestamp AS INT64) IS NOT NULL then TIMESTAMP_MILLIS(CAST(timestamp AS INT64))
        else CAST(timestamp AS TIMESTAMP)
    end as event_time
{% else %}
    -- Fallback: attempt numeric division like earlier (best-effort)
    to_timestamp(timestamp/1000) as event_time
{% endif %}
from {{ env_var('BIGQUERY_TABLE_ID') }}