select
    timestamp,
    symbol,
    price,
    quantity,
    -- Handle different adapters and input types for timestamp:
    -- - If the source already has a TIMESTAMP, keep it.
    -- - If the source has epoch milliseconds, convert to timestamp.
    -- Use adapter-aware logic so this model works both with DuckDB and BigQuery.

    case
        when typeof(timestamp) = 'TIMESTAMP' then timestamp
        when typeof(timestamp) in ('BIGINT', 'INTEGER', 'DOUBLE') then to_timestamp(CAST(timestamp AS DOUBLE)/1000)
        else to_timestamp(CAST(timestamp AS DOUBLE)/1000)
    end as event_time

from crypto_analytics.btc_trades