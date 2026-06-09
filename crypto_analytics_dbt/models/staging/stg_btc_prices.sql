select
    timestamp,
    symbol,
    price,
    quantity,
    cast(timestamp as timestamp) as event_time
from {{ env_var('BIGQUERY_TABLE_ID') }}