select
    timestamp,
    symbol,
    price,
    quantity,
    -- some sources store epoch milliseconds; to_timestamp expects seconds
    to_timestamp(timestamp/1000) as event_time
from {{ env_var('BIGQUERY_TABLE_ID') }}