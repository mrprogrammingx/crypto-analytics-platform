select
    event_time,
    symbol,
    price,
    quantity
from {{ ref('stg_btc_prices') }}
where price is not null