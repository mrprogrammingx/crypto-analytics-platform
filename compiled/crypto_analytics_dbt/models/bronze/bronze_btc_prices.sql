select
    event_time,
    symbol,
    price,
    quantity
from "analytics"."main"."stg_btc_prices"
where price is not null