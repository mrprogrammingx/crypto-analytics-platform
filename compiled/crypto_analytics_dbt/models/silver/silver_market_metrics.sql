select
    symbol,
    date(event_time) as trade_date,
    avg(price) as avg_price,
    max(price) as max_price,
    min(price) as min_price,
    sum(quantity) as total_quantity
from "analytics"."main"."bronze_btc_prices"
group by 1,2