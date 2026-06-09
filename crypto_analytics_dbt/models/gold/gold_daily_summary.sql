select
    trade_date,
    avg(avg_price) as market_avg_price,
    sum(total_quantity) as market_quantity
from {{ ref('silver_market_metrics') }}
group by 1