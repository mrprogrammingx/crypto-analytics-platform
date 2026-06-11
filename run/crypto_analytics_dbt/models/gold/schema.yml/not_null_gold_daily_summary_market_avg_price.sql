
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select market_avg_price
from "analytics"."main"."gold_daily_summary"
where market_avg_price is null



  
  
      
    ) dbt_internal_test