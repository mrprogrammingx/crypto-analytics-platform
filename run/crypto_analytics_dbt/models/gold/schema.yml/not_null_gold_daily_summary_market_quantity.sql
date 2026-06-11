
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select market_quantity
from "analytics"."main"."gold_daily_summary"
where market_quantity is null



  
  
      
    ) dbt_internal_test