
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    trade_date as unique_field,
    count(*) as n_records

from "analytics"."main"."gold_daily_summary"
where trade_date is not null
group by trade_date
having count(*) > 1



  
  
      
    ) dbt_internal_test