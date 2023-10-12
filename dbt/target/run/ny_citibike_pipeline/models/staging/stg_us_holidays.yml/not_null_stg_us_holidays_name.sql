select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select name
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_us_holidays`
where name is null



      
    ) dbt_internal_test