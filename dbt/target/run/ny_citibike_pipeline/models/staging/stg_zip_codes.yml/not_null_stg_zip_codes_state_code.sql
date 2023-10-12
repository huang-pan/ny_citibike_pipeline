select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select state_code
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_zip_codes`
where state_code is null



      
    ) dbt_internal_test