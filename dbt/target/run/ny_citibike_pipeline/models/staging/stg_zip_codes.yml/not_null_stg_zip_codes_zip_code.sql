select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select zip_code
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_zip_codes`
where zip_code is null



      
    ) dbt_internal_test