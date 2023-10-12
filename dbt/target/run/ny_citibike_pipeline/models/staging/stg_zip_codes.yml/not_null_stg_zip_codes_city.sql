select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select city
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_zip_codes`
where city is null



      
    ) dbt_internal_test