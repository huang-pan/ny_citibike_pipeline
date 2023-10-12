select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select area_water_meters
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_zip_codes`
where area_water_meters is null



      
    ) dbt_internal_test