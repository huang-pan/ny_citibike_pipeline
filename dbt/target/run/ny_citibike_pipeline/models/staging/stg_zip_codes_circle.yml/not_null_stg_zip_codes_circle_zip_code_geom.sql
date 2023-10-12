select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select zip_code_geom
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_zip_codes_circle`
where zip_code_geom is null



      
    ) dbt_internal_test