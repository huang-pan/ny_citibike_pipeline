select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select internal_point_lon
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_zip_codes`
where internal_point_lon is null



      
    ) dbt_internal_test