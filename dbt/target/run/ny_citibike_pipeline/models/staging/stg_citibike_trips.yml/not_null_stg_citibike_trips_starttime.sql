select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select starttime
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_citibike_trips`
where starttime is null



      
    ) dbt_internal_test