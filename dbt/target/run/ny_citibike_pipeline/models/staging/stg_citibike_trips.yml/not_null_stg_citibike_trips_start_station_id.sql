select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select start_station_id
from `ny-citibike-pipeline`.`cbdev_stg`.`stg_citibike_trips`
where start_station_id is null



      
    ) dbt_internal_test