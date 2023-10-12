select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select num_bikerides_during_holidays
from `ny-citibike-pipeline`.`cbdev_mrt`.`bikerides_during_holidays`
where num_bikerides_during_holidays is null



      
    ) dbt_internal_test