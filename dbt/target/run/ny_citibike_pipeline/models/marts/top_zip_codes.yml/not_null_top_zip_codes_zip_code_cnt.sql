select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select zip_code_cnt
from `ny-citibike-pipeline`.`cbdev_mrt`.`top_zip_codes`
where zip_code_cnt is null



      
    ) dbt_internal_test