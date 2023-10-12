
    
    

with dbt_test__target as (

  select zip_code as unique_field
  from `ny-citibike-pipeline`.`cbdev_mrt`.`top_zip_codes`
  where zip_code is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


