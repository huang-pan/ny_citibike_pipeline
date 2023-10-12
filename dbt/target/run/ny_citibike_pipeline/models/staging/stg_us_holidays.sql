
  
    

    create or replace table `ny-citibike-pipeline`.`cbdev_stg`.`stg_us_holidays`
      
    
    

    OPTIONS()
    as (
      with raw_us_holidays as (
        select * from `ny-citibike-pipeline`.`raw`.`us_holidays_2013`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`us_holidays_2014`
)
select
        date,
        localName,
        name,
        global,
        counties,
        cast(launchYear as int) launchYear,
        type
from raw_us_holidays
where type='Public' -- only public holidays
order by date asc
    );
  