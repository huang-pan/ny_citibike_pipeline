with citibike_trips as (          
        select starttime, stoptime, start_station_id, bikeid
        from {{ ref('stg_citibike_trips') }}
), us_holidays AS (
        select date from {{ ref('stg_us_holidays') }}
)
select count(*) num_bikerides_during_holidays
from citibike_trips
where date(starttime) in (select date from us_holidays)
      or date(stoptime) in (select date from us_holidays) -- trips that start or end on a holiday