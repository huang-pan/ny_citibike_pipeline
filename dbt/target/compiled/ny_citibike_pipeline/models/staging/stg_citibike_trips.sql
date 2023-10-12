with raw_citibike_trips as (
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201306`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201307`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201308`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201309`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201310`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201311`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201312`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201401`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201402`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201403`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201404`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201405`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201406`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201407`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201408`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201409`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201410`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201411`
        union all
        select * from `ny-citibike-pipeline`.`raw`.`citibike_trips_201412`
)
select
        *
from raw_citibike_trips