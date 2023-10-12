with ssid_all_points as (
        select start_station_id, ST_GEOGPOINT(start_station_longitude, start_station_latitude) point
        from {{ ref('stg_citibike_trips') }} -- starting trips only
), ssid_zip_codes as (
        select start_station_id, zip_code
        from {{ ref('stg_zip_codes') }}
        join ssid_all_points
        on ST_WITHIN(point, zip_code_geom)
)
select zip_code, count(*) zip_code_cnt
from ssid_zip_codes
group by zip_code
order by zip_code_cnt desc -- sort by most popular zip code