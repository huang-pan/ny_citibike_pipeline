with raw_zip_codes as (
        select cast(ZIP as string) zip_code,
               LAT lat,
               LNG lon,
               ST_Buffer(ST_GEOGPOINT(LNG, LAT), 1609.34) zip_code_geom -- one mile radius = 1609.34 meters
        from `ny-citibike-pipeline`.`raw`.`us_zip_codes`
)
select
        *
from raw_zip_codes