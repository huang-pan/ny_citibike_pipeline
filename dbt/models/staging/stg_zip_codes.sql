with raw_zip_codes as (
        select * from {{ source('us-zip-codes-public', 'zip_codes') }}
)
select
        zip_code,
        city,
        county,
        state_code,
        state_name,
        area_land_meters,
        area_water_meters,
        internal_point_lat,
        internal_point_lon,
        internal_point_geom,
        zip_code_geom
from raw_zip_codes