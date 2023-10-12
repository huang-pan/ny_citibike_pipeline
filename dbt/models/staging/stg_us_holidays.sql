with raw_us_holidays as (
        select * from {{ source('us-holidays', 'us_holidays_2013') }}
        union all
        select * from {{ source('us-holidays', 'us_holidays_2014') }}
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