with relation_columns as (

        
        select
            cast('STARTTIME' as STRING) as relation_column,
            cast('DATETIME' as STRING) as relation_column_type
        union all
        
        select
            cast('STOPTIME' as STRING) as relation_column,
            cast('DATETIME' as STRING) as relation_column_type
        union all
        
        select
            cast('START_STATION_ID' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('START_STATION_LATITUDE' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        union all
        
        select
            cast('START_STATION_LONGITUDE' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        union all
        
        select
            cast('BIKEID' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'START_STATION_ID'
            and
            relation_column_type not in ('INTEGER')

    )
    select *
    from test_data