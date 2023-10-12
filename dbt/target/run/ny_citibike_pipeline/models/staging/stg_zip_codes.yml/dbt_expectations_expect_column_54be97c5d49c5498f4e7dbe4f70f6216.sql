select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with relation_columns as (

        
        select
            cast('ZIP_CODE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('CITY' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('COUNTY' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('STATE_CODE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('STATE_NAME' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('AREA_LAND_METERS' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        union all
        
        select
            cast('AREA_WATER_METERS' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        union all
        
        select
            cast('INTERNAL_POINT_LAT' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        union all
        
        select
            cast('INTERNAL_POINT_LON' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        union all
        
        select
            cast('INTERNAL_POINT_GEOM' as STRING) as relation_column,
            cast('GEOGRAPHY' as STRING) as relation_column_type
        union all
        
        select
            cast('ZIP_CODE_GEOM' as STRING) as relation_column,
            cast('GEOGRAPHY' as STRING) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'INTERNAL_POINT_LAT'
            and
            relation_column_type not in ('FLOAT')

    )
    select *
    from test_data
      
    ) dbt_internal_test