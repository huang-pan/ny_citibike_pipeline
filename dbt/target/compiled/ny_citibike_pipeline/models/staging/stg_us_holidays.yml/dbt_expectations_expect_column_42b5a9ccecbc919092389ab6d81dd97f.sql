with relation_columns as (

        
        select
            cast('DATE' as STRING) as relation_column,
            cast('DATE' as STRING) as relation_column_type
        union all
        
        select
            cast('LOCALNAME' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('NAME' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('GLOBAL' as STRING) as relation_column,
            cast('BOOLEAN' as STRING) as relation_column_type
        union all
        
        select
            cast('COUNTIES' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LAUNCHYEAR' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('TYPE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'LAUNCHYEAR'
            and
            relation_column_type not in ('INTEGER')

    )
    select *
    from test_data