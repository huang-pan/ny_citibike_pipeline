select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with relation_columns as (

        
        select
            cast('NUM_BIKERIDES_DURING_HOLIDAYS' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'NUM_BIKERIDES_DURING_HOLIDAYS'
            and
            relation_column_type not in ('INTEGER')

    )
    select *
    from test_data
      
    ) dbt_internal_test