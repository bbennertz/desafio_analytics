with 
    source as (
        select *
        from {{source('analytics', 'raw_product')}}
    )


select * from source