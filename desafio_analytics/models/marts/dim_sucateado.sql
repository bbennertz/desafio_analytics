with
    staging as (
        select 
            *
        from{{ref('stg_scrapreason')}}
    )

    , transformed as (
        select
            {{ dbt_utils.surrogate_key('id_sucateamento', 'data_modificacao') }} as sk_sucateamento
            , id_sucateamento
            , motivo_sucateamento
            , data_modificacao
        from staging
    )
select * from transformed 
