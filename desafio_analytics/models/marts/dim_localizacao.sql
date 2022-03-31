with
    staging as (
        select 
            *
        from{{ref('stg_location')}}
    )

    , transformed as (
        select
            {{ dbt_utils.surrogate_key('id_localizacao', 'data_modificacao') }} as sk_localizacao
            , id_localizacao
            , nome_local
            , capacidade_manufatura
        from staging
    )
select * from transformed 