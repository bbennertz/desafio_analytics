with
    staging as (
        select *
        from{{ref('stg_location')}}
    )

    , transformed as (
        select
        row_number() over (order by id_localização) as sk_localização
        , id_localização
        , custo_hora
        , nome_local
        , capacidade_manufatura
        , data_modificação
        from staging
    )
select * from transformed