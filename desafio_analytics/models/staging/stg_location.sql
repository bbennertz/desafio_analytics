with
    source as (
        select *
        from {{ source('analytics', 'raw_location') }}
    )
    , transformed as (
        select 
            locationid as id_localização
            , name as nome_local
            , costrate as custo_hora
            , availability as capacidade_manufatura
            , modifieddate as data_modificação
        from source
    )   
select * 
from transformed