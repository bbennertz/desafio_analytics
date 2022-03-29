with
    source as (
        select 
            locationid as id_localização
            , name as nome_local
            , costrate as custo_hora
            , availability as capacidade_manufatura
            , modifieddate as data_modificação
        from {{source('analytics','raw_location')}}
    )
    
select * from source