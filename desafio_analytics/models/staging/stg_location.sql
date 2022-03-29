with
    source as(
        select 
            locationid as id_localização
            , costrate as custo_hora
            , name as nome_local
            , availability as capacidade_manufatura
            , modifieddate as data_modificação
        from {{source('analytics','raw_location')}}
    )
    
select * from source