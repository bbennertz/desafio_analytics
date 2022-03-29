with
    source as (
        select 
            scrapreasonid as id_razãosucateamento
            , name as motivo_sucateamento
            , modifieddate as data_modificação
        from {{source('analytics','raw_scrapreason')}}
    )

select * from source