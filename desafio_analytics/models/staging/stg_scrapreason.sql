with
    source as(
        select *
        from {{ source('analytics','raw_scrapreason') }}
    )
    , transformed as (
        select 
            scrapreasonid as id_razãosucateamento
            , name as motivo_sucateamento
            , modifieddate as data_modificação
        from source
    )

select * 
from transformed