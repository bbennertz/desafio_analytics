with
    source as(
        select *
        from {{ source('analytics','raw_productinventory') }}
    )
    , transformed as ( 
        select 
            locationid as id_localização
            , productid as id_produto
            , shelf as prateleira
            , bin as caixa
            , quantity as quantidade_no_inventário
            , rowguid as guia_linha
            , modifieddate as data_modificação
        from source
    )
select * 
from transformed