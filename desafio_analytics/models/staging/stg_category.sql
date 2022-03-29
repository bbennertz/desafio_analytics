with 
    source as (
        select
            "productcategoryid" as id_categoria_produto
            , "name" as nome
            , "rowguid" as guia_linha
            , "modifieddate" as data_modificação
        from {{source('analytics','raw_productcategory')}}
    )

select * from source