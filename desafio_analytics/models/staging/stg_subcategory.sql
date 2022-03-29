with 
    source as (
        select 
            "productsubcategoryid" as id_subcategoria_produto
            , "productcategoryid" id_categoria_produto
            , "name" as nome
            , "rowguid" as guia_linha
            , "modifieddate" as data_modificação
        from {{source('analytics','raw_productsubcategory')}}
    )

select * from source