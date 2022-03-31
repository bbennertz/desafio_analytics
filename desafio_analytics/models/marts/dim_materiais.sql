with
    stg_materiais as (
        select *
        from {{ ref('stg_billofmaterials') }}
    )
    , stg_materiais_with_sk as (
        select 
            {{ 
                dbt_utils.surrogate_key(['id_material']) 
            }} as sk_material
            , {{ 
                dbt_utils.surrogate_key(['id_produto_pai']) 
            }} as sk_produto_pai
            , {{ 
                dbt_utils.surrogate_key(['id_componente']) 
            }} as sk_componente
            , *
        from stg_materiais
    )   
    , transformed as (
        select
            sk_material
            , sk_produto_pai
            , sk_componente
            , bomlevel
            , qtd_por_pontagem
            , data_inicio
            , data_fim
            , data_modificada
        from stg_materiais_with_sk
        left join {{ ref('dim_produtos')}} 
            on stg_materiais_with_sk.sk_produto_pai = dim_produtos.sk_produto
            and stg_materiais_with_sk.sk_componente = dim_produtos.sk_produto
    )
select * 
from transformed