with
    stg_materiais as (
        select *
        from {{ ref('stg_billofmaterials') }}
    )
    , stg_materiais_with_sk as (
        select 
            {{ 
                dbt_utils.surrogate_key(['id_material', 'data_modificacao']) 
            }} as sk_material
            , {{ 
                dbt_utils.surrogate_key(['id_produto_pai', 'data_modificacao']) 
            }} as sk_produto_pai
            , {{ 
                dbt_utils.surrogate_key(['id_componente', 'data_modificacao']) 
            }} as sk_componente
            , *
        from stg_materiais
    )   
    , transformed as (
        select
            sk_material
            , sk_produto_pai
            , sk_componente
            , id_material
            , id_produto_pai
            , id_componente
            , bomlevel
            , qtd_por_pontagem
            , data_inicio
            , data_fim
            , data_modificacao
        from stg_materiais_with_sk
        left join {{ ref('dim_produtos')}} 
            on stg_materiais_with_sk.sk_produto_pai = dim_produtos.sk_produtos
            and stg_materiais_with_sk.sk_componente = dim_produtos.sk_produtos
    )
select * 
from transformed