with
    source as (
        select *
        from {{ ref('stg_workorderrouting') }}
    ) 
    , fact_fabricacao_detalhes as (
        select 
            id_ordem_servico 
            , id_produto 
            , id_localizacao 
            , sequencia_operacao
            , horas_fabricacao
            , custo_estimado
            , custo_real
            , data_planejada_inicio_fabricacao
            , data_planejada_fim_fabricacao
            , data_inicio_real
            , data_fim_real
        from source        
    )
    , fact_fabricacao_detalhes_with_sk as (
        select
            {{ 
                dbt_utils.surrogate_key(['id_ordem_servico']) 
            }} as sk_ordem_servico   /*ver com os meninos -- join com ordem servico*/
            , dim_produtos.sk_produto
            , dim_localizacao.sk_localização  /*ver com os meninos*/
            , fact_fabricacao_detalhes.id_ordem_servico 
            , fact_fabricacao_detalhes.id_produto 
            , fact_fabricacao_detalhes.id_localizacao 
            , fact_fabricacao_detalhes.sequencia_operacao
            , fact_fabricacao_detalhes.horas_fabricacao
            , fact_fabricacao_detalhes.custo_estimado
            , fact_fabricacao_detalhes.custo_real
            , fact_fabricacao_detalhes.data_planejada_inicio_fabricacao
            , fact_fabricacao_detalhes.data_planejada_fim_fabricacao
            , fact_fabricacao_detalhes.data_inicio_real
            , fact_fabricacao_detalhes.data_fim_real
        from fact_fabricacao_detalhes    
        left join {{ ref('dim_produtos') }} 
            on fact_fabricacao_detalhes.id_produto = dim_produtos.id_produto
        left join {{ ref('dim_localizacao') }}     
            on fact_fabricacao_detalhes.id_localizacao = dim_localizacao.id_localização
    )
select *
from fact_fabricacao_detalhes_with_sk