with
    source as (
        select *
        from {{ ref('stg_ordem_servico_detalhes') }}
    ) 
    , fact_ordem_servico_detalhes as (
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
            , data_modificacao
        from source        
    )
    , fact_ordem_servico_detalhes_with_sk as (
        select
            fact_ordens_servicos.sk_ordens_servicos
            , dim_produtos.sk_produtos
            , dim_localizacao.sk_localizacao
            , fact_ordem_servico_detalhes.id_ordem_servico 
            , fact_ordem_servico_detalhes.id_produto 
            , fact_ordem_servico_detalhes.id_localizacao 
            , fact_ordem_servico_detalhes.sequencia_operacao
            , fact_ordem_servico_detalhes.horas_fabricacao
            , fact_ordem_servico_detalhes.custo_estimado
            , fact_ordem_servico_detalhes.custo_real
            , fact_ordem_servico_detalhes.data_planejada_inicio_fabricacao
            , fact_ordem_servico_detalhes.data_planejada_fim_fabricacao
            , fact_ordem_servico_detalhes.data_inicio_real
            , fact_ordem_servico_detalhes.data_fim_real
            , fact_ordem_servico_detalhes.data_modificacao
        from fact_ordem_servico_detalhes
        left join {{ ref('fact_ordens_servicos') }}
            on fact_ordem_servico_detalhes.id_ordem_servico = fact_ordens_servicos.id_ordem_servico
        left join {{ ref('dim_produtos') }} 
            on fact_ordem_servico_detalhes.id_produto = dim_produtos.id_produto
        left join {{ ref('dim_localizacao') }}     
            on fact_ordem_servico_detalhes.id_localizacao = dim_localizacao.id_localizacao
    )
select *
from fact_ordem_servico_detalhes_with_sk