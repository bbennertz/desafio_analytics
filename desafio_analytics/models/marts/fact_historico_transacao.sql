with
    source as (
        select *
        from {{ ref('stg_transactionhistory') }}
    ) 
    , fact_historico_transacao as (
        select 
            id_transacao
            , id_produto
            , data_transacao
            , tipo_transacao
            , qtd_produto
            , custo_produto
            , data_modificacao
        from source        
    )
    , fact_historico_transacao_with_sk as (
        select
            {{ 
                dbt_utils.surrogate_key(['id_transacao']) 
            }} as sk_historico_transacao  
            , dim_produtos.sk_produto
            , fact_historico_transacao.id_transacao 
            , fact_historico_transacao.id_produto
            , fact_historico_transacao.data_transacao
            , fact_historico_transacao.tipo_transacao
            , fact_historico_transacao.qtd_produto
            , fact_historico_transacao.custo_produto
            , fact_historico_transacao.data_modificacao
        from fact_historico_transacao    
        left join {{ ref('dim_produtos') }} 
            on fact_historico_transacao.id_produto = dim_produtos.id_produto
    )
select *
from fact_historico_transacao_with_sk