with 
    source as (
        select *
        from {{ source('analytics', 'raw_workorderrouting') }}
    )
    , transformed as (
        select
            workorderid as id_fabricacao
            , productid as id_produto
            , locationid as id_localizacao
            , operationsequence as sequencia_operacao
            , actualresourcehrs as horas_fabricacao
            , plannedcost as custo_estimado
            , actualcost as custo_real
            , scheduledstartdate as data_planejada_inicio_fabricacao
            , scheduledenddate as data_planejada_fim_fabricacao
            , actualstartdate as data_inicio_real
            , actualenddate as data_fim_real
        from source
    )
select *
from transformed
