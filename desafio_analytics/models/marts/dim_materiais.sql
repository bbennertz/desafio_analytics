with
    dim_materiais as (
        select *
        from {{ ref('stg_billofmaterials') }}
    )
select *
from dim_materiais