{{ config(materialized='view') }}

with ranked as (

    select
        *,
        row_number() over (
            partition by trim(lower(item))
            order by item
        ) as rn

    from {{ ref('ref_wpl') }}

)

select
    *
from ranked
where rn = 1