{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_castle__dj') }}
),

-- 1) Roll up to discrete job level
productionorder_rows as (
    select
        'castle'                  as company,
        discrete_job_no           as dj_nbr,

        max(component)                   as comp_item,
        max(comp_uom)                    as comp_uom,
        max(comp_qty_per_assy)           as comp_qty_per_assy,
        max(comp_req_qty)           as comp_expected_qty
        

        
        

    from src
    group by
        discrete_job_no
        
)

select *
from productionorder_rows
