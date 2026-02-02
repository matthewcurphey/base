{{ config(materialized='view') }}

with route_steps as (
    select *
    from {{ ref('int_foundation_banner__mfg_routesteps') }}
),

op_name as (
    select *
    from {{ ref('ref_opname') }}
),

yield_rank as (
    select *
    from {{ ref('ref_yieldrank') }}
),

route_enriched as (

    select
        rs.company as company,
        rs.production_order_number as production_order_number,
        rs.operation_id as operation_id,
        rs.operation_number as operation_number,
        
        op.op_name as operation_name,

        yr.yield_rank_ban as yield_rank



    from route_steps rs
    left join op_name op
        on rs.operation_id = op.op_code
    left join yield_rank yr
        on op.op_name = yr.op_name
),

job_min_rank as  (
    select
        company,
        production_order_number,
        min(yield_rank) as yield_loss_rank
    from route_enriched
    group by
        company,
        production_order_number
)

select
    re.company,
    re.production_order_number,

    -- ordered route
    string_agg(
        re.operation_id::text,
        ' → '
        order by re.operation_number
    ) as operation_ids,

    string_agg(
        re.operation_name,
        ' → '
        order by re.operation_number
    ) as operation_names,

    -- yield loss operation (lowest rank used in job)
    max(re.operation_id) filter (
        where re.yield_rank = jmr.yield_loss_rank
    ) as yield_loss_operation_id,

    max(re.operation_name) filter (
        where re.yield_rank = jmr.yield_loss_rank
    ) as yield_loss_operation_name,

    jmr.yield_loss_rank

from route_enriched re
join job_min_rank jmr
    on re.company = jmr.company
    and re.production_order_number = jmr.production_order_number
group by
    re.company,
    re.production_order_number,
    jmr.yield_loss_rank
