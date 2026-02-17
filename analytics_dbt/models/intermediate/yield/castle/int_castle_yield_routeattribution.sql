{{ config(materialized='view') }}

with route_steps as (
    select *
    from {{ ref('int_foundation_castle__mfg_operations') }}
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
        rs.dj_nbr as dj_nbr,
        rs.operation_code as operation_code,
        rs.operation_sequence as operation_sequence,
        
        op.op_name as operation_name,

        yr.yield_rank_cas as yield_rank



    from route_steps rs
    left join op_name op
        on rs.operation_code = op.op_code
    left join yield_rank yr
        on op.op_name = yr.op_name
),

job_min_rank as  (
    select
        company,
        dj_nbr,
        min(yield_rank) as yield_loss_rank
    from route_enriched
    group by
        company,
        dj_nbr
)

select
    re.company,
    re.dj_nbr,

    -- ordered route
    string_agg(
        re.operation_code::text,
        ' → '
        order by re.operation_sequence
    ) as operation_codes,

    string_agg(
        re.operation_name,
        ' → '
        order by re.operation_sequence
    ) as operation_names,

    -- yield loss operation (lowest rank used in job)
    max(re.operation_code) filter (
        where re.yield_rank = jmr.yield_loss_rank
    ) as yield_loss_operation_code,

    max(re.operation_name) filter (
        where re.yield_rank = jmr.yield_loss_rank
    ) as yield_loss_operation_name,

    jmr.yield_loss_rank

from route_enriched re
join job_min_rank jmr
    on re.company = jmr.company
    and re.dj_nbr = jmr.dj_nbr
group by
    re.company,
    re.dj_nbr,
    jmr.yield_loss_rank
