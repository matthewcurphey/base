{{ config(materialized='view') }}

with src as (

    select *
    from {{ ref('int_foundation_stgcastledj_fxwpl') }}

),

-- 1️⃣ Roll up to discrete job + operation level
productionorder_rows as (

    select
        'castle'                        as company,
        max(org)                        as org,
        discrete_job_no                 as dj_nbr,
        operation_code                  as operation_code,
        max(dj_quantity_completed)      as dj_quantity_completed,

        sum(hrs_earned)                 as raw_earned_hrs

    from src
    group by
        discrete_job_no,
        operation_code

),

-- 2️⃣ Apply formula adjustments by operation + org
formula_adjusted as (

    select
        company,
        org,
        dj_nbr,
        operation_code,
        dj_quantity_completed,
        raw_earned_hrs,

        case
            when operation_code = 'EXT'
                then (dj_quantity_completed * 0.0125) + 0.08333333
            when operation_code = 'WJC' and org = 'ENT'
                then raw_earned_hrs * 0.25
            when operation_code = 'HWK' and org = 'MXM'
                then (dj_quantity_completed + 5) / 60.0
            when operation_code = 'SHT' and org = 'MXM'
                then (dj_quantity_completed + 5) / 60.0
            when operation_code = 'CHF' and org = 'CHA'
                then ((dj_quantity_completed * 1.5) + 5) / 60.0
            else raw_earned_hrs
        end as calc_earned_hrs

    from productionorder_rows

),

-- 3️⃣ Manual adjustments seed — overrides calc_earned_hrs when a match exists
manual_adjustments as (

    select *
    from {{ ref('ref_castle_earnedhrsadjust') }}

),

-- 4️⃣ Include processes reference
include_processes as (

    select *
    from {{ ref('ref_castle_includeprocesses') }}

)

select
    f.company,
    f.org,
    f.dj_nbr,
    f.operation_code,
    f.dj_quantity_completed,
    f.raw_earned_hrs,
    f.calc_earned_hrs,

    coalesce(m.manual_hrs, f.calc_earned_hrs)   as earned_hrs,

    (m.manual_hrs is not null)                  as manual_adjust_flag,

    (
        f.operation_code = 'EXT'
        or (f.operation_code = 'WJC' and f.org = 'ENT')
        or (f.operation_code = 'HWK' and f.org = 'MXM')
        or (f.operation_code = 'SHT' and f.org = 'MXM')
        or (f.operation_code = 'CHF' and f.org = 'CHA')
    )                                           as calc_adjust_flag,

    ip.include                                  as include_flag

from formula_adjusted f

left join manual_adjustments m
    on  f.dj_nbr    = m.dj_number
    and f.operation_code = m.op_code

left join include_processes ip
    on  f.org            = ip.org
    and f.operation_code = ip.process
