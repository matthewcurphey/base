{{ config(materialized='table') }}

-- Castle earned hours at DJ / operation grain with calendar date
-- Sources: DJ + PPS Receive/Ship (always included)
-- Mirrors output_earned_hrs_detail from productivity.py
-- Retains include_flag so consumers can filter to Include? = Y

with earned_hrs as (

    select *
    from {{ ref('int_foundation_castle__mfg_earnedhrs') }}

),

dj_dates as (

    select
        discrete_job_no,
        max(date_completed) as date_completed

    from {{ ref('int_foundation_stgcastledj_fxwpl') }}
    group by discrete_job_no

),

cal445 as (

    select
        cast(date as date) as cal_date,
        month,
        year

    from {{ ref('ref_calendar445') }}

),

pps as (

    select *
    from {{ ref('stg_castle__ppsrcvshp') }}

),

prodorder as (

    select
        dj_nbr,
        product_form,
        product_commodity,
        product_grade,
        product_item_number,
        comp_complete_lbs,
        job_status

    from {{ ref('int_foundation_castle__mfg_prodorder') }}

)

-- DJ earned hours
select
    e.org,
    c.year,
    c.month,
    d.date_completed,
    e.dj_nbr,
    e.operation_code,
    e.dj_quantity_completed,
    e.earned_hrs,
    e.include_flag,
    p.product_form,
    p.product_commodity,
    p.product_grade,
    p.product_item_number,
    p.comp_complete_lbs,
    p.job_status

from earned_hrs e

inner join dj_dates d
    on e.dj_nbr = d.discrete_job_no

inner join cal445 c
    on d.date_completed = c.cal_date

left join prodorder p
    on e.dj_nbr = p.dj_nbr

union all

-- PPS Receive/Ship earned hours (always included)
select
    p.org,
    c.year,
    c.month,
    p.date_value                    as date_completed,
    p.identifier                    as dj_nbr,
    p.operation_code,
    null                            as dj_quantity_completed,
    p.hrs_earned                    as earned_hrs,
    true                            as include_flag,
    null                            as product_form,
    null                            as product_commodity,
    null                            as product_grade,
    null                            as product_item_number,
    null                            as comp_complete_lbs,
    null                            as job_status

from pps p

inner join cal445 c
    on p.date_value = c.cal_date
