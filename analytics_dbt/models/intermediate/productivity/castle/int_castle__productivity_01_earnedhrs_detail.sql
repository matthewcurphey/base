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
    e.include_flag

from earned_hrs e

inner join dj_dates d
    on e.dj_nbr = d.discrete_job_no

inner join cal445 c
    on d.date_completed = c.cal_date

union all

-- PPS Receive/Ship earned hours (always included)
select
    p.org,
    c.year,
    c.month,
    p.date_value                    as date_completed,
    p.identifier                            as dj_nbr,
    p.operation_code,
    null                            as dj_quantity_completed,
    p.hrs_earned                    as earned_hrs,
    true                            as include_flag

from pps p

inner join cal445 c
    on p.date_value = c.cal_date
