{{ config(materialized='table') }}

-- Castle earned hours aggregated to org / year / month grain
-- Sources: DJ (include_flag-filtered) + PPS Receive/Ship (always included)
-- Year / month resolved via 445 calendar join on date_completed / date_value

with earned_hrs as (

    select *
    from {{ ref('int_foundation_castle__mfg_earnedhrs') }}

),

-- 1️⃣ Collapse DJ rows to a single date_completed per discrete job
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

-- 2️⃣ Join DJ earned hours to dates, apply include filter, get 445 year/month
dj_joined as (

    select
        e.org,
        c.year,
        c.month,
        e.earned_hrs

    from earned_hrs e

    inner join dj_dates d
        on e.dj_nbr = d.discrete_job_no

    inner join cal445 c
        on d.date_completed = c.cal_date

    where e.include_flag = true

),

-- 3️⃣ Join PPS Receive/Ship earned hours to 445 calendar (always included)
pps_joined as (

    select
        p.org,
        c.year,
        c.month,
        p.hrs_earned as earned_hrs

    from pps p

    inner join cal445 c
        on p.date_value = c.cal_date

),

all_earned as (

    select * from dj_joined
    union all
    select * from pps_joined

)

-- 4️⃣ Roll up to org / year / month
select
    org,
    year,
    month,
    sum(earned_hrs) as earned_hrs

from all_earned
group by
    org,
    year,
    month
