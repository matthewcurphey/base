{{ config(materialized='table') }}

with prodorder as (

    select
        company,
        org,
        dj_nbr,
        job_status,
        complete_date,
        so_nbr,
        so_line,
        so_shipment,
        product_form,
        product_commodity,
        product_grade,
        product_temper,
        product_item_number,
        comp_item,
        comp_complete_lbs

    from {{ ref('int_foundation_castle__mfg_prodorder') }}

),

expectedusage as (

    select
        dj_nbr,
        comp_expected_lbs

    from {{ ref('int_foundation_castle__mfg_expectedusage') }}

),

actualusage as (

    select
        dj_nbr,
        comp_issued_lbs,
        comp_issued_usd

    from {{ ref('int_foundation_castle__mfg_actualusage') }}

),

operations as (

    select
        dj_nbr,
        operation_codes,
        operation_names

    from {{ ref('int_castle_yield_routeattribution') }}

),

cal445 as (

    select
        cast(date as date) as cal_date,
        month,
        year

    from {{ ref('ref_calendar445') }}

)

select
    pr.company,
    pr.org                          as org_code,
    pr.dj_nbr                       as prod_number,
    pr.job_status                   as prod_status,
    pr.complete_date,
    c.year,
    c.month,

    pr.so_nbr,
    pr.so_line,
    pr.so_shipment,

    pr.product_form                 as form,
    pr.product_commodity            as commodity,
    pr.product_grade                as grade,
    pr.product_temper               as temper,
    pr.product_item_number          as item,
    pr.comp_item                    as picked_items,

    o.operation_codes,
    o.operation_names,

    ac.comp_issued_lbs                                                              as picked_lbs,
    ac.comp_issued_usd                                                              as picked_usd,

    pr.comp_complete_lbs                                                            as complete_lbs,
    (ac.comp_issued_usd / nullif(ac.comp_issued_lbs, 0)) * pr.comp_complete_lbs    as complete_usd,

    ex.comp_expected_lbs                                                            as engineered_lbs,
    (ac.comp_issued_usd / nullif(ac.comp_issued_lbs, 0)) * ex.comp_expected_lbs    as engineered_usd

from prodorder pr
left join expectedusage ex  on pr.dj_nbr = ex.dj_nbr
left join actualusage   ac  on pr.dj_nbr = ac.dj_nbr
left join operations    o   on pr.dj_nbr = o.dj_nbr
inner join cal445 c         on pr.complete_date = c.cal_date

where pr.job_status in ('Complete', 'Closed')
  and ac.comp_issued_lbs > 0
  and pr.comp_complete_lbs <> -99999
  and pr.complete_date >= '2024-01-01'
  and pr.complete_date <= current_date
