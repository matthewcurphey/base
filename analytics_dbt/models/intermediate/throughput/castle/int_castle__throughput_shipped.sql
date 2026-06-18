{{ config(materialized='table') }}

with src as (

    select *
    from {{ ref('int_foundation_castle__sales_salesorder') }}

),

cal445 as (

    select
        cast(date as date) as cal_date,
        month,
        year

    from {{ ref('ref_calendar445') }}

)

select
    src.company,
    src.inv_org_code                as org_code,
    src.branch_name,

    src.so_nbr,
    src.so_line,
    src.shipment_nbr,
    src.item_nbr,

    c.year,
    c.month,
    src.actual_ship_date            as ship_date,

    src.sales_type,
    src.product_source_type,

    src.product_form                as form,
    src.product_commodity           as commodity,
    src.product_grade               as grade,
    src.product_temper              as temper,

    src.weight_lbs                  as shipped_lbs,
    src.gross_weight_lbs            as shipped_gross_lbs

from src
inner join cal445 c
    on src.actual_ship_date = c.cal_date

where src.sales_type in ('Invoice', 'Shipped')
  and src.weight_lbs > 0
  and src.actual_ship_date >= '2024-01-01'
  and src.actual_ship_date <= current_date
