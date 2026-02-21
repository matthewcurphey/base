{{ config(materialized='table') }}

with prod_expected_actual as (

    select
        *
    from {{ ref('int_castle__yield_01_prodexpectedactual') }}

)

select
    pea.company as company,
    pea.org as org_code,

    pea.dj_nbr as prod_number,
    pea.job_status as prod_status,

    pea.complete_date as complete_date,

    pea.so_nbr as so_nbr,
    pea.so_line as so_line,
    pea.so_shipment as so_shipment,

    pea.product_form as form,
    pea.product_commodity as commodity,
    pea.product_grade as grade,
    pea.product_temper as temper,
    pea.product_item_number as item,

    pea.comp_item as picked_items,

    pea.comp_issued_lbs as picked_lbs,
    pea.comp_issued_usd as picked_usd,
    
    pea.comp_complete_lbs as complete_lbs,
    (pea.comp_issued_usd / nullif(pea.comp_issued_lbs, 0)) * pea.comp_complete_lbs as complete_usd,
    
    pea.comp_expected_lbs as engineered_lbs,
    (pea.comp_issued_usd / nullif(pea.comp_issued_lbs, 0)) * pea.comp_expected_lbs as engineered_usd
    
from prod_expected_actual pea

