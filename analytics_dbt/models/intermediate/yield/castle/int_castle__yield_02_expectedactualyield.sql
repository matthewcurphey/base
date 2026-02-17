{{ config(materialized='view') }}

with prod_expected_actual as (

    select
        *
    from {{ ref('int_castle__yield_01_prodexpectedactual') }}

)

select
    pea.company as company,
    pea.org as org,
    pea.dj_nbr as dj_nbr,
    pea.job_status as job_status,
    pea.complete_date as complete_date,
    pea.so_nbr as so_nbr,
    pea.so_line as so_line,
    pea.so_shipment as so_shipment,
    pea.product_form as product_form,
    pea.product_commodity as product_commodity,
    pea.product_grade as product_grade,
    pea.product_temper as product_temper,
    pea.product_item_number as product_item_number,
    pea.start_qty as start_qty,
    pea.complete_qty as complete_qty,
    pea.job_uom as job_uom,
    pea.comp_item_clean as comp_item_clean,
    (pea.comp_issued_usd / nullif(pea.comp_issued_lbs, 0)) * pea.comp_complete_lbs as comp_complete_usd,
    pea.comp_complete_lbs as comp_complete_lbs,
    (pea.comp_issued_usd / nullif(pea.comp_issued_lbs, 0)) * pea.comp_expected_lbs as comp_expected_usd,
    pea.comp_expected_lbs as comp_expected_lbs,
    pea.comp_issued_usd as comp_issued_usd,
    pea.comp_issued_lbs as comp_issued_lbs,
    pea.comp_expected_lbs / nullif(pea.comp_issued_lbs, 0) as engineered_yield,
    pea.comp_complete_lbs / nullif(pea.comp_issued_lbs, 0) as actual_yield

from prod_expected_actual pea

