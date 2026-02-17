{{ config(materialized='view') }}

with pr as (

    select
        -- only columns needed for joins or final outputs
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
        start_qty,
        complete_qty,
        job_uom,
        comp_item_clean,
        comp_complete_lbs

    from {{ ref('int_foundation_castle__mfg_prodorder') }}

),

ex as (

    select
        dj_nbr,
        comp_expected_lbs

    from {{ ref('int_foundation_castle__mfg_expectedusage') }}

),

pr_ex as (

    select
        pr.company,
        pr.org,
        pr.dj_nbr,
        pr.job_status,
        pr.complete_date,
        pr.so_nbr,
        pr.so_line,
        pr.so_shipment,
        pr.product_form,
        pr.product_commodity,
        pr.product_grade,
        pr.product_temper,
        pr.product_item_number,
        pr.start_qty,
        pr.complete_qty,
        pr.job_uom,
        pr.comp_item_clean,
        pr.comp_complete_lbs,
        ex.comp_expected_lbs

    from pr
    left join ex
        on pr.dj_nbr = ex.dj_nbr

),

ac as (

    select
        dj_nbr,
        comp_issued_usd,
        comp_issued_lbs

    from {{ ref('int_foundation_castle__mfg_actualusage') }}

)

select
    -- ✅ FINAL schema definition lives here
        pe.company as company,
        pe.org as org,
        pe.dj_nbr as dj_nbr,
        pe.job_status as job_status,
        pe.complete_date as complete_date,
        pe.so_nbr as so_nbr,
        pe.so_line as so_line,
        pe.so_shipment as so_shipment,
        pe.product_form as product_form,
        pe.product_commodity as product_commodity,
        pe.product_grade as product_grade,
        pe.product_temper as product_temper,
        pe.product_item_number as product_item_number,
        pe.start_qty as start_qty,
        pe.complete_qty as complete_qty,
        pe.job_uom as job_uom,
        pe.comp_item_clean as comp_item_clean,
        pe.comp_complete_lbs as comp_complete_lbs,
        pe.comp_expected_lbs as comp_expected_lbs,
        ac.comp_issued_usd as comp_issued_usd,
        ac.comp_issued_lbs as comp_issued_lbs

from pr_ex pe
left join ac
    on pe.dj_nbr = ac.dj_nbr