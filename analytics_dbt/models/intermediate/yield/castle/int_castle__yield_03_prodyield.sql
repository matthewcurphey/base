{{ config(materialized='table') }}

with expected_actual as (

    select
        *
    from {{ ref('int_castle__yield_02_expectedactualyield') }}

)

select
    ea.company as company,
    ea.org_code as org_code,

    ea.prod_number as prod_number,
    ea.prod_status as prod_status,

    ea.complete_date as complete_date,

    ea.so_nbr as so_nbr,
    ea.so_line as so_line,
    ea.so_shipment as so_shipment,

    ea.item as item,
    ea.form as form,
    ea.commodity as commodity,
    ea.grade as grade,
    ea.temper as temper,

    ea.picked_items as picked_items,

    ea.picked_lbs as picked_lbs,
    ea.picked_usd as picked_usd,
    
    ea.complete_lbs as complete_lbs,
    ea.complete_usd as complete_usd,
    
    ea.engineered_lbs as engineered_lbs,
    ea.engineered_usd as engineered_usd,

    ea.picked_lbs - ea.complete_lbs as yieldloss_lbs,
    ea.picked_usd - ea.complete_usd as yieldloss_usd,

    ea.picked_lbs - ea.engineered_lbs as yieldvar_lbs,
    ea.picked_usd - ea.engineered_usd as yieldvar_usd,

    ea.complete_lbs / nullif(ea.engineered_lbs,0) as engineered_yield,
    ea.complete_lbs / nullif(ea.picked_lbs, 0) as actual_yield





    
    
from expected_actual ea

   






