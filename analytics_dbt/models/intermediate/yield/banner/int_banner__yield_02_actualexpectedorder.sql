{{ config(materialized='view') }}

with actual_expected_item as (
    select *
    from {{ ref('int_banner__yield_01_actualexpecteditem') }}
),

expected_order as (
    select *
    from {{ ref('int_foundation_banner__mfg_expectedusageorder') }}
)

select
    /* =======================
       IDENTIFIERS
       ======================= */
    aei.company,
    aei.production_order_number,
    aei.item_number,

    /* =======================
       ACTUAL USAGE
       ======================= */
    aei.picked_lbs,
    aei.picked_usd,

    /* =======================
       BOM / ENGINEERING CONTEXT
       ======================= */
    aei.expected_yield_item,
    eo.expected_yield as expected_yield_order,

    /* =======================
       FINAL EXPECTED 
       ======================= */
    coalesce(
        aei.expected_yield_item,
        eo.expected_yield,
        1
    ) as expected_yield,

    aei.picked_lbs
        * coalesce(
            aei.expected_yield_item,
            eo.expected_yield,
            1
          ) as expected_output_lbs



from actual_expected_item aei

left join expected_order eo
    on  aei.company = eo.company
    and aei.production_order_number = eo.production_order_number

