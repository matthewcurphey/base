{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_banner__inventorytransactions') }}
),

-- 1) Filter to valid production picks only
filtered_trans as (
    select
        company,
        ref_id               as production_order_number,
        item_number,
        quantity,
        financial_cost_amount,
        physical_cost_amount
    from src
    where
        -- company-specific validity rule
        grade is not null
        and trim(grade) <> ''

        -- Optional: ensure this is actually production-related
        and ref_id is not null
        and trim(ref_id) <> ''
),

-- 2) Roll up to production order × item
trans_rows as (
    select
        /* =======================
           IDENTIFIERS & GRAIN
           ======================= */
        company,
        production_order_number,
        item_number,

        /* =======================
           PICKED QUANTITIES
           (sign-normalized)
           ======================= */
        sum(-quantity)                    as picked_quantity,

        /* =======================
           COSTS
           ======================= */
        sum(-financial_cost_amount)       as picked_financial_cost,
        sum(-physical_cost_amount)        as picked_physical_cost

    from filtered_trans
    group by
        company,
        production_order_number,
        item_number
)

select *
from trans_rows
