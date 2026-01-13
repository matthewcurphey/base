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
        quantity
    from src
    where
        reference = 'Production'
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
        sum(quantity)                    as complete_lbs

    from filtered_trans
    group by
        company,
        production_order_number,
        item_number
)

select *
from trans_rows
