{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('base_banner__inventorytransactions') }}
),

staged as (

    select
        -- Natural keys
        company::text                                 as company,
        record_id::text                               as record_id,

        -- Inventory identifiers
        item_number::text                             as item_number,
        invoice::text                                 as invoice,
        financial_voucher::text                       as financial_voucher,
        packing_slip::text                            as packing_slip,
        physical_voucher::text                        as physical_voucher,

        -- Status fields
        receipt_status::text                          as receipt_status,
        issue_status::text                            as issue_status,

        -- Reference + lot info
        return_lot_id::text                           as return_lot_id,
        reference::text                               as reference,
        reference_lot::text                           as reference_lot,
        ref_id::text                                  as ref_id,

        lot_id::text                                  as lot_id,
        lot_id2::text                                 as lot_id2,
        dimension_number::text                        as dimension_number,
        dimension_number2::text                       as dimension_number2,

        -- Quantities
        quantity::numeric(18,4)                       as quantity,

        -- Dates
        physical_date::date                           as physical_date,
        financial_date::date                          as financial_date,
        financially_closed::date                      as financially_closed,

        -- Costs (monetary)
        financial_cost_amount::numeric(18,2)          as financial_cost_amount,
        physical_cost_amount::numeric(18,2)           as physical_cost_amount,
        profit_loss_posted_amount::numeric(18,2)      as profit_loss_posted_amount,
        adjustment::numeric(18,2)                     as adjustment,

        -- Location info
        site::text                                    as site,
        warehouse::text                               as warehouse,

        -- Dimension table attributes
        grade::text                                   as grade,
        batch_number::text                            as batch_number

    from base
)

select * from staged
