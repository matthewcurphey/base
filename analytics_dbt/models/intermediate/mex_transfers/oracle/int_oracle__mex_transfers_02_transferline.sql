{{ config(materialized='table') }}

/*
    Transfer lines into the Mexico branches (so/line/ship grain), with exact
    receipt status from int_oracle__mex_transfers_01_deliveries.

    Join back to the TRANSFER report is on so_nbr + item_nbr + ship date (the
    Oracle shipping transaction date equals the TRANSFER ship date). Lines are
    only tracked where their shipping branch has a transfer_orders_<ORG> export
    covering the ship date - anything else is 'Not tracked', never guessed.
*/

with orgs as (
    select distinct org_name, org_code
    from {{ ref('ref_orginfo') }}
),

deliveries as (
    select * from {{ ref('int_oracle__mex_transfers_01_deliveries') }}
),

-- shipping branches with an export, and how far back each export goes
export_coverage as (
    select
        shipping_org_code,
        min(shipped_date)                                   as tracked_from_date
    from {{ ref('stg_castle_oracle__transfer_shipments') }}
    group by 1
),

transfers as (
    select
        tl.*,
        -- the TRANSFER report spells two shipping branches differently from ref_orginfo
        coalesce(
            f.org_code,
            case
                when tl.ship_from_branch like 'Wichita%'       then 'WIE'
                when tl.ship_from_branch like 'HA Industries%' then 'HAI'
            end
        )                                                   as ship_from_org_code,
        d.org_code                                          as ship_to_org_code
    from {{ ref('int_foundation_castle__transfers_transferline') }} tl
    left join orgs f on f.org_name = tl.ship_from_branch
    left join orgs d on d.org_name = tl.ship_to_branch
),

-- a line can ship on more than one delivery on the same day
line_deliveries as (
    select
        t.so_nbr,
        t.so_line,
        t.shipment_nbr,
        min(dl.receiving_org_code)                          as receiving_org_code,
        string_agg(distinct dl.delivery_nbr, ', ')          as delivery_nbrs,
        min(dl.requisition_nbr)                             as requisition_nbr,
        count(*)                                            as delivery_count,
        count(*) filter (where dl.delivery_status = 'Received')           as deliveries_received,
        count(*) filter (where dl.delivery_status = 'Partially received') as deliveries_partial,
        min(dl.first_received_date)                         as first_received_date,
        max(dl.last_received_date)                          as last_received_date,
        bool_or(dl.delivery_order_count > 1)                as is_shared_delivery,
        min(dl.waybill)                                     as waybill
    from transfers t
    join deliveries dl
        on  dl.so_nbr       = t.so_nbr
        and dl.item_nbr     = t.item_nbr
        and dl.shipped_date = t.ship_date
    group by 1, 2, 3
),

final as (
    select
        /* =======================
           TRANSFER LINE
           ======================= */
        t.company,
        t.so_nbr,
        t.so_line,
        t.shipment_nbr,
        t.item_nbr,
        t.product_item_description,
        t.product_form,
        t.product_grade,
        t.weight_lbs,
        t.ordered_qty,
        t.ordered_uom,

        /* =======================
           LANE
           Receiving branch is blank on some TRANSFER rows - the Oracle
           shipment's transfer org fills it.
           ======================= */
        t.ship_from_branch,
        t.ship_from_org_code,
        coalesce(t.ship_to_org_code, ld.receiving_org_code) as ship_to_org_code,
        t.sales_status,
        t.sales_type,

        /* =======================
           DATES
           ======================= */
        t.requisition_date,
        t.need_by_date,
        t.promise_date,
        t.ship_date,
        ld.first_received_date                              as received_date,
        ld.last_received_date,

        /* =======================
           ORACLE REFERENCES
           ======================= */
        ld.delivery_nbrs,
        ld.requisition_nbr,
        ld.waybill,
        coalesce(ld.is_shared_delivery, false)              as is_shared_delivery,

        /* =======================
           STATUS & DURATIONS (days)
           ======================= */
        case
            when t.sales_status = 'Cancelled'                                   then 'Cancelled'
            when t.ship_date is null                                            then 'Open'
            when ec.tracked_from_date is null
              or t.ship_date < ec.tracked_from_date                             then 'Not tracked'
            when ld.so_nbr is null                                              then 'Not tracked'
            when ld.deliveries_received = ld.delivery_count                     then 'Received'
            when ld.deliveries_received + ld.deliveries_partial > 0             then 'Partially received'
            else 'In transit'
        end                                                 as transfer_status,
        ld.first_received_date - t.ship_date                as days_in_transit,
        case
            when t.ship_date is not null and ld.so_nbr is not null and ld.first_received_date is null
                then current_date - t.ship_date
        end                                                 as days_in_transit_open,
        ld.first_received_date - t.requisition_date         as days_requisition_to_received,
        ld.first_received_date - t.need_by_date             as days_received_vs_need_by

    from transfers t
    left join line_deliveries ld
        on  ld.so_nbr       = t.so_nbr
        and ld.so_line      = t.so_line
        and ld.shipment_nbr = t.shipment_nbr
    left join export_coverage ec
        on  ec.shipping_org_code = t.ship_from_org_code
)

select *
from final
where ship_to_org_code in ('MXM', 'MXQ', 'MTY', 'MCH')
