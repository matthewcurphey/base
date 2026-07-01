{{ config(materialized='table') }}

with inventory as (

    select * from {{ ref('stg_castle_oracle__inventory') }}

),

di_uom as (

    -- One row per item; UOM is consistent across orgs so min() is safe.
    -- Pre-aggregating avoids a large fan-out join and handles items that
    -- exist in DI under a different org than the inventory record.
    select
        item_number,
        min(unit_of_measure) as unit_of_measure
    from {{ ref('stg_castle_oracle__di') }}
    group by item_number

),

joined as (

    select

        /* =======================
           IDENTIFIERS
           ======================= */
        inv.org,
        inv.org_name,
        inv.item,
        regexp_replace(inv.item, '(_1(-P)?(\.(BO|CO|MO|MC))?|-P(\.(BO|CO|MO|MC))?|\.(BO|CO|MO|MC))$', '')        as item_clean,

        /* =======================
           ASSEMBLY ITEM PARSING
           Assembly items carry item in format: so_nbr*so_line*shipment_nbr
           Material cut for a specific McMaster order and staged as WIP.
           Standard items contain no * and resolve to null.
           ======================= */
        position('*' in inv.item) > 0                                                                               as is_assembly,

        case
            when position('*' in inv.item) > 0
            then split_part(inv.item, '*', 1)
        end                                                                                                         as assembly_so_nbr,

        case
            when position('*' in inv.item) > 0
            then split_part(inv.item, '*', 2)
        end                                                                                                         as assembly_so_line,

        case
            when position('*' in inv.item) > 0
            then split_part(inv.item, '*', 3)
        end                                                                                                         as assembly_shipment_nbr,


        /* =======================
           PRODUCT ATTRIBUTES
           ======================= */
        inv.form,
        inv.alloy,
        inv.temper,
        inv.spec,
        inv.thk,
        inv.item_desc,

        /* =======================
           LOT DETAILS
           ======================= */
        inv.lot_nbr,
        inv.heat_nbr,
        inv.lot_length,
        inv.lot_width,

        /* =======================
           LOCATION
           ======================= */
        inv.sub_inv,
        inv.locator,

        /* =======================
           ON HAND QUANTITIES
           ======================= */
        inv.on_hand_lbs,
        inv.on_hand_usd,

        /* =======================
           UOM
           Joined from DI on clean item only (strips .BO/.CO suffixes).
           UOM is item-level not org-level, so the pre-aggregated lookup
           covers cross-org misses automatically.
           ======================= */
        di_uom.unit_of_measure,

        /* =======================
           ON HAND IN NATIVE UOM
           on_hand_pcs stores total linear inches for length-based items.
           ======================= */
        case di_uom.unit_of_measure
            when 'PCS'    then round(inv.on_hand_pcs / nullif(inv.lot_length, 0), 0)
            when 'FOOT'   then inv.on_hand_pcs / 12.0
            when 'INCH'   then inv.on_hand_pcs
            when 'KGS'    then inv.on_hand_weight_kg
            when 'POUNDS' then inv.on_hand_lbs
            else null
        end                                     as on_hand_uom_qty,

        /* =======================
           ADDITIONAL ATTRIBUTES
           ======================= */
        inv.mill,
        inv.amc_age

    from inventory inv
    left join di_uom
        on regexp_replace(inv.item, '(_1(-P)?(\.(BO|CO|MO|MC))?|-P(\.(BO|CO|MO|MC))?|\.(BO|CO|MO|MC))$', '') = di_uom.item_number

)

select * from joined
