{{ config(materialized='table') }}

with src as (

    select * from {{ ref('base_castle__inventory') }}

),

inventory_rows as (

    select

        'castle'                            as company,

        /* =======================
           IDENTIFIERS
           ======================= */
        inv_org_code,
        item_nbr,
        sub_inv_code,
        lot_nbr,

        /* =======================
           PRODUCT ATTRIBUTES
           ======================= */
        product_primary_item_nbr,
        item_desc,
        product_commodity,
        product_shape,
        product_form,
        product_grade,
        product_temper,
        product_amc_container_spec,
        product_primary_dimension,
        product_length,
        product_width,
        product_item_type,
        product_stocking_uom,

        /* =======================
           ASSEMBLY ITEM PARSING
           Assembly items carry product_primary_item_nbr in the format:
               so_nbr * so_line * shipment_nbr [* ignore_segment]
           Standard component items contain no * and resolve to null.
           ======================= */
        position('*' in product_primary_item_nbr) > 0          as is_assembly,

        case
            when position('*' in product_primary_item_nbr) > 0
            then split_part(product_primary_item_nbr, '*', 1)
        end                                                     as assembly_so_nbr,

        case
            when position('*' in product_primary_item_nbr) > 0
            then split_part(product_primary_item_nbr, '*', 2)
        end                                                     as assembly_so_line,

        case
            when position('*' in product_primary_item_nbr) > 0
            then split_part(product_primary_item_nbr, '*', 3)
        end                                                     as assembly_shipment_nbr,

        /* =======================
           ON HAND QUANTITIES
           ======================= */
        on_hand_units,
        on_hand_lbs,
        on_hand_usd,
        on_hand_material_usd,

        /* =======================
           LOT DETAILS
           ======================= */
        lot_length,
        lot_width,
        heat_nbr,
        lot_creation_date,
        lot_aging_in_days,
        lot_aging_category,
        branch_aging_days,
        company_aging_days,

        /* =======================
           ORIGIN / PROVENANCE
           ======================= */
        company_origin_date,
        company_origin_lot,
        company_origin_org,
        branch_origin_date,
        branch_origin_lot,

        /* =======================
           LOCATION & STORAGE
           ======================= */
        warehouse_locator,
        uom_code,
        uom,

        /* =======================
           PURCHASING
           ======================= */
        inv_source,
        inventory_type,
        po_number,
        supplier_name,
        supplier_number,
        supplier_site,

        /* =======================
           QUALITY
           ======================= */
        prime_or_odd,
        prime_or_odd_code,
        mill

    from src

)

select * from inventory_rows
