{{ config(materialized='table') }}

select

    item_clean as item,
    short_org,
    lines_short,
    total_short_qty as qty_short,
    unit_of_measure as uom,
    donor_org,
    donor_available_qty as donor_avail_qty,
    qty_needed,
    lines_coverage_pct/100 as lines_cover_pct,
    full_lines_cover,
    donor_overcommitted

from {{ ref('int_oracle__mcmaster_exception_cross_ship') }}
