{{ config(materialized='table') }}

select

    item_clean as item,
    uom,
    total_lines_short as total_lines,
    total_qty_short as total_qty,
    lines_short_atl as atl_lines,
    lines_short_cle as cle_lines,
    lines_short_dal as dal_lines,
    lines_short_jvl as jvl_lines,
    lines_short_los as los_lines,
    lines_short_wie as wie_lines,
    po_details,

    avail_aaa as aaa,
    avail_adc as adc,
    avail_asc as asc,
    avail_cha as cha,
    avail_con as con,
    avail_ena as ena,
    avail_ent as ent,
    avail_hai as hai,
    avail_mch as mch,
    avail_mty as mty,
    avail_mxm as mxm,
    avail_mxq as mxq,
    avail_sgp as sgp,
    avail_sto as sto,
    avail_tor as tor,
    avail_win as win

from {{ ref('int_oracle__mcmaster_exception_hot_components') }}
