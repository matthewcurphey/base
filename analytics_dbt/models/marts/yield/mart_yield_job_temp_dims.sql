{{ config(materialized='view') }}

-- TEMPORARY: bolt-on view adding best-guess DJ item_type/dimensions and
-- matching sales dimensions onto mart_yield_job, without touching that
-- mart itself. Scoped to what we actually need (Castle, US, yield-loss
-- op = APS) to keep this cheap. Join branches on the DJ's own item_type:
-- AI ties to sales via so_nbr/so_line, FG ties to sales via item number.
-- Each lookup is pre-collapsed to a unique key, so this can't fan out
-- mart_yield_job's row count.

with job as (

    select *
    from {{ ref('mart_yield_job') }}
    where company = 'Castle'
      and country = 'US'
      and yield_op_id = 'APS'

),

dj_dims as (
    select * from {{ ref('int_castle__yield_temp_dj_dims') }}
),

sales_by_soline as (
    select * from {{ ref('int_castle__yield_temp_sales_by_soline') }}
),

sales_by_item as (
    select * from {{ ref('int_castle__yield_temp_sales_by_item') }}
),

inventory_by_item as (
    select * from {{ ref('int_castle__yield_temp_inventory_by_item') }}
)

select
    j.*,

    dj.item_type                  as dj_item_type,
    dj.product_length             as dj_product_length,
    dj.product_width              as dj_product_width,

    -- Actual cut/finished piece size. AI: sales' own cut dimension. FG:
    -- the item's product dimension IS the cut size - FG has no separate
    -- "cut from something bigger" step the way AI does.
    case dj.item_type
        when 'AI' then soline.cut_length
        when 'FG' then item.product_length
    end                            as cut_length,

    case dj.item_type
        when 'AI' then soline.cut_width
        when 'FG' then item.product_width
    end                            as cut_width,

    -- Raw/parent stock dimension (the material the cut piece came from).
    -- AI: sales' own product dimension. FG: the DJ's own component item,
    -- looked up against inventory's item master (95.5% coverage).
    case dj.item_type
        when 'AI' then soline.product_length
        when 'FG' then inv.product_length
    end                            as component_length,

    case dj.item_type
        when 'AI' then soline.product_width
        when 'FG' then inv.product_width
    end                            as component_width,

    -- Purchased/raw-material item. Same field for both paths in practice -
    -- for AI, the DJ's own item/product_item_number is actually an
    -- assembly pseudo-code (so_nbr*so_line*shipment), not a real item;
    -- the real raw item lives in comp_item_clean for AI just like FG
    -- (confirmed it matches the sales line's own item number, modulo a
    -- ".MO" suffix sales carries that the DJ side doesn't).
    dj.comp_item_clean            as purchased_item,

    -- Completed quantity from the DJ, with its UOM alongside rather than
    -- assumed - 21,559/21,561 APS rows are PCS, but 2 are LBS.
    dj.complete_qty                as dj_complete_qty,
    dj.job_uom                     as dj_job_uom

from job j
left join dj_dims dj
    on j.prod_number = dj.dj_nbr
left join sales_by_soline soline
    on  dj.item_type = 'AI'
    and dj.so_nbr    = soline.so_nbr
    and dj.so_line   = soline.so_line
left join sales_by_item item
    on  dj.item_type = 'FG'
    and dj.product_item_number = item.item_nbr
left join inventory_by_item inv
    on  dj.item_type = 'FG'
    and dj.comp_item_clean = inv.item_nbr
