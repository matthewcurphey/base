{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('int_castle__mcmaster_01_sales_production') }}
    where sales_type = 'Order'

),

tallied as (

    select
        *,

        /* =====================================================
           RUNNING TALLY PER ORG PER ITEM
           Partitioned by item, ordered by promise date.
           Each org's tally only consumes from lines at that org —
           so every line shows all 6 org balances simultaneously.
           ROWS UNBOUNDED PRECEDING includes the current row,
           so a line that pushes the balance negative is itself short.
        ===================================================== */

        inv_atl - sum(case when inv_org_code = 'ATL' then coalesce(comp_inv_req, 0) else 0 end)
            over (partition by product_primary_item_nbr order by promise_date nulls last, so_nbr, so_line rows unbounded preceding)
            as tally_atl,

        inv_cle - sum(case when inv_org_code = 'CLE' then coalesce(comp_inv_req, 0) else 0 end)
            over (partition by product_primary_item_nbr order by promise_date nulls last, so_nbr, so_line rows unbounded preceding)
            as tally_cle,

        inv_dal - sum(case when inv_org_code = 'DAL' then coalesce(comp_inv_req, 0) else 0 end)
            over (partition by product_primary_item_nbr order by promise_date nulls last, so_nbr, so_line rows unbounded preceding)
            as tally_dal,

        inv_jvl - sum(case when inv_org_code = 'JVL' then coalesce(comp_inv_req, 0) else 0 end)
            over (partition by product_primary_item_nbr order by promise_date nulls last, so_nbr, so_line rows unbounded preceding)
            as tally_jvl,

        inv_los - sum(case when inv_org_code = 'LOS' then coalesce(comp_inv_req, 0) else 0 end)
            over (partition by product_primary_item_nbr order by promise_date nulls last, so_nbr, so_line rows unbounded preceding)
            as tally_los,

        inv_wie - sum(case when inv_org_code = 'WIE' then coalesce(comp_inv_req, 0) else 0 end)
            over (partition by product_primary_item_nbr order by promise_date nulls last, so_nbr, so_line rows unbounded preceding)
            as tally_wie

    from base

),

floored as (

    select
        *,

        /* =====================================================
           FLOOR PER ORG PER ITEM
           Lowest point each org's tally reaches across all demand
           for this item. Positive floor = true surplus after all
           committed demand is satisfied — safe to cross-ship.
        ===================================================== */

        min(tally_atl) over (partition by product_primary_item_nbr)     as floor_atl,
        min(tally_cle) over (partition by product_primary_item_nbr)     as floor_cle,
        min(tally_dal) over (partition by product_primary_item_nbr)     as floor_dal,
        min(tally_jvl) over (partition by product_primary_item_nbr)     as floor_jvl,
        min(tally_los) over (partition by product_primary_item_nbr)     as floor_los,
        min(tally_wie) over (partition by product_primary_item_nbr)     as floor_wie,

        /* =====================================================
           HOME ORG CONVENIENCE COLUMNS
        ===================================================== */

        case inv_org_code
            when 'ATL' then tally_atl
            when 'CLE' then tally_cle
            when 'DAL' then tally_dal
            when 'JVL' then tally_jvl
            when 'LOS' then tally_los
            when 'WIE' then tally_wie
        end                                                             as tally_home_org,

        case inv_org_code
            when 'ATL' then tally_atl < 0 and comp_inv_req > 0
            when 'CLE' then tally_cle < 0 and comp_inv_req > 0
            when 'DAL' then tally_dal < 0 and comp_inv_req > 0
            when 'JVL' then tally_jvl < 0 and comp_inv_req > 0
            when 'LOS' then tally_los < 0 and comp_inv_req > 0
            when 'WIE' then tally_wie < 0 and comp_inv_req > 0
        end                                                             as is_short

    from tallied

)

select *
from floored
