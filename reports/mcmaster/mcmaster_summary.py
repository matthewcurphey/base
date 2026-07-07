import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

CHART_PATH = os.path.join("reports", "mcmaster", "backlog_trend.png")

# McMaster's 6-org sphere — consistent with every other mcmaster model/mart.
# CHA/PHI/STO have had zero McMaster activity for 7+ months (not dormant this
# week specifically, just not part of this business) and would only add rows
# of permanent zeros to an exec-facing table.
SPHERE_ORGS = ["ATL", "CLE", "DAL", "JVL", "LOS", "WIE"]

# Palette slots from the dataviz skill's reference palette (validated set),
# not arbitrary picks. Red/green here also carry a positional redundancy —
# new sits above the zero line, shipped sits below — so identity doesn't
# depend on color discrimination alone. Backlog is neutral (secondary ink),
# not a categorical hue — it's the scale/context the red/green signal reads
# against, not a series competing for identity.
COLOR_BACKLOG = "#52514e"   # secondary ink (neutral grey)
COLOR_NEW = "#e34948"       # categorical slot 6 (red)
COLOR_SHIPPED = "#008300"   # categorical slot 4 (green)
COLOR_AXIS = "#c3c2b7"
COLOR_GRID = "#e1e0d9"
COLOR_TEXT_SECONDARY = "#52514e"


def build_trend_chart(backlog_daily_df, days=90, output_path=CHART_PATH):
    """
    90-day (default) company-wide trend: daily backlog level as bars, and
    daily new/shipped orders as a thin area straddling zero on the SAME
    axis — new above zero, shipped below (plotted as negative so it renders
    underneath the line). Raw daily values, no smoothing: "this is what
    actually happened," weekend zeros included as-is.
    """
    df = backlog_daily_df[backlog_daily_df["inv_org_code"].isin(SPHERE_ORGS)]

    daily_total = (
        df.groupby("dt", as_index=False)[["open_orders", "new_orders", "shipped_orders"]]
        .sum()
        .sort_values("dt")
    )
    daily_total = daily_total.tail(days)

    fig, ax = plt.subplots(figsize=(14, 5), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    ax.bar(
        daily_total["dt"], daily_total["open_orders"],
        color=COLOR_BACKLOG, width=0.9, label="Backlog (open orders)", zorder=2,
    )
    ax.fill_between(
        daily_total["dt"], 0, daily_total["new_orders"],
        color=COLOR_NEW, alpha=0.85, linewidth=0, label="New orders", zorder=3,
    )
    ax.fill_between(
        daily_total["dt"], 0, -daily_total["shipped_orders"],
        color=COLOR_SHIPPED, alpha=0.85, linewidth=0, label="Shipped", zorder=3,
    )

    ax.axhline(0, color=COLOR_AXIS, linewidth=1)
    ax.grid(axis="y", color=COLOR_GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)

    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color(COLOR_AXIS)

    ax.tick_params(colors=COLOR_TEXT_SECONDARY, labelsize=9)
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO, interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d-%b"))
    fig.autofmt_xdate(rotation=0, ha="center")

    fig.suptitle(
        f"McMaster Open Backlog — {days}-Day Trend",
        fontsize=13, color="#0b0b0b", x=0.01, ha="left", y=0.98,
    )
    handles, labels = ax.get_legend_handles_labels()
    order = [labels.index(name) for name in ("Backlog (open orders)", "New orders", "Shipped")]
    ax.legend(
        [handles[i] for i in order], [labels[i] for i in order],
        loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3,
        frameon=False, fontsize=9, labelcolor=COLOR_TEXT_SECONDARY,
    )

    fig.tight_layout(rect=(0, 0.04, 1, 0.93))
    fig.savefig(output_path, facecolor=fig.get_facecolor())
    plt.close(fig)

    return output_path
