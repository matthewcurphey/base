import json
import os
import re
import shutil
from collections import Counter
from datetime import date

import openpyxl
import pandas as pd
from openpyxl.styles import Border, Font, NamedStyle, PatternFill, Side
from openpyxl.styles.colors import Color

from etl.utils.connect_postgres import get_postgres_connection
from reports.mcmaster.mcmaster_summary import update_live_summary_sheet

TEMPLATE_PATH = os.path.join("reports", "mcmaster", "mcmaster_template.xlsx")
OUTPUT_PATH = os.path.join("reports", "mcmaster", "mcmaster_report.xlsx")
ARCHIVE_DIR = os.path.join("reports", "mcmaster", "archive")
SHAREPOINT_DIR = r"C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\mcmaster"

# sheet name -> (source mart, header row). First data row is always header_row + 1.
SHEETS = {
    "open_backlog_detail": ("mart_mcmaster__open_backlog", 2),
    "cross_ship": ("mart_mcmaster__cross_ship", 2),
    "hot_components": ("mart_mcmaster__hot_components", 2),
    "to_cancel": ("mart_mcmaster__to_cancel", 1),
    "dj_review": ("mart_mcmaster__dj_review", 2),
}

# sheet name -> (columns, ascending flags). Applied in pandas, not SQL — a
# mart's own ORDER BY doesn't survive a later "SELECT * FROM mart" with no
# ORDER BY of its own, and sorting here means it still holds even if the
# dataframe gets trimmed/reshaped after loading.
SORT_KEYS = {
    "open_backlog_detail": (["prom_dt", "so_nbr", "line"], [True, True, True]),
    "cross_ship": (["full_lines_cover", "donor_overcommitted", "lines_cover_pct"], [False, True, False]),
    "hot_components": (["total_lines"], [False]),
    "to_cancel": (["org", "so_nbr", "line", "shp"], [True, True, True, True]),
    "dj_review": (["so_nbr", "line", "shp"], [True, True, True]),
}

_PURE_INT_RE = re.compile(r"-?\d+")


def _stringify_json_columns(df):
    """
    jsonb columns (e.g. hot_components.po_details) come back from Postgres
    as native Python list/dict objects — openpyxl can't write those into a
    cell directly. Convert any column containing list/dict values to a JSON
    string so it lands as one readable cell instead of erroring.
    """
    for col in df.columns:
        s = df[col]
        is_json_like = s.map(lambda v: isinstance(v, (list, dict)))
        if not is_json_like.any():
            continue
        df.loc[is_json_like, col] = s[is_json_like].map(json.dumps)
    return df


def _coerce_numeric_text(df):
    """
    Postgres stores identifiers like item/dj/so_nbr as text on purpose, but
    Excel flags any text cell that's a valid integer with the "Number
    Stored as Text" warning. Convert those to real ints so the output reads
    clean like the template does — but only per-column, and only when every
    digit-like value in that column round-trips through int() unchanged.
    Some columns (cust_po, cust_item, assy_lots) rely on leading zeros;
    converting those would silently corrupt them, so those columns are
    left as text and keep the warning.
    """
    for col in df.columns:
        s = df[col]
        not_na = s[s.notna()]
        not_na = not_na[not_na.map(lambda v: isinstance(v, str))]
        if not_na.empty:
            # Nothing to coerce — column is purely numeric/bool/etc already,
            # or entirely null. Empty object Series breaks .str (pandas
            # can't infer string dtype with no values to look at).
            continue
        digit_like = not_na.str.fullmatch(_PURE_INT_RE)
        if not digit_like.any():
            continue
        candidates = not_na[digit_like]
        if not (candidates.apply(lambda v: str(int(v)) == v)).all():
            continue
        df.loc[candidates.index, col] = candidates.astype(int)
    return df


def _color_key(color):
    # Color.rgb is only meaningful when type == 'rgb' — reading it off a
    # theme/indexed/auto color returns a descriptor validation placeholder,
    # not a usable value. Theme colors are common here (e.g. the default
    # text color is typically theme index 1), so this isn't an edge case.
    if color is None:
        return None
    return (color.type, getattr(color, color.type), color.tint)


def _color_from_key(key):
    if key is None:
        return None
    color_type, value, tint = key
    return Color(type=color_type, tint=tint, **{color_type: value})


def _border_key(border):
    return tuple(
        (side.style, _color_key(side.color))
        for side in (border.left, border.right, border.top, border.bottom)
    )


def _font_key(font):
    return (font.bold, font.italic, font.name, font.size, _color_key(font.color))


def _fill_key(fill):
    return (fill.patternType, _color_key(fill.fgColor))


def _font_from_key(key):
    bold, italic, name, size, color_key = key
    return Font(bold=bold, italic=italic, name=name, size=size, color=_color_from_key(color_key))


def _border_from_key(key):
    sides = [Side(style=style, color=_color_from_key(color_key)) if style else Side() for style, color_key in key]
    left, right, top, bottom = sides
    return Border(left=left, right=right, top=top, bottom=bottom)


def _fill_from_key(key):
    pattern_type, color_key = key
    if pattern_type is None:
        return PatternFill()
    return PatternFill(patternType=pattern_type, fgColor=_color_from_key(color_key))


def _column_styles(ws, first_row, last_row, n_cols):
    """
    Most common font/border/fill/number_format per column across the
    template's existing rows. The template's sample data has been hand-built
    and re-sorted over time, so a handful of rows are inconsistently
    formatted — taking the majority per column recovers the intended design
    without trusting any single row to be clean.

    cell.font/.border/.fill return a StyleProxy, not a plain Font/Border/Fill,
    so we can't reuse them directly (NamedStyle rejects the type) and the
    generic way to detach one — copy.copy() — round-trips through XML and is
    far too slow to call per cell. Instead we read out the plain attribute
    values for grouping and rebuild fresh style objects from the winning key.
    """
    counters = [Counter() for _ in range(n_cols)]
    number_formats = [Counter() for _ in range(n_cols)]

    for row in ws.iter_rows(min_row=first_row, max_row=last_row, max_col=n_cols):
        for cell in row:
            col_idx = cell.column - 1
            key = (_font_key(cell.font), _border_key(cell.border), _fill_key(cell.fill))
            counters[col_idx][key] += 1
            number_formats[col_idx][cell.number_format] += 1

    styles = []
    for col_idx in range(n_cols):
        if not counters[col_idx]:
            styles.append((Font(), Border(), PatternFill(), "General"))
            continue
        font_key, border_key, fill_key = counters[col_idx].most_common(1)[0][0]
        number_format = number_formats[col_idx].most_common(1)[0][0]
        styles.append((_font_from_key(font_key), _border_from_key(border_key), _fill_from_key(fill_key), number_format))

    return styles


def _write_sheet(wb, sheet_name, header_row, df):
    """
    Paste df into wb[sheet_name] starting at header_row + 1, preserving the
    sheet's existing formatting (majority-derived per column, explicitly
    reapplied — see _column_styles) regardless of how many rows df has this
    run versus last run.
    """
    first_data_row = header_row + 1
    ws = wb[sheet_name]

    template_headers = [ws.cell(row=header_row, column=c).value for c in range(1, ws.max_column + 1)]
    while template_headers and template_headers[-1] is None:
        # Excel can inflate the sheet's used range with stray formatting on
        # blank trailing columns (e.g. a column width tweak) — harmless, ignore.
        template_headers.pop()
    mart_headers = list(df.columns)

    if template_headers != mart_headers:
        raise ValueError(
            f"[{sheet_name}] Template headers do not match mart columns — fix before writing data.\n"
            f"Template: {template_headers}\n"
            f"Mart:     {mart_headers}"
        )

    n_cols = len(mart_headers)
    old_max_row = ws.max_row

    # Work out the "correct" per-column formatting from whatever data is
    # already there, then stamp it onto every cell we write — explicitly,
    # not inherited — so borders/fills/number formats are consistent no
    # matter how many rows go in this run. Any conditional formatting on
    # the sheet is untouched — full-column CF ranges apply regardless of
    # row count and don't need copying down.
    column_styles = _column_styles(ws, first_data_row, max(old_max_row, first_data_row), n_cols)

    # Register one NamedStyle per column and apply it as a single
    # cell.style assignment. Setting font/border/fill separately makes
    # openpyxl dedupe each one individually against its internal style
    # table (three lookups per cell); a NamedStyle collapses that to one.
    # Names are sheet-scoped since NamedStyle names must be workbook-unique.
    col_style_names = []
    for j, (font, border, fill, number_format) in enumerate(column_styles):
        style = NamedStyle(name=f"mcm_{sheet_name}_col_{j}")
        style.font = font
        style.border = border
        style.fill = fill
        style.number_format = number_format
        wb.add_named_style(style)
        col_style_names.append(style.name)

    new_last_row = first_data_row + len(df) - 1

    for i, record in enumerate(df.itertuples(index=False)):
        row = first_data_row + i
        for j, value in enumerate(record):
            # ws.cell(..., value=None) leaves the existing value untouched
            # instead of clearing it (None means "no value passed", not
            # "set to blank") — so a genuine NULL from the mart would
            # silently leave whatever stale text the template's old sample
            # data had at that position. Set .value directly instead.
            cell = ws.cell(row=row, column=j + 1)
            cell.value = None if pd.isna(value) else value
            cell.style = col_style_names[j]

    # Clear any leftover rows if this run has fewer rows than last time —
    # otherwise stale content lingers below the real data. Keep the same
    # per-column style rather than wiping to a blank one: borders/fonts are
    # just the sheet's structural grid, not data-driven, so stripping them
    # made the grid visibly stop partway down the sheet. Conditional
    # formatting is still safe either way — it fires on cell value, and
    # the value here is cleared.
    if old_max_row > new_last_row:
        for row in ws.iter_rows(min_row=new_last_row + 1, max_row=old_max_row, max_col=n_cols):
            for j, cell in enumerate(row):
                cell.value = None
                cell.style = col_style_names[j]

    last_col_letter = ws.cell(row=header_row, column=n_cols).column_letter
    ws.auto_filter.ref = f"A{header_row}:{last_col_letter}{new_last_row}"

    print(f"  {sheet_name}: {len(df)} rows")


def mcmaster_output():

    # ------------------------------------------------------------------
    # 1. Load every mart from Postgres
    # ------------------------------------------------------------------
    engine = get_postgres_connection()
    dataframes = {
        sheet_name: pd.read_sql(f"SELECT * FROM analytics_marts.{mart}", engine)
        for sheet_name, (mart, _header_row) in SHEETS.items()
    }
    backlog_daily_df = pd.read_sql("SELECT * FROM analytics_marts.mart_mcmaster__backlog_daily", engine)
    backlog_status_df = pd.read_sql("SELECT * FROM analytics_marts.mart_mcmaster__backlog_status", engine)
    engine.dispose()

    for sheet_name, df in dataframes.items():
        df = _stringify_json_columns(df)
        df = _coerce_numeric_text(df)
        columns, ascending = SORT_KEYS[sheet_name]
        df = df.sort_values(columns, ascending=ascending, na_position="last")
        dataframes[sheet_name] = df

    # ------------------------------------------------------------------
    # 2. Paste each mart into its sheet in the template
    # ------------------------------------------------------------------
    wb = openpyxl.load_workbook(TEMPLATE_PATH)

    print("Writing sheets:")
    for sheet_name, (_mart, header_row) in SHEETS.items():
        _write_sheet(wb, sheet_name, header_row, dataframes[sheet_name])

    update_live_summary_sheet(wb, backlog_daily_df, backlog_status_df)
    print("  summary: trend + status charts, 7-day table, status pivot")

    # ------------------------------------------------------------------
    # 3. Save locally, drop a copy in the SharePoint-synced library,
    #    and keep a dated archive copy.
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    wb.save(OUTPUT_PATH)
    print(f"Report written: {OUTPUT_PATH}")

    os.makedirs(SHAREPOINT_DIR, exist_ok=True)
    shutil.copy2(OUTPUT_PATH, os.path.join(SHAREPOINT_DIR, "mcmaster_report.xlsx"))
    print(f"Copied to SharePoint: {SHAREPOINT_DIR}")

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    dated_name = f"mcmaster_report_{date.today():%Y_%m_%d}.xlsx"
    archive_path = os.path.join(ARCHIVE_DIR, dated_name)
    shutil.copy2(OUTPUT_PATH, archive_path)
    print(f"Archived: {archive_path}")
