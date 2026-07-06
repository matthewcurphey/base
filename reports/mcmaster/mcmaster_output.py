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

TEMPLATE_PATH = os.path.join("reports", "mcmaster", "mcmaster_template.xlsx")
OUTPUT_PATH = os.path.join("reports", "mcmaster", "mcmaster_report.xlsx")
ARCHIVE_DIR = os.path.join("reports", "mcmaster", "archive")
SHAREPOINT_DIR = r"C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\mcmaster"

SHEET_NAME = "open_backlog_detail"
HEADER_ROW = 2
FIRST_DATA_ROW = 3

_PURE_INT_RE = re.compile(r"-?\d+")


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
        not_na = s[s.notna()].astype(str)
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


def mcmaster_output():

    # ------------------------------------------------------------------
    # 1. Load the mart from Postgres
    # ------------------------------------------------------------------
    engine = get_postgres_connection()
    df = pd.read_sql("SELECT * FROM analytics_marts.mart_mcmaster__open_backlog", engine)
    engine.dispose()

    # Convert safely-numeric text columns to real ints before sorting, so
    # the so_nbr/line tie-break below compares numerically, not as strings.
    df = _coerce_numeric_text(df)

    # Most overdue first. Mirrors the tie-break already used by the tally
    # calc in SQL (promise date, then so_nbr/so_line) — display order only,
    # doesn't touch the inventory math.
    df = df.sort_values(["prom_dt", "so_nbr", "line"], na_position="last")

    # ------------------------------------------------------------------
    # 2. Open the formatted template and check it still lines up with
    #    the mart — this is a hand-maintained mapping, so a silent
    #    mismatch here means real values land under the wrong headers.
    # ------------------------------------------------------------------
    wb = openpyxl.load_workbook(TEMPLATE_PATH)
    ws = wb[SHEET_NAME]

    template_headers = [ws.cell(row=HEADER_ROW, column=c).value for c in range(1, ws.max_column + 1)]
    while template_headers and template_headers[-1] is None:
        # Excel can inflate the sheet's used range with stray formatting on
        # blank trailing columns (e.g. a column width tweak) — harmless, ignore.
        template_headers.pop()
    mart_headers = list(df.columns)

    if template_headers != mart_headers:
        raise ValueError(
            "Template headers do not match mart columns — fix before writing data.\n"
            f"Template: {template_headers}\n"
            f"Mart:     {mart_headers}"
        )

    n_cols = len(mart_headers)
    old_max_row = ws.max_row

    # ------------------------------------------------------------------
    # 3. Work out the "correct" per-column formatting from whatever data
    #    is already there, then stamp it onto every cell we write —
    #    explicitly, not inherited — so borders/fills/number formats are
    #    consistent no matter how many rows go in this run. Conditional
    #    formatting (AK, AV) is untouched: both rules already span the
    #    full column (up to row 999999/1048576) so they apply regardless
    #    of row count and don't need copying down.
    # ------------------------------------------------------------------
    column_styles = _column_styles(ws, FIRST_DATA_ROW, max(old_max_row, FIRST_DATA_ROW), n_cols)

    # Register one NamedStyle per column and apply it as a single
    # cell.style assignment. Setting font/border/fill separately makes
    # openpyxl dedupe each one individually against its internal style
    # table (three lookups per cell); a NamedStyle collapses that to one.
    col_style_names = []
    for j, (font, border, fill, number_format) in enumerate(column_styles):
        style = NamedStyle(name=f"mcm_col_{j}")
        style.font = font
        style.border = border
        style.fill = fill
        style.number_format = number_format
        wb.add_named_style(style)
        col_style_names.append(style.name)

    blank_style = NamedStyle(name="mcm_blank")
    wb.add_named_style(blank_style)

    new_last_row = FIRST_DATA_ROW + len(df) - 1

    for i, record in enumerate(df.itertuples(index=False)):
        row = FIRST_DATA_ROW + i
        for j, value in enumerate(record):
            # ws.cell(..., value=None) leaves the existing value untouched
            # instead of clearing it (None means "no value passed", not
            # "set to blank") — so a genuine NULL from the mart would
            # silently leave whatever stale text the template's old sample
            # data had at that position. Set .value directly instead.
            cell = ws.cell(row=row, column=j + 1)
            cell.value = None if pd.isna(value) else value
            cell.style = col_style_names[j]

    # Reset any leftover rows if this run has fewer rows than last time —
    # otherwise stale formatted-but-empty rows linger below the real data.
    if old_max_row > new_last_row:
        for row in ws.iter_rows(min_row=new_last_row + 1, max_row=old_max_row, max_col=n_cols):
            for cell in row:
                cell.value = None
                cell.style = blank_style.name

    last_col_letter = ws.cell(row=HEADER_ROW, column=n_cols).column_letter
    ws.auto_filter.ref = f"A{HEADER_ROW}:{last_col_letter}{new_last_row}"

    # ------------------------------------------------------------------
    # 4. Save locally, drop a copy in the SharePoint-synced library,
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
