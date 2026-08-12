# run.py Commands

Run all commands from `c:\base` as the working directory:

```
python run.py <task> [args...]
```

---

## Cheat Sheet

### Daily — the two scheduled tasks

| Run | Does |
|-----|------|
| `python run.py daily-core` | **Always run daily**, independent of McMaster's fate: Banner ingest, Castle (OBIEE download + ingest) |
| `python run.py daily-mcmaster` | **Currently daily**, tied specifically to the McMaster report: SharePoint, Oracle, dbt run, then McMaster output + email (show) |

These two are what get wired into Windows Task Scheduler as two separate tasks
(not one combined job) — see `pipelines/run_daily.py`. Kept separate because
McMaster's future is genuinely undecided ("could go away, could stay, it's its
own thing"), so it should be independently disable/reschedule-able without
touching core ingest.

| Run | Does |
|-----|------|
| `python run.py daily-all` | Manual convenience only — `daily-core` then `daily-mcmaster` back to back, for running the whole current workstream by hand. Not a third scheduled task. |

### Daily — individual pieces (for manual re-runs / debugging)

Everything below is also called automatically by the two commands above — these
exist so any single piece can be re-run on its own if something fails partway
through, without re-running the whole sequence.

**daily-core:**

| Run | Saves to |
|-----|----------|
| `python run.py banner-ingest` | Auto-completes the AAD sign-in popup, pulls 9 Banner tables live into `raw.banner_*` |
| `python run.py castle-obiee-download` | Logs into OBIEE via browser automation, exports all 7 reports, saves to `etl/data_raw/castle/` (overwrites) |
| `python run.py castle-ingest` | Loads those 7 CSVs into `raw.castle_*` Postgres tables |

**daily-mcmaster:**

| Run | Saves to |
|-----|----------|
| `python run.py sharepoint-download` | Browser automation downloads `All open orders.xlsx` from a colleague's OneDrive, saves to `etl/data_raw/sharepoint/` (overwrites) |
| `python run.py sharepoint-ingest` | Loads that file into `raw.cx_orders` |
| `python run.py oracle-download` | Reads Outlook's Reports folder, fetches latest inventory (×6 orgs) + open orders report links, saves to `etl/data_raw/oracle/` |
| `python run.py oracle-ingest` | Loads those into `raw.castle_oracle_inventory` + `raw.castle_oracle_open_orders` |
| `python run.py dbt-run` | Runs `dbt run` from `analytics_dbt/` |
| `python run.py mcmaster-output` | `reports/mcmaster/mcmaster_report.xlsx` + SharePoint + OneDrive `Reporting\McMaster\` (report, charts, dated archive) |
| `python run.py mcmaster-email show` (or `send`) | Outlook email — trend + status charts inline, SharePoint link at top |

### Weekly — Earned Hrs by Week

| Run | Saves to |
|-----|----------|
| `python run.py earnedhrs-by-week-output` | `reports/productivity/earnedhrs_by_week.xlsx` + SharePoint + OneDrive `Reporting\Productivity\Earned Hours by Week\` |

### Monthly — Productivity

| Run | Saves to |
|-----|----------|
| `python run.py productivity-output 2026 7` | `reports/productivity/results/<year>/<month>/` + OneDrive `Reporting\Productivity\Productivity Incentive Payouts\<year>\<month>\` |
| `python run.py productivity-email 2026 7 Jul26 show` (or `send`) | Outlook email, per branch |

### AdHoc — Yield

| Run | Saves to |
|-----|----------|
| `python run.py yield-output` | `reports/yield/mart_yield_siteopdate.csv` + `mart_yield_output.csv` — local, SharePoint, and dated archive copies |

---

## Ingest

```
python run.py ingest
```

Legacy catch-all/scratch pipeline — hand-toggle which `run_all_*_ingestions()` calls
are active in `pipelines/run_all_ingest.py`'s `run_all_ingestions()`. Not part of the
daily-core / daily-mcmaster flow below; kept around for manually running whatever
isn't yet a standalone command (Vorne, FX rates, HR, Castle DI item master).

---

## Daily — Core

```
python run.py daily-core
```

Always run daily, independent of McMaster's fate: Banner ingest, then Castle
(OBIEE download + ingest). Defined in `pipelines/run_daily.py` as
`run_daily_core()`, composed from the same functions each individual command
below calls — re-run any single piece on its own if something fails partway
through. No arguments.

---

## Daily — McMaster

```
python run.py daily-mcmaster
```

Currently daily, tied specifically to the McMaster report (this bundle could
change if McMaster's setup changes — see `pipelines/run_daily.py`'s docstring):
SharePoint download + ingest, Oracle download + ingest, `dbt run`, then McMaster
output and email (`show`, not `send` — you still review before sending). Defined
as `run_daily_mcmaster()`. No arguments.

---

## Daily — All

```
python run.py daily-all
```

Manual convenience only: `daily-core` then `daily-mcmaster` back to back, for
running the whole current daily workstream by hand before/without Task
Scheduler. Defined as `run_daily_all()` in `pipelines/run_daily.py` — Task
Scheduler calls the two sequences separately (see above), not this. No
arguments.

---

## Castle — OBIEE Download

```
python run.py castle-obiee-download
```

Replaces the manual "open OBIEE, export CSV, save it" step. Drives a real (visible,
non-headless) Chromium browser via Playwright: logs into OBIEE (credentials in `.env`
as `OBIEE_USER`/`OBIEE_PASSWORD`), opens each of the 7 saved reports, and exports via
the Data > CSV Format menu. Saves straight to `etl/data_raw/castle/` — never touches
the real Windows Downloads folder — overwriting the previous day's file for:
`SALES.csv`, `DJ.csv`, `PPS_RCV_SHP.csv`, `INVENTORY.csv`, `PO OPEN.csv`,
`PO RECEIPTS.csv`, `TRANSFERS.csv`.

Runs one report at a time — each waits for its actual browser download event (not a
timer) before starting the next, since some of these reports are 300k+ rows and can
take a couple of minutes to render and export.

No arguments. Report catalog paths and target filenames are configured in
`config/obiee.py` (`OBIEE_REPORTS`).

---

## Castle — Ingest

```
python run.py castle-ingest
```

Loads the 7 CSVs from `etl/data_raw/castle/` (see castle-obiee-download above) into
their `raw.castle_*` Postgres tables. Run `castle-obiee-download` first — this step
does not check file freshness.

---

## Banner — Ingest

```
python run.py banner-ingest
```

Pulls all 9 Banner tables (customers, inventory transactions, BOM, route
transactions, production orders, sales order lines/headers, invoice lines/headers)
live from the Fabric Lakehouse SQL endpoint — no raw CSV step, `pd.read_sql` straight
into `raw.banner_*` Postgres tables.

The connection uses `ActiveDirectoryInteractive` auth, which pops a native Microsoft
sign-in window. `etl/utils/lakehouse_auth.py` automates it: spawns a **separate
Python process** (`etl/utils/connect_lakehouse.py`) that fills in the email
(`LAKEHOUSE_LOGIN_EMAIL` in `.env`) via real keystroke simulation and clicks Next,
then silent SSO (Windows Hello/PRT) completes sign-in with no password or MFA. Must
be a separate process, not a background thread — driving the popup from a thread in
the same process that's blocked in `pyodbc.connect()` fails silently every time.

No arguments.

---

## SharePoint — Download

```
python run.py sharepoint-download
```

Replaces the manual "open the link, File > Create a Copy > Download a Copy" step
for `All open orders.xlsx` — a file that lives in a colleague's OneDrive
(`jbates_amcastle_com`), not this account's own SharePoint. Drives a real (visible)
Chromium browser via Playwright, using a **persistent browser profile**
(`etl/extract/sharepoint/.browser_profile/`, gitignored) so the Microsoft 365
sign-in only ever has to happen once, by hand, in the visible window — every
subsequent run reuses that session's cookies automatically, no repeated login.

This workbook (21+ daily-snapshot tabs) keeps loading in the background even once
the ribbon looks interactive. Clicking before it's genuinely ready pops a "Please
Wait" message instead of doing anything, and — since Excel Online's File button is
a **toggle**, not a plain button — blindly re-clicking it on retry can close an
already-open (but still-loading) menu instead of retrying anything, which looks
like "success" (no more "Please Wait" visible) while nothing actually opened. The
retry logic in `etl/extract/sharepoint/sharepoint_download.py` checks whether the
File backstage panel is already open (via the "Create a Copy" item's visibility)
before ever clicking File again.

Saves straight to `etl/data_raw/sharepoint/All open orders.xlsx` (overwrites),
never touching the real Windows Downloads folder. No arguments.

---

## SharePoint — Ingest

```
python run.py sharepoint-ingest
```

Loads `All open orders.xlsx` (see sharepoint-download above) into `raw.cx_orders`.
Run `sharepoint-download` first — this step does not check file freshness.

---

## Oracle — Download

```
python run.py oracle-download
```

Replaces the manual "click each Oracle Workflow Notification email link, scan the
inventory report's content to see which org it's for, save with the right filename"
step. Oracle sends a fresh batch every hour into the "Reports" Outlook folder (the
only notifications that land there): 6 inventory reports (one per org — the subject
line is identical for all six, so the org is only knowable from the report's own
content) plus 1 open orders report.

Reads the Reports folder via Outlook's COM interface (`win32com`) rather than a
browser — the report link itself (`FNDWRR.exe?temp_id=...`, Oracle EBS's "Web
Report Review" endpoint) needs no login and is fetched with a plain HTTP request.
Each link is one-time-use though (a second fetch of the same URL fails), so a
report is only ever fetched once per run.

Org is detected by reading column 9 of the inventory file (a city name, consistent
across every row) and mapping it via `CITY_TO_ORG` in `config/oracle.py`.

**Strictly same-hour batch, no cross-hour fallback**: the target hour is whichever
hour the single most recent report email falls in — every report (all 6 orgs +
open orders) must come from that same hour or it's reported `[MISSING]`, never
silently substituted with an older hour's report. If a link within the target hour
turns out already-consumed (already fetched by an earlier run, or manually clicked
in Outlook), that specific report is reported missing rather than falling back —
deliberate, so a stale-but-present file is never mistaken for this hour's real data.

Saves to `etl/data_raw/oracle/`: `Inventory_<ORG>.txt` ×6 and
`AMC_Open_Orders_Report.xls` (only the ones actually found this hour — a missing
report's existing file from a previous run is left untouched, not overwritten with
garbage). No arguments.

---

## Oracle — Ingest

```
python run.py oracle-ingest
```

Loads the 7 files from `etl/data_raw/oracle/` (see oracle-download above) into
`raw.castle_oracle_inventory` and `raw.castle_oracle_open_orders`. Run
`oracle-download` first — this step does not check file freshness.

---

## dbt — Run

```
python run.py dbt-run
```

Runs `dbt run` from `analytics_dbt/` via `subprocess` (`pipelines/run_dbt.py`).
Requires `~/.dbt/profiles.yml` set up separately (see requirements.txt) — not
checked into this repo. No arguments.

---

## Productivity — Output

```
python run.py productivity-output <year> <month>
```

Generates Excel files for the given month and saves them to:
- `reports/productivity/results/<year>/<month>/` (local)
- `C:\Users\mcurphey\OneDrive - A. M. Castle & Co\Operations - Documents\Reporting\Productivity\Productivity Incentive Payouts\<year>\<month>\` (OneDrive — no `results` folder, year/month straight under the base path)

**Arguments:**
| Arg | Type | Example | Description |
|-----|------|---------|-------------|
| year | int | `2026` | Output year |
| month | int | `3` | Output month (1–12) |

**Example:**
```
python run.py productivity-output 2026 6
```

---

## Productivity — Email

```
python run.py productivity-email <year> <month> <subject_month> [show|send]
```

Sends (or previews) per-branch productivity incentive emails via Outlook.
Excel files must already exist — run `productivity-output` first.

**Arguments:**
| Arg | Type | Example | Description |
|-----|------|---------|-------------|
| year | int | `2026` | Output year |
| month | int | `3` | Output month (1–12) |
| subject_month | str | `Mar26` | Label used in the email subject line |
| show\send | str | `show` | `show` to preview in Outlook, `send` to send (default: `show`) |

**Examples:**
```
python run.py productivity-email 2026 6 Jun26 show
python run.py productivity-email 2026 6 May26 send
```

---

## Earned Hours by Week — Output

```
python run.py earnedhrs-by-week-output
```

Exports `int_castle__productivity_01_earnedhrs_by_week` (org / operation / week-commencing,
complete weeks only) to Excel, with the last complete week called out beside the data.
Saves to:
- `reports/productivity/earnedhrs_by_week.xlsx` (local)
- `C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\productivity\earnedhrs_by_week.xlsx` (SharePoint)
- `C:\Users\mcurphey\OneDrive - A. M. Castle & Co\Operations - Documents\Reporting\Productivity\Earned Hours by Week\earnedhrs_by_week.xlsx` (OneDrive)

No arguments — always reflects the current state of the underlying model.

---

## Yield — Output

```
python run.py yield-output
```

Exports both yield marts to CSV:

- `mart_yield_siteopdate` — the file the Yield Loss Power BI dataset refreshes from, exported as-is (no filtering). Saves to:
  - `reports/yield/mart_yield_siteopdate.csv` (local)
  - `C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\etl_dumps\mart_yield_siteopdate.csv` (SharePoint)
  - `reports/yield/archive/mart_yield_siteopdate_<date>.csv` (dated archive copy)
- `mart_yield_output` — trimmed job-level mart, limited to yesterday's completed jobs (`complete_date = current_date - 1`). Saves to:
  - `reports/yield/mart_yield_output.csv` (local)
  - `C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\yield\mart_yield_output.csv` (SharePoint)
  - `reports/yield/archive/mart_yield_output_<date>.csv` (dated archive copy)

No arguments — always reflects the current state of the underlying marts.

---

## McMaster — Output

```
python run.py mcmaster-output
```

Pulls all 7 McMaster marts from Postgres and pastes them into `mcmaster_template.xlsx`:
`open_backlog_detail`, `cross_ship`, `hot_components`, `to_cancel`, `dj_review` (straight
mart-to-tab pastes), plus `summary` (90-day trend + 7-day activity table + backlog status
chart/pivot, built from `mart_mcmaster__backlog_daily`/`mart_mcmaster__backlog_status`).
Saves the result to:
- `reports/mcmaster/mcmaster_report.xlsx` (local)
- `C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\mcmaster\mcmaster_report.xlsx` (SharePoint)
- `reports/mcmaster/archive/mcmaster_report_<date>.xlsx` (dated archive copy)
- `C:\Users\mcurphey\OneDrive - A. M. Castle & Co\Operations - Documents\Reporting\McMaster\` — report, `backlog_trend.png`, `status_chart.png`, plus `archive\mcmaster_report_<date>.xlsx` (OneDrive)

No arguments — always reflects the current state of the underlying marts.

Two further marts, `mart_mcmaster__material_availability_daily` (append-only daily
snapshot log, order age vs. lead time) and `mart_mcmaster__daily_target_performance`
(target vs. actual, variance/pct-to-target) exist for Chris Roeder's daily-target ask
but aren't pasted into a tab here yet — no Excel tab built for them.

---

## McMaster — Email

```
python run.py mcmaster-email show [show|send]
```

Sends (or previews) the daily McMaster backlog email — SharePoint link at the top,
then the trend chart and status chart inline (no tables, no commentary). Run
`mcmaster-output` first so `reports/mcmaster/backlog_trend.png` and `status_chart.png`
are current. Single recipient for now (`mcurphey@amcastle.com`); more to be added later.

**Arguments:**
| Arg | Type | Example | Description |
|-----|------|---------|-------------|
| show\send | str | `show` | `show` to preview in Outlook, `send` to send (default: `show`) |

---

## Outputs

```
python run.py outputs
```

Runs scheduled report outputs (currently unused/commented out).

---

## Custom

```
python run.py custom
```

Runs one-off custom scripts (currently unused/commented out).
