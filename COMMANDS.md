# run.py Commands

Run all commands from `c:\base` as the working directory:

```
python run.py <task> [args...]
```

---

## Ingest

```
python run.py ingest
```

Runs all ETL ingestion jobs (Castle, Banner, HR, etc.) and loads data into PostgreSQL.

---

## Productivity — Output

```
python run.py productivity-output <year> <month>
```

Generates Excel files for the given month and saves them to:
`reports/productivity/results/<year>/<month>/`

**Arguments:**
| Arg | Type | Example | Description |
|-----|------|---------|-------------|
| year | int | `2026` | Output year |
| month | int | `3` | Output month (1–12) |

**Example:**
```
python run.py productivity-output 2026 5
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
python run.py productivity-email 2026 5 May26 show
python run.py productivity-email 2026 5 May26 send
```

---

## McMaster — Output

```
python run.py mcmaster-output
```

Pulls the open backlog mart from Postgres, pastes it into the `mcmaster_template.xlsx`
template, and saves the result to:
- `reports/mcmaster/mcmaster_report.xlsx` (local)
- `C:\Users\mcurphey\A. M. Castle & Co\Analytics_ETL - Documents\mcmaster\mcmaster_report.xlsx` (SharePoint)
- `reports/mcmaster/archive/mcmaster_report_<date>.xlsx` (dated archive copy)

No arguments — always reflects the current state of `analytics_marts.mart_mcmaster__open_backlog`.

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
