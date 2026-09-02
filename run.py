from pipelines.run_all_ingest import (
    run_all_ingestions,
    run_castle_obiee_download,
    run_all_castle_ingestions,
    run_all_banner_ingestions,
    run_sharepoint_download,
    run_all_sharepoint_ingestions,
    run_oracle_download,
    run_all_oracle_ingestions,
    run_all_hr_ingestions,
)
from pipelines.run_dbt import run_dbt
from pipelines.run_daily import run_daily_core, run_daily_mcmaster, run_daily_all
# from etl.output.overship_daily_dashboard import overship_daily_dashboard
import sys


def run_outputs():
    print("Running outputs...\n")

    # overship_daily_dashboard()

    print("\nOutputs completed.")


def run_custom():
    print("Running custom...\n")

    # salesforce_data_dump()

    print("\nCustom completed.")


def run_productivity_output(year: int, month: int):
    from reports.productivity.productivity_output import productivity_output
    print(f"Generating Excel files for {year}-{month:02d}...")
    productivity_output(year, month)


def run_productivity_email(year: int, month: int, subject_month: str, send_or_show: str = "show"):
    from reports.productivity.productivity_email import productivity_email
    print(f"Sending emails ({send_or_show})...")
    productivity_email(year, month, subject_month, send_or_show=send_or_show)


def run_yield_output():
    # "yield" is a reserved keyword, so this package can't be imported with
    # a normal `from reports.yield...` statement — importlib works fine
    # since it resolves the dotted path from a string, not parsed syntax.
    import importlib
    yield_module = importlib.import_module("reports.yield.yield_output")
    print("Generating yield loss export...")
    yield_module.yield_output()
    print("Generating yield mart output export...")
    yield_module.yield_mart_output()


def run_earnedhrs_by_week_output():
    from reports.productivity.earnedhrs_by_week_output import earnedhrs_by_week_output
    print("Generating earned hours by week export...")
    earnedhrs_by_week_output()


def run_mcmaster_output():
    from reports.mcmaster.mcmaster_output import mcmaster_output
    print("Generating McMaster backlog report...")
    mcmaster_output()


def run_mcmaster_email(send_or_show: str = "show"):
    from reports.mcmaster.mcmaster_email import mcmaster_email
    print(f"Sending McMaster email ({send_or_show})...")
    mcmaster_email(send_or_show=send_or_show)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py <task> [args...]  (see COMMANDS.md)")
        sys.exit(1)
    task = sys.argv[1]

    if task == "ingest":
        run_all_ingestions()

    elif task == "castle-obiee-download":
        # Usage: python run.py castle-obiee-download
        run_castle_obiee_download()

    elif task == "castle-ingest":
        # Usage: python run.py castle-ingest
        run_all_castle_ingestions()

    elif task == "banner-ingest":
        # Usage: python run.py banner-ingest
        run_all_banner_ingestions()

    elif task == "sharepoint-download":
        # Usage: python run.py sharepoint-download
        run_sharepoint_download()

    elif task == "sharepoint-ingest":
        # Usage: python run.py sharepoint-ingest
        run_all_sharepoint_ingestions()

    elif task == "oracle-download":
        # Usage: python run.py oracle-download
        run_oracle_download()

    elif task == "oracle-ingest":
        # Usage: python run.py oracle-ingest
        run_all_oracle_ingestions()

    elif task == "hr-ingest":
        # Usage: python run.py hr-ingest
        run_all_hr_ingestions()

    elif task == "dbt-run":
        # Usage: python run.py dbt-run
        run_dbt()

    elif task == "daily-core":
        # Usage: python run.py daily-core
        # Always run daily: Banner ingest + Castle (OBIEE download + ingest).
        run_daily_core()

    elif task == "daily-mcmaster":
        # Usage: python run.py daily-mcmaster
        # Currently daily, tied to the McMaster report: SharePoint, Oracle,
        # dbt, then the report + email (show).
        run_daily_mcmaster()

    elif task == "daily-all":
        # Usage: python run.py daily-all
        # Manual convenience — daily-core then daily-mcmaster back to back,
        # for running the whole current workstream by hand before this is
        # on Task Scheduler (which calls the two separately).
        run_daily_all()

    elif task == "outputs":
        run_outputs()

    elif task == "custom":
        run_custom()

    elif task == "productivity-output":
        # Usage: python run.py productivity-output 2026 3
        year  = int(sys.argv[2])
        month = int(sys.argv[3])
        run_productivity_output(year, month)

    elif task == "productivity-email":
        # Usage: python run.py productivity-email 2026 3 Mar26 show
        year          = int(sys.argv[2])
        month         = int(sys.argv[3])
        subject_month = sys.argv[4]           # e.g. "Mar26"
        send_or_show  = sys.argv[5] if len(sys.argv) > 5 else "show"
        run_productivity_email(year, month, subject_month, send_or_show)

    elif task == "yield-output":
        # Usage: python run.py yield-output
        run_yield_output()

    elif task == "earnedhrs-by-week-output":
        # Usage: python run.py earnedhrs-by-week-output
        run_earnedhrs_by_week_output()

    elif task == "mcmaster-output":
        # Usage: python run.py mcmaster-output
        run_mcmaster_output()

    elif task == "mcmaster-email":
        # Usage: python run.py mcmaster-email [show|send]
        send_or_show = sys.argv[2] if len(sys.argv) > 2 else "show"
        run_mcmaster_email(send_or_show)

    else:
        print(f"❌ Unknown task: {task}")

