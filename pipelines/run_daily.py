"""
Composes the individual per-source download/ingest functions (defined in
pipelines/run_all_ingest.py) plus dbt and the McMaster report into the two
sequences that get scheduled — see COMMANDS.md for the full breakdown of
what's in each and why they're split this way:

- run_daily_core(): always run daily, independent of McMaster's fate —
  Banner ingest, Castle (OBIEE download + ingest).
- run_daily_mcmaster(): currently daily but tied specifically to the
  McMaster report — SharePoint, Oracle, dbt, then the report + email.

Each piece here is independently runnable too (see run.py's individual
`castle-obiee-download`, `banner-ingest`, etc. tasks) — these two functions
just chain them in the right order for the two scheduled tasks, printing an
explicit confirmation line after each step so a run's output is a readable
checklist, not just whatever each step happens to print internally.

A step raising stops the whole sequence — later steps depend on earlier
ones succeeding (dbt on ingestion, McMaster on dbt), so silently continuing
past a failure would just produce a wrong report downstream.
"""
from pipelines.run_all_ingest import (
    run_all_banner_ingestions,
    run_castle_obiee_download,
    run_all_castle_ingestions,
    run_sharepoint_download,
    run_all_sharepoint_ingestions,
    run_oracle_download,
    run_all_oracle_ingestions,
)
from pipelines.run_dbt import run_dbt


def _confirm(label, detail=""):
    print(f"[OK] {label}" + (f" — {detail}" if detail else ""))


def run_daily_core():
    print("=== Daily Core: starting ===")

    #run_all_banner_ingestions()
    #_confirm("Banner ingest ran")

    obiee_paths = run_castle_obiee_download()
    _confirm("OBIEE download", f"{len(obiee_paths)}/7 reports saved")

    run_all_castle_ingestions()
    _confirm("Castle ingest ran")

    print("=== Daily Core: completed ===")


def run_daily_mcmaster():
    from reports.mcmaster.mcmaster_output import mcmaster_output
    from reports.mcmaster.mcmaster_email import mcmaster_email

    print("=== Daily McMaster: starting ===")

    sharepoint_path = run_sharepoint_download()
    _confirm("SharePoint download", f"saved {sharepoint_path}")

    run_all_sharepoint_ingestions()
    _confirm("SharePoint ingest ran")

    oracle_result = run_oracle_download()
    target_hour = oracle_result["target_hour"]
    orgs = oracle_result["orgs"]
    open_orders = oracle_result["open_orders"]
    print(f"[Oracle target hour: {target_hour}]")
    for org, info in sorted(orgs.items()):
        _confirm(f"Oracle {org} inventory", f"received {info['received']}")
    missing = {"CLE", "DAL", "LOS", "JVL", "WIE", "ATL"} - set(orgs)
    if missing:
        print(f"[MISSING] Oracle inventory not found for {sorted(missing)} within target hour {target_hour}")
    if open_orders:
        _confirm("Oracle open orders", f"received {open_orders['received']}")
    else:
        print(f"[MISSING] Oracle open orders not found within target hour {target_hour}")

    run_all_oracle_ingestions()
    _confirm("Oracle ingest ran")

    run_dbt()
    _confirm("dbt run completed")

    print("Generating McMaster backlog report...")
    mcmaster_output()
    _confirm("McMaster output generated")

    print("Sending McMaster email (show)...")
    mcmaster_email(send_or_show="show")
    _confirm("McMaster email shown")

    print("=== Daily McMaster: completed ===")


def run_daily_all():
    """
    Both sequences back to back, in one call — for manually running the
    whole current daily workstream before this is on Task Scheduler.
    Same two functions Task Scheduler will end up calling separately; this
    is purely a manual-run convenience, not a third scheduled task.
    """
    run_daily_core()
    run_daily_mcmaster()


if __name__ == "__main__":
    import sys
    task = sys.argv[1] if len(sys.argv) > 1 else None
    if task == "core":
        run_daily_core()
    elif task == "mcmaster":
        run_daily_mcmaster()
    elif task == "all":
        run_daily_all()
    else:
        print("Usage: python -m pipelines.run_daily [core|mcmaster]")
