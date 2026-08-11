"""
Replaces the manual "click each Oracle Workflow Notification email link,
scan the inventory report's content to see which org it's for, save with
the right filename" step. Oracle sends a fresh batch every hour: 6
inventory reports (one per org — the subject line is identical for all
six, so the org is only knowable from the report's own content) plus 1
open orders report, arriving via email into the "Reports" Outlook folder
(only these notifications land there).

Reads that folder via Outlook's COM interface (win32com) rather than a
browser — the report link itself (Oracle EBS's FNDWRR.exe "Web Report
Review" endpoint) needs no login and is fetched with a plain HTTP request,
no Playwright involved. Each link is one-time-use though: a second fetch
of the same URL fails, so a report is only ever fetched once per run.

"Latest complete set" means: for the open orders report, the single most
recent one; for inventory, each org's own most recent report, independent
of which hourly batch it happened to arrive in (a delayed/retried org
report from an earlier hour is fine to mix with a newer hour's other
orgs — this deliberately doesn't require all 6 to share the same batch).
"""
import os
from datetime import datetime

import requests
import win32com.client

from config.oracle import CITY_TO_ORG, INVENTORY_SUBJECT, OPEN_ORDERS_SUBJECT, REPORTS_FOLDER_NAME, REPORT_URL_RE
from config.paths import ORACLE_RAW_DIR

MANIFEST_PATH = os.path.join(ORACLE_RAW_DIR, "_download_manifest.txt")

# Safety cap on how many inventory-report links we're willing to fetch (and
# thereby consume) in one run — normally only ~6-8 are ever needed even
# accounting for retries/duplicates in a given hour.
MAX_INVENTORY_FETCHES = 20


def _find_folder(ns, name):
    queue = [ns.Folders.Item(i + 1) for i in range(ns.Folders.Count)]
    while queue:
        f = queue.pop(0)
        if f.Name == name:
            return f
        try:
            for i in range(f.Folders.Count):
                queue.append(f.Folders.Item(i + 1))
        except Exception:
            pass
    return None


def _get_reports_items():
    outlook = win32com.client.Dispatch("Outlook.Application")
    ns = outlook.GetNamespace("MAPI")
    folder = _find_folder(ns, REPORTS_FOLDER_NAME)
    if folder is None:
        raise RuntimeError(f"Outlook folder '{REPORTS_FOLDER_NAME}' not found")
    items = folder.Items
    items.Sort("[ReceivedTime]", True)  # newest first
    return items


def _detect_org(content):
    for line in content.splitlines():
        fields = line.split(",")
        if len(fields) <= 8:
            continue
        city = fields[8].strip()
        for city_name, org in CITY_TO_ORG.items():
            if city_name.lower() == city.lower():
                return org
    return None


def download_oracle_reports():
    os.makedirs(ORACLE_RAW_DIR, exist_ok=True)
    items = _get_reports_items()

    open_orders = None  # {"received": ..., "subject": ...} once saved
    orgs = {}  # org -> {"received": ..., "subject": ...}
    inventory_fetches = 0
    all_orgs = set(CITY_TO_ORG.values())
    manifest_lines = [f"Download run at {datetime.now():%Y-%m-%d %H:%M:%S}"]

    for item in items:
        if open_orders is not None and set(orgs) >= all_orgs:
            break
        try:
            subject = item.Subject or ""
            body = item.Body or ""
            received = item.ReceivedTime
        except Exception:
            continue

        match = REPORT_URL_RE.search(body)
        if not match:
            continue
        url = match.group(0)

        if open_orders is None and OPEN_ORDERS_SUBJECT in subject:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            target = os.path.join(ORACLE_RAW_DIR, "AMC_Open_Orders_Report.xls")
            with open(target, "wb") as f:
                f.write(resp.content)
            print(f"Saved open orders report: {target} ({len(resp.content)} bytes) — source: {subject!r} received {received}")
            manifest_lines.append(f"AMC_Open_Orders_Report.xls <- {subject!r} received {received}")
            open_orders = {"received": received, "subject": subject}
            continue

        if set(orgs) < all_orgs and INVENTORY_SUBJECT in subject:
            if inventory_fetches >= MAX_INVENTORY_FETCHES:
                continue
            inventory_fetches += 1
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            org = _detect_org(resp.text)
            if org is None:
                print(f"Could not detect org for inventory report (subject={subject!r}, received={received}) — skipping")
                continue
            if org in orgs:
                # This link is now consumed either way — a newer report for
                # this org was already saved, nothing more to do with it.
                continue
            target = os.path.join(ORACLE_RAW_DIR, f"Inventory_{org}.txt")
            with open(target, "wb") as f:
                f.write(resp.content)
            print(f"Saved {org} inventory report: {target} ({len(resp.content)} bytes) — source: {subject!r} received {received}")
            manifest_lines.append(f"Inventory_{org}.txt <- {subject!r} received {received}")
            orgs[org] = {"received": received, "subject": subject}

    missing_orgs = all_orgs - set(orgs)
    if missing_orgs:
        print(f"WARNING: no inventory report found for: {sorted(missing_orgs)}")
        manifest_lines.append(f"WARNING: no inventory report found for: {sorted(missing_orgs)}")
    if open_orders is None:
        print("WARNING: no open orders report found")
        manifest_lines.append("WARNING: no open orders report found")

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(manifest_lines) + "\n")

    return {"orgs": orgs, "open_orders": open_orders}


if __name__ == "__main__":
    download_oracle_reports()
