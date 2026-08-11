"""
Replaces the manual "open OBIEE, export CSV, save it" step for the Castle
raw files. Drives a real Chromium browser via Playwright so it can follow
the exact same login + export click-path a person does by hand.

OBIEE's flyout export menu (export icon -> Data -> CSV Format) only responds
reliably to continuous real mouse movement between menu levels — Playwright's
locator .click()/.hover() teleport the pointer between calls and the flyout
closes before the next click lands. Everything below drives page.mouse
directly instead, moving in from each menu level rather than jumping to it.

The CSV itself downloads via a popup page OBIEE opens for the export, not
the report page itself — the download listener has to be attached to
whatever page it lands on, not just the one we navigated.
"""
import os
import urllib.parse

from playwright.sync_api import sync_playwright

from config.obiee import OBIEE_CONFIG, OBIEE_REPORTS
from config.paths import CASTLE_RAW_DIR

DEBUG_DIR = os.path.join("etl", "extract", "castle", "obiee_debug")

# Some of these reports are hundreds of thousands of rows — OBIEE renders and
# exports them server-side before the download even starts, which alone can
# take a couple of minutes.
DOWNLOAD_TIMEOUT_S = 5 * 60
PAGE_LOAD_TIMEOUT_MS = 3 * 60 * 1000


def _report_url(path: str) -> str:
    encoded_path = urllib.parse.quote(path, safe="")
    return f"{OBIEE_CONFIG['base_url']}/analytics/saw.dll?Answers&path={encoded_path}"


def _center(box):
    return box["x"] + box["width"] / 2, box["y"] + box["height"] / 2


def _wait_for_report_ready(page, timeout_ms):
    page.wait_for_selector("[title='Export this analysis']", timeout=timeout_ms)


def _login_if_needed(page, timeout_ms):
    # If the session cookie from a prior run is still valid, OBIEE serves
    # the report directly and there's no login form to fill in.
    user_field = page.locator("#sawlogonuser")
    if user_field.count() == 0:
        return

    user_field.fill(OBIEE_CONFIG["user"])
    page.locator("#sawlogonpwd").fill(OBIEE_CONFIG["password"])
    page.locator("#idlogon").click()
    _wait_for_report_ready(page, timeout_ms)


def _open_export_menu(page):
    export_btn = page.get_by_title("Export this analysis")
    x, y = _center(export_btn.bounding_box())
    page.mouse.move(x, y, steps=5)
    page.wait_for_timeout(300)
    page.mouse.click(x, y)
    page.wait_for_timeout(1000)


def _click_csv_format(page):
    data_item = page.get_by_text("Data", exact=True)
    dx, dy = _center(data_item.bounding_box())
    page.mouse.move(dx, dy, steps=5)
    page.wait_for_timeout(800)

    csv_item = page.get_by_text("CSV Format", exact=True).first
    cx, cy = _center(csv_item.bounding_box())
    # Move in partway from Data's position rather than teleporting straight
    # to CSV Format — the flyout tracks continuous pointer movement between
    # the parent item and its submenu, not just the final hover target.
    page.mouse.move(dx + (cx - dx) * 0.5, dy + (cy - dy) * 0.5, steps=3)
    page.wait_for_timeout(150)
    page.mouse.move(cx, cy, steps=3)
    page.wait_for_timeout(300)
    page.mouse.click(cx, cy)


def download_obiee_report(context, page, report_key: str) -> str:
    path, filename = OBIEE_REPORTS[report_key]

    downloads = []
    page.on("download", lambda d: downloads.append(d))
    context.on("page", lambda p2: p2.on("download", lambda d: downloads.append(d)))

    page.goto(_report_url(path))
    # A large report can take a while to render — wait for whichever shows up
    # first, the login form (fresh session) or the export button (report
    # already loaded on a still-valid session), rather than a fixed sleep.
    page.wait_for_selector("#sawlogonuser, [title='Export this analysis']", timeout=PAGE_LOAD_TIMEOUT_MS)
    _login_if_needed(page, PAGE_LOAD_TIMEOUT_MS)
    _wait_for_report_ready(page, PAGE_LOAD_TIMEOUT_MS)

    _open_export_menu(page)
    _click_csv_format(page)

    for _ in range(DOWNLOAD_TIMEOUT_S):
        if downloads:
            break
        page.wait_for_timeout(1000)
    else:
        raise TimeoutError(f"No download started for {report_key} within {DOWNLOAD_TIMEOUT_S}s")

    download = downloads[0]
    target_path = os.path.join(CASTLE_RAW_DIR, filename)
    download.save_as(target_path)
    return target_path


def download_all_obiee_reports(report_keys=None):
    keys = report_keys or list(OBIEE_REPORTS)
    os.makedirs(CASTLE_RAW_DIR, exist_ok=True)
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for key in keys:
            print(f"Downloading {key}...")
            try:
                target_path = download_obiee_report(context, page, key)
                print(f"  saved: {target_path}")
                results.append(target_path)
            except Exception:
                os.makedirs(DEBUG_DIR, exist_ok=True)
                shot_path = os.path.join(DEBUG_DIR, f"{key}_failure.png")
                page.screenshot(path=shot_path)
                print(f"  FAILED — screenshot saved to {shot_path}")
                raise

        browser.close()

    return results


if __name__ == "__main__":
    download_all_obiee_reports()
