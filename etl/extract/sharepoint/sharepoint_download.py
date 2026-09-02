"""
Replaces the manual "open the SharePoint link, File > Create a Copy >
Download a Copy" step for All open orders.xlsx. Drives a real Chromium
browser via Playwright, using a PERSISTENT browser profile so the
Microsoft 365 sign-in only ever has to happen once (by hand, in the visible
window) — every subsequent run reuses that session's cookies.

This workbook is large (21+ daily-snapshot tabs) and keeps loading in the
background even after the ribbon looks interactive. Clicking before it's
genuinely ready pops a "Please Wait" message instead of doing anything —
that message is reactive (shown in response to a premature click), not a
passive loading banner, so the only way to know is to click and check.
"""
import os

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

from config.paths import SHAREPOINT_RAW_DIR
from config.sharepoint import BROWSER_PROFILE_DIR, OPEN_ORDERS_URL

TARGET_FILENAME = "All open orders.xlsx"
DOWNLOAD_TIMEOUT_S = 120
PLEASE_WAIT_MAX_ATTEMPTS = 8
PLEASE_WAIT_RETRY_S = 8
GOTO_MAX_ATTEMPTS = 5
GOTO_RETRY_S = 10
DEBUG_DIR = os.path.join("etl", "extract", "sharepoint", "sharepoint_debug")


def _goto_with_retry(page, url):
    # net::ERR_SOCKET_NOT_CONNECTED / ERR_CONNECTION_* / ERR_NAME_NOT_RESOLVED
    # show up when this runs right after wake-from-sleep, before the VPN/NIC
    # has come back up — transient, so worth a few spaced-out retries rather
    # than failing the whole daily run outright.
    for attempt in range(GOTO_MAX_ATTEMPTS):
        try:
            page.goto(url, wait_until="domcontentloaded")
            return
        except PlaywrightError as e:
            if "net::ERR_" not in str(e) or attempt == GOTO_MAX_ATTEMPTS - 1:
                raise
            print(f"goto {url} hit {e!r} (attempt {attempt + 1}/{GOTO_MAX_ATTEMPTS}), retrying in {GOTO_RETRY_S}s...")
            page.wait_for_timeout(GOTO_RETRY_S * 1000)


def _wait_for_login(page, timeout_s=300):
    if "login.microsoftonline.com" not in page.url:
        return
    for _ in range(timeout_s):
        page.wait_for_timeout(1000)
        if "login.microsoftonline.com" not in page.url:
            return
    raise TimeoutError(f"Still on the Microsoft login page after {timeout_s}s")


def _get_wac_frame(page):
    return next((f for f in page.frames if f.name == "WacFrame_Excel_0"), None)


def _showing_please_wait(wac):
    # .count() alone isn't enough — the element stays in the DOM hidden
    # (display:none) after the message clears rather than being removed, so
    # a plain existence check reports "still showing" forever after the
    # first real occurrence.
    try:
        loc = wac.get_by_text("Please Wait", exact=False)
        return loc.count() > 0 and loc.first.is_visible()
    except Exception:
        return False


def _click_with_retry(page, wac, click_fn, label):
    for attempt in range(PLEASE_WAIT_MAX_ATTEMPTS):
        try:
            click_fn()
            page.wait_for_timeout(1500)
            if not _showing_please_wait(wac):
                return
            reason = "hit 'Please Wait'"
        except Exception as e:
            # The target not being there/clickable yet (still loading) looks
            # the same as "Please Wait" for retry purposes — both just mean
            # the app wasn't ready, so retry rather than propagate.
            reason = f"raised {e!r}"

        print(f"'{label}' click {reason} (attempt {attempt + 1}/{PLEASE_WAIT_MAX_ATTEMPTS}), retrying in {PLEASE_WAIT_RETRY_S}s...")
        page.wait_for_timeout(PLEASE_WAIT_RETRY_S * 1000)
    raise TimeoutError(f"'{label}' never succeeded after {PLEASE_WAIT_MAX_ATTEMPTS} attempts")


def _file_menu_is_open(wac):
    loc = wac.get_by_text("Create a Copy", exact=True)
    return loc.count() > 0 and loc.first.is_visible()


def _open_file_menu_if_needed(page, wac):
    # File is a toggle, not a plain button — clicking it while the backstage
    # panel is already open (even mid-"Please Wait") closes it instead of
    # doing anything useful. Only click it when it's confirmed closed.
    if _file_menu_is_open(wac):
        return
    for f in page.frames:
        loc = f.get_by_label("File", exact=True)
        if loc.count() > 0 and loc.first.is_visible():
            loc.first.click(timeout=5000)
            return
    raise RuntimeError("Could not find the File menu in any frame")


def download_open_orders():
    os.makedirs(SHAREPOINT_RAW_DIR, exist_ok=True)
    os.makedirs(BROWSER_PROFILE_DIR, exist_ok=True)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            BROWSER_PROFILE_DIR,
            headless=False,
            accept_downloads=True,
        )
        page = context.pages[0] if context.pages else context.new_page()

        downloads = []
        page.on("download", lambda d: downloads.append(d))
        context.on("page", lambda p2: p2.on("download", lambda d: downloads.append(d)))

        _goto_with_retry(page, OPEN_ORDERS_URL)
        page.wait_for_timeout(5000)
        _wait_for_login(page)
        page.wait_for_timeout(3000)

        wac = _get_wac_frame(page)
        if wac is None:
            raise RuntimeError("Excel Online frame (WacFrame_Excel_0) never appeared")

        try:
            _click_with_retry(page, wac, lambda: _open_file_menu_if_needed(page, wac), "File")
            _click_with_retry(page, wac, lambda: wac.get_by_text("Create a Copy", exact=True).first.click(timeout=5000), "Create a Copy")
            _click_with_retry(page, wac, lambda: wac.get_by_text("Download a Copy", exact=True).first.click(timeout=5000), "Download a Copy")

            for _ in range(DOWNLOAD_TIMEOUT_S):
                if downloads:
                    break
                page.wait_for_timeout(1000)
            else:
                raise TimeoutError(f"No download started within {DOWNLOAD_TIMEOUT_S}s")
        except Exception:
            os.makedirs(DEBUG_DIR, exist_ok=True)
            page.screenshot(path=os.path.join(DEBUG_DIR, "failure.png"))
            raise

        target_path = os.path.join(SHAREPOINT_RAW_DIR, TARGET_FILENAME)
        downloads[0].save_as(target_path)
        print(f"Saved: {target_path}")

        context.close()
        return target_path


if __name__ == "__main__":
    download_open_orders()
