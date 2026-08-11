"""
Automates the interactive Entra ID sign-in popup that ODBC Driver 18's
ActiveDirectoryInteractive authentication spawns for the Fabric Lakehouse
connection ("Authenticate to database on ..."). The popup is an embedded
IE control (BasicEmbeddedBrowser / Internet Explorer_Server), not a
browser window Playwright can attach to — it's driven via Windows UI
Automation (pywinauto) instead.

On a trusted, already-signed-in machine this is genuinely just an email
field and a Next button — Windows Hello/PRT-based SSO completes the sign-in
silently with no password or MFA prompt, closing the popup within ~1
second of clicking Next. If SSO isn't available (new machine, expired
token, etc.) the popup will instead show a password or MFA step that
this won't complete — it's a best-effort automation of the common case,
not a replacement for a human when something is genuinely amiss.
"""
import time

import comtypes
from pywinauto import Desktop

AUTH_WINDOW_PREFIX = "Authenticate to database"
EMAIL_FIELD_ID = "emailTextInput"
NEXT_BUTTON_ID = "nextButton"


def _find_auth_window(timeout_s):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        wins = Desktop(backend="uia").windows()
        match = [w for w in wins if w.window_text().startswith(AUTH_WINDOW_PREFIX)]
        if match:
            return match[0]
        time.sleep(0.5)
    return None


def _fill_and_submit(win, email):
    # win.set_focus() (pywinauto's own UIA-based focus/activation) is what
    # actually works here — hand-rolled SetForegroundWindow/AttachThreadInput/
    # topmost-toggle attempts all made things *worse*, not better.
    #
    # set_edit_text() also has to be avoided, even after a real click: it
    # sets the raw value via UIA's ValuePattern without firing the browser's
    # actual input/keydown events, so this page's floating-label state and
    # its own JS validation never see it as typed — the field ends up with
    # the value physically present but the page still insisting it's empty
    # or invalid. type_keys() simulates genuine keystrokes instead, which
    # this page actually reacts to like a real user typing.
    win.set_focus()
    time.sleep(0.3)
    descendants = win.descendants()

    email_field = next(d for d in descendants if d.element_info.automation_id == EMAIL_FIELD_ID)
    email_field.click_input()
    email_field.type_keys("^a{DEL}", pause=0.05)
    email_field.type_keys(email, with_spaces=True, pause=0.03)

    next_btn = next(d for d in descendants if d.element_info.automation_id == NEXT_BUTTON_ID)
    next_btn.click_input()


def auto_complete_login(email, window_timeout_s=30):
    """
    Watches for the Entra ID sign-in popup and completes it automatically.
    Meant to run as a separate OS process started just before pyodbc.connect()
    — the popup only exists for the duration of that blocking call, so this
    has to be racing it concurrently rather than running after.

    This has to be a separate process, not a background thread in the same
    process as the pyodbc.connect() call — empirically, driving the popup
    from a thread inside that same process fails silently every time
    (clicks/set_edit_text report success but never take), while an
    independent process targeting the same already-open popup works
    reliably. Likely a COM/UI-Automation apartment conflict with whatever
    the ODBC driver's own broker is doing on that process's threads.
    """
    comtypes.CoInitialize()
    try:
        win = _find_auth_window(window_timeout_s)
        if win is None:
            # No popup appeared — e.g. a session token was already cached
            # and pyodbc.connect() will just return without one.
            return
        try:
            _fill_and_submit(win, email)
        except Exception as e:
            # Best-effort: if the popup's layout isn't what we expect (a
            # real password/MFA prompt, a changed control ID, etc.) leave
            # it alone rather than crash the connection attempt — a human
            # watching the screen can still complete it manually. Still
            # surface it, since a silent failure here just looks like a
            # hang otherwise.
            print(f"[lakehouse_auth] auto-login step failed, left for manual completion: {e!r}")
    finally:
        comtypes.CoUninitialize()


if __name__ == "__main__":
    import sys
    auto_complete_login(sys.argv[1])
