import os

# Target file lives in a colleague's OneDrive, shared with this account for
# editing — not something this repo owns, so the URL is the only way in
# (there's no local sync path). Source: the "All open orders.xlsx" internet
# shortcut in this OneDrive root, which just points here.
OPEN_ORDERS_URL = (
    "https://amcastle-my.sharepoint.com/personal/jbates_amcastle_com/Documents/"
    "All%20open%20orders.xlsx?web=1"
)

# Playwright persistent browser profile — log into Microsoft 365 here once
# (headed run) and the session cookie is reused on every future run, same
# tenant as everything else (Banner's Entra ID, etc).
BROWSER_PROFILE_DIR = os.path.join("etl", "extract", "sharepoint", ".browser_profile")
