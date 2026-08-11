import re

# Outlook folder that receives ONLY Oracle Workflow Notification emails for
# the hourly report batch (6 inventory reports, 1 open orders report).
REPORTS_FOLDER_NAME = "Reports"

# Report link format: a one-time-use Oracle EBS "Web Report Review" URL.
# No login needed, but each temp_id can only be fetched successfully once —
# a second fetch of the same URL fails.
REPORT_URL_RE = re.compile(r"http://erpprod\.amcastle\.com:8000/OA_CGI/FNDWRR\.exe\?temp_id=\d+")

INVENTORY_SUBJECT = "AMC AIRBUS Forecast Inventory Extract Plus"
OPEN_ORDERS_SUBJECT = "AMC Open Orders Report by Shipping Org"

# The inventory report's subject line is identical for all 6 orgs — the org
# is only knowable from the report's own content (a city name, consistently
# in the same column across every row of that file).
CITY_TO_ORG = {
    "Cleveland": "CLE",
    "Dallas": "DAL",
    "Los Angeles": "LOS",
    "Janesville": "JVL",
    "Wichita East": "WIE",
    "Kennesaw": "ATL",
}
