import os
from datetime import date

import win32com.client as win32

# TODO: add more recipients once this is validated — single recipient for now
RECIPIENTS = "mcurphey@amcastle.com"

SHAREPOINT_LINK = "https://amcastle.sharepoint.com/:x:/s/Analytics_ETL/IQDTz5ZPOa4sSoDsdsp5vwERAdpTwZQr5kPnU5fafdS2wnA?e=jpRFI0"

TREND_CHART_PATH = os.path.join("reports", "mcmaster", "backlog_trend.png")
STATUS_CHART_PATH = os.path.join("reports", "mcmaster", "status_chart.png")

# MAPI property tag used to give an attachment a Content-ID so it can be
# referenced inline via cid: in the HTML body instead of showing as a
# regular file attachment.
PR_ATTACH_CONTENT_ID = "http://schemas.microsoft.com/mapi/proptag/0x3712001F"


def mcmaster_email(send_or_show: str = "show"):
    """
    Daily McMaster backlog email. SharePoint link at the top, then the two
    summary charts (trend, status) inline — no tables, no commentary. Same
    principle as the report itself: boring, factual, automated.
    """
    outlook = win32.Dispatch("outlook.application")
    mail = outlook.CreateItem(0)

    mail.To = RECIPIENTS
    mail.Subject = f"McMaster Backlog Report — {date.today():%d-%b-%Y}"

    mail.HTMLBody = f"""<div style="font-family: Calibri; font-size: 11pt;">
<p style="font-size: 16pt; font-weight: bold;"><a href="{SHAREPOINT_LINK}">McMaster Report - SharePoint</a></p>
<p><img src="cid:mcmaster_trend_chart" width="450"></p>
<p><img src="cid:mcmaster_status_chart" width="450"></p>
</div>"""

    for path, cid in [(TREND_CHART_PATH, "mcmaster_trend_chart"), (STATUS_CHART_PATH, "mcmaster_status_chart")]:
        attachment = mail.Attachments.Add(Source=os.path.abspath(path))
        attachment.PropertyAccessor.SetProperty(PR_ATTACH_CONTENT_ID, cid)

    if send_or_show == "send":
        mail.Send()
        print("Email sent")
    else:
        mail.Display(True)
        print("Email displayed")
