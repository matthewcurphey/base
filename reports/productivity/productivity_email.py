import os
import win32com.client as win32
from reports.productivity.branch_config import BRANCHES


def productivity_email(
    output_year: int,
    output_month: int,
    subject_month: str,
    email_body: str,
    send_or_show: str = "show",
):
    """
    Send (or preview) per-branch productivity incentive emails via Outlook.

    Args:
        output_year:   e.g. 2026
        output_month:  e.g. 3
        subject_month: display string for subject line, e.g. "Mar26"
        email_body:    HTML string for the body (below the greeting)
        send_or_show:  "send" to send, anything else to Display for review
    """

    file_dir = os.path.abspath(
        os.path.join(
            "reports", "productivity", "results",
            str(output_year), f"{output_month:02d}"
        )
    )

    for org, cfg in BRANCHES.items():

        if not cfg["active"]:
            print(f"  {org} skipped (inactive)")
            continue

        attachment = os.path.join(file_dir, f"productivity_incentive_{output_year}_{output_month:02d}_{org}.xlsx")

        if not os.path.exists(attachment):
            print(f"  {org} skipped — file not found: {attachment}")
            continue

        outlook = win32.Dispatch("outlook.application")
        mail = outlook.CreateItem(0)

        mail.To      = cfg["to"]
        mail.CC      = cfg["cc"]
        mail.Subject = f"{org} - Productivity Incentive Results - {subject_month}"

        # Preserve Outlook signature
        mail.GetInspector
        idx = mail.HTMLBody.find(">", mail.HTMLBody.find("<body"))
        greeting_html = f'<p style="font-family: Calibri;">{cfg["greeting"]}</p>'
        mail.HTMLBody = mail.HTMLBody[:idx + 1] + greeting_html + email_body + mail.HTMLBody[idx + 1:]

        mail.Attachments.Add(Source=attachment)

        if send_or_show == "send":
            mail.Send()
            print(f"  {org} sent")
        else:
            mail.Display(True)
            print(f"  {org} displayed")
