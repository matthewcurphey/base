from pipelines.run_all_ingest import run_all_ingestions
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


EMAIL_BODY = """<div style="font-family: Calibri;">
    <p>Attached are your Productivity Incentive Results.</p>
    <ul>
    <li>For US/CAN branches: employees should receive their payments this week. Please communicate the results with your branch.</li>
    <li>International branches, please arrange for the USD amount given in the 'worked' tab, column L, to be paid ASAP.</li>
    </ul>
    <p>Please note that whilst all hours worked for the month are included in the files regardless of employment status, any employees no longer with the business will not receive a payout.</p>
    <p>For any payment related queries, please contact mreilly@amcastle.com directly.</p>
    <p>Thanks</p>
</div>"""


def run_productivity_output(year: int, month: int):
    from reports.productivity.productivity_output import productivity_output
    print(f"Generating Excel files for {year}-{month:02d}...")
    productivity_output(year, month)


def run_productivity_email(year: int, month: int, subject_month: str, send_or_show: str = "show"):
    from reports.productivity.productivity_email import productivity_email
    print(f"Sending emails ({send_or_show})...")
    productivity_email(year, month, subject_month, EMAIL_BODY, send_or_show)


if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if task == "ingest":
        run_all_ingestions()

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

    else:
        print(f"❌ Unknown task: {task}")





