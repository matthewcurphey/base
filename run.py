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


if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if task == "ingest":
        run_all_ingestions()

    elif task == "outputs":
        run_outputs()

    elif task == "custom":
        run_custom()

    else:
        print(f"❌ Unknown task: {task}")





