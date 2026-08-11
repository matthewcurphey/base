import subprocess


def run_dbt():
    print("Running dbt...")
    subprocess.run(["dbt", "run"], cwd="analytics_dbt", check=True)
    print("dbt run completed successfully!")


if __name__ == "__main__":
    run_dbt()
