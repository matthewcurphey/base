import requests
import pandas as pd

ips = [
    "172.16.13.32",
    "172.16.10.32",
    # add more here
]

def fetch_asset_name(ip):
    url = f"http://{ip}/rest/cpe/attributes"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()["asset_name"]


def fetch_vorne_table(ip, table_name="production_metric"):
    base_url = f"http://{ip}/api/v0/tables/{table_name}"

    # Get asset name
    asset_name = fetch_asset_name(ip)

    # Get column headers
    headers_response = requests.get(base_url, timeout=5)
    headers_response.raise_for_status()
    header_json = headers_response.json()

    column_meta = header_json["data"]["columns"]
    column_names = [col["name"] for col in column_meta]

    # Get records
    records_response = requests.get(f"{base_url}/records", timeout=10)
    records_response.raise_for_status()
    records_json = records_response.json()

    records = records_json["data"]["records"]

    if not records:
        print(f"No records returned from {ip}")
        return None

    # Build DataFrame (positional mapping)
    df = pd.DataFrame(records, columns=column_names)

    # Add asset column
    df.insert(0, "asset_name", asset_name)

    return df


# =============================
# MAIN UNION LOGIC
# =============================
def extract_vorne():
    all_dfs = []

    for ip in ips:
        try:
            print(f"\nPulling data from {ip}...")

            df = fetch_vorne_table(ip)

            if df is not None:
                all_dfs.append(df)
                print(f"Success: {ip} | Rows: {len(df)}")
            else:
                print(f"No data for {ip}")

        except Exception as e:
            print(f"Skipped {ip} — Error: {e}")
            continue


    # =============================
    # UNION + EXPORT
    # =============================

    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        return df



    else:
        print("No data retrieved from any device.")
