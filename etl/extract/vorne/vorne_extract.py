
import requests
import pandas as pd
from requests.exceptions import Timeout, ConnectionError

ips = [
    "172.16.11.32",
    "172.16.11.34",
    "172.16.11.36",
    "172.16.11.38",
    "172.16.11.40",
    "172.16.11.42",
    "172.16.11.44",
    "172.16.11.46",
    "172.16.11.48",
    "172.16.11.50",
    "172.16.11.52",
    "172.16.10.32",
    "172.16.10.34",
    "172.16.10.40",
    "172.16.10.38",
    "172.16.10.36",
    "172.16.10.42",
    "172.16.13.32",
    "172.16.13.34",
    "172.16.13.36",
    "172.16.13.38",
    "172.16.13.40",
    "172.16.13.42",
    "172.16.13.44",
    "172.16.13.46",
    "172.16.12.32",
    "172.16.15.32",
    "172.16.15.34",
    "172.16.15.36",
    "172.16.15.38",
    "172.16.15.40",
    "172.16.15.42",
]

CONNECT_TIMEOUT = 8
READ_TIMEOUT = 25
MAX_RETRIES = 2


def safe_get(session, url):
    for attempt in range(MAX_RETRIES + 1):
        try:
            return session.get(
                url,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )
        except (Timeout, ConnectionError) as e:
            if attempt == MAX_RETRIES:
                raise
            print(f"Retrying {url} ({attempt+1}/{MAX_RETRIES})...")


def fetch_asset_name(session, ip):
    url = f"http://{ip}/rest/cpe/attributes"
    response = safe_get(session, url)
    response.raise_for_status()
    return response.json()["asset_name"]


def fetch_vorne_table(session, ip, table_name="production_metric"):
    base_url = f"http://{ip}/api/v0/tables/{table_name}"

    asset_name = fetch_asset_name(session, ip)

    headers_response = safe_get(session, base_url)
    headers_response.raise_for_status()
    header_json = headers_response.json()

    column_meta = header_json["data"]["columns"]
    column_names = [col["name"] for col in column_meta]

    records_response = safe_get(session, f"{base_url}/records")
    records_response.raise_for_status()
    records_json = records_response.json()

    records = records_json["data"]["records"]

    if not records:
        print(f"No records returned from {ip}")
        return None

    df = pd.DataFrame(records, columns=column_names)
    df.insert(0, "asset_name", asset_name)
    df.insert(0, "ip_address", ip)

    return df


def extract_vorne():
    all_dfs = []

    with requests.Session() as session:

        for ip in ips:
            try:
                print(f"\nPulling data from {ip}...")

                df = fetch_vorne_table(session, ip)

                if df is not None:
                    all_dfs.append(df)
                    print(f"Success: {ip} | Rows: {len(df)}")
                else:
                    print(f"No data for {ip}")

            except Exception as e:
                print(f"Skipped {ip} — Error: {e}")
                continue

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        print("No data retrieved from any device.")
        return None