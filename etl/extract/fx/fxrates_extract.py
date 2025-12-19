import requests
import pandas as pd
from datetime import date


def extract_fxrates(
    start_date: date,
    end_date: date,
    target_currencies: list[str] = ["EUR", "MXN"],
    source_name: str = "frankfurter.ecb"
) -> pd.DataFrame:

    url = f"https://api.frankfurter.app/{start_date.isoformat()}..{end_date.isoformat()}"

    params = {
        "from": "USD",
        "to": ",".join(target_currencies),
    }

    resp = requests.get(
        url,
        params=params,
        timeout=30,
        verify=False  # TEMP: corporate SSL inspection
    )
    resp.raise_for_status()

    payload = resp.json()

    if "rates" not in payload:
        raise ValueError(f"Unexpected FX response: {payload}")

    rows = []

    for fx_date, day_rates in payload["rates"].items():
        for currency, usd_to_cur in day_rates.items():
            if usd_to_cur in (None, 0):
                continue

            rows.append({
                "fx_date": pd.to_datetime(fx_date).date(),
                "from_currency": currency,
                "to_currency": "USD",
                "fx_rate": round(1 / usd_to_cur, 8),  # invert USD→CUR
                "source": source_name
            })

    return pd.DataFrame(rows)




