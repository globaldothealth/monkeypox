"""
Use WHO timeseries for Map

Outputs (in aggregates bucket):
├── country
│   └── latest.json
├── timeseries
│   ├── timeseries-confirmed.csv
│   ├── timeseries-country-confirmed.csv
│   ├── confirmed.json
│   └── country_confirmed.json
└── total
    └── latest.json
"""
import os
import io
import json
import logging

import boto3
import requests
import pandas as pd

WHO_URL = os.getenv(
    "WHO_URL"
)  # https://extranet.who.int/publicemergency/api/Monkeypox/
S3_BUCKET = os.getenv("S3_BUCKET")
LOCALSTACK_URL = os.getenv("LOCALSTACK_URL")


def fetch_who() -> pd.DataFrame:
    if WHO_URL is None:
        raise ValueError("Missing required environment variable WHO_URL")
    try:
        res = requests.post(url=WHO_URL, json={})
    except ConnectionError:
        logging.error(f"Failed to fetch data from {WHO_URL}")
    if res.status_code != 200:
        logging.error(f"HTTP request failed with code {res.status_code}")
    data = res.json()
    df = pd.DataFrame(data["Data"])
    assert {
        "DATEREP",
        "NEW_CONFCASES",
        "NEW_CONFDEATHS",
        "NEW_PROBCASES",
        "TOTAL_CONFCASES",
        "TOTAL_PROBCASES",
    } <= set(df.columns)
    df["DATEREP"] = pd.to_datetime(df.DATEREP)
    return df


def to_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", date_format="iso", indent=2)


def to_csv(df: pd.DataFrame) -> str:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def country_counts(df: pd.DataFrame) -> dict[str]:
    "Returns latest country counts of confirmed and suspected (probable) cases"

    data = []
    last_updated = df.DATEREP.max()
    for country_iso3, country_iso3_data in df.groupby("ISO3"):
        last_row = country_iso3_data[
            country_iso3_data.DATEREP == country_iso3_data.DATEREP.max()
        ]
        data.append(
            {
                country_iso3: {
                    "confirmed": last_row.TOTAL_CONFCASES.values[0],
                    "suspected": last_row.TOTAL_PROBCASES.values[0],
                }
            }
        )
    return {last_updated.date().isoformat(): data}


def total_counts(df: pd.DataFrame) -> dict[str]:
    "Returns latest total counts of confirmed and suspected (probable) cases"
    confirmed = df.NEW_CONFCASES.sum()
    return {"total": confirmed + df.NEW_PROBCASES.sum(), "confirmed": confirmed}


def by_confirmed(df: pd.DataFrame) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases"""

    counts = df.groupby("DATEREP").NEW_CONFCASES.agg("sum")
    deaths = df.groupby("DATEREP").NEW_CONFDEATHS.agg("sum")
    mindate, maxdate = counts.index.min(), counts.index.max()
    counts = counts.reindex(pd.date_range(mindate, maxdate), fill_value=0)
    deaths = deaths.reindex(pd.date_range(mindate, maxdate), fill_value=0)
    return (
        pd.DataFrame(
            [
                counts.rename("Cases"),
                counts.cumsum().rename("Cumulative_cases"),
                deaths.rename("Deaths"),
                deaths.cumsum().rename("Cumulative_deaths"),
            ]
        )
        .T.reset_index()
        .rename(columns={"index": "Date"})
    )


def by_country_confirmed(df: pd.DataFrame) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases by country"""

    dfs = []
    for country, country_df in df.groupby("ISO3"):
        dfs.append(by_confirmed(country_df).assign(Country_ISO3=country))
    return pd.concat(dfs).reset_index().drop("index", axis=1)


def store(bucket: str, key: str, data: str):
    "Stores data in S3 bucket with key"
    try:
        s3 = boto3.resource("s3")
        if LOCALSTACK_URL:
            s3 = boto3.resource("s3", endpoint_url=LOCALSTACK_URL)
        s3.Object(bucket, key).put(Body=data)
    except Exception:
        logging.exception(f"An exception occurred while trying to upload {key}")
        raise


if __name__ == "__main__":
    if S3_BUCKET is None:
        raise ValueError("Missing required environment variable S3_BUCKET")
    who = fetch_who()
    df_confirmed = by_confirmed(who)
    df_country_confirmed = by_country_confirmed(who)

    store(S3_BUCKET, "total/latest.json", json.dumps(total_counts(who)))
    store(S3_BUCKET, "country/latest.json", json.dumps(country_counts(who)))
    store(S3_BUCKET, "timeseries/timeseries-confirmed.csv", to_csv(df_confirmed))
    store(
        S3_BUCKET,
        "timeseries/timeseries-country-confirmed.csv",
        to_csv(df_country_confirmed),
    )
    store(S3_BUCKET, "timeseries/confirmed.json", to_json(df_confirmed))
    store(
        S3_BUCKET,
        "timeseries/country_confirmed.json",
        to_json(df_country_confirmed),
    )
