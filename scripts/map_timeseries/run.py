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
import sys
import json
import logging

import boto3
import requests
import pymongo
import pandas as pd


WHO_URL = os.getenv(
    "WHO_URL"
)  # https://extranet.who.int/publicemergency/api/Monkeypox/
S3_BUCKET = os.getenv("S3_BUCKET")
LOCALSTACK_URL = os.getenv("LOCALSTACK_URL")

DB_CONNECTION = os.getenv("DB_CONNECTION")
DATABASE_NAME = os.getenv("DATABASE_NAME")
TIMESERIES_COLLECTION = os.getenv("TIMESERIES_COLLECTION", "who_timeseries")


def setup_logger() -> None:
    h = logging.StreamHandler(sys.stdout)
    rootLogger = logging.getLogger()
    rootLogger.addHandler(h)
    rootLogger.setLevel(logging.INFO)


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


def subdict(d: dict, keys: list[str]):
    "Returns sub-dictionary with a subset of keys"
    assert set(keys) <= set(d)
    return {k: d[k] for k in keys}


def store_as_collection(df: pd.DataFrame, db_connection: str, db_name: str, collection_name: str):
    "Store dataframe in collection"
    date_fields = list(df.select_dtypes("datetime").columns)
    collection = pymongo.MongoClient(db_connection)[db_name][collection_name]
    for row in df.to_dict(orient="records"):
        for field in date_fields:
            row[field] = row[field].to_pydatetime()
        try:
            collection.replace_one(subdict(row, ["ISO3", "DATEREP"]), row, upsert=True)
        except Exception:
            logging.exception(f"Exception when trying to insert to {collection_name}")
            raise


def get_country_counts(df: pd.DataFrame) -> str:
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
                    "confirmed": int(last_row.TOTAL_CONFCASES.values[0]),
                    "suspected": int(last_row.TOTAL_PROBCASES.values[0]),
                }
            }
        )
    return json.dumps({last_updated.date().isoformat(): data})


def get_total_counts(df: pd.DataFrame) -> str:
    "Returns latest total counts of confirmed and suspected (probable) cases"
    confirmed = int(df.NEW_CONFCASES.sum())
    suspected = int(df.NEW_PROBCASES.sum())
    return json.dumps({"total": confirmed + suspected, "confirmed": confirmed})


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
    setup_logger()
    if S3_BUCKET is None:
        raise ValueError("Missing required environment variable S3_BUCKET")
    who = fetch_who()
    store_as_collection(who, DB_CONNECTION, DATABASE_NAME, TIMESERIES_COLLECTION)
    confirmed = by_confirmed(who)
    country_confirmed = by_country_confirmed(who)
    confirmed_csv = confirmed.to_csv(index=False)
    confirmed_json = confirmed.to_json(orient="records", date_format="iso", indent=2)
    country_csv = country_confirmed.to_csv(index=False)
    country_json = country_confirmed.to_json(orient="records", date_format="iso", indent=2)
    who_total_json = get_total_counts(who)
    who_country_json = get_country_counts(who)

    store(S3_BUCKET, "total/latest.json", who_total_json)
    store(S3_BUCKET, "country/latest.json", who_country_json)
    store(S3_BUCKET, "timeseries/timeseries-confirmed.csv", confirmed_csv)
    store(
        S3_BUCKET,
        "timeseries/timeseries-country-confirmed.csv",
        country_csv,
    )
    store(S3_BUCKET, "timeseries/confirmed.json", confirmed_json)
    store(
        S3_BUCKET,
        "timeseries/country_confirmed.json",
        country_json,
    )
