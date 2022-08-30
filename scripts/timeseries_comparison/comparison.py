"""
Compare G.h data with WHO timeseries
"""
import io
import os
import json
import datetime
import logging

import boto3
import pandas as pd
import pycountry

s3 = boto3.resource("s3")

UK_COUNTRIES = ["England", "Wales", "Scotland", "Northern Ireland"]

COUNTRY_ISO3_QUIRKS = {
    "democratic republic of the congo": "COD",
    "iran": "IRN",
    "republic of congo": "COG",
    "russia": "RUS",
}


def require_env(vars: list[str]):
    "Returns required environment variables in a dictionary or aborts"
    env = {v: os.getenv(v) for v in vars}
    if absent_vars := [k for k, v in env.items() if v is None]:
        raise ValueError(f"No value specified for {', '.join(absent_vars)}")
    return env


def read_key_content(bucket: str, key: str) -> str:
    try:
        obj = s3.Object(bucket, key)
        return obj.get()["Body"].read().decode("utf-8")
    except Exception:
        logging.error(f"Could not read {bucket}:{key}")
        raise


def timeseries_by_country_confirmed(
    df: pd.DataFrame, last_date: pd.Timestamp = None
) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases by country"""

    confirmed = df[df.Status == "confirmed"]
    confirmed = confirmed.assign(
        Date=pd.to_datetime(confirmed.Date_confirmation),
        Country=confirmed.Country.replace(UK_COUNTRIES, "United Kingdom"),
    )
    dfs = []
    for country, country_df in confirmed.groupby("Country"):
        country_counts = country_df.groupby("Date").size().resample("D").sum()
        country_counts = country_counts.reindex(
            pd.date_range(country_counts.index.min(), last_date),
            fill_value=0,
        )
        dfs.append(
            pd.DataFrame([country_counts, country_counts.cumsum()])
            .T.reset_index()
            .rename(
                columns={
                    0: "GH_confirmed_cases",
                    1: "GH_cumulative_confirmed_cases",
                    "index": "Date",
                }
            )
            .assign(GH_country=country)
        )
    df = pd.concat(dfs).reset_index().drop("index", axis=1)
    df["ISO3"] = df.GH_country.map(get_country_iso3)
    return df


def who_df(who_data: dict) -> pd.DataFrame:
    df = pd.DataFrame(who_data["Data"])
    df["DATEREP"] = pd.to_datetime(df.DATEREP)
    return df.rename(
        {
            "COUNTRY": "WHO_country",
            "DATEREP": "Date",
            "TOTAL_CONFCASES": "WHO_cumulative_confirmed_cases",
            "TOTAL_PROBCASES": "WHO_cumulative_probable_cases",
            "TOTAL_ConfDeaths": "WHO_cumulative_confirmed_deaths",
            "NEW_CONFCASES": "WHO_confirmed_cases",
            "NEW_PROBCASES": "WHO_probable_cases",
            "NEW_CONFDEATHS": "WHO_confirmed_deaths",
        },
        axis=1,
    )


def get_country_iso3(country: str) -> str:
    country = country.lower()
    if country in COUNTRY_ISO3_QUIRKS:
        return COUNTRY_ISO3_QUIRKS[country]
    got_country = pycountry.countries.lookup(country)
    if not got_country:
        raise ValueError(f"Could not find country: {country}")
    else:
        return got_country.alpha_3


def merge_data(gh: pd.DataFrame, who: pd.DataFrame) -> pd.DataFrame:
    return gh.merge(who, on=["Date", "ISO3"], how="left")


def get_links_s3(bucket: str, prefix: str, suffix: str = "") -> list[str]:
    "Retrieves list of archives from S3 bucket"
    return [
        obj.key
        for obj in s3.Bucket(bucket).objects.all()
        if obj.key.startswith(prefix) and obj.key.endswith(suffix)
    ]


def most_recent_s3_keys(bucket: str, date: datetime.date) -> tuple[str, str]:
    who_s3key = max(get_links_s3(bucket, f"WHO/WHO_MPXV_{date}", ".json"))
    # check last data from yesterday
    yesterday = date - datetime.timedelta(days=1)
    gh_s3key = max(get_links_s3(bucket, f"archives/{yesterday}", ".csv"))
    return gh_s3key, who_s3key


def store(data: pd.DataFrame, bucket: str, date: datetime.date, metadata: dict) -> None:
    logging.info(f"Uploading comparison data to bucket")
    buf = io.StringIO()
    data.to_csv(buf, index=False)
    bufstr = buf.getvalue()
    try:
        s3.Object(bucket, f"timeseries-comparison/{date}.csv").put(
            Body=bufstr, ContentType="text/csv"
        )
        s3.Object(bucket, f"timeseries-comparison/{date}_metadata.json").put(
            Body=json.dumps(metadata), ContentType="application/json"
        )
    except Exception as e:
        logging.error("Exception when trying to upload WHO comparison data")


def main(
    date: datetime.date, env: dict[str, str]
) -> tuple[pd.DataFrame, dict[str, str]]:
    "Generate WHO vs G.h comparison for date, bucket parameters in env"
    logging.info("Comparing WHO vs G.h timeseries data")
    gh_key, who_key = most_recent_s3_keys(env["FETCH_BUCKET"], date)
    gh = timeseries_by_country_confirmed(
        pd.read_csv(
            io.StringIO(read_key_content(env["FETCH_BUCKET"], gh_key)), dtype=str
        ),
        date,
    )
    who = who_df(json.loads(read_key_content(env["FETCH_BUCKET"], who_key)))
    data = merge_data(gh, who)
    store(data, env["STORE_BUCKET"], {"gh_file": gh_key, "who_file": who_key})


if __name__ == "__main__":
    env = require_env(["STORE_BUCKET", "FETCH_BUCKET"])
    main(datetime.datetime.today().date(), env)
