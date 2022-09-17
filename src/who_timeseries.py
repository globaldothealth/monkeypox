"""
Use WHO timeseries, but output in CSV format

Based on code by @tannervarrelman

"""
import io
import logging
from pprint import pprint

import requests
import pandas as pd

today = pd.Timestamp.today()

WHO_ENDPOINT = "https://extranet.who.int/publicemergency/api/Monkeypox/"


def fetch_who() -> pd.DataFrame:
    try:
        res = requests.post(url=WHO_ENDPOINT, json={})
    except ConnectionError:
        logging.error(f"Failed to fetch data from {WHO_ENDPOINT}")
    if res.status_code != 200:
        logging.error(f"HTTP request failed with code {res.status_code}")
    data = res.json()
    df = pd.DataFrame(data["Data"])
    df["DATEREP"] = pd.to_datetime(df.DATEREP)
    return df


def to_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", date_format="iso", indent=2)


def to_csv(df: pd.DataFrame) -> str:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def by_confirmed(df: pd.DataFrame, last_date: pd.Timestamp = None) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases"""

    last_date = last_date or today
    counts = df.groupby("DATEREP").NEW_CONFCASES.agg("sum")
    counts = counts.reindex(pd.date_range(counts.index.min(), last_date), fill_value=0)
    counts = counts.rename("Cases")
    cumulative_counts = counts.cumsum().rename("Cumulative_cases")
    return (
        pd.DataFrame([counts, cumulative_counts])
        .T.reset_index()
        .rename(columns={"index": "Date"})
    )


def by_country_confirmed(
    df: pd.DataFrame, last_date: pd.Timestamp = None
) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases by country"""

    last_date = last_date or today
    dfs = []
    for country, country_df in df.groupby("ISO3"):
        country_counts = country_df.groupby("DATEREP").size().resample("D").sum()
        country_counts = country_counts.reindex(
            pd.date_range(country_counts.index.min(), last_date),
            fill_value=0,
        )
        dfs.append(
            pd.DataFrame(
                [
                    country_counts.rename("Cases"),
                    country_counts.cumsum().rename("Cumulative_cases"),
                ]
            )
            .T.reset_index()
            .rename(columns={"index": "Date"})
            .assign(Country_ISO3=country)
        )
    return pd.concat(dfs).reset_index().drop("index", axis=1)


if __name__ == "__main__":
    df = fetch_who()
    print(by_country_confirmed(df))
