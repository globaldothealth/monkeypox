import csv
import os
import io

import boto3
import pytest
import pandas as pd

from pandas import Timestamp

from run import (
    fetch_who,
    country_counts,
    total_counts,
    by_confirmed,
    by_country_confirmed,
)


def read_who_data(data: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(data))
    df["DATEREP"] = pd.to_datetime(df.DATEREP)
    return df


LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")
WHO_CSV = (
    "COUNTRY,ISO3,WHO_REGION,DATEREP,TOTAL_CONFCASES,TOTAL_PROBCASES,TOTAL_ConfDeaths,NEW_CONFCASES,NEW_PROBCASES,NEW_CONFDEATHS\n"
    "Argentina,ARG,AMRO,03 Jun 2022,2,0,0,2,0,0\n"
    "Australia,AUS,WPRO,20 May 2022,2,0,0,2,0,0\n"
    "Australia,AUS,WPRO,02 Jun 2022,3,0,0,1,0,0\n"
    "Australia,AUS,WPRO,03 Jun 2022,5,0,0,2,0,0\n"
    "Australia,AUS,WPRO,04 Jun 2022,5,1,1,0,1,1\n"
)
WHO_DATA = read_who_data(WHO_CSV)


def get_contents(file_name: str) -> str:
    s3 = boto3.resource("s3", endpoint_url=LOCALSTACK_URL)
    obj = s3.Object(S3_BUCKET, file_name)
    return obj.get()["Body"].read().decode("utf-8")


def is_valid_csv(data: str) -> bool:
    reader = csv.DictReader(data)
    for row in reader:
        if len(row) == 0:
            return False
    return True


@pytest.mark.skipif(
    not os.environ.get("DOCKERIZED", False),
    reason="Running e2e tests outside of mock environment disabled",
)
def test_fetch_who():
    df = fetch_who()
    assert df.equals(WHO_DATA)


def test_country_counts():
    assert country_counts(WHO_DATA) == {
        "2022-06-04": [
            {"ARG": {"confirmed": 2, "suspected": 0}},
            {"AUS": {"confirmed": 5, "suspected": 1}},
        ]
    }


def test_total_counts():
    assert total_counts(WHO_DATA) == {"confirmed": 7, "total": 8}


def test_by_confirmed():
    assert by_confirmed(WHO_DATA).to_dict(orient="records") == [
        {
            "Date": Timestamp("2022-05-20 00:00:00"),
            "Cases": 2,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-21 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-22 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-23 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-24 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-25 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-26 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-27 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-28 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-29 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-30 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-05-31 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-06-01 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-06-02 00:00:00"),
            "Cases": 1,
            "Cumulative_cases": 3,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-06-03 00:00:00"),
            "Cases": 4,
            "Cumulative_cases": 7,
            "Deaths": 0,
            "Cumulative_deaths": 0,
        },
        {
            "Date": Timestamp("2022-06-04 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 7,
            "Deaths": 1,
            "Cumulative_deaths": 1,
        },
    ]


def test_by_country_confirmed():
    assert by_country_confirmed(WHO_DATA).to_dict(orient="records") == [
        {
            "Date": Timestamp("2022-06-03 00:00:00"),
            "Cases": 2,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "ARG",
        },
        {
            "Date": Timestamp("2022-05-20 00:00:00"),
            "Cases": 2,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-21 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-22 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-23 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-24 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-25 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-26 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-27 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-28 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-29 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-30 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-05-31 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-06-01 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 2,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-06-02 00:00:00"),
            "Cases": 1,
            "Cumulative_cases": 3,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-06-03 00:00:00"),
            "Cases": 2,
            "Cumulative_cases": 5,
            "Deaths": 0,
            "Cumulative_deaths": 0,
            "Country_ISO3": "AUS",
        },
        {
            "Date": Timestamp("2022-06-04 00:00:00"),
            "Cases": 0,
            "Cumulative_cases": 5,
            "Deaths": 1,
            "Cumulative_deaths": 1,
            "Country_ISO3": "AUS",
        },
    ]
