import io
import json

import pandas as pd
from pandas import Timestamp

import comparison

TODAY = Timestamp(2022, 6, 9)

DATA = pd.read_csv("gh-timeseries.csv")

with open("who-endpoint-response.json") as fp:
    WHO_DATA = json.load(fp)

WHO_DATAFRAME = pd.read_csv("who.csv", parse_dates=["Date"])

BY_COUNTRY_CONFIRMED = [
    {
        "Date": Timestamp("2022-06-02 00:00:00"),
        "GH_confirmed_cases": 1,
        "GH_cumulative_confirmed_cases": 1,
        "GH_country": "USA",
        "ISO3": "USA",
    },
    {
        "Date": Timestamp("2022-06-03 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 1,
        "GH_country": "USA",
        "ISO3": "USA",
    },
    {
        "Date": Timestamp("2022-06-04 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 1,
        "GH_country": "USA",
        "ISO3": "USA",
    },
    {
        "Date": Timestamp("2022-06-05 00:00:00"),
        "GH_confirmed_cases": 4,
        "GH_cumulative_confirmed_cases": 5,
        "GH_country": "USA",
        "ISO3": "USA",
    },
    {
        "Date": Timestamp("2022-06-06 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 5,
        "GH_country": "USA",
        "ISO3": "USA",
    },
    {
        "Date": Timestamp("2022-06-07 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 5,
        "GH_country": "USA",
        "ISO3": "USA",
    },
    {
        "Date": Timestamp("2022-06-08 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 5,
        "GH_country": "USA",
        "ISO3": "USA",
    },
    {
        "Date": Timestamp("2022-06-09 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 5,
        "GH_country": "USA",
        "ISO3": "USA",
    },
    {
        "Date": Timestamp("2022-06-01 00:00:00"),
        "GH_confirmed_cases": 1,
        "GH_cumulative_confirmed_cases": 1,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
    {
        "Date": Timestamp("2022-06-02 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 1,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
    {
        "Date": Timestamp("2022-06-03 00:00:00"),
        "GH_confirmed_cases": 3,
        "GH_cumulative_confirmed_cases": 4,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
    {
        "Date": Timestamp("2022-06-04 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 4,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
    {
        "Date": Timestamp("2022-06-05 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 4,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
    {
        "Date": Timestamp("2022-06-06 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 4,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
    {
        "Date": Timestamp("2022-06-07 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 4,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
    {
        "Date": Timestamp("2022-06-08 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 4,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
    {
        "Date": Timestamp("2022-06-09 00:00:00"),
        "GH_confirmed_cases": 0,
        "GH_cumulative_confirmed_cases": 4,
        "GH_country": "United Kingdom",
        "ISO3": "GBR",
    },
]


def test_by_country_confirmed():
    assert (
        comparison.timeseries_by_country_confirmed(DATA, TODAY).to_dict("records")
        == BY_COUNTRY_CONFIRMED
    )


def test_who_df():
    assert comparison.who_df(WHO_DATA).equals(WHO_DATAFRAME)


def test_merge():
    merged = pd.read_csv("merged-who-gh.csv", parse_dates=["Date"])
    assert comparison.merge_data(
        pd.DataFrame(BY_COUNTRY_CONFIRMED), WHO_DATAFRAME
    ).equals(merged)
