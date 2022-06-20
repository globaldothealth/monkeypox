import io

import pandas as pd
from pandas import Timestamp

import timeseries

DATA = pd.read_csv(
    io.StringIO(
        """
Status,Date_confirmation,Country
confirmed,2022-06-01,England
confirmed,2022-06-03,England
confirmed,2022-06-03,England
confirmed,2022-06-03,England
confirmed,2022-06-02,USA
confirmed,2022-06-05,USA
confirmed,2022-06-05,USA
confirmed,2022-06-05,USA
confirmed,2022-06-05,USA
suspected,2022-06-05,USA
discarded,2022-06-07,USA
"""
    )
)

BY_CONFIRMED = [
    {"Date": Timestamp("2022-06-01 00:00:00"), "Cases": 1, "Cumulative_cases": 1},
    {"Date": Timestamp("2022-06-02 00:00:00"), "Cases": 1, "Cumulative_cases": 2},
    {"Date": Timestamp("2022-06-03 00:00:00"), "Cases": 3, "Cumulative_cases": 5},
    {"Date": Timestamp("2022-06-04 00:00:00"), "Cases": 0, "Cumulative_cases": 5},
    {"Date": Timestamp("2022-06-05 00:00:00"), "Cases": 4, "Cumulative_cases": 9},
]

BY_COUNTRY_CONFIRMED = [
    {
        "Date": Timestamp("2022-06-01 00:00:00"),
        "Cases": 1,
        "Cumulative_cases": 1,
        "Country": "England",
    },
    {
        "Date": Timestamp("2022-06-02 00:00:00"),
        "Cases": 0,
        "Cumulative_cases": 1,
        "Country": "England",
    },
    {
        "Date": Timestamp("2022-06-03 00:00:00"),
        "Cases": 3,
        "Cumulative_cases": 4,
        "Country": "England",
    },
    {
        "Date": Timestamp("2022-06-02 00:00:00"),
        "Cases": 1,
        "Cumulative_cases": 1,
        "Country": "USA",
    },
    {
        "Date": Timestamp("2022-06-03 00:00:00"),
        "Cases": 0,
        "Cumulative_cases": 1,
        "Country": "USA",
    },
    {
        "Date": Timestamp("2022-06-04 00:00:00"),
        "Cases": 0,
        "Cumulative_cases": 1,
        "Country": "USA",
    },
    {
        "Date": Timestamp("2022-06-05 00:00:00"),
        "Cases": 4,
        "Cumulative_cases": 5,
        "Country": "USA",
    },
]


def test_by_confirmed():
    assert timeseries.by_confirmed(DATA).to_dict("records") == BY_CONFIRMED


def test_by_country_confirmed():
    assert (
        timeseries.by_country_confirmed(DATA).to_dict("records") == BY_COUNTRY_CONFIRMED
    )
