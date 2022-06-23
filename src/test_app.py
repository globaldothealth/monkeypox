import json
import pytest
from pprint import pprint

import app

CLEANED_OUTPUT = [
    {
        "Date_confirmation": "2021-05-12",
        "Country": "England",
        "Country_ISO3": "GBR",
        "Status": "confirmed",
    },
    {
        "Date_confirmation": "2022-05-05",
        "Country": "USA",
        "Country_ISO3": "USA",
        "Status": "suspected",
    },
    {
        "Date_confirmation": "2022-01-01",
        "Country": "Spain",
        "Country_ISO3": "ESP",
        "Status": "discarded",
    },
]


@pytest.mark.parametrize(
    "source,expected",
    [
        ("England", "GBR"),
        ("Northern Ireland", "GBR"),
        ("scotland", "GBR"),
        ("WALES", "GBR"),
        ("India", "IND"),
    ],
)
def test_lookup_iso3(source: str, expected: str):
    assert app.lookup_iso3(source) == expected


def test_get_source_urls():
    input_data = [
        {"Source": "http://foo.bar", "Source_II": "http://bar.baz"},
        {"Source": "http://foo.bar", "Source_II": ""},
        {"Source_II": "http://bar.baz.top"},
    ]
    assert app.get_source_urls(input_data) == {
        "http://foo.bar",
        "http://bar.baz",
        "http://bar.baz.top",
    }


def test_clean_data():
    input_data = [
        {
            "Date_confirmation": "2021-05-12",
            "Curator_initials": "ZZ",
            "Notes": "example note",
            "Country": "England",
            "Status": "confirmed",
        },
        {
            "Date_confirmation": "2022-05-05",
            "Curator_initials": "ZZ",
            "Notes": "another example note",
            "Country": "USA",
            "Status": "suspected",
        },
        {
            "Date_confirmation": "2022-01-01",
            "Curator_initials": "ZZ",
            "Notes": "yet another example note",
            "Country": "Spain",
            "Status": "discarded",
        },
    ]

    assert app.clean_data(input_data) == CLEANED_OUTPUT


def test_format_data():
    expected_JSON = json.dumps(CLEANED_OUTPUT)
    expected_CSV = """Date_confirmation,Country,Country_ISO3,Status
2021-05-12,England,GBR,confirmed
2022-05-05,USA,USA,suspected
2022-01-01,Spain,ESP,discarded
"""
    actual_JSON, actual_CSV = app.format_data(CLEANED_OUTPUT)
    assert actual_JSON == expected_JSON
    assert actual_CSV.splitlines() == expected_CSV.splitlines()


def test_aggregate_data():
    expected_total = {"total": 2, "confirmed": 1}
    expected_country_aggregate = {
        "2022-06-05": [
            {"England": {"confirmed": 1, "suspected": 0}},
            {"USA": {"confirmed": 0, "suspected": 1}},
        ]
    }
    pprint(app.aggregate_data(CLEANED_OUTPUT, today="2022-06-06"))
    assert app.aggregate_data(CLEANED_OUTPUT, today="2022-06-05") == (
        expected_total,
        expected_country_aggregate,
    )
