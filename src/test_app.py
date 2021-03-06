import json
import pytest
from pprint import pprint

import app

CLEANED_OUTPUT = [
    {
        "ID": "N1",
        "Date_confirmation": "2021-05-12",
        "Country": "England",
        "Country_ISO3": "GBR",
        "Status": "confirmed",
    },
    {
        "ID": "N2",
        "Date_confirmation": "2022-05-05",
        "Country": "USA",
        "Country_ISO3": "USA",
        "Status": "suspected",
    },
    {
        "ID": "N3",
        "Date_confirmation": "2022-01-01",
        "Country": "Spain",
        "Country_ISO3": "ESP",
        "Status": "discarded",
    },
    {
        "ID": "N4",
        "Date_confirmation": "2022-03-03",
        "Country": "Australia",
        "Country_ISO3": "AUS",
        "Status": "omit_error",
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
            "ID": 1,
            "Date_confirmation": "2021-05-12",
            "Curator_initials": "ZZ",
            "Notes": "example note",
            "Country": "England",
            "Status": "confirmed",
        },
        {
            "ID": 2,
            "Date_confirmation": "2022-05-05",
            "Curator_initials": "ZZ",
            "Notes": "another example note",
            "Country": "USA",
            "Status": "suspected",
        },
        {
            "ID": 3,
            "Date_confirmation": "2022-01-01",
            "Curator_initials": "ZZ",
            "Notes": "yet another example note",
            "Country": "Spain",
            "Status": "discarded",
        },
        {
            "ID": 4,
            "Date_confirmation": "2022-03-03",
            "Curator_initials": "ZZ",
            "Notes": "yet another example note",
            "Country": "Australia",
            "Status": "omit_error",
        },
    ]

    assert app.clean_data(input_data, id_prefix="N") == CLEANED_OUTPUT


def test_format_data():
    expected_JSON = json.dumps(CLEANED_OUTPUT)
    expected_CSV = """ID,Date_confirmation,Country,Country_ISO3,Status
N1,2021-05-12,England,GBR,confirmed
N2,2022-05-05,USA,USA,suspected
N3,2022-01-01,Spain,ESP,discarded
N4,2022-03-03,Australia,AUS,omit_error
"""
    actual_JSON, actual_CSV = app.format_data(
        CLEANED_OUTPUT,
        fields=["ID", "Date_confirmation", "Country", "Country_ISO3", "Status"]
    )
    assert actual_JSON == expected_JSON
    assert actual_CSV.splitlines() == expected_CSV.splitlines()


def test_aggregate_data():
    expected_total = {"total": 2, "confirmed": 1}
    expected_country_aggregate = {
        "2022-06-05": [
            {"GBR": {"confirmed": 1, "suspected": 0}},
            {"USA": {"confirmed": 0, "suspected": 1}},
        ]
    }
    pprint(app.aggregate_data(CLEANED_OUTPUT, today="2022-06-06"))
    assert app.aggregate_data(CLEANED_OUTPUT, today="2022-06-05") == (
        expected_total,
        expected_country_aggregate,
    )
