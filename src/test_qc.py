import pytest

import lint


@pytest.mark.parametrize(
    "source,expected", [("2020-01-01", True), ("", True), ("hello", False)]
)
def test_valid_date(source, expected):
    assert lint.valid_date(source) == expected


@pytest.mark.parametrize("source,expected", [("1", True), ("hello", False)])
def test_valid_int(source, expected):
    assert lint.valid_int(source) == expected


@pytest.mark.parametrize(
    "source,expected", [("Female", True), ("male", True), ("unknown", False)]
)
def test_valid_enum(source, expected):
    assert lint.valid_enum(source, ["male", "female", "other"]) == expected


@pytest.mark.parametrize(
    "source,expected",
    [
        ("https://www.wikipedia.org", True),
        ("mailto:hello@example.com", False),
        ("unknown", False),
    ],
)
def test_valid_url(source, expected):
    assert lint.valid_url(source) == expected


@pytest.mark.parametrize(
    "source,expected", [("<40", True), (">5", True), ("20-25", True), ("50-10", False)]
)
def test_valid_integer_range(source, expected):
    assert lint.valid_integer_range(source) == expected


@pytest.mark.parametrize(
    "source,expected", [("", True), (float("nan"), True), ("hello", False)]
)
def test_is_empty(source, expected):
    assert lint.is_empty(source) == expected
