import pytest
from bs4 import BeautifulSoup

import ecdc


@pytest.mark.parametrize(
    "source,expected",
    [
        (
            "Date: 2022-05-05<br />count:  1<br />ReportingCountry: New Zealand",
            {"date": "2022-05-05", "count": 1, "country": "New Zealand"},
        ),
        (
            "Date: 2022-06-03<br />count: 10<br />ReportingCountry: New Zealand",
            {"date": "2022-06-03", "count": 10, "country": "New Zealand"},
        ),
        ("Date: 2022-05<br />count:  1<br />ReportingCountry: New Zealand", None),
        ("Date: 2122-01-01<br />count: 100<br />ReportingCountry: New Zealand", None),
        (
            "Date: 2022-02-03<br />count:  9<br />ReportingCountry: Belgium",
            {"date": "2022-02-03", "count": 9, "country": "Belgium"},
        ),
    ],
)
def test_parse_line(source, expected):
    assert ecdc.parse_line(source) == expected


def test_get_json_data() -> dict[str]:
    html = """
    <div id="by-date-of-onset-and-by-country-or-area">
    <script>{"x":1}</script>
    </div>
    """
    assert ecdc.get_json_data(BeautifulSoup(html, "html5lib")) == {"x": 1}
