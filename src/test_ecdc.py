import pytest
from bs4 import BeautifulSoup

from ecdc import get_json_data, parse_line, NOTIF_DIV_ID, ONSET_DATE_DIV_ID, ONSET_OCA_DIV_ID


@pytest.mark.parametrize(
    "source,div,expected",
    [
        (
            "DateNotif: 2022-05-05<br />count:   2",
            NOTIF_DIV_ID,
            {"date": "2022-05-05", "count": 2},
        ),
        (
            "DateNotif: 2022-07-12<br />count: 471",
            NOTIF_DIV_ID,
            {"date": "2022-07-12", "count": 471},
        ),
        (
            "Date: 2022-05-05<br />count:   1<br />TypeDate: Notification",
            ONSET_DATE_DIV_ID,
            {"date": "2022-05-05", "count": 1, "type": "Notification"}
        ),
        (
            "Date: 2022-07-07<br />count:  85<br />TypeDate: Notification",
            ONSET_DATE_DIV_ID,
            {"date": "2022-07-07", "count": 85, "type": "Notification"}
        ),
        (
            "Date: 2022-05-05<br />count:  1<br />ReportingCountry: New Zealand",
            ONSET_OCA_DIV_ID,
            {"date": "2022-05-05", "count": 1, "country": "New Zealand"},
        ),
        (
            "Date: 2022-06-03<br />count: 10<br />ReportingCountry: New Zealand",
            ONSET_OCA_DIV_ID,
            {"date": "2022-06-03", "count": 10, "country": "New Zealand"},
        ),
        ("Date: 2022-05<br />count:  1<br />ReportingCountry: New Zealand", ONSET_OCA_DIV_ID, None),
        ("Date: 2122-01-01<br />count: 100<br />ReportingCountry: New Zealand", ONSET_OCA_DIV_ID, None),
        (
            "Date: 2022-02-03<br />count:  9<br />ReportingCountry: Belgium",
            ONSET_OCA_DIV_ID,
            {"date": "2022-02-03", "count": 9, "country": "Belgium"},
        )
    ]
)
def test_parse_line(source, div, expected):
    result = parse_line(source, div)
    assert result == expected, f"Expected {expected}, got {result}"


def test_get_json_data() -> dict[str]:
    html = """
    <div id="by-date-of-onset-and-by-country-or-area">
    <script>{"x":1}</script>
    </div>
    """
    result = None
    try:
        result = get_json_data(BeautifulSoup(html, "html5lib"))
    except Exception:
        result = get_json_data(BeautifulSoup(html, "html.parser"))
    assert result == {"x": 1}
