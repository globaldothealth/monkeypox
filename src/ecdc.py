"""
Fetch ECDC data and parse as CSV
"""

import io
import re
import csv
import json
from enum import Enum
from typing import Optional

import requests
from bs4 import BeautifulSoup

URL = "https://monkeypoxreport.ecdc.europa.eu"
ONSET_DIV_ID = "by-date-of-onset-and-by-country-or-area"
ONSET_FIELDS = ["date", "country", "count"]
Output = Enum("Output", "CSV JSON Native")


def fetch_soup(url: str) -> str:
    return BeautifulSoup(requests.get(url).content.decode("utf-8"), "html5lib")


def parse_line(line: str) -> Optional[dict[str, str | int]]:
    """Returns comma separated values from line
    where line is of the form

    Date: YYYY-MM-DD<br />count: N<br />ReportingCountry: NAME
    """
    if match := re.match(
        r"Date: (20\d\d-[0-1]\d-\d\d)<br />count:\s+(\d+)<br />ReportingCountry:\s+(.*)",
        line,
    ):
        date, count, country = match.groups()
        return {"date": date, "count": int(count), "country": country}
    else:
        return None


def get_json_data(soup) -> dict[str]:
    div = soup.find("div", id=ONSET_DIV_ID)
    if div is None:
        raise ValueError(f"div[id='{ONSET_DIV_ID}'] not found")
    script = div.find("script")
    if script is None:
        raise ValueError("No JSON data found in div")
    return json.loads(script.contents[0])


def process_json(json_data: dict[str]) -> list[dict[str, str | int]]:
    records = []
    for country in json_data["x"]["data"]:
        text = country["text"]
        # countries with only one entry are not in a list
        text = text if isinstance(text, list) else [text]
        # parse each line and remove invalid lines
        records.extend(list(filter(None, map(parse_line, text))))
    return records


def to_csv(json_data: list[dict[str, str | int]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=ONSET_FIELDS)
    writer.writeheader()
    for row in json_data:
        writer.writerow(row)
    return buf.getvalue()


def run(
    url: str = URL, output: Output = Output.CSV
) -> str | list[dict[str, str | int]]:
    data = process_json(get_json_data(fetch_soup(URL)))
    if output == Output.CSV:
        return to_csv(data)
    elif output == Output.JSON:
        return json.dumps(data)
    else:
        return data


if __name__ == "__main__":
    print(run())
