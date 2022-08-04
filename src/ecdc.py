"""
Fetch ECDC data and parse as CSV
"""

import io
import re
import csv
import json
from enum import Enum
from itertools import repeat
from typing import Optional

import requests
from bs4 import BeautifulSoup

URL = "https://monkeypoxreport.ecdc.europa.eu"
ONSET_OCA_DIV_ID = "by-date-of-onset-and-by-country-or-area"
ONSET_OCA_FIELDS = ["date", "country", "count"]
NOTIF_DIV_ID = "overall-by-date-of-notification"
NOTIF_FIELDS = ["date", "count"]
ONSET_DATE_DIV_ID = "overall-by-date-of-symptom-onset"
ONSET_DATE_FIELDS = ["date", "count", "type"]

TARGET_DIVS = [ONSET_OCA_DIV_ID, NOTIF_DIV_ID, ONSET_DATE_DIV_ID]


Output = Enum("Output", "CSV JSON Native")

REGEXES = {
    ONSET_OCA_DIV_ID: r"Date: (20\d\d-[0-1]\d-\d\d)<br />count:\s+(\d+)<br />ReportingCountry:\s+(.*)",
    NOTIF_DIV_ID: r"DateNotif: (20\d\d-[0-1]\d-\d\d)<br />count:\s+(\d+)",
    ONSET_DATE_DIV_ID: r"Date: (20\d\d-[0-1]\d-\d\d)<br />count:\s+(\d+)<br />TypeDate: (\w+)"
}

FIELDS: {
    ONSET_OCA_DIV_ID: ONSET_OCA_FIELDS,
    NOTIF_DIV_ID: NOTIF_FIELDS,
    ONSET_DATE_DIV_ID: ONSET_DATE_FIELDS
}


def fetch_soup(url: str) -> str:
    return BeautifulSoup(requests.get(url).content.decode("utf-8"), "html5lib")


# Yep, that JSON contains HTML.
def parse_line(line: str, div: str) -> Optional[dict[str, str | int]]:
    """Returns comma separated values from line
    where line is of a form given in REGEXES.values()
    """
    if match := re.match(REGEXES.get(div), line):
        if div == ONSET_OCA_DIV_ID:
            date, count, country = match.groups()
            return {"date": date, "count": int(count), "country": country}
        if div == NOTIF_DIV_ID:
            date, count = match.groups()
            return {"date": date, "count": int(count)}
        if div == ONSET_DATE_DIV_ID:
            date, count, onset_type = match.groups()
            return {"date": date, "count": int(count), "type": onset_type}
    return None


def get_json_data(soup) -> dict[str]:
    div = soup.find("div", id=ONSET_OCA_DIV_ID)
    if div is None:
        raise ValueError(f"div[id='{ONSET_OCA_DIV_ID}'] not found")
    script = div.find("script")
    if script is None:
        raise ValueError("No JSON data found in div")
    return json.loads(script.contents[0])


def process_json(json_data: dict[str], div: str) -> list[dict[str, str | int]]:
    records = []
    if div == ONSET_OCA_DIV_ID:
        for country in json_data["x"]["data"]:
            text = country["text"]
            # countries with only one entry are not in a list
            text = text if isinstance(text, list) else [text]
            # parse each line and remove invalid lines
            records.extend(list(filter(None, map(parse_line, text, repeat(div)))))
    if div in [NOTIF_DIV_ID, ONSET_DATE_DIV_ID]:
        text = json_data["x"]["data"]["text"]
        lines = text.split(",")
        records.extend(list(filter(None, map(parse_line, lines, repeat(div)))))

    return records


def to_csv(json_data: list[dict[str, str | int]], field_names: str) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=ONSET_OCA_FIELDS)
    writer.writeheader()
    for row in json_data:
        writer.writerow(row)
    return buf.getvalue()


def get_ecdc_data(
    url: str = URL, div: str = ONSET_OCA_DIV_ID, output: Output = Output.CSV
) -> str | list[dict[str, str | int]]:
    json_soup = get_json_data(fetch_soup(URL))
    data = process_json(json_soup, div)
    if output == Output.CSV:
        return to_csv(data)
    elif output == Output.JSON:
        return json.dumps(data)
    else:
        return data


if __name__ == "__main__":
    for div in TARGET_DIVS:
        print(get_ecdc_data(div=div))
