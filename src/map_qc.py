# Check that map is up to date
import datetime
import logging

import requests

COUNTRY_URL = "https://monkeypox-aggregates.s3.amazonaws.com/country/latest.json"
TOTAL_URL = "https://monkeypox-aggregates.s3.amazonaws.com/total/latest.json"

def fetch(data: str) -> dict:
    try:
        r = requests.get(data)
        return r.json()
    except Exception as e:
        logging.error(f"Failed to fetch {data}: {e}")
        raise


def extract_date(aggregates_data):
    try:
        return datetime.datetime.fromisoformat(next(iter(aggregates_data))).date()  # first key is date
    except Exception as e:
        logging.error(f"Could not parse date: {e}")
        raise


def check_total(total_data):
    try:
        return total_data.get("confirmed") < total_data.get("total")
    except TypeError as e:
        logging.error(f"Failed to parse total data: {e}")


def check_aggregates(aggregates_data):
    today = datetime.datetime.today().date()
    return (today - extract_date(aggregates_data)).days < 2  # ok to be within 1 day, some hours


if __name__ == "__main__":
    print("country", check_aggregates(fetch(COUNTRY_URL)))
    print("  total", check_total(fetch(TOTAL_URL)))
