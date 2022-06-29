"""
Quality check monkeypox data at https://github.com/globaldothealth/monkeypox

Raises errors and warnings to a Slack channel which should have
a defined webhook at LINT_WEBHOOK_URL

Can also be run from the command line to show a list of errors and warnings
"""

import io
import os
import sys
import math
import yaml
import datetime
import logging
from typing import Any, Optional

import requests
import pandas as pd


with open("data_dictionary.yml") as fp:
    data_dictionary = yaml.safe_load(fp)
    types = {f["name"]: f["type"] for f in data_dictionary["fields"]}
    required = {f["name"]: f.get("required", False) for f in data_dictionary["fields"]}


def valid_date(s: str) -> bool:
    try:
        if s == "":
            return True
        datetime.datetime.strptime(s, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def valid_int(s: str) -> bool:
    try:
        if s == "":
            return True
        int(s)
        return True
    except ValueError:
        return False


def valid_enum(s: str, values: list[str]) -> bool:
    return str(s).strip().lower() in [v.lower() for v in values]


def valid_url(s: str) -> bool:
    # this is not enough for a full validation, but will do for now
    try:
        return s.startswith("http://") or s.startswith("https://")
    except (AttributeError, ValueError):
        return False


def valid_integer_range(value: str | float | int) -> bool:
    if isinstance(value, float) or isinstance(value, int):
        return False  # not a range
    if "<" in value:
        return valid_int(value.replace("<", ""))
    if ">" in value:
        return valid_int(value.replace(">", ""))
    try:
        x, y = value.split("-")
    except ValueError:  # too many or too few values to unpack
        return False
    return valid_int(x) and valid_int(y) and int(x) < int(y)


def is_empty(value: Any) -> bool:
    if isinstance(value, str) and value == "":
        return True
    if (isinstance(value, int) or isinstance(value, float)) and math.isnan(value):
        return True
    return False


def validate_field(
    value: Any, field_name: str, field_type: str, required: bool = False
) -> bool:
    if not required and is_empty(value):
        return True
    if "|" in field_type:
        return valid_enum(value, field_type.split(" | "))
    elif field_type == "integer":
        return valid_int(value)
    elif field_type == "iso8601date":
        return valid_date(value)
    elif field_type == "url":
        return valid_url(value)
    elif field_type == "integer-range":
        return valid_integer_range(value)
    elif field_name == "Country_ISO3":
        return isinstance(value, str) and value.isupper() and len(value) == 3
    else:
        return True


def validate_row(row: dict[str, Any]) -> Optional[list[dict[str, str]]]:
    if row["Status"] == "confirmed" and is_empty(row["Date_confirmation"]):
        return [
            {
                "field": "Date_confirmation",
                "value": "",
                "message": "Status=confirmed requires Date_confirmation",
            }
        ]
    return None


def lint(df: pd.DataFrame) -> list[dict[str, Any]]:
    linting_result = []
    line = 0
    for row in df.to_dict("records"):
        line += 1
        invalid_fields = [
            (field_name, value)
            for field_name, value in row.items()
            if not validate_field(
                value, field_name, types[field_name], required[field_name]
            )
        ]
        if row_errors := validate_row(row):
            linting_result.append({"id": row["ID"], "line": line, "errors": row_errors})

        if invalid_fields:
            linting_result.append(
                {
                    "id": row["ID"],
                    "line": line,
                    "errors": [
                        {"field": field_name, "value": value}
                        for field_name, value in invalid_fields
                    ],
                }
            )

    return linting_result


def lint_url_or_file(url_or_file: str) -> list[dict[str, Any]]:
    return lint(pd.read_csv(url_or_file))


def lint_string(string: str) -> list[dict[str, Any]]:
    return lint(pd.read_csv(io.StringIO(string)))


def pretty_lint_results(results, header=""):
    if results:
        return (
            header
            + "\n"
            + "\n".join(
                f"- *{row['id']}* (line {row['line']}): "
                + ", ".join(
                    f"{e['field']}={e['value']} {e.get('message', '')}"
                    for e in row["errors"]
                )
                for row in results
            )
        )


def send_slack_message(webhook_url: str, message: str) -> None:
    if (
        response := requests.post(webhook_url, json={"text": message})
    ).status_code != 200:
        logging.error(
            f"Slack notification failed with {response.status_code}: {response.text}"
        )


if __name__ == "__main__":
    results = pretty_lint_results(
        lint_url_or_file(sys.argv[1]), header=f"QC for {sys.argv[1]}:"
    )
    if results and (webhook_url := os.getenv("WEBHOOK_URL")):
        send_slack_message(webhook_url, results)
    if results:
        print(results)
        sys.exit(1)
