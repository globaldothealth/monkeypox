"""
Lint monkeypox data at https://github.com/globaldothealth/monkeypox

Raises errors and warnings to a Slack channel which should have
a defined webhook at WEBHOOK_URL

Can also be run from the command line to show a list of errors and warnings
"""

import sys
import math
import yaml
import datetime
import logging

import pandas as pd


with open("data_dictionary.yml") as fp:
    data_dictionary = yaml.safe_load(fp)
    types = {f["name"]: f["type"] for f in data_dictionary["fields"]}
    required = {f["name"]: f.get("required", False) for f in data_dictionary["fields"]}


def valid_date(s):
    try:
        if s == "":
            return True
        datetime.datetime.strptime(s, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def valid_int(s):
    try:
        if s == "":
            return True
        int(s)
        return True
    except ValueError:
        return False


def valid_enum(s, values):
    return s.lower() in [v.lower() for v in values]


def valid_url(s):
    # this is not enough for a full validation, but will do for now
    try:
        return s.startswith("http://") or s.startswith("https://")
    except (AttributeError, ValueError):
        return False


def valid_integer_range(value):
    if "<" in value:
        return valid_int(value.replace("<", ""))
    if ">" in value:
        return valid_int(value.replace(">", ""))
    nums = value.split("-")
    return len(nums) == 2 and valid_int(x := nums[0]) and valid_int(y := nums[1]) and x < y


def is_empty(value):
    if isinstance(value, str) and value == "":
        return True
    if (isinstance(value, int) or isinstance(value, float)) and math.isnan(value):
        return True
    return False


def validate_field(value, field_name, field_type, optional=True):
    if optional and is_empty(value):
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
        return value.isupper() and len(value) == 3
    else:
        return True


def lint(file_or_url):
    linting_result = []
    for row in pd.read_csv(file_or_url).to_dict("records"):
        invalid_fields = [
            (field_name, value)
            for field_name, value in row.items()
            if not validate_field(value, field_name, types[field_name])
        ]
        if invalid_fields:
            linting_result.append(
                {
                    "id": row["ID"],
                    "errors": [
                        {"field": field_name, "value": value}
                        for field_name, value in invalid_fields
                    ],
                }
            )

    return linting_result


def pretty_lint_results(results, header=""):
    return header + "\n" + "\n".join(
        f"* *{row['id']}*: "
        + ", ".join(f"{e['field']}={e['value']}" for e in row["errors"])
        for row in results
    )


if __name__ == "__main__":
    print(pretty_lint_results(lint(sys.argv[1]), header=f"Linting {sys.argv[1]}:"))
