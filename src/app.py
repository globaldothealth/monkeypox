from collections.abc import Iterable
from datetime import date, datetime
import json
import logging
import os
import io
import sys
import csv
from urllib.parse import urlparse
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional
import concurrent
from itertools import repeat

import boto3
import yaml
import pdfkit
import pygsheets
import pycountry
import requests
import pandas as pd
import click

import qc
import timeseries
from ecdc import get_ecdc_data, TARGET_DIVS


Data = list[dict[str, Any]]
DATA_BUCKET = os.environ.get("DATA_BUCKET")
AGGREGATES_BUCKET = os.environ.get("AGGREGATES_BUCKET")
DOCUMENT_ID = os.environ.get("DOCUMENT_ID")
FIELDS: list[str] = []

with open("data_dictionary.yml") as fp:
    data_dictionary = yaml.safe_load(fp)
    FIELDS = [f["name"] for f in data_dictionary["fields"]]


S3 = boto3.resource("s3")

DATA_FOLDER = "archives"
SOURCES_FOLDER = "sources"
CASE_DEFINITIONS_FOLDER = "case-definitions"

BUCKET_CONTENTS: list[str] = []

ISO3_QUIRKS = {
    "england": "GBR",
    "scotland": "GBR",
    "northern ireland": "GBR",
    "wales": "GBR",
    "democratic republic of the congo": "COD",
    "republic of the congo": "COG",
    "republic of congo": "COG",
}

VALID_STATUSES = ["suspected", "confirmed", "discarded", "omit_error"]


def lookup_iso3(country: Optional[str]) -> str:
    if country is None:
        return ""
    if country.lower() in ISO3_QUIRKS:
        return ISO3_QUIRKS[country.lower()]
    try:
        matches = pycountry.countries.search_fuzzy(country)
        if not matches:
            logging.warning(f"No match found for country: {country}")
            return ""
    except Exception:
        logging.exception(f"An exception occurred while trying to find an ISO country code for {country}")
        return ""
    return matches[0].alpha_3


def setup_logger():
    h = logging.StreamHandler(sys.stdout)
    rootLogger = logging.getLogger()
    rootLogger.addHandler(h)
    rootLogger.setLevel(logging.INFO)


def get_data(worksheet_title="Confirmed/Suspected") -> Data:
    logging.info("Getting data from Google Sheets")
    client = pygsheets.authorize(service_account_env_var="GOOGLE_CREDENTIALS")
    spreadsheet = client.open_by_key(DOCUMENT_ID)

    try:
        return spreadsheet.worksheet("title", worksheet_title).get_all_records()
    except pygsheets.WorksheetNotFound:
        logging.error(f"Could not find worksheet with title={worksheet_title}")
        raise


def run_quality_checks(csv_data):
    if qc_results := qc.lint_string(csv_data):
        logging.error("Quality check failed")
        logging.error(pretty_results := qc.pretty_lint_results(qc_results))
        if (webhook_url := os.getenv("WEBHOOK_URL")):
            qc.send_slack_message(webhook_url, pretty_results)
        sys.exit(1)


def calculate_timeseries(csv_data) -> (pd.DataFrame, pd.DataFrame):
    logging.info("Calculating timeseries")
    df = pd.read_csv(io.StringIO(csv_data))
    timeseries_confirmed = timeseries.by_confirmed(df)
    timeseries_country_confirmed = timeseries.by_country_confirmed(df)
    return timeseries_confirmed, timeseries_country_confirmed


def get_source_urls(data: Data) -> set[str]:
    logging.info("Getting source urls from data")
    source_urls = set()
    for case in data:
        source_urls.add(case.get("Source", ""))
        source_urls.add(case.get("Source_II", ""))
    source_urls.remove("")
    return source_urls


def clean_data(data: Data, id_prefix: str = "") -> Data:
    logging.info("Cleaning data")
    cleaned_data = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for result in executor.map(clean_case, data, repeat(id_prefix)):
            cleaned_data.append(result)
    return cleaned_data


def clean_case(case: dict, id_prefix: str):
    case["ID"] = id_prefix + str(case["ID"])
    case["Country_ISO3"] = lookup_iso3(case.get("Country"))
    # remove keys which are not in data dictionary
    for key in set(case.keys()) - set(FIELDS):
        case.pop(key)
    return case


def format_data(data: Data, fields: Optional[list[str]] = FIELDS) -> tuple[str, str]:
    logging.info("Formatting data")
    json_data = json.dumps(data)
    csv_data = io.StringIO()
    csv_writer = csv.DictWriter(csv_data, fieldnames=fields)
    csv_writer.writeheader()
    for row in data:
        csv_writer.writerow(row)
    return json_data, csv_data.getvalue()


def store_data(json_data: str, csv_data: str,
               timeseries_confirmed: str, timeseries_country_confirmed: str):
    logging.info("Uploading data to S3")
    now = datetime.today()
    try:
        S3.Object(DATA_BUCKET, f"{DATA_FOLDER}/{now}.csv").put(Body=csv_data)
        S3.Object(DATA_BUCKET, "latest.csv").put(Body=csv_data)
        S3.Object(DATA_BUCKET, f"{DATA_FOLDER}/{now}.json").put(Body=json_data)
        S3.Object(DATA_BUCKET, "latest.json").put(Body=json_data)
        S3.Object(DATA_BUCKET, "timeseries-confirmed.csv").put(Body=timeseries_confirmed)
        S3.Object(DATA_BUCKET, "timeseries-country-confirmed.csv").put(Body=timeseries_country_confirmed)
    except Exception as exc:
        logging.exception(f"An exception occurred while trying to upload data files")
        raise


def urls_to_pdfs(source_urls: list[str] | set[str], folder: str, names: list[str]=None) -> list[str]:
    logging.info("Converting websites into PDFs")
    pdfs = []
    if not names:
        names = [f"{urlparse(source_url).path.replace('/', '_')[1:]}.pdf" for source_url in source_urls]
    else:
        try:
            assert len(names) == len(source_urls)
        except AssertionError:
            logging.error("urls_to_pdfs: Source urls and names should be of the same length")
            raise

    names = [((n + ".pdf") if not n.endswith(".pdf") else n) for n in names]  # ensure .pdf suffix
    for source_url, name in zip(source_urls, names):
        if bucket_contains(name, folder):
            logging.info(f"Found {name} in bucket, skipping it")
            continue
        logging.info(f"Saving content from {source_url} to {name}")
        if ".pdf" not in source_url:
            try:
                pdfkit.from_url(source_url, name, options={"page-size": "Letter"})
                pdfs.append(name)
            except Exception:
                logging.exception(f"An exception occurred while trying to convert {source_url} to {name}")
        else:
            try:
                r = requests.get(source_url)
                with open(name, 'wb') as fp:
                    fp.write(r.content)
                pdfs.append(name)
            except Exception:
                logging.exception(f"An exception occurred while trying to download {source_url} to {name}")

    return pdfs


def bucket_contains(file_name: str, folder: str) -> bool:
    global BUCKET_CONTENTS
    if not BUCKET_CONTENTS:
        objects = S3.Bucket(DATA_BUCKET).objects.all()
        BUCKET_CONTENTS = [o.key.split("/")[1] for o in objects if o.key.startswith(f"{folder}/")]
    return file_name in BUCKET_CONTENTS


def store_pdfs(pdfs: list[str], folder: str):
    logging.info("Uploading PDFs to S3")
    for pdf in pdfs:
        try:
            S3.Object(DATA_BUCKET, f"{folder}/{pdf}").upload_file(pdf)
        except Exception:
            logging.exception(f"An exception occurred while trying to upload {pdf}")
            raise


def aggregate_data(data: Data, today: str=None) -> tuple[dict[str, int], dict[str, list[dict[str, Any]]]]:
    logging.info("Getting total counts of cases")
    today = today or date.today().strftime("%Y-%m-%d")
    total_count = {"total": 0, "confirmed": 0}
    aggregates: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))  # nested defaultdict
    for case in data:
        country = case.get("Country_ISO3")
        if not country:
            raise ValueError(f"No country found for case: {case}")
        status = case.get("Status")
        if not status:
            raise ValueError(f"No status found for case: {case}")
        if not status in VALID_STATUSES:
            logging.warning(f"Case status {status} not in {VALID_STATUSES}")
        if status in ["discarded", "omit_error"]:
            continue
        aggregates[country][status] += 1
        total_count["total"] += 1
        if status == "confirmed":
            total_count["confirmed"] += 1
    country_aggregates = {today: [{k: {"confirmed": v["confirmed"], "suspected": v["suspected"]}} for k, v in aggregates.items()]}
    return total_count, country_aggregates


def store_aggregates(total_count: str, country_aggregates: str):
    logging.info("Uploading case counts to S3")
    try:
        S3.Object(AGGREGATES_BUCKET, "total/latest.json").put(Body=total_count)
        S3.Object(AGGREGATES_BUCKET, "country/latest.json").put(Body=country_aggregates)
    except Exception as exc:
        logging.exception("An exception occurred while trying to upload latest aggregates and totals files")
        raise


def store_timeseries(by_confirmed: pd.DataFrame, by_country_confirmed: pd.DataFrame):
    logging.info("Uploading timeseries to aggregates")
    try:
        S3.Object(AGGREGATES_BUCKET, "timeseries/confirmed.json").put(Body=timeseries.to_json(by_confirmed))
        S3.Object(AGGREGATES_BUCKET, "timeseries/country_confirmed.json").put(Body=timeseries.to_json(by_country_confirmed))
    except Exception as exc:
        logging.exception("An exception occurred while trying to upload timeseries to aggregates")
        raise


def store_case_definitions(case_definition_urls: Path):
    """Retrieve and store case definitions"""
    with case_definition_urls.open() as fp:
        case_definitions = json.load(fp)
        pdfs = urls_to_pdfs(
                source_urls=case_definitions.values(),
                folder=CASE_DEFINITIONS_FOLDER,
                names=case_definitions.keys()
        )
        store_pdfs(pdfs, folder=CASE_DEFINITIONS_FOLDER)


def store_ecdc():
    logging.info("Fetching and storing ECDC data")
    for div in TARGET_DIVS:
        now = datetime.today()
        logging.info(f"Getting data from div {div}")
        file_name = f"ecdc/ecdc-{div}.csv"
        S3.Object(DATA_BUCKET, file_name).put(Body=get_ecdc_data(div=div))
        file_name = f"ecdc-archives/{now}-ecdc-{div}.csv"
        S3.Object(DATA_BUCKET, file_name).put(Body=get_ecdc_data(div=div))


@click.command()
@click.option("--gsheets", is_flag=True, show_default=True, default=True, help="Backup data from Google Sheets")
@click.option("--sources", is_flag=True, show_default=True, default=False, help="Backup source URLs as PDFs")
@click.option("--casedefs", is_flag=True, show_default=True, default=True, help="Backup case definition files")
@click.option("--ecdc", is_flag=True, show_default=True, default=True, help="Backup ECDC data")
def run(gsheets, sources, casedefs, ecdc):
    setup_logger()
    logging.info("Starting script")
    data = get_data()
    if gsheets:
        endemic_data = get_data("Endemic Countries")
        data = clean_data(data, id_prefix="N")
        endemic_data = clean_data(endemic_data, id_prefix="E")
        json_data, csv_data = format_data(data + endemic_data)

        run_quality_checks(csv_data)

        ts_conf, ts_ctry_conf = calculate_timeseries(csv_data)

        store_data(json_data, csv_data,
                   timeseries.to_csv(ts_conf),
                   timeseries.to_csv(ts_ctry_conf))

        total_count, country_aggregates = aggregate_data(data + endemic_data)
        store_aggregates(json.dumps(total_count), json.dumps(country_aggregates))
        store_timeseries(ts_conf, ts_ctry_conf)

    if sources:
        try:
            source_urls = get_source_urls(data)
            pdfs = urls_to_pdfs(source_urls, folder=SOURCES_FOLDER)
            store_pdfs(pdfs, folder=SOURCES_FOLDER)
        except Exception as e:
            logging.error(f"Error occurred in saving source URLs: {e}")

    if casedefs:
        store_case_definitions(Path('case-definitions.json'))

    if ecdc:
        store_ecdc()
    logging.info("Script completed")


if __name__ == "__main__":
    run()
