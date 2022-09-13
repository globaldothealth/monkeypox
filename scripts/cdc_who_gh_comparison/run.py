import codecs
from contextlib import closing
import csv
from datetime import date
import io
import json
import logging
import os
import sys
from time import sleep

import click
import pygsheets
import requests


DOCUMENT_ID = os.environ.get("DOCUMENT_ID")

LINE_LIST_SHEET = "Confirmed/Suspected"
COUNTRY_COUNT_SHEET = "Cases by Country"

CDC_ENDPOINT = os.environ.get("CDC_ENDPOINT")
CDC_ENDPOINT = "https://www.cdc.gov/wcms/vizdata/poxvirus/monkeypox/data/USmap_counts.csv"
WHO_ENDPOINT = os.environ.get("WHO_ENDPOINT")
WHO_ENDPOINT = "https://extranet.who.int/publicemergency/api/Monkeypox/CaseSummary"

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

STATES = ["Alabama", "Alaska", "Arizona", "Arkansas", "California",
	"Colorado", "Connecticut", "Delaware", "District of Columbia", "Florida",
	"Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
	"Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
	"Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
	"New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
	"North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
	"Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
	"Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
	"Wyoming"
]

CDC_SOURCE = "https://www.cdc.gov/poxvirus/monkeypox/response/2022/us-map.html"

WHO_TO_GH = {
    "Republic of Korea": "South Korea",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Türkiye": "Turkey",
    "T\u00fcrkiye": "Turkey",
    "Bosnia and Herzegovina": "Bosnia And Herzegovina",
    "Czechia": "Czech Republic",
    "Bolivia (Plurinational State of)": "Bolivia",
    "Russian Federation": "Russia",
    "Saint Martin": "Saint Martin (French part)",
    "Republic of Moldova": "Moldova",
    "Iran (Islamic Republic of)": "Iran"
}

OMIT_ERROR = "omit_error"


def setup_logger():
	h = logging.StreamHandler(sys.stdout)
	rootLogger = logging.getLogger()
	rootLogger.addHandler(h)
	rootLogger.setLevel(logging.INFO)


def get_gh_data(worksheet_title: str, as_lists=True) -> list[dict[str, str|int|None]]:
	logging.info("Getting data from Google Sheets")
	client = pygsheets.authorize(service_account_env_var="GOOGLE_CREDENTIALS")
	spreadsheet = client.open_by_key(DOCUMENT_ID)

	try:
		if as_lists:
			# get_all_records() on a pivot table returns "COUNTA" and "Status_derived" keys
			vals = spreadsheet.worksheet("title", worksheet_title).get_all_values()
			return convert_lists(vals)
		return spreadsheet.worksheet("title", worksheet_title).get_all_records()
	except pygsheets.WorksheetNotFound:
		logging.error(f"Could not find worksheet with title={worksheet_title}")
		raise


def convert_lists(data: list[list]) -> list[dict]:
	logging.info("Converting tabular data to associative array")
	if "COUNTA" in data[0][0]:
		data.remove(data[0])
	if "Country" not in data[0][0]:
		raise Exception("Could not locate column labels")

	rows = "\n".join([f"{','.join(d)}" for d in data])
	f = io.StringIO(rows)
	reader = csv.DictReader(f)
	return [{
		"Country": row["Country"],
		"confirmed": row["confirmed"],
		"death": row["death"]
		} for row in reader]


def get_cdc_data() -> list[dict[str, str|int|None]]:
	logging.info("Getting CDC data")
	try:
		response = requests.get(CDC_ENDPOINT)
		reader = csv.DictReader(codecs.iterdecode(response.iter_lines(), "utf-8"))
		return [row for row in reader]
	except Exception:
		logging.exception("Something went wrong when trying to retrieve CDC data")


def get_who_data() -> list[dict[str, str|int|None]]:
	logging.info("Getting WHO data")
	try:
		response = requests.post(WHO_ENDPOINT, json={})
		return response.json().get("Data")
	except Exception:
		logging.exception("Something went wrong when trying to retrieve WHO data")


def format_cdc_data(data: list[dict[str, str|int|None]]) -> dict[str, int]:
	logging.info("Formatting CDC data")
	clean_data = {}
	for entry in data:
		if entry["Location"] in ["Non-US Resident", "Puerto Rico"]:
			continue
		clean_data[entry["Location"]] = entry["Cases"]
	return clean_data


def format_who_data(data: list[dict[str, str|int|None]]) -> dict[str, int]:
	logging.info("Formatting WHO data")
	clean_data = {}
	for entry in data:
		name = entry["COUNTRY"]
		if "WHO" in name and "Region" in name:
			continue
		if name in ["USA", "UNITED KINGDOM"]:
			continue
		for who_name, gh_name in WHO_TO_GH.items():
			if name.title() == who_name:
				clean_data[gh_name] = entry["CasesAll"]
		clean_data[name.title()] = entry["CasesAll"]
	return clean_data


def format_gh_usa_data(data: list[dict[str, str|int|None]]) -> dict[str, int]:
	logging.info("Formatting G.h USA data")
	gh_state_counts = {}
	for entry in data:
		if entry.get("Country", "") == "United States":
			for state in STATES:
				if state in entry["Location"]:
					if state in gh_state_counts:
						gh_state_counts[state] += 1
					else:
						gh_state_counts[state] = 1
					continue

	return gh_state_counts


def format_gh_global_data(data: list[dict[str, str|int|None]]) -> dict[str, int]:
	logging.info("Formatting G.h global data")
	gh_global_counts = {}
	for entry in data:
		confirmed = entry.get("confirmed", "")
		if len(confirmed) == 0:
			confirmed = 0
		deaths = entry.get("death", "")
		if len(deaths) == 0:
			deaths = 0
		gh_global_counts[entry["Country"]] = int(confirmed) + int(deaths)
	return gh_global_counts


def compare_cdc_data(gh_data: dict[str, int], cdc_data: dict[str, int]) -> dict[str, int]:
	logging.info("Comparing CDC and G.h USA data")
	diffs = {}
	for state, cdc_count in cdc_data.items():
		logging.info(f"{state} CDC count: {cdc_count} G.h count: {gh_data.get(state, 0)}")
		diff = int(cdc_count) - gh_data.get(state, 0)
		if diff:
			diffs[state] = diff
	if cdc_missing := gh_data.keys() - cdc_data.keys():
		for state in cdc_missing:
			diffs[state] = -gh_data.get(state, 0)
	return diffs


def compare_who_data(gh_data: dict[str, int], who_data: dict[str, int]) -> dict[str, int]:
	logging.info("Comparing WHO and G.h global data")
	diffs = {}
	for country, who_count in who_data.items():
		logging.info(f"{country} WHO count: {who_count} G.h count: {gh_data.get(country, 0)}")
		diff = who_count - gh_data.get(country, 0)
		if diff > 0:
			diffs[country] = diff
	return diffs


def change_gh_data(changes: dict[str, int], dry_run: bool) -> None:
	logging.info("Changing G.h USA data")
	client = pygsheets.authorize(service_account_env_var="GOOGLE_CREDENTIALS")
	client.set_batch_mode(True)
	spreadsheet = client.open_by_key(DOCUMENT_ID)

	sheet = spreadsheet.worksheet("title", LINE_LIST_SHEET)

	# Finding column indices based on values breaks when using enums...
	state_col = 3
	status_col = 2
	date_conf_col = 9
	date_entry_col = 29
	date_mod_col = 30

	columns = sheet.get_row(1)
	today = date.today().strftime("%Y-%m-%d")
	records = sheet.get_all_records()
	row = len(records)

	for state, count in changes.items():
		if count > 0:
			new_cases = []
			for _ in range(count):
				new_cases.append(format_new_case(row, today, state, columns))
				row += 1
			for case in new_cases:
				if dry_run:
					logging.info(f"Not appending case: {case}")
				else:
					sheet.append_table(case.split(","))
			client.run_batch()
		else:
			extra_rows = find_extra_rows(records, state, -count)
			if dry_run:
				logging.info(f"Not omitting {-count} cases for {state} from rows: {extra_rows}")
			else:
				logging.info(f"Omitting {-count} cases for {state} from rows: {extra_rows}")
				omit_cases(sheet, extra_rows, status_col, date_mod_col, today, -count)


def format_new_case(case_id: int, today: str, state: str, columns: str) -> str:
	output = io.StringIO()
	writer = csv.DictWriter(output, fieldnames=columns)
	writer.writerow({
		"ID": case_id,
		"Location": state,
		"Country": "United States",
		"Status": "confirmed",
		"Date_confirmation": today,
		"Date_entry": today,
		"Date_last_modified": today,  # FIXME: seeing "44782" in sheet
		"Source": CDC_SOURCE
	})
	return output.getvalue().removesuffix("\r\n")


def find_extra_rows(records: list[dict[str, str|int|None]], state: str, count: int) -> list[int]:
	extra_rows = []
	logging.info(f"Finding {count} extra records for {state}")
	for index, row in enumerate(records):
		if row.get("Country", "") == "United States" and state in row.get("Location", "") and row.get("Status", "") != OMIT_ERROR:
			extra_rows.append(index + 2) # 0-based indexing plus column labels
			count -= 1
		if not count:
			return extra_rows
	logging.warning(f"Failed to find {count} rows to omit for {state}")
	return extra_rows


def omit_cases(sheet: pygsheets.Worksheet, rows: [int], status_col: int, date_col: int, today: int, count: int):
	remaining = count
	for row in rows:
		cell = sheet.cell((row, status_col))
		if cell.value == OMIT_ERROR:
			continue
		cell.set_value(OMIT_ERROR)
		cell = sheet.cell((row, date_col))
		cell.set_value(today)
		remaining -= 1
		if not remaining:
			return

	logging.warning(f"{remaining} cases could not be omitted")


def format_slack_message(data: dict[str, int], cdc: bool, who: bool) -> str:
	msg = ""
	if cdc:
		msg = "Comparison of US state Monkeypox data (CDC - G.h):\n"
	elif who:
		msg = "Comparison of global Monkeypox data (WHO - G.h > 0):\n"
	
	msg += json.dumps(data, indent=4, sort_keys=True)

	return msg


def send_slack_message(message: str, slack_enabled: bool) -> None:
	if not slack_enabled:
		logging.info(f"Not sending the following message during dry run:\n{message}")
		return

	if SLACK_WEBHOOK_URL:
		logging.info("Sending Slack message")
		try:
			response = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
			if response.status_code != 200:
				logging.error(f"Slack notification failed with {response.status_code}: {response.text}")
		except Exception:
			logging.exception("Slack notification failed due to an error")
	else:
		logging.info("No target for Slack message")


@click.command()
@click.option("--cdc", is_flag=True, show_default=True, default=False, help="Compare G.h data to CDC data and update spreadsheet")
@click.option("--who", is_flag=True, show_default=True, default=False, help="Compare G.h data to WHO data")
@click.option("--slack", is_flag=True, show_default=True, default=False, help="Report comparison data via Slack")
@click.option("--dry", is_flag=True, show_default=True, default=False, help="Dry run (do not update sheet)")
def run(cdc, who, slack, dry):
	setup_logger()
	logging.info("Starting run")
	diff = {}
	if cdc and not who:
		logging.info("Retrieving and comparing CDC and G.h USA data")
		gh_data = get_gh_data(LINE_LIST_SHEET, as_lists=False)
		cdc_data = get_cdc_data()
		fmt_cdc_data = format_cdc_data(cdc_data)
		fmt_gh_data = format_gh_usa_data(gh_data)
		diff = compare_cdc_data(fmt_gh_data, fmt_cdc_data)
		change_gh_data(diff, dry)
	elif who and not cdc:
		logging.info("Retrieving and comparing WHO and G.h global data")
		gh_data = get_gh_data(COUNTRY_COUNT_SHEET, as_lists=True)
		who_data = get_who_data()
		fmt_who_data = format_who_data(who_data)
		fmt_gh_data = format_gh_global_data(gh_data)
		diff = compare_who_data(fmt_gh_data, fmt_who_data)
	else:
		raise Exception("Must run with either --cdc (exclusive) or --who set")

	msg = format_slack_message(diff, cdc, who)
	send_slack_message(msg, slack)
	logging.info("Work complete")


if __name__ == "__main__":
	run()
