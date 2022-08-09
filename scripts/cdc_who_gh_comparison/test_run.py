import csv
import io
import os
from unittest.mock import patch

import pygsheets
import pytest
import requests

from run import (LINE_LIST_SHEET, COUNTRY_COUNT_SHEET, OMIT_ERROR, format_new_case,
	format_cdc_data, format_who_data, format_gh_usa_data, format_gh_global_data,
	format_slack_message, compare_cdc_data, compare_who_data, send_slack_message,
	get_gh_data, change_gh_data)


MOUNTEBANK_URL = os.environ.get("MOUNTEBANK_URL")
SLACK_MB_PORT = os.environ.get("IMPOSTER_PORT")

USA_COLUMN_NAMES = [
	"ID", "Status", "Location", "City", "Country", "Country_ISO3", "Age",
	"Gender", "Date_onset", "Date_confirmation", "Symptoms", "Hospitalised (Y/N/NA)",
	"Date_hospitalisation", "Isolated (Y/N/NA)", "Date_isolation", "Outcome",
	"Contact_comment", "Contact_ID", "Contact_location"	, "Travel_history (Y/N/NA)",
	"Travel_history_entry", "Travel_history_start", "Travel_history_location",
	"Travel_history_country", "Genomics_Metadata", "Confirmation_method",
	"Source", "Source_II", "Source_III", "Source_IV", "Source_V", "Source_VI",
	"Source_VII", "Date_entry", "Date_death", "Date_last_modified"
]

GLOBAL_COLUMN_NAMES = [
	"Country", "confirmed", "death", "suspected", "Grand Total"
]

CDC_DATA = [
	{
		"Location": "Maine",
		"Cases": 1
	},
	{
		"Location": "Massachusetts",
		"Cases": 2
	},
	{
		"Location": "Mississippi",
		"Cases": 5
	}
]

WHO_DATA = [
	{
		"COUNTRY": "USA",
		"CasesAll": 42
	},
	{
		"COUNTRY": "CANADA",
		"CasesAll": 42
	},
	{
		"COUNTRY": "CAMBODIA",
		"CasesAll": 7
	}
]


@pytest.fixture
def mock_response(monkeypatch):

    def mock_authorize(*args, **kwargs):
        return MOCK_SHEET

    monkeypatch.setattr(pygsheets, "authorize", mock_authorize)


class MockGSheetsClient():

	def __init__(self):
		self.ms = None

	def open_by_key(self, key):
		if not self.ms:
			self.ms = MockSpreadsheet(key)
		return self.ms

	def set_batch_mode(self, mode):
		pass

	def run_batch(self):
		pass


class MockSpreadsheet():

	def __init__(self, key):
		self.key = key
		self.us_data = []
		self.global_data = []
		self.us_ws = None
		self.global_ws = None

	def worksheet(self, title, name):
		if name == LINE_LIST_SHEET:
			if not self.us_data:
				self.us_data = create_usa_data(CDC_DATA)
				self.us_ws = MockWorksheet(title, name, self.us_data)
			return self.us_ws
		elif name == COUNTRY_COUNT_SHEET:
			if not self.global_data:
				self.global_data = create_global_data(WHO_DATA)
				self.global_ws = MockWorksheet(title, name, self.global_data)
			return self.global_ws


class MockWorksheet():

	def __init__(self, title, name, data):
		self.title = title
		self.name = name
		self.records = data
		self.column_names = list(data[0].keys())

	def get_all_values(self):
		rows = "\n".join([f"{','.join(r)}" for r in self.records])
		f = io.StringIO(rows)
		reader = csv.DictReader(f)
		return [row for row in reader]

	def get_all_records(self):
		return self.records

	def get_row(self, num):
		return self.column_names

	def append_table(self, row: list[str]):
		as_dict = {k: v for k, v in zip(self.column_names, row)}
		self.records.append(as_dict)

	def cell(self, addr):
		# return MockCell(addr)
		(r, c) = addr
		col = self.column_names[c - 1]
		return MockCell(r, col)


class MockCell():

	def __init__(self, row: int, col: str, val=""):
		self.row = row
		self.col = col
		self.value = val

	def set_value(self, value):
		self.value = value
		sheet = MOCK_SHEET.open_by_key("").worksheet("title", LINE_LIST_SHEET)
		sheet.records[self.row - 2][self.col] = self.value


MOCK_SHEET = MockGSheetsClient()


def create_usa_data(cdc_data: list[dict[str, int]]={}, mode=""):
	cases = []
	gh_row = 1
	if cdc_data:
		for row in cdc_data:
			count = row["Cases"]
			if mode == "create":
				count -= 1
			elif mode == "omit":
				count += 1
			for _ in range(count):
				case = format_new_case(gh_row, "2022-01-01", row["Location"], USA_COLUMN_NAMES)
				cases.append(case)
				gh_row += 1
	
	rows = "\n".join(c for c in cases)
	f = io.StringIO(rows)
	reader = csv.DictReader(f, fieldnames=USA_COLUMN_NAMES)
	return [row for row in reader]


def create_global_data(who_data: list[dict[str, int]]={}, mode=""):
	gh_data = []
	if who_data:
		for row in who_data:
			if row["COUNTRY"] == "USA":
				continue
			r = {k: None for k in GLOBAL_COLUMN_NAMES}
			count = row["CasesAll"]
			if mode == "count":
				count -= 1			
			gh_data.append({"Country": row["COUNTRY"].title(), "confirmed": str(count), "deaths": ""})

	return gh_data


@pytest.mark.skipif(not (MOUNTEBANK_URL and SLACK_MB_PORT),reason="Must set MOUNTEBANK_URL and SLACK_MB_PORT")
def test_slack_cdc():
	gh_data = create_usa_data(CDC_DATA, "create")
	fmt_cdc_data = format_cdc_data(CDC_DATA)
	fmt_gh_data = format_gh_usa_data(gh_data)
	diffs = compare_cdc_data(fmt_gh_data, fmt_cdc_data)
	msg = format_slack_message(diffs, True, False)
	send_slack_message(msg, True)
	response = requests.get(f"{MOUNTEBANK_URL}/imposters/{SLACK_MB_PORT}")
	slacks = response.json().get("requests")
	assert len(slacks) > 0
	messages = [m.get("body") for m in slacks]
	for message in messages:
		if "CDC" in message:
			assert all(s in message for s in [s["Location"] for s in  CDC_DATA])


@pytest.mark.skipif(not (MOUNTEBANK_URL and SLACK_MB_PORT),reason="Must set MOUNTEBANK_URL and SLACK_MB_PORT")
def test_slack_who():
	gh_data = create_global_data(WHO_DATA, "count")
	fmt_who_data = format_who_data(WHO_DATA)
	fmt_gh_data = format_gh_global_data(gh_data)
	diffs = compare_who_data(fmt_gh_data, fmt_who_data)
	msg = format_slack_message(diffs, False, True)
	send_slack_message(msg, True)
	response = requests.get(f"{MOUNTEBANK_URL}/imposters/{SLACK_MB_PORT}")
	slacks = response.json().get("requests")
	assert len(slacks) > 0
	messages = [m.get("body") for m in slacks]
	for message in messages:
		if "WHO" in message:
			assert all(c in message for c in ["Cambodia", "Canada"])


@pytest.mark.parametrize(
	"gh_data, cdc_data, created, omitted",
	[
		(create_usa_data(CDC_DATA, "create"), CDC_DATA, {s: 1 for s in [s["Location"] for s in CDC_DATA]}, {}),
		(create_usa_data(CDC_DATA, "omit"), CDC_DATA, {}, {s: -1 for s in [s["Location"] for s in CDC_DATA]}),
		(create_usa_data(CDC_DATA, "equal"), CDC_DATA, {}, {})
	]
)
def test_gh_cdc_comparison(mock_response, gh_data, cdc_data, created, omitted):
	sheet = MOCK_SHEET.open_by_key("").worksheet("", LINE_LIST_SHEET)
	sheet.records = gh_data
	sheet.column_names = list(gh_data[0].keys())
	old_fmt_gh_data = format_gh_usa_data(gh_data)
	fmt_cdc_data = format_cdc_data(cdc_data)
	diff_data = compare_cdc_data(old_fmt_gh_data, fmt_cdc_data)
	for state, count in diff_data.items():
		if count > 0:
			assert state in created
			assert count == created[state]
		elif count < 0:
			assert state in omitted
			assert count == omitted[state]
	change_gh_data(diff_data, dry_run=False)
	
	updated_gh_data = get_gh_data(LINE_LIST_SHEET, False)
	fmt_gh_data = format_gh_usa_data(updated_gh_data)
	for state, count in fmt_gh_data.items():
		if created.get(state, 0) > 0:
			assert fmt_gh_data[state] == created[state] + old_fmt_gh_data.get(state, 0)
		elif omitted.get(state, 0) < 0:
			assert fmt_gh_data[state] == old_fmt_gh_data[state]
			assert case_omitted(updated_gh_data, state)
		else:
			assert fmt_gh_data[state] == old_fmt_gh_data[state]


def case_omitted(gh_data: list[dict], state: str) -> bool:
	for row in gh_data:
		if state in row.get("Location", "") and row.get("Status", "") == OMIT_ERROR:
			return True
	return False


@pytest.mark.parametrize(
	"gh_data, who_data, counted",
	[
		(create_global_data(WHO_DATA, "count"), WHO_DATA, {"Canada": 1, "Cambodia": 1}),
		(create_global_data(WHO_DATA), WHO_DATA, {})
	]
)
def test_gh_who_comparison(gh_data, who_data, counted):
	fmt_gh_data = format_gh_global_data(gh_data)
	fmt_who_data = format_who_data(who_data)
	diff_data = compare_who_data(fmt_gh_data, fmt_who_data)
	for country, count in diff_data.items():
		if count > 0:
			assert country in counted
			assert count == counted[country]
