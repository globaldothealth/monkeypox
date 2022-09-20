import codecs
import csv
from datetime import date
import io
from itertools import repeat
import json
import logging
import os
import re
import sys

import boto3
from bs4 import BeautifulSoup
import click
import requests


S3_BUCKET = os.environ.get("S3_BUCKET")
LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")

CDC_URL = os.environ.get("CDC_URL")
ECDC_URL = os.environ.get("ECDC_URL")
WHO_URL = os.environ.get("WHO_URL")

AGENCIES = {
	"CDC": os.environ.get("CDC_FOLDER"),
	"ECDC": os.environ.get("ECDC_FOLDER"),
	"WHO": os.environ.get("WHO_FOLDER")
}

# ECDC scraping targets
ONSET_OCA_DIV_ID = "by-date-of-onset-and-by-country-or-area"
ONSET_OCA_FIELDS = ["date", "country", "count"]
NOTIF_DIV_ID = "overall-by-date-of-notification"
NOTIF_FIELDS = ["date", "count"]
ONSET_DATE_DIV_ID = "overall-by-date-of-symptom-onset"
ONSET_DATE_FIELDS = ["date", "count", "type"]

TARGET_DIVS = [ONSET_OCA_DIV_ID, NOTIF_DIV_ID, ONSET_DATE_DIV_ID]

REGEXES = {
	ONSET_OCA_DIV_ID: r"Date: (20\d\d-[0-1]\d-\d\d)<br />count:\s+(\d+)<br />ReportingCountry:\s+(.*)",
	NOTIF_DIV_ID: r"DateNotif: (20\d\d-[0-1]\d-\d\d)<br />count:\s+(\d+)",
	ONSET_DATE_DIV_ID: r"Date: (20\d\d-[0-1]\d-\d\d)<br />count:\s+(\d+)<br />TypeDate: (\w+)"
}

FIELDS = {
	ONSET_OCA_DIV_ID: ONSET_OCA_FIELDS,
	NOTIF_DIV_ID: NOTIF_FIELDS,
	ONSET_DATE_DIV_ID: ONSET_DATE_FIELDS
}

TODAY = date.today()


def setup_logger() -> None:
	h = logging.StreamHandler(sys.stdout)
	rootLogger = logging.getLogger()
	rootLogger.addHandler(h)
	rootLogger.setLevel(logging.INFO)


class AgencyIngestor():

	def __init__(self, name: str, url: str):
		if name not in AGENCIES:
			raise Exception(f"{name} must be one of {AGENCIES}")
		self.name = name
		self.url = url
		self.data = []
		self.csv_data = ""

	def ingest_data(self):
		logging.info(f"Ingesting {self.name} data")
		self.get_data()
		self.data_to_csv(self.data[0])
		folder = AGENCIES.get(self.name, self.name)
		self.store_data(f"{folder}/{TODAY}.csv")
		self.store_data(f"{self.name.lower()}_latest.csv")

	def get_data(self):
		raise NotImplementedError("The base class' method does nothing")

	def data_to_csv(self, field_names: list[str]):
		logging.info("Converting data to CSV")
		buf = io.StringIO()
		writer = csv.DictWriter(buf, fieldnames=field_names)
		writer.writeheader()
		for row in self.data:
			writer.writerow(row)
		self.csv_data = buf.getvalue()

	def store_data(self, file_name: str):
		logging.info(f"Storing {file_name}")
		try:
			s3 = boto3.resource("s3")
			if LOCALSTACK_URL:
				s3 = boto3.resource("s3", endpoint_url=LOCALSTACK_URL)
			s3.Object(S3_BUCKET, file_name).put(Body=self.csv_data)
		except Exception as exc:
			logging.exception(f"An exception occurred while trying to upload {file_name}")
			raise


class CDCIngestor(AgencyIngestor):

	def __init__(self):
		super().__init__("CDC", CDC_URL)

	def get_data(self):
		logging.info(f"Getting {self.name} data")
		try:
			response = requests.get(CDC_URL)
			reader = csv.DictReader(codecs.iterdecode(response.iter_lines(), "utf-8"))
			self.data = [row for row in reader]
		except Exception:
			logging.exception(f"Something went wrong when trying to retrieve {self.name} data")
			raise


class WHOIngestor(AgencyIngestor):

	def __init__(self):
		super().__init__("WHO", WHO_URL)

	def get_data(self):
		logging.info(f"Getting {self.name} data")
		try:
			self.data = requests.post(WHO_URL, json={}).json().get("Data")
		except Exception:
			logging.exception(f"Something went wrong when trying to retrieve {self.name} data")
			raise


class ECDCIngestor(AgencyIngestor):

	def __init__(self):
		super().__init__("ECDC", ECDC_URL)
		self.soup = None
		self.site_content = ""
		self.json_soup = {}

	def ingest_data(self):
		logging.info("Ingesting ECDC data")
		self.get_site_content()
		self.make_soup()
		for div in TARGET_DIVS:
			self.get_json(div)
			self.process_json(div)
			self.data_to_csv(FIELDS.get(div))
			self.store_data(f"{AGENCIES['ECDC']}/{TODAY}_{div}.csv")
			self.store_data(f"ecdc_{div}_latest.csv")
			self.data = []

	def get_site_content(self):
		logging.info("Getting contents of ECDC website")
		try:
			self.site_content = requests.get(ECDC_URL).content.decode("utf-8")
		except Exception:
			logging.exception("Something went wrong getting HTML from ECDC website")
			raise

	def make_soup(self):
		logging.info("Making BeautifulSoup")
		try:
			self.soup = BeautifulSoup(self.site_content, "html5lib")
		except Exception:
			try:
				self.soup = BeautifulSoup(self.site_content, "html.parser")
			except Exception:
				logging.exception("Something went wrong trying to make BeautifulSoup")
				raise

	def get_json(self, div: str):
		logging.info("Getting HTML from ECDC website")
		html = self.soup.find("div", id=div)
		if html is None:
			raise ValueError(f"div[id='{div}'] not found")
		script = html.find("script")
		if script is None:
			raise ValueError("No JSON data found in div")
		try:
			self.json_soup = json.loads(script.contents[0])
		except Exception:
			logging.exception("Something went wrong getting JSON from <script>")
			raise

	def process_json(self, div: str) -> list[dict[str, str | int]]:
		logging.info("Processing JSON data")
		for group in self.json_soup["x"]["data"]:
			text = group["text"]
			text = text if isinstance(text, list) else [text]
			# parse each line and remove invalid lines
			self.data.extend(list(filter(None, map(self.parse_line, text, repeat(div)))))

	@classmethod
	def parse_line(cls, line: str, div: str) -> [dict[str, str | int]]:
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
		return {}


@click.command()
@click.option("--cdc", is_flag=True, show_default=True, default=False, help="Ingest CDC data")
@click.option("--ecdc", is_flag=True, show_default=True, default=False, help="Ingest ECDC data")
@click.option("--who", is_flag=True, show_default=True, default=False, help="Ingest WHO data")
def run(cdc, ecdc, who):
	setup_logger()
	if not any([cdc, ecdc, who]):
		raise Exception("This script requires at least one target agency for data ingestion")
	logging.info("Starting run")
	if cdc:
		ingestor = CDCIngestor()
		ingestor.ingest_data()
	if ecdc:
		ingestor = ECDCIngestor()
		ingestor.ingest_data()
	if who:
		ingestor = WHOIngestor()
		ingestor.ingest_data()


if __name__ == "__main__":
	run()
