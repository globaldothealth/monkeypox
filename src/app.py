from datetime import datetime
import json
import logging
from os import environ
import sys
from urllib.parse import urlparse

import boto3
import pdfkit
import pygsheets


S3_BUCKET = environ.get("S3_BUCKET")
DOCUMENT_ID = environ.get("DOCUMENT_ID")

S3 = boto3.resource("s3")

DATA_FOLDER = "archives"
SOURCES_FOLDER = "sources"

BUCKET_CONTENTS = []


def setup_logger():
    h = logging.StreamHandler(sys.stdout)
    rootLogger = logging.getLogger()
    rootLogger.addHandler(h)
    rootLogger.setLevel(logging.DEBUG)


def get_data():
	logging.info("Getting data from Google Sheets")
	client = pygsheets.authorize(service_account_env_var="GOOGLE_CREDENTIALS")
	spreadsheet = client.open_by_key(DOCUMENT_ID)

	return spreadsheet[0].get_all_records()


def get_source_urls(data):
	logging.info("Getting source urls from data")
	source_urls = set(())
	for case in data:
		source_urls.add(case.get("Source"))
		source_urls.add(case.get("Source_II"))
	source_urls.remove("")
	return source_urls


def clean_data(data):
	logging.info("Cleaning data")
	for case in data:
		case.pop("Curator_initials")
	return data


def format_data(data):
	logging.info("Formatting data")
	json_data = json.dumps(data)
	csv_data = ""
	column_names = data[0].keys()
	for name in column_names:
		csv_data += f"{name},"
	csv_data += "\n"
	for row in data:
		for val in row.values():
			csv_data += f"{str(val).replace(',', ';')},"
		csv_data += "\n"
	return json_data, csv_data


def store_data(json_data, csv_data):
	logging.info("Uploading data to S3")
	now = datetime.today()
	for fmt in ["csv", "json"]:
		try:
			S3.Object(S3_BUCKET, f"{DATA_FOLDER}/{now}.{fmt}").put(Body=csv_data)
			S3.Object(S3_BUCKET, f"latest.{fmt}").put(Body=csv_data)
		except Exception as exc:
			logging.exception(f"An exception occurred while trying to upload {fmt} files")
			raise


def source_urls_to_pdfs(source_urls):
	logging.info("Converting source websites into PDFs")
	pdfs = []
	for source_url in source_urls:
		parsed_url = urlparse(source_url)
		name = f"{parsed_url.path.replace('/', '_')[1:]}.pdf"
		if bucket_contains(name):
			logging.info(f"Found {name} in bucket, skipping it")
			continue
		logging.info(f"Saving content from {source_url} to {name}")
		try:
			pdfkit.from_url(source_url, name, options={"page-size": "Letter"})
			pdfs.append(name)
		except Exception:
			logging.exception(f"An exception occurred while trying to convert {source_url} to {name}")
	return pdfs


def bucket_contains(file_name):
	global BUCKET_CONTENTS
	if not BUCKET_CONTENTS:
		objects = S3.Bucket(S3_BUCKET).objects.all()
		BUCKET_CONTENTS = [o.key.split("/")[1] for o in objects if o.key.startswith(f"{SOURCES_FOLDER}/")]
	return file_name in BUCKET_CONTENTS


def store_pdfs(pdfs):
	logging.info("Uploading sources to S3")
	for pdf in pdfs:
		try:
			S3.Object(S3_BUCKET, f"{SOURCES_FOLDER}/{pdf}").upload_file(pdf)
		except Exception:
			logging.exception(f"An exception occurred while trying to upload {pdf}")
			raise


if __name__ == "__main__":
	setup_logger()
	logging.info("Starting script")
	data = get_data()
	data = clean_data(data)
	json_data, csv_data = format_data(data)
	store_data(json_data, csv_data)
	source_urls = get_source_urls(data)
	pdfs = source_urls_to_pdfs(source_urls)
	store_pdfs(pdfs)
	logging.info("Script completed")
