import csv
import json
import logging
import os
from time import sleep

import boto3
import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError


LOCALSTACK_URL = os.environ.get("AWS_ENDPOINT", "http://localstack:4566")
S3_BUCKET = os.environ.get("S3_BUCKET", "test")

DB_CONNECTION = os.environ.get("DB_CONNECTION", "test")
DATABASE_NAME = os.environ.get("DB_NAME", "monkeypox")

CDC_COLLECTION = os.environ.get("CDC_COLLECTION", "cdc")
GH_COLLECTION = os.environ.get("GH_COLLECTION", "gh")
WHO_COLLECTION = os.environ.get("WHO_COLLECTION", "who")

MOUNTEBANK_URL = os.environ.get("MOUNTEBANK_URL", "http://mountebank:2525")
CDC_DATA_CSV = os.environ.get("CDC_DATA_CSV", "cdc_data.csv")
ECDC_HTML = os.environ.get("ECDC_HTML", "ecdc_site.html")
WHO_DATA_JSON = os.environ.get("WHO_DATA_JSON", "who_data.json")
CDC_STUB_JSON = os.environ.get("CDC_STUB_JSON", "cdc_stub_request.json")
ECDC_STUB_JSON = os.environ.get("ECDC_STUB_JSON", "ecdc_stub_request.json")
WHO_STUB_JSON = os.environ.get("WHO_STUB_JSON", "who_stub_request.json")
CDC_STUB_PORT = os.environ.get("CDC_STUB_PORT", 4242)
ECDC_STUB_PORT = os.environ.get("ECDC_STUB_PORT", 4243)
WHO_STUB_PORT = os.environ.get("WHO_STUB_PORT", 4244)

MAX_ATTEMPTS = 42
WAIT_TIME = 5


def wait_for_localstack():
	logging.info("Waiting for localstack")
	healthcheck_url = "".join([LOCALSTACK_URL, "/health"])
	counter = 0
	while counter < MAX_ATTEMPTS:
		try:
			response = requests.get(healthcheck_url)
			s3_status = response.json().get("services", {}).get("s3")
			if s3_status == "running":
				return
		except requests.exceptions.ConnectionError:
			pass
		counter += 1
		sleep(WAIT_TIME)
	raise Exception("Localstack not available")


def wait_for_database():
	logging.info("Waiting for database")
	counter = 0
	while counter < MAX_ATTEMPTS:
		try:
			client = MongoClient(DB_CONNECTION)
			logging.info(f"Connected with access to: {client.list_database_names()}")
			return
		except PyMongoError:
			logging.info(f"Database service not ready yet, retrying in {WAIT_TIME} seconds")
			pass
		counter += 1
		sleep(WAIT_TIME)
	raise Exception("Database service not available")


def create_bucket(bucket_name:str) -> None:
	logging.info(f"Creating S3 bucket {bucket_name}")
	s3_client = boto3.client("s3", endpoint_url=LOCALSTACK_URL)
	s3_client.create_bucket(Bucket=bucket_name)


def create_database():
	logging.info(f"Creating {DATABASE_NAME} database, or confirming it exists")
	client = MongoClient(DB_CONNECTION)
	database = client[DATABASE_NAME]
	logging.info("Creating collections")
	_ = database[CDC_COLLECTION]
	_ = database[WHO_COLLECTION]


def create_stubs():
	cdc_stub = get_json(CDC_STUB_JSON)
	ecdc_stub = get_json(ECDC_STUB_JSON)
	who_stub = get_json(WHO_STUB_JSON)
	cdc_data = get_csv(CDC_DATA_CSV)
	ecdc_html = get_html(ECDC_HTML)
	who_data = get_json(WHO_DATA_JSON)
	cdc_stub["port"] = CDC_STUB_PORT
	ecdc_stub["port"] = ECDC_STUB_PORT
	who_stub["port"] = WHO_STUB_PORT
	cdc_stub["stubs"][0]["responses"][0]["is"]["body"] = cdc_data
	ecdc_stub["stubs"][0]["responses"][0]["is"]["body"] = ecdc_html
	who_stub["stubs"][0]["responses"][0]["is"]["body"] = who_data
	create_stub(CDC_STUB_PORT, cdc_stub)
	create_stub(ECDC_STUB_PORT, ecdc_stub)
	create_stub(WHO_STUB_PORT, who_stub)


def get_csv(file_name: str) -> str:
	logging.info("Getting CSV from {file_name}")
	csv_str = ""
	with open(file_name) as fh:
		reader = csv.reader(fh)
		for row in reader:
			csv_str += f"{','.join(row)}\r\n"
	return csv_str


def get_json(file_name: str) -> dict:
	logging.info("Getting JSON from {file_name}")
	with open(file_name) as fh:
		return json.load(fh)


def get_html(file_name: str):
	logging.info("Getting HTML from {file_name}")
	with open(file_name) as fh:
		return fh.read()


def create_stub(port: int, json: dict) -> None:
	logging.info(f"Creating stub at port {port}")
	requests.post(f"{MOUNTEBANK_URL}/imposters", json=json)


if __name__ == "__main__":
	logging.info("Starting local/testing setup script")
	wait_for_localstack()
	wait_for_database()
	create_database()
	create_bucket(S3_BUCKET)
	create_stubs()
	logging.info("Done")
