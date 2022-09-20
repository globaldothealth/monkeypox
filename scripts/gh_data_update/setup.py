import csv
import json
import logging
import os
from time import sleep

import boto3
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import requests

from run import setup_logger


MAX_ATTEMPTS = 42
WAIT_TIME = 5

LOCALSTACK_URL = os.environ.get("AWS_ENDPOINT", "http://localstack:4566")
S3_BUCKET = os.environ.get("S3_BUCKET", "test")

DB_CONNECTION = os.environ.get("DB_CONNECTION", "test")

DATABASE_NAME = os.environ.get("DB_NAME", "monkeypox")
CDC_COLLECTION = os.environ.get("CDC_COLLECTION", "cdc")
GH_COLLECTION = os.environ.get("GH_COLLECTION", "cdc")
WHO_COLLECTION = os.environ.get("WHO_COLLECTION", "who")

CDC_DATA_CSV = os.environ.get("CDC_DATA_CSV", "cdc_data.csv")
WHO_DATA_JSON = os.environ.get("WHO_DATA_JSON", "who_data.json")


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
			logging.info(f"Localstack not ready yet, retrying in {WAIT_TIME} seconds")
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


def create_bucket(bucket_name: str):
	logging.info(f"Creating S3 bucket: {bucket_name}")
	s3_client = boto3.client("s3", endpoint_url=LOCALSTACK_URL)
	s3_client.create_bucket(Bucket=bucket_name)


def create_database():
	logging.info(f"Creating {DATABASE_NAME} database, or confirming it exists")
	client = MongoClient(DB_CONNECTION)
	database = client[DATABASE_NAME]
	logging.info("Creating collections")
	_ = database[CDC_COLLECTION]
	_ = database[GH_COLLECTION]
	_ = database[WHO_COLLECTION]


def get_cdc_data() -> list[dict]:
	logging.info("Getting CSV from {CDC_DATA_CSV}")
	with open(CDC_DATA_CSV) as fh:
		reader = csv.DictReader(fh)
		return [row for row in reader]


def get_who_data() -> list[dict]:
	logging.info("Getting JSON from {WHO_DATA_JSON}")
	with open(WHO_DATA_JSON) as fh:
		return json.load(fh)


def insert_data(collection: str, data: list[dict]):
	logging.info(f"Adding data to {collection} collection")
	client = MongoClient(DB_CONNECTION)
	database = client[DATABASE_NAME]
	database[collection].insert_many(data)


if __name__ == "__main__":
	setup_logger()
	logging.info("Starting local/testing setup script")
	wait_for_localstack()
	create_bucket(S3_BUCKET)
	wait_for_database()
	create_database()
	cdc_data = get_cdc_data()
	who_data = get_who_data()
	insert_data(CDC_COLLECTION, cdc_data)
	insert_data(WHO_COLLECTION, who_data)
	logging.info("Done")
