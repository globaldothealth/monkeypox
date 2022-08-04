import csv
from decimal import Decimal
from datetime import date
import json
import logging
import os
from time import sleep

import boto3
from faker import Faker
import requests

from logger import setup_logger


LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL", "http://localstack:4566")
S3_BUCKET = os.environ.get("S3_BUCKET", "monkeypox")

FOLDERS = ["archives", "case-definitions", "ecdc", "ecdc-archives"]

S3_CLIENT = boto3.client("s3", endpoint_url=LOCALSTACK_URL)
FAKE = Faker()


# For some of the faker.profile()-created fields
class DateDecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, date):
        	return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def wait_for_localstack():
	logging.info("Waiting for localstack")
	healthcheck_url = "".join([LOCALSTACK_URL, "/health"])
	counter = 0
	while counter < 42:
		try:
			response = requests.get(healthcheck_url)
			s3_status = response.json().get("services", {}).get("s3")
			if s3_status == "running":
				return
		except requests.exceptions.ConnectionError:
			pass
		counter += 1
		sleep(5)
	raise Exception("Localstack not available")


def create_bucket(bucket_name: str) -> None:
	logging.info(f"Creating bucket {bucket_name}")
	S3_CLIENT.create_bucket(Bucket=bucket_name)


def create_fake_data() -> list[dict]:
	logging.info("Creating fake data")
	return [FAKE.profile() for _ in range(0, 42)]


def create_fake_file(data: [dict], fmt: str) -> str:
	file_name = f"{FAKE.file_name()}.{fmt}"  # fake
	logging.info(f"Using fake data to create file {file_name}")
	if fmt == "json":
		with open(file_name, "w") as fh:
			json.dump(data, fh, cls=DateDecimalEncoder)
		return file_name
	if fmt == "csv":
		with open(file_name, "w") as fh:
			fields = list(data[0].keys())
			writer = csv.DictWriter(fh, fieldnames=fields)
			writer.writeheader()
			for row in data:
				writer.writerow(row)
		return file_name
	raise Exception(f"Format {fmt} not valid")


def upload_file(folder: str, file_name: str) -> None:
	logging.info(f"Uploading file {file_name} to folder {folder}")
	S3_CLIENT.upload_file(file_name, S3_BUCKET, f"{folder}/{file_name}")


if __name__ == "__main__":
	setup_logger()
	logging.info("Starting script")
	wait_for_localstack()
	create_bucket(S3_BUCKET)
	for folder in FOLDERS:
		for _ in range(0, 3):
			data = create_fake_data()
			for fmt in ["csv", "json"]:
				fn = create_fake_file(data, fmt)
				upload_file(folder, fn)
