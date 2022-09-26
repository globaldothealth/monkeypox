import csv
from decimal import Decimal
from datetime import date
import logging
import os
from time import sleep

import boto3
from faker import Faker
import requests

from logger import setup_logger
from run import FOLDERS


LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL", "http://localstack:4566")
S3_BUCKET = os.environ.get("S3_BUCKET", "monkeypox")
S3_CLIENT = boto3.client("s3", endpoint_url=LOCALSTACK_URL)

FAKE = Faker()


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


def create_fake_file(file_name: str=FAKE.file_name(), data: list[dict]=[]) -> str:
	print(f"File name: {file_name}")
	fn_w_ext = f"{file_name}.csv"
	logging.info(f"Using fake data to create file {fn_w_ext}")
	with open(fn_w_ext, "w") as fh:
		fields = list(data[0].keys())
		writer = csv.DictWriter(fh, fieldnames=fields)
		writer.writeheader()
		for row in data:
			writer.writerow(row)
	return fn_w_ext


def upload_file(folder: str, file_name: str) -> None:
	logging.info(f"Uploading file {file_name} to folder {S3_BUCKET}/{folder}")
	S3_CLIENT.upload_file(file_name, S3_BUCKET, f"{folder}/{file_name}")


if __name__ == "__main__":
	setup_logger()
	logging.info("Starting script")
	wait_for_localstack()
	create_bucket(S3_BUCKET)
	for folder in FOLDERS:
		print(f"Folder in setup: {folder}")
		for _ in range(0, 3):
			data = create_fake_data()
			file_name = create_fake_file(data=data)
			upload_file(folder, file_name)
