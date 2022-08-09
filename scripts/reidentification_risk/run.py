import csv
from datetime import datetime
import logging
import os

import boto3
import requests


SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")
DATA_FILE = "/output/sdcmicro.csv"
OUTPUT_DIR = "/output/"
S3_PREFIX = "reidentification-risk"

TODAY = datetime.now().strftime("%d_%m_%Y")


def get_data() -> None:
	logging.info(f"Getting data from {DATA_FILE}")
	reader = csv.DictReader(open(DATA_FILE))
	return next(reader)


def format_message(data: dict) -> str:
	logging.info("Formatting slack message")
	hros = data.get("HigherRiskObservations", "")
	reids = data.get("ExpectedReIdentifications", "")
	pct = data.get("PercentExpectedReId", "")

	message = f"SDCMicro risk measures for {TODAY}:\n\n"
	message += f"Number of observations with higher risk than the main part of the data: {hros}\n"
	message += f"Expected number of reidentifications: {reids} ({pct}%)"

	return message


def send_slack_message(slack_message: str) -> None:
	if SLACK_WEBHOOK_URL:
		logging.info("Sending Slack message")
		try:
			response = requests.post(SLACK_WEBHOOK_URL, json={"text": slack_message})
			if response.status_code != 200:
				logging.error(f"Slack notification failed with {response.status_code}: {response.text}")
		except Exception:
			logging.exception("Slack notification failed due to an error")
	else:
		logging.info("No target for Slack message")


def upload_output_files() -> None:
	logging.info("Uploading output files to S3")
	s3_client = None
	if LOCALSTACK_URL:
		s3_client = boto3.client("s3", endpoint_url=LOCALSTACK_URL)
	else:
		s3_client = boto3.client("s3")
	for (root, dirs, files) in os.walk(OUTPUT_DIR):
		for f in files:
			try:
				s3_client.upload_file(f"{OUTPUT_DIR}/{f}", S3_BUCKET, f"{S3_PREFIX}/{TODAY}_{f}")
			except Exception:
				logging.exception(f"Error uploading {f} to S3")
				raise


if __name__ == "__main__":
	logging.info("Starting data reporting and backup script")
	data = get_data()
	msg = format_message(data)
	send_slack_message(msg)
	upload_output_files()
	logging.info("Done")
