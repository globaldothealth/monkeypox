from datetime import date
import json
from os import environ

import boto3
import pygsheets


S3_BUCKET = environ.get("S3_BUCKET")

DOCUMENT_ID = environ.get("DOCUMENT_ID")

# FIXME: env var
CREDS_FILE = "credentials.json"


def get_data():
	client = pygsheets.authorize(service_file=CREDS_FILE)
	spreadsheet = client.open_by_key(DOCUMENT_ID)

	data = spreadsheet[0].get_all_records()

	return data


def clean_data(data):
	for point in data:
		point.pop("Curator_initials")
	return data


def format_data(data):
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

	s3 = boto3.resource("s3")

	now = date.today()

	file_name = f"{now}.json"

	s3.Object(S3_BUCKET, file_name).put(Body=json_data)
	s3.Object(S3_BUCKET, "latest.json").put(Body=json_data)

	file_name = f"{now}.csv"

	s3.Object(S3_BUCKET, file_name).put(Body=csv_data)
	s3.Object(S3_BUCKET, "latest.csv").put(Body=csv_data)


if __name__ == "__main__":
	data = get_data()
	data = clean_data(data)
	json_data, csv_data = format_data(data)
	store_data(json_data, csv_data)
