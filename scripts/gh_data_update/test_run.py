import csv
import io
import os

import boto3
from pymongo import MongoClient
import pytest

from run import (update_gh_data, gh_data_to_s3, gh_data_to_csv, get_cdc_data, get_who_data,
	get_gh_usa_data, get_gh_world_data, cases_to_csv, country_name_to_titlecase,
	DB_CONNECTION, DATABASE_NAME, GH_COLLECTION, TODAY)


LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")
GH_FOLDER = os.environ.get("GH_DATA_FOLDER")


def get_contents(file_name: str) -> str:
    s3 = boto3.resource("s3", endpoint_url=LOCALSTACK_URL)
    obj = s3.Object(S3_BUCKET, file_name)
    return obj.get()["Body"].read().decode("utf-8")


def is_valid_csv(data: str) -> bool:
	reader = csv.DictReader(data)
	for row in reader:
		if len(row) == 0:
			return False
	return True


def csv_to_dict(data: str) -> list[dict]:
	f = io.StringIO(data)
	reader = csv.DictReader(f)
	return [row for row in reader]


def test_run():
	MongoClient(DB_CONNECTION)[DATABASE_NAME].drop_collection(GH_COLLECTION)
	update_gh_data()
	gh_data_to_s3()
	data_a = get_contents("latest.csv")
	data_b = get_contents(f"{GH_FOLDER}/{TODAY}.csv")
	assert is_valid_csv(data_a)
	assert data_a == data_b
	db_data = gh_data_to_csv()
	assert data_a == db_data
	cdc_counts = get_cdc_data()
	cdc_counts.pop("Total")
	tmp = get_gh_usa_data()
	gh_usa_counts = {}
	for state, count in tmp.items():
		gh_usa_counts[state.split(",")[0]] = tmp[state]
	assert cdc_counts == gh_usa_counts
	gh_global_counts = get_gh_world_data()
	tmp = get_who_data()
	who_counts = {}
	for country, count in tmp.items():
		if "Region" in country or country.upper() == "UNITED STATES OF AMERICA" or count == 0:
			continue
		who_counts[country_name_to_titlecase(country)] = count
	assert who_counts == gh_global_counts


def test_idempotence():
	MongoClient(DB_CONNECTION)[DATABASE_NAME].drop_collection(GH_COLLECTION)
	update_gh_data()
	gh_data_to_s3()
	counts_before = get_gh_world_data()
	update_gh_data()
	gh_data_to_s3()
	counts_after = get_gh_world_data()
	assert counts_before == counts_after


def test_cases_to_csv():
	case_gen = (x for x in [{"A": 1, "B": 2}, {"A": 3, "B": 4}])
	csv_data = cases_to_csv(case_gen)
	assert csv_data == "A,B\r\n1,2\r\n3,4\r\n"
