import csv
from datetime import date
import io
import logging
import os
import sys
from collections.abc import Iterable

import boto3
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.operations import InsertOne, UpdateOne

from case import dict_to_case, case_to_dict, Case, EMPTY_CASE_AS_DICT


DB_CONNECTION = os.environ.get("DB_CONNECTION")
DATABASE_NAME = os.environ.get("DATABASE_NAME")

CDC_COLLECTION = os.environ.get("CDC_COLLECTION", "cdc")
WHO_COLLECTION = os.environ.get("WHO_COLLECTION", "who")
GH_COLLECTION = os.environ.get("GH_COLLECTION", "gh")

LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")
GH_FOLDER = os.environ.get("GH_DATA_FOLDER")

LOWERCASE_WORDS_IN_COUNTRY_NAMES = [
	"of", "the", "and", "part", 
]

TODAY = date.today()


def setup_logger() -> None:
	h = logging.StreamHandler(sys.stdout)
	rootLogger = logging.getLogger()
	rootLogger.addHandler(h)
	rootLogger.setLevel(logging.INFO)


def update_gh_data():
	logging.info("Updating G.h data")
	try:
		cdc_data = get_cdc_data()
		who_data = get_who_data()
		gh_usa_data = get_gh_usa_data()
		gh_world_data = get_gh_world_data()
		cdc_to_gh(cdc_data, gh_usa_data)
		who_to_gh(who_data, gh_world_data)
	except Exception:
		logging.exception("An error occurred while trying to update G.h data")
		raise


def get_cdc_data() -> dict:
	logging.info("Getting CDC data from the database")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][CDC_COLLECTION]
	cdc_data = collection.find({})
	return {x["Location"]: int(x["Cases"]) for x in cdc_data}


def get_who_data() -> dict:
	logging.info("Getting WHO data from the database")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][WHO_COLLECTION]
	who_data = collection.find({})
	return {x["COUNTRY"]: int(x["TOTAL_CONF_CASES"]) for x in who_data}


def get_gh_usa_data() -> dict:
	logging.info("Getting G.h USA data from the database")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][GH_COLLECTION]
	data = collection.find({"Case_status": "confirmed"})
	counts = {}
	for case in data:
		location = case["Location_information"]
		if "United States" in location and "Total" not in location and location not in counts:
			counts[location] = 1
		elif "United States" in location and "Total" not in location and location in counts:
			counts[location] += 1
	return counts


def get_gh_world_data() -> dict:
	logging.info("Getting G.h global data from the database")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][GH_COLLECTION]
	data = collection.find({"Case_status": "confirmed"})
	counts = {}
	for case in data:
		location = case["Location_information"]
		if "United States" not in location and "Region" not in location and location not in counts:
			counts[location] = 1
		elif "United States" not in location and "Region" not in location and location in counts:
			counts[location] += 1
	return counts


def cdc_to_gh(cdc_data, gh_usa_data):
	logging.info("Adjusting G.h data to match CDC counts")
	for state, cdc_count in cdc_data.items():
		if state == "Total":
			continue
		if delta := cdc_count - gh_usa_data.get(f"{state}, United States", 0):
			add_or_remove_cases(delta, f"{state}, United States")
	cdc_states = [f"{state}, United States" for state in cdc_data]
	if not_in_cdc := list(set(gh_usa_data) - set(cdc_states)):
		for location in not_in_cdc:
			delta = -gh_usa_data.get(location.split(",")[0], 0)
			add_or_remove_cases(delta, location)


def who_to_gh(who_data, gh_world_data):
	logging.info("Adjusting G.h data to match WHO counts")
	for country, count in who_data.items():
		if country.upper() == "UNITED STATES OF AMERICA" or "Region" in country:
			continue
		title = country_name_to_titlecase(country)
		if delta := count - gh_world_data.get(title, 0):
			add_or_remove_cases(delta, title)
	who_countries = [country_name_to_titlecase(name) for name in list(who_data.keys())]
	if not_in_who := list(set(gh_world_data) - set(who_countries)):
		for country in not_in_who:
			delta = -gh_world_data.get(country, 0)
			add_or_remove_cases(delta, country)


def country_name_to_titlecase(country_name):
	if len(country_name.split(" ")) == 1:
		return country_name.capitalize()
	if "(" in country_name:
		outer = country_name.split("(")[0]
		inner = country_name[country_name.find("(")+1:country_name.find(")")]
		return f"{country_name_to_titlecase(outer)}({country_name_to_titlecase(inner)})"
	else:
		return " ".join(
			[word.lower() if word.lower() in LOWERCASE_WORDS_IN_COUNTRY_NAMES 
			else word.capitalize() for word in country_name.split(" ")]
		)


def add_or_remove_cases(count, location):
	if count > 0:
		logging.debug(f"Adding {count} cases for {location}")
		add_cases(count, location)
	elif count < 0:
		logging.debug(f"Removing {-count} cases for {location}")
		remove_cases(-count, location)


def add_cases(count, location):
	logging.info(f"Adding {count} cases for {location}")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][GH_COLLECTION]
	bulk_request = []
	for _ in range(count):
		case_dict = EMPTY_CASE_AS_DICT.copy()
		case_dict["Location_information"] = location
		case_dict["Case_status"] = "confirmed"
		case = dict_to_case(case_dict)  # Janky way to get default values
		case_dict = case_to_dict(case)
		bulk_request.append(InsertOne(case_dict))
	try:
		collection.bulk_write(bulk_request)
	except PyMongoError:
		logging.exception(f"An error occurred trying to add cases for {location}")
		raise


def remove_cases(count, location):
	logging.info(f"Removing {count} cases for {location}")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][GH_COLLECTION]
	bulk_request = []
	query = {"$and": [{"Location_information": location}, {"Case_status": "confirmed"}]}
	for record in collection.find(query).limit(count):
		bulk_request.append(UpdateOne({"_id": record["_id"]}, {"$set": {"Case_status": "discarded"}}))
	try:
		collection.bulk_write(bulk_request)
	except PyMongoError:
		logging.exception(f"An error occurred trying to remove cases for {location}")
		raise


def gh_data_to_s3():
	logging.info("Storing G.h data as CSV files in S3")
	csv_data = gh_data_to_csv()
	store_file("latest.csv", csv_data)
	store_file(f"{GH_FOLDER}/{TODAY}.csv", csv_data)


def gh_data_to_csv() -> str:
	logging.info("Converting G.h data to CSV")
	try:
		collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][GH_COLLECTION]
		cursor = collection.find({"Case_status": "confirmed"})
		cases_gen = (case for case in cursor)
		return cases_to_csv(cases_gen)
	except Exception:
		logging.exception("An error occurred while converting G.h data to CSV")
		raise


def cases_to_csv(cases: Iterable[dict]) -> str:
	logging.info("Converting cases to CSV")
	buf = io.StringIO()
	first_case = next(iter(cases))
	writer = csv.DictWriter(buf, fieldnames=first_case)
	writer.writeheader()
	writer.writerow(first_case)
	for case in cases:
		writer.writerow(case)
	return buf.getvalue()


def store_file(file_name: str, data: str):
	logging.info(f"Storing {file_name}")
	try:
		s3 = boto3.resource("s3")
		if LOCALSTACK_URL:
			s3 = boto3.resource("s3", endpoint_url=LOCALSTACK_URL)
		s3.Object(S3_BUCKET, file_name).put(Body=data)
	except Exception as exc:
		logging.exception(f"An exception occurred while trying to upload {file_name}")
		raise


if __name__ == "__main__":
	setup_logger()
	logging.info("Starting run")
	update_gh_data()
	gh_data_to_s3()
