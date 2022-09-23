import csv
from datetime import date
import io
import logging
import os
import sys

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

WHO_TO_GH = {
	"Republic Of Korea": "South Korea",
	"Venezuela (Bolivarian Republic of)": "Venezuela",
	"Türkiye": "Turkey",
	"T\u00fcrkiye": "Turkey",
	"Bosnia And Herzegovina": "Bosnia And Herzegovina",
	"Czechia": "Czech Republic",
	"Bolivia (Plurinational State of)": "Bolivia",
	"Russian Federation": "Russia",
	"Saint Martin": "Saint Martin (French part)",
	"Republic Of Moldova": "Moldova",
	"Iran (Islamic Republic of)": "Iran"
}

TODAY = date.today()

COUNT = 1


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


def get_cdc_data():
	logging.info("Getting CDC data from the database")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][CDC_COLLECTION]
	cdc_data = collection.find({})
	return {x["Location"]: int(x["Cases"]) for x in cdc_data}


def get_who_data():
	logging.info("Getting WHO data from the database")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][WHO_COLLECTION]
	who_data = collection.find({})
	return {x["COUNTRY"]: int(x["CasesAll"]) for x in who_data}


def get_gh_usa_data():
	logging.info("Getting G.h USA data from the database")
	collection = MongoClient(DB_CONNECTION)[DATABASE_NAME][GH_COLLECTION]
	data = collection.find({"Case_status": "confirmed"})  #, cursor_type=TAILABLE_AWAIT)
	counts = {}
	for case in data:
		location = case["Location_information"]
		if "United States" in location and "Total" not in location and location not in counts:
			counts[location] = 1
		elif "United States" in location and "Total" not in location and location in counts:
			counts[location] += 1
	return counts


def get_gh_world_data():
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
		if delta := cdc_count - gh_usa_data.get(state, 0):
			add_or_remove_cases(delta, f"{state}, United States")
	if not_in_cdc := list(set(gh_usa_data) - set(cdc_data)):
		for state in not_in_cdc:
			delta = -gh_usa_data.get(state, 0)
			add_or_remove_cases(delta, f"{state}, United States")


def who_to_gh(who_data, gh_world_data):
	logging.info("Adjusting G.h data to match WHO counts")
	for country, count in who_data.items():
		title = country.title()
		if country == "USA" or title == "United States Of America" or "Region" in title:
			continue
		country = WHO_TO_GH.get(title, title)
		if delta := count - gh_world_data.get(country, 0):
			add_or_remove_cases(delta, country)
	if not_in_who := list(set(gh_world_data) - set(who_data)):
		for country in not_in_who:
			who_count = 0
			if country in WHO_TO_GH.values():
				who_name = WHO_TO_GH[list(WHO_TO_GH.values()).index(country)]
				who_count = who_data[who_name]
			delta = who_count - gh_world_data.get(country, 0)
			add_or_remove_cases(delta, country)


def add_or_remove_cases(count, location):
	if count > 0:
		add_cases(count, location)
	elif count < 0:
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
		cases = []
		for case in cases_gen:
			cases.append(case)
		return cases_to_csv(cases)
	except Exception:
		logging.exception("An error occurred while converting G.h data to CSV")
		raise


def cases_to_csv(cases: list[dict]) -> str:
	logging.info("Converting cases to CSV")
	buf = io.StringIO()
	writer = csv.DictWriter(buf, fieldnames=cases[0])
	writer.writeheader()
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
