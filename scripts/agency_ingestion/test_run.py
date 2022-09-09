import csv
import os

import boto3
import pytest

from run import (AgencyIngestor, CDCIngestor, ECDCIngestor, WHOIngestor, TODAY,
	S3_BUCKET, AGENCIES, TARGET_DIVS, NOTIF_DIV_ID, ONSET_DATE_DIV_ID, ONSET_OCA_DIV_ID)

from setup import get_csv, get_html, get_json, CDC_DATA_CSV, ECDC_HTML, WHO_DATA_JSON


LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")


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


@pytest.mark.skipif(not os.environ.get("DOCKERIZED", False),
						reason="Running e2e tests outside of mock environment disabled")
def test_ingest_cdc_data():
	ingestor = CDCIngestor()
	ingestor.ingest_data()
	data_a = get_contents(f"{AGENCIES['CDC']}/{TODAY}.csv")
	data_b = get_contents("cdc_latest.csv")
	assert is_valid_csv(data_a)
	assert data_a == data_b


@pytest.mark.skipif(not os.environ.get("DOCKERIZED", False),
						reason="Running integration tests outside of mock environment disabled")
def test_get_cdc_data():
	ingestor = CDCIngestor()
	ingestor.get_data()
	ingestor.data_to_csv(ingestor.data[0])
	data = get_csv(CDC_DATA_CSV)
	assert ingestor.csv_data == data


def test_data_to_csv():
	ingestor = AgencyIngestor("WHO", "")
	ingestor.data = [{"A": 1, "B": 2}, {"A": "X", "B": "Y"}]
	ingestor.data_to_csv(ingestor.data[0])
	assert ingestor.csv_data == "A,B\r\n1,2\r\nX,Y\r\n"


def test_store_data():
	ingestor = AgencyIngestor("WHO", "")
	ingestor.csv_data = "foo"
	ingestor.store_data("test.txt")
	assert get_contents("test.txt") == ingestor.csv_data


@pytest.mark.skipif(not os.environ.get("DOCKERIZED", False),
						reason="Running e2e tests outside of mock environment disabled")
def test_ingest_who_data():
	ingestor = WHOIngestor()
	ingestor.ingest_data()
	data_a = get_contents(f"{AGENCIES['WHO']}/{TODAY}.csv")
	data_b = get_contents("who_latest.csv")
	assert is_valid_csv(data_a)
	assert data_a == data_b


@pytest.mark.skipif(not os.environ.get("DOCKERIZED", False),
						reason="Running integration tests outside of mock environment disabled")
def test_get_who_data():
	ingestor = WHOIngestor()
	ingestor.get_data()
	data = get_json(WHO_DATA_JSON)
	assert ingestor.data == data["Data"]


@pytest.mark.skipif(not os.environ.get("DOCKERIZED", False),
						reason="Running e2e tests outside of mock environment disabled")
def test_ingest_ecdc_data():
	ingestor = ECDCIngestor()
	ingestor.ingest_data()
	for div in TARGET_DIVS:
		data_a = get_contents(f"{AGENCIES['ECDC']}/{TODAY}_{div}.csv")
		data_b = get_contents(f"ecdc_{div}_latest.csv")
		assert is_valid_csv(data_a)
		assert data_a == data_b


def test_get_ecdc_site_content():
	ingestor = ECDCIngestor()
	ingestor.get_site_content()
	html = get_html(ECDC_HTML)
	assert ingestor.site_content == html


def test_get_ecdc_json():
	html = """
    <div id="by-date-of-onset-and-by-country-or-area">
    <script>{"x":1}</script>
    </div>
    """
	ingestor = ECDCIngestor()
	ingestor.site_content = html
	ingestor.make_soup()
	ingestor.get_json(ONSET_OCA_DIV_ID)
	assert ingestor.json_soup == {"x": 1}


@pytest.mark.parametrize(
	"source,div,expected",
	[
		(
			"DateNotif: 2022-05-05<br />count:   2",
			NOTIF_DIV_ID,
			{"date": "2022-05-05", "count": 2},
		),
		(
			"DateNotif: 2022-07-12<br />count: 471",
			NOTIF_DIV_ID,
			{"date": "2022-07-12", "count": 471},
		),
		(
			"Date: 2022-05-05<br />count:   1<br />TypeDate: Notification",
			ONSET_DATE_DIV_ID,
			{"date": "2022-05-05", "count": 1, "type": "Notification"}
		),
		(
			"Date: 2022-07-07<br />count:  85<br />TypeDate: Notification",
			ONSET_DATE_DIV_ID,
			{"date": "2022-07-07", "count": 85, "type": "Notification"}
		),
		(
			"Date: 2022-05-05<br />count:  1<br />ReportingCountry: New Zealand",
			ONSET_OCA_DIV_ID,
			{"date": "2022-05-05", "count": 1, "country": "New Zealand"},
		),
		(
			"Date: 2022-06-03<br />count: 10<br />ReportingCountry: New Zealand",
			ONSET_OCA_DIV_ID,
			{"date": "2022-06-03", "count": 10, "country": "New Zealand"},
		),
		("Date: 2022-05<br />count:  1<br />ReportingCountry: New Zealand", ONSET_OCA_DIV_ID, {}),
		("Date: 2122-01-01<br />count: 100<br />ReportingCountry: New Zealand", ONSET_OCA_DIV_ID, {}),
		(
			"Date: 2022-02-03<br />count:  9<br />ReportingCountry: Belgium",
			ONSET_OCA_DIV_ID,
			{"date": "2022-02-03", "count": 9, "country": "Belgium"},
		)
	]
)
def test_parse_line(source, div, expected):
	ingestor = ECDCIngestor()
	result = ingestor.parse_line(source, div)
	assert result == expected, f"Expected {expected}, got {result}"
