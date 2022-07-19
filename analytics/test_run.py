import csv
import os

import boto3
import pytest
import requests


MOUNTEBANK_URL = os.environ.get("MOUNTEBANK_URL")
LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")
IMPOSTER_PORT = os.environ.get("IMPOSTER_PORT")


@pytest.mark.skipif(not (LOCALSTACK_URL or S3_BUCKET), reason="Must set LOCALSTACK_URL and S3_BUCKET")
def test_csvs_stored():
	# Analytics scripts should output valid csvs with data
	s3_client = boto3.client("s3", endpoint_url=LOCALSTACK_URL)
	objects = s3_client.list_objects(Bucket=S3_BUCKET)
	for obj in objects.get("Contents"):
		key = obj.get("Key")
		s3_client.download_file(S3_BUCKET, key, key)
		reader = csv.DictReader(open(key))
		data = next(reader)
		assert len(data) > 0


@pytest.mark.skipif(not (MOUNTEBANK_URL or IMPOSTER_PORT), reason="Must set MOUNTEBANK_URL and IMPOSTER_PORT")
def test_slack_messages_sent():
	# Slack imposter should intercept messages
	response = requests.get(f"{MOUNTEBANK_URL}/imposters/{IMPOSTER_PORT}")
	messages = response.json().get("requests")
	assert len(messages) > 0
	for message in messages:
		assert message.get("body")
