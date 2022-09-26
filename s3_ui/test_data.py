import os
import random
import re
from urllib.parse import urlparse

from flask import url_for
import pytest

from run import (APP, list_bucket_contents, FOLDERS)


LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")


@pytest.fixture()
def flask_app():
	APP.config.update({
		"TESTING": True,
	})
	yield APP


@pytest.fixture()
def client(flask_app):
	with APP.app_context():
		with APP.test_client() as client:
			yield client

@pytest.fixture()
def runner(flask_app):
	return flask_app.test_cli_runner()


def test_folders_displayed(client):
	response = client.get("/")
	assert "Line List Archives</a>" in response.text
	assert "Case definitions</a>" in response.text
	assert "CDC</a>" in response.text
	assert "ECDC</a>" in response.text
	assert "WHO</a>" in response.text


@pytest.mark.parametrize("endpoint", FOLDERS)
def test_folders_contain_files(client, endpoint):
	response = client.get(f"/{endpoint}")
	assert "csv</a>" in response.text


@pytest.mark.skipif(not (S3_BUCKET or LOCALSTACK_URL), reason="Target S3 bucket must be set")
@pytest.mark.parametrize("folder", FOLDERS)
def test_files_downloadable(client, folder):
	print(f"Folder in test: {folder}")
	file_name = random.choice([f.split("/")[1] for f in list_bucket_contents(folder)])
	response = client.get(f"/url/{folder}/{file_name}")

	assert response.status_code == 302

	redirect = ""
	if match := re.search(r"href=[\"']?([^\"' >]+)", response.text):
		redirect = match.group(1)
	else:
		pytest.fail("The web page should show a hyperlink")

	try:
		_ = urlparse(redirect)
	except:
		pytest.fail("The service should template query params its endpoint")
	assert f"{folder}/{file_name}" in redirect
	endpoint = url_for("get_presigned_url", folder=folder, file_name=file_name)
	response = client.get(endpoint)

	assert response.status_code == 302

	presigned = ""

	if match := re.search(r"href=[\"']?([^\"' >]+)", response.text):
		presigned = match.group(1)
	else:
		pytest.fail("The endpoint should return a presigned URL")

	assert redirect == presigned, f"URLs do not match: expected {redirect}, got {presigned}"
