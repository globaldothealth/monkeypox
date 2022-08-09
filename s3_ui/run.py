import logging
import os

import boto3
from flask import Flask, render_template, redirect
import serverless_wsgi

from logger import setup_logger


LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
S3_BUCKET = os.environ.get("S3_BUCKET")

ARCHIVES = "archives"
CASE_DEFINITIONS = "case-definitions"
ECDC = "ecdc"
ECDC_ARCHIVES = "ecdc-archives"

FLASK_HOST = os.environ.get("FLASK_HOST", "0.0.0.0")
FLASK_PORT = os.environ.get("FLASK_PORT", 5000)
FLASK_DEBUG = os.environ.get("FLASK_DEBUG", False)

app = Flask(__name__)
setup_logger()


@app.route("/")
def home():
    return render_template("index.html")


@app.route(f"/{ARCHIVES}")
def get_archive_files():
    try:
        files = [f.split("/")[1] for f in list_bucket_contents(ARCHIVES)]
        logging.debug(f"Files in {ARCHIVES} folder: {files}")
        return render_template("folder.html", folder=ARCHIVES, files=files)
    except Exception as exc:
        return f"Exception: {exc}"


@app.route(f"/{CASE_DEFINITIONS}")
def get_case_definition_files():
    files = [f.split("/")[1] for f in list_bucket_contents(CASE_DEFINITIONS)]
    logging.debug(f"Files in {CASE_DEFINITIONS} folder: {files}")
    return render_template("folder.html", folder=CASE_DEFINITIONS, files=files)


@app.route(f"/{ECDC}")
def get_ecdc_files():
    files = [f.split("/")[1] for f in list_bucket_contents(ECDC)]
    logging.debug(f"Files in {ECDC} folder: {files}")
    return render_template("folder.html", folder=ECDC, files=files)


@app.route(f"/{ECDC_ARCHIVES}")
def get_ecdc_archive_files():
    files = [f.split("/")[1] for f in list_bucket_contents(ECDC_ARCHIVES)]
    logging.debug(f"Files in {ECDC_ARCHIVES} folder: {files}")
    return render_template("folder.html", folder=ECDC_ARCHIVES, files=files)


def list_bucket_contents(folder: str) -> list[str]:
    logging.debug(f"Listing bucket contents for folder {folder}")
    client = create_s3_client()
    response = client.list_objects(Bucket=S3_BUCKET,
                                 Prefix=f"{folder}/",
                                 Delimiter="/"
                                )
    contents = []
    for obj in response.get("Contents", []):
        contents.append(obj.get("Key"))

    logging.debug(f"Listed objects for prefix {folder}: {contents}")
    return contents


@app.route("/url/<folder>/<file_name>")
def get_presigned_url(folder, file_name):
    logging.debug(f"Creating presigned URL for {folder}/{file_name}")
    client = create_s3_client()
    params = {"Bucket": S3_BUCKET, "Key": f"{folder}/{file_name}"}
    return redirect(client.generate_presigned_url("get_object", Params=params, ExpiresIn=60))


def create_s3_client() -> object:
    if LOCALSTACK_URL:
        logging.debug(f"Creating an S3 client using Localstack at {LOCALSTACK_URL}")
        return boto3.client("s3", endpoint_url=LOCALSTACK_URL)
    logging.debug("Creating an S3 client using AWS")
    return boto3.client("s3")


def handler(event, context):
    return serverless_wsgi.handle_request(app, event, context)


if __name__ == "__main__":
    setup_logger()
    logging.info("Starting Flask...")
    app.run(FLASK_HOST, FLASK_PORT, debug=FLASK_DEBUG)
