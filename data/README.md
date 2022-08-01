# Monkeypox Data Navigator

## What it does

This folder contains the code needed to run a web service that exposes archived Monkeypox files,
including historical line list snapshots, case definition files, and ECDC files.

Users can use their web browsers to navigate and download files by using templated links.
The server finds files in desired folders and exposes on-demand presigned URLs to S3 objects.

## How to run

Developers can run the application via `./run.py`, building from the `Dockerfile` and running the created container, and running `run_stack.sh`.

Running using `run_stack.sh` also creates and uses a mock AWS S3 service and adds fake data
(csv and json files).

Developers can test the application via `./test.py`, building from the `Dockerfile` and running the created image, and running `test_stack.sh`.

## How to deploy

This service runs on AWS Lambda in a Docker container.
Deployment consists of building an image, deploying it to ECR, and restarting the Lambda function.

To build the image, run `poetry export -f requirements.txt --output requirements.txt --without-hashes`, then `docker build -f Dockerfile-lambda -t lambda_s3 .`.
To push it to ECR, follow the [instructions given by AWS](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html).
To restart the Lambda function, use boto3 or the AWS web UI to deploy the new image to the function, then recreate the API Gateway deployment (stage).
