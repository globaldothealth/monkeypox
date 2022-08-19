# Monkeypox analytics scripts

This folder contains analytics and helper scripts, configuration for their runtime and testing environments.

The analysis performed by the scripts in this folder includes:
* Calculating risk of re-identification
* Comparing G.h data to CDC and WHO data

## Use

To start the stack, run `run_stack.sh`.
This runs the scripts, using mocks of AWS and Slack to receive their outputs.

## Testing

To test the stack, run `test_stack.sh`.
This runs the scripts, using mocks of AWS and Slack to receive their outputs, followed by a set of assertions about behavior (e.g. the AWS mock should contain data, the Slack mock should contain messages).

## Deployment

Scripts can run inside containers on AWS Batch with Fargate.

To build the image, run `docker build -f ./../Dockerfile -t <TAG> .`.
To push it to ECR, follow the [instructions given by AWS](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html). 
