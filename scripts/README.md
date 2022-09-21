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
To push it to ECR, run:
```
docker tag <LOCAL_IMAGE_TAG> <ACCOUNT_NUMBER>.dkr.ecr.eu-central-1.amazonaws.com/<ECR_REPO>:<ECR_IMAGE_TAG>
aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin <ACCOUNT_NUMBER>.dkr.ecr.eu-central-1.amazonaws.com
docker push <ACCOUNT_NUMBER>.dkr.ecr.eu-central-1.amazonaws.com/<ECR_REPO>:<ECR_IMAGE_TAG>
```

For more information, see the [AWS docs](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html).
