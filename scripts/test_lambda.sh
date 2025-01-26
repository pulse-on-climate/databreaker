#!/bin/bash

# Load environment
ENV=${1:-local}  # Use 'local' as default environment

# Start LocalStack if running locally
if [ "$ENV" = "local" ] && ! nc -z localhost 4566; then
    ./scripts/start_localstack.sh
fi

# Load environment configuration
ENV_CONFIG=$(cat config/environments.json | jq -r ".$ENV.sqs")

# Set environment variables
export AWS_SAM_LOCAL=true
export AWS_REGION=$(echo $ENV_CONFIG | jq -r '.region')
export QUEUE_URL=$(echo $ENV_CONFIG | jq -r '.queue_url')
export SOURCE_BUCKET_NAME=$(cat config/buckets.json | jq -r '.source_bucket.name')
export DEST_BUCKET_NAME=$(cat config/buckets.json | jq -r '.destination_bucket.name')

if [ "$ENV" = "local" ]; then
    export SQS_ENDPOINT_URL=$(echo $ENV_CONFIG | jq -r '.endpoint_url')
    export AWS_ACCESS_KEY_ID=$(echo $ENV_CONFIG | jq -r '.access_key_id')
    export AWS_SECRET_ACCESS_KEY=$(echo $ENV_CONFIG | jq -r '.secret_access_key')
fi

# Test the Lambda function with Docker network settings
sam local invoke NetCDFEventDispatcher \
    -e events/s3-put.json \
    --docker-network host 