#!/bin/bash

# Start LocalStack in the background
localstack start -d

# Wait for LocalStack to be ready
echo "Waiting for LocalStack to start..."
sleep 10

# Create the test queue using AWS CLI with LocalStack endpoint
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name test-queue 