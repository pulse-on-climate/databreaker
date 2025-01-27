#!/bin/bash
set -e

echo "Setting up test environment..."

# Start localstack
docker compose -f docker/docker-compose.yml up -d localstack

# Wait for LocalStack
echo "Waiting for LocalStack..."
until curl -s http://localhost:4566/_localstack/health >/dev/null 2>&1; do
    sleep 2
done

# Additional wait for S3 service
echo "Waiting for S3 service..."
until aws --endpoint-url=http://localhost:4566 s3 ls >/dev/null 2>&1; do
    sleep 2
done

# Create buckets
echo "Creating buckets..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://noaa-oisst-nc
aws --endpoint-url=http://localhost:4566 s3 mb s3://noaa-oisst-zarr

# Upload test file
echo "Uploading test file..."
aws --endpoint-url=http://localhost:4566 s3 cp \
    tests/data/sample.nc \
    s3://noaa-oisst-nc/incoming/sample.nc

echo "✅ Test environment ready"

echo "Starting converter..."
# Run converter container
docker compose -f docker/docker-compose.yml up --build converter 