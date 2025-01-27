#!/bin/bash
set -e

echo "Starting local environment..."

# Start docker compose
docker compose -f docker/docker-compose.yml up -d

# Wait for LocalStack
echo "Waiting for LocalStack..."
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3":"available"'; do
    sleep 2
done

# Create buckets and register task
aws --endpoint-url=http://localhost:4566 s3 mb s3://noaa-oisst-nc
aws --endpoint-url=http://localhost:4566 s3 mb s3://noaa-oisst-zarr

# Register ECS task and create cluster
./scripts/register_task.sh

# Deploy Lambda
./scripts/deploy_lambda.sh

echo "✅ Environment ready for testing" 