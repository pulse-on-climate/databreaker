#!/bin/bash
set -e

# Get the directory containing this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$DIR/.." && pwd )"

# Kill any existing containers
echo "Cleaning up existing containers..."
docker kill $(docker ps -q) 2>/dev/null || true
docker rm $(docker ps -a -q) 2>/dev/null || true

# Start required services
docker-compose -f "$PROJECT_ROOT/docker/docker-compose.yml" up -d localstack

# Wait for LocalStack to be ready
echo "Waiting for LocalStack..."
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "available"' && \
      curl -s http://localhost:4566/_localstack/health | grep -q '"sqs": "available"' && \
      curl -s http://localhost:4566/_localstack/health | grep -q '"lambda": "available"'; do
    sleep 1
    echo -n "."
done
echo "LocalStack services are ready!"

# Run the test
PYTHONPATH=$PROJECT_ROOT python3 scripts/test_conversion_flow.py

# # Cleanup
# echo "Cleaning up..."
# docker-compose -f "$PROJECT_ROOT/docker/docker-compose.yml" down 