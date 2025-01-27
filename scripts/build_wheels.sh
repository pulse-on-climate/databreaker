#!/bin/bash
set -e

# Create wheels directory if it doesn't exist
mkdir -p .wheels

# Build the wheel
docker build -t numcodecs-builder -f docker/Dockerfile.builder .

# Copy the wheel from the container
docker run --rm \
  -v $(pwd)/.wheels:/out \
  numcodecs-builder \
  bash -c "cp /wheels/numcodecs-*.whl /out/ && ls -la /out/"

echo "✅ Wheel built and copied to .wheels/" 
