#!/bin/bash

# Build the wheel
docker build -t numcodecs-builder -f docker/builder.Dockerfile .

# Create wheels directory if it doesn't exist
mkdir -p wheels

# Copy the wheel from the container
docker run --rm \
  -v $(pwd)/wheels:/out \
  numcodecs-builder \
  cp /wheels/numcodecs*.whl /out/ 