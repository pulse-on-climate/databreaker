#!/bin/bash

# Get the directory containing this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Create wheels directory if it doesn't exist
mkdir -p "$DIR/../wheels"

# Build the wheel
docker build -t numcodecs-builder -f "$DIR/Dockerfile" "$DIR/.."

# Copy the wheel from the container
docker run --rm \
  -v "$DIR/../wheels:/out" \
  numcodecs-builder \
  bash -c "cp /wheels/numcodecs-*.whl /out/ || { echo 'Wheel not found. Contents of /wheels:' && ls -la /wheels; exit 1; }" 