#!/bin/bash
set -e

# Ensure the .wheels directory exists with subdirectories for each platform
mkdir -p .wheels/amd64
mkdir -p .wheels/aarch64

echo -e "\033[1;33mBuilding wheels for linux/amd64...\033[0m"
docker buildx build \
  --platform linux/amd64 \
  --target wheels \
  -o type=local,dest=.wheels/amd64 \
  -f builder/Dockerfile .

echo -e "\033[1;33mBuilding wheels for linux/arm64 (aarch64)...\033[0m"
docker buildx build \
  --platform linux/arm64 \
  --target wheels \
  -o type=local,dest=.wheels/aarch64 \
  -f builder/Dockerfile .

echo "Wheels built successfully for both linux/amd64 and linux/arm64." 
