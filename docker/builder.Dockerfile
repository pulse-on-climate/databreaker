# Use the same base image as our worker
FROM python:3.11.11-slim

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install numcodecs with specific version
RUN pip install --no-cache-dir numcodecs==0.14.1

# Copy wheels to a known location
RUN mkdir /wheels && \
    pip wheel --no-deps --wheel-dir=/wheels numcodecs==0.14.1

# The wheel will be in /wheels/numcodecs-0.14.1-*.whl 