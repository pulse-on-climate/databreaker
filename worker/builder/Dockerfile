# Use the same base image as our worker
FROM python:3.11.11-slim

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Create wheels directory
RUN mkdir /wheels

# Build the wheel
RUN pip wheel --no-deps --wheel-dir=/wheels numcodecs==0.14.1 && \
    ls -la /wheels && \
    echo "Platform: $(uname -m)" && \
    echo "Wheel built successfully"

# The wheel will be in /wheels/numcodecs-0.14.1-*.whl 