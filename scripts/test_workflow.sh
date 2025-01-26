#!/bin/bash

# Function to clean up existing services
cleanup() {
    echo "Cleaning up existing services..."
    # Update path to docker-compose.yml
    docker-compose -f docker/docker-compose.yml down -v 2>/dev/null || true
    
    # Remove any leftover containers
    docker rm -f databreaker-localstack databreaker-scheduler databreaker-worker 2>/dev/null || true
    # Remove any test containers
    docker rm -f databreaker-test 2>/dev/null || true
    
    # Wait for ports to be freed
    sleep 5
}

# Clean up before starting
cleanup

# Start all services in parallel
echo "Starting services..."
docker-compose -f docker/docker-compose.yml up -d

# Wait for scheduler only (worker can take its time)
echo "Waiting for Dask scheduler..."
for i in {1..30}; do
    if curl -s http://localhost:8787/info > /dev/null; 
    then
        echo "✅ Dask scheduler is running"
        break
    fi
    echo "Waiting for scheduler... ($i/60)"
    sleep 4
done

# Wait for worker to be ready
echo "Waiting for worker to be ready..."
for i in {1..60}; do
    if curl -s http://localhost:8788/status > /dev/null; then
        echo "✅ Dask worker is running"
        break
    fi
    echo "Waiting for worker... ($i/60)"
    sleep 5
done

# Additional wait for worker to register with scheduler
echo "Waiting for worker to register with scheduler..."
for i in {1..30}; do
    worker_count=$(curl -s http://localhost:8787/info/main/workers.html | grep -o "<td><a href=" | wc -l)
    if [ "$worker_count" -gt 0 ]; then
        echo "✅ Worker registered with scheduler"
        break
    fi
    echo "Waiting for worker registration... ($i/30)"
    sleep 2
done

# Start setting up other infrastructure in parallel with worker startup
echo "Setting up infrastructure..."

# Build Lambda dependencies
echo "Building Lambda dependencies..."
./scripts/build_layer.sh

# Create buckets and upload test data in parallel with worker startup
(
    echo "Setting up test data..."
    until aws --endpoint-url=http://localhost:4566 s3 mb s3://noaa-oisst-nc 2>/dev/null; do
        sleep 2
    done
    aws --endpoint-url=http://localhost:4566 s3 mb s3://noaa-oisst-zarr
    aws --endpoint-url=http://localhost:4566 s3 cp \
        tests/data/sample.nc \
        s3://noaa-oisst-nc/test/sample.nc \
        --content-type "application/x-netcdf4"
) &

# Create SQS queue in parallel
(
    echo "Creating SQS queue..."
    until aws --endpoint-url=http://localhost:4566 sqs create-queue \
        --queue-name test-queue >/dev/null 2>&1; do
        sleep 2
    done
) &

# Wait for background tasks
wait

# Verify Dask client connection and submit test task
echo "Testing Dask connection and submitting task..."
docker-compose -f docker/docker-compose.yml --profile test run --rm dask-test

# Test the workflow by uploading a file and watching the conversion
echo "Testing workflow with sample file..."

# First, ensure our test file exists
if [ ! -f "tests/data/sample.nc" ]; then
    echo "❌ Test file not found at tests/data/sample.nc"
    exit 1
fi

# Upload the test file to trigger the conversion
echo "Uploading test file to source bucket..."
aws --endpoint-url=http://localhost:4566 s3 cp \
    tests/data/sample.nc \
    s3://noaa-oisst-nc/incoming/sample.nc

# Monitor the destination bucket for results
echo "Monitoring destination bucket for results..."
max_attempts=30
attempt=1
while [ $attempt -le $max_attempts ]; do
    if aws --endpoint-url=http://localhost:4566 s3 ls s3://noaa-oisst-zarr/sample/ >/dev/null 2>&1; then
        echo "✅ Conversion complete! Found output in destination bucket"
        break
    fi
    echo "Waiting for conversion... ($attempt/$max_attempts)"
    sleep 10
    attempt=$((attempt + 1))
done

if [ $attempt -gt $max_attempts ]; then
    echo "❌ Timed out waiting for conversion"
    exit 1
fi

# List the results
echo "Final contents of destination bucket:"
aws --endpoint-url=http://localhost:4566 s3 ls s3://noaa-oisst-zarr/ --recursive

# Optional: Download results for verification
echo "Downloading results for verification..."
rm -rf tests/output
mkdir -p tests/output
aws --endpoint-url=http://localhost:4566 s3 cp \
    s3://noaa-oisst-zarr/ \
    tests/output/ \
    --recursive

# Clean up (optional - uncomment if needed)
# cleanup