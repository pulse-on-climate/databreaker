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
        echo -e "\r✅ Dask worker is running                                  "
        break
    fi
    echo -ne "\r⏳ Waiting for worker... ($i/60)                           "
    sleep 5
done
echo  # New line after completion

# Additional wait for worker to register with scheduler
echo "Waiting for worker to register with scheduler..."
for i in {1..30}; do
    worker_count=$(curl -s http://localhost:8787/info/main/workers.html | grep -o "<td><a href=" | wc -l)
    if [ "$worker_count" -gt 0 ]; then
        echo -e "\r✅ Worker registered with scheduler                        "
        break
    fi
    echo -ne "\r⏳ Waiting for worker registration... ($i/30)              "
    sleep 2
done
echo  # New line after completion

# Start setting up other infrastructure in parallel with worker startup
echo "Setting up infrastructure..."

# Create buckets and upload test data in parallel with worker startup
(
    echo "Setting up test data..."
    # Create buckets first
    until aws --endpoint-url=http://localhost:4566 s3 mb s3://noaa-oisst-nc 2>/dev/null; do
        sleep 2
    done
    aws --endpoint-url=http://localhost:4566 s3 mb s3://noaa-oisst-zarr
    echo "✅ S3 buckets created"
) &

# Create SQS queue in parallel
(
    echo "Creating SQS queue..."
    until aws --endpoint-url=http://localhost:4566 sqs create-queue \
        --queue-name test-queue >/dev/null 2>&1; do
        sleep 2
    done
    echo "✅ SQS queue created"
) &

echo "Waiting for infrastructure setup to complete..."
echo "This may take a few minutes..."

# Store background process IDs
BUCKET_PID=$!

# Monitor progress while waiting
while kill -0 $BUCKET_PID 2>/dev/null; do
    echo "Checking setup status..."
    
    # Check S3 buckets
    if aws --endpoint-url=http://localhost:4566 s3 ls s3://noaa-oisst-nc >/dev/null 2>&1; then
        echo "✅ Source bucket ready"
    else
        echo "⏳ Waiting for source bucket..."
    fi
    
    # Check SQS queue
    if aws --endpoint-url=http://localhost:4566 sqs list-queues 2>/dev/null | grep -q "test-queue"; then
        echo "✅ SQS queue ready"
    else
        echo "⏳ Creating SQS queue..."
    fi
    
    echo "-------------------"
    sleep 5
done

# Wait for background tasks to complete
wait

# Verify Dask client connection and submit test task
echo "Testing Dask connection and submitting task..."
docker-compose -f docker/docker-compose.yml --profile test run --rm dask-test

echo "✅ Infrastructure setup complete and verified"