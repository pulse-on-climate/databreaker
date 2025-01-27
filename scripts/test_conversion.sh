#!/bin/bash

# Function to check if infrastructure is running
check_infrastructure() {
    echo "Checking infrastructure..."
    
    # Check Lambda function
    if ! aws --endpoint-url=http://localhost:4566 lambda get-function \
        --function-name NetCDFEventDispatcher >/dev/null 2>&1; then
        echo "❌ Lambda function not found. Please run deploy_lambda.sh first"
        exit 1
    fi
    
    # Check Dask scheduler
    if ! curl -s http://localhost:8787/info > /dev/null; then
        echo "❌ Dask scheduler not running. Please run test_workflow.sh first"
        exit 1
    fi
    
    # Check Dask worker
    if ! curl -s http://localhost:8788/status > /dev/null; then
        echo "❌ Dask worker not running. Please run test_workflow.sh first"
        exit 1
    fi
    
    # Check LocalStack and buckets
    echo "Checking LocalStack and buckets..."
    echo "Existing buckets:"
    aws --endpoint-url=http://localhost:4566 s3 ls
    
    # Verify buckets exist
    if ! aws --endpoint-url=http://localhost:4566 s3 ls s3://noaa-oisst-nc > /dev/null 2>&1; then
        echo "❌ Source bucket not found. Please run test_workflow.sh first"
        exit 1
    fi
    if ! aws --endpoint-url=http://localhost:4566 s3 ls s3://noaa-oisst-zarr > /dev/null 2>&1; then
        echo "❌ Destination bucket not found. Please run test_workflow.sh first"
        exit 1
    fi
    
    echo "✅ Infrastructure check passed"
}

# Function to upload test file and monitor conversion
test_conversion() {
    local input_file=$1
    local filename=$(basename "$input_file")
    
    echo "Testing conversion of $filename..."
    
    # Upload file to source bucket
    echo "Uploading test file to source bucket..."
    aws --endpoint-url=http://localhost:4566 s3 cp \
        "$input_file" \
        "s3://noaa-oisst-nc/incoming/$filename"
    
    # Check Lambda logs
    echo "Checking Lambda logs..."
    aws --endpoint-url=http://localhost:4566 logs tail \
        /aws/lambda/NetCDFEventDispatcher --since 1m
    
    # Verify the file was uploaded
    echo "Verifying file upload..."
    aws --endpoint-url=http://localhost:4566 s3 ls s3://noaa-oisst-nc/incoming/$filename || {
        echo "❌ File upload failed"
        exit 1
    }
    
    # Check S3 bucket notification configuration
    echo "Checking S3 bucket notification configuration..."
    aws --endpoint-url=http://localhost:4566 s3api get-bucket-notification-configuration \
        --bucket noaa-oisst-nc
    
    # Check if Lambda function exists
    echo "Checking Lambda function..."
    aws --endpoint-url=http://localhost:4566 lambda list-functions
    
    # Show Lambda logs
    echo "Checking Lambda logs..."
    sleep 5
    
    # Check Dask scheduler status
    echo "Checking Dask scheduler status..."
    curl -s http://localhost:8787/info | python -m json.tool
    
    # Monitor the destination bucket for results
    echo "Monitoring destination bucket for results..."
    max_attempts=30
    attempt=1
    while [ $attempt -le $max_attempts ]; do
        echo "Checking Dask scheduler status..."
        if ! curl -s http://localhost:8787/status | grep -q "\"status\": \"running\""; then
            echo "⚠️  Dask scheduler not responding"
        fi
        
        if aws --endpoint-url=http://localhost:4566 s3 ls \
            "s3://noaa-oisst-zarr/incoming/${filename%.*}/" >/dev/null 2>&1; then
            echo "✅ Conversion complete! Found output in destination bucket"
            break
        fi
        echo "Waiting for conversion... ($attempt/$max_attempts)"
        # Check source bucket contents
        echo "Source bucket contents:"
        aws --endpoint-url=http://localhost:4566 s3 ls s3://noaa-oisst-nc/incoming/ --recursive
        # Check destination bucket contents
        echo "Destination bucket contents:"
        aws --endpoint-url=http://localhost:4566 s3 ls s3://noaa-oisst-zarr/incoming/ --recursive
        sleep 10
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -gt $max_attempts ]; then
        echo "❌ Timed out waiting for conversion"
        exit 1
    fi
    
    # List the results
    echo "Final contents of destination bucket:"
    aws --endpoint-url=http://localhost:4566 s3 ls \
        s3://noaa-oisst-zarr/incoming/ --recursive
}

# Main execution
check_infrastructure

# Check if file argument provided
if [ -z "$1" ]; then
    echo "Using default test file: tests/data/sample.nc"
    test_conversion "tests/data/sample.nc"
else
    if [ ! -f "$1" ]; then
        echo "❌ File not found: $1"
        exit 1
    fi
    test_conversion "$1"
fi 