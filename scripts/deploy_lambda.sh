#!/bin/bash
set -e

# Ensure we're in the project root directory
cd "$(dirname "$0")/.."

# Debug: Show current directory
echo "Deploy script running from: $(pwd)"
echo "Looking for lambda_function.zip in current directory:"
ls -la lambda_function.zip || echo "lambda_function.zip not found!"

echo "Deploying Lambda function..."

# Package Lambda code
./scripts/package_lambda.sh

# Debug: Check if zip was created
echo "After packaging, checking for lambda_function.zip:"
ls -la lambda_function.zip || echo "lambda_function.zip still not found!"

# Check if Lambda function already exists and remove it
echo "Checking for existing Lambda function..."
aws --endpoint-url=http://localhost:4566 lambda delete-function \
    --function-name NetCDFEventDispatcher 2>/dev/null || true

# Create and configure Lambda function
echo "Creating Lambda function..."
aws --endpoint-url=http://localhost:4566 lambda create-function \
    --function-name NetCDFEventDispatcher \
    --runtime python3.11 \
    --handler handler.main \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 30 \
    --memory-size 1024 \
    --zip-file fileb://lambda_function.zip \
    --publish \
    --no-cli-pager \
    --output text \
    --environment Variables="{
        AWS_ENDPOINT_URL=http://localstack:4566,
        DASK_SCHEDULER=tcp://dask-scheduler:8786,
        SOURCE_BUCKET_NAME=noaa-oisst-nc,
        DEST_BUCKET_NAME=noaa-oisst-zarr,
        AWS_DEFAULT_REGION=us-east-1,
        AWS_ACCESS_KEY_ID=test,
        AWS_SECRET_ACCESS_KEY=test,
        PYTHONPATH=/var/task
    }"

# Wait for Lambda to be Active
echo "Waiting for Lambda function to be active..."
for i in {1..30}; do
    status=$(aws --endpoint-url=http://localhost:4566 lambda get-function \
        --function-name NetCDFEventDispatcher \
        --query 'Configuration.State' \
        --no-cli-pager \
        --output text)
    if [ "$status" = "Active" ]; then
        echo -e "\r✅ Lambda function is active                               "
        break
    fi
    echo -ne "\r⏳ Lambda function status: $status ($i/30)                  "
    sleep 2
done

if [ "$status" != "Active" ]; then
    echo "❌ Lambda function failed to become active"
    exit 1
fi

# Configure S3 to Lambda permissions
aws --endpoint-url=http://localhost:4566 lambda add-permission \
    --function-name NetCDFEventDispatcher \
    --statement-id S3InvokeFunction \
    --action lambda:InvokeFunction \
    --principal s3.amazonaws.com \
    --source-arn arn:aws:s3:::noaa-oisst-nc

# Configure S3 bucket notifications
echo "Configuring S3 bucket notifications..."
aws --endpoint-url=http://localhost:4566 s3api put-bucket-notification-configuration \
    --bucket noaa-oisst-nc \
    --notification-configuration '{
        "LambdaFunctionConfigurations": [{
            "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:NetCDFEventDispatcher",
            "Events": ["s3:ObjectCreated:*"]
        }]
    }'

# Verify notification configuration
echo "Verifying S3 notification configuration..."
aws --endpoint-url=http://localhost:4566 s3api get-bucket-notification-configuration \
    --bucket noaa-oisst-nc

echo "✅ Lambda function deployed and configured" 