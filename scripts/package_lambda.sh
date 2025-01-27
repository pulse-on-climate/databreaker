#!/bin/bash
set -e

# Debug: Show current directory
echo "Current directory: $(pwd)"

# Create a temporary directory for building
BUILD_DIR=.lambda-package
rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR

# Copy Lambda function code
cp -r lambda/* $BUILD_DIR/

# Install dependencies into the package
cd $BUILD_DIR
echo "Now in directory: $(pwd)"
echo "Contents of current directory:"
ls -la

# Install only essential dependencies
pip install -q \
    boto3 \
    botocore \
    --target . >/dev/null 2>&1

echo "After pip install, contents:"
ls -la

# Create deployment package
zip -r ../lambda_function.zip .
echo "Created zip in: $(pwd)"
ls -lh ../lambda_function.zip

# Check if package is too large (max 50MB for LocalStack)
ZIP_SIZE=$(stat -f%z ../lambda_function.zip)
MAX_SIZE=52428800
if [ $ZIP_SIZE -gt $MAX_SIZE ]; then
    echo "❌ Warning: Lambda package is too large ($ZIP_SIZE bytes > $MAX_SIZE bytes)"
    echo "Try reducing dependencies or using layer for large packages"
    exit 1
fi

# Clean up
cd ..
rm -rf $BUILD_DIR

# Verify final location
echo "Final location of zip file:"
find "$(pwd)" -name lambda_function.zip

echo "Created deployment package: lambda_function.zip" 