#!/bin/bash
set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Error handling function
error_exit() {
    echo -e "${RED}Error: $1${NC}" >&2
    exit 1
}

# Ensure we're in the project root directory
cd "$(dirname "$0")/.." || error_exit "Failed to change to project root directory"

# Create build directory if it doesn't exist
mkdir -p build

# Create a temporary directory for packaging
TEMP_DIR=$(mktemp -d)
echo "Using temporary directory: $TEMP_DIR"

# Copy Polling Lambda function code and its requirements
cp lambda/polling_handler.py "$TEMP_DIR/"
cp lambda/requirements.txt "$TEMP_DIR/"

# Install dependencies into the temporary directory
cd "$TEMP_DIR"
python3 -m pip install --target . -r requirements.txt || error_exit "Failed to install dependencies"

# Remove unnecessary files
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type d -name "*.dist-info" -exec rm -rf {} +
find . -type d -name "*.egg-info" -exec rm -rf {} +
rm -f requirements.txt

# Create zip file for the polling handler Lambda
zip -r polling_handler.zip . || error_exit "Failed to create zip file"

# Move the zip file to the build directory
mv polling_handler.zip "$OLDPWD/build/" || error_exit "Failed to move zip to build directory"

# Clean up the temporary directory
cd ..
rm -rf "$TEMP_DIR"

echo -e "${GREEN}Successfully packaged Polling Lambda function to build/polling_handler.zip${NC}" 