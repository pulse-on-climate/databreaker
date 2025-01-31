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

# Check for required environment variables
[[ -z "${AWS_DEFAULT_REGION}" ]] && error_exit "AWS_DEFAULT_REGION is not set"
[[ -z "${AWS_ACCOUNT_ID}" ]] && error_exit "AWS_ACCOUNT_ID is not set"

# Ensure we're in the project root directory
cd "$(dirname "$0")/.." || error_exit "Failed to change to project root directory"

echo -e "${YELLOW}Checking AWS credentials...${NC}"
aws sts get-caller-identity > /dev/null 2>&1 || error_exit "AWS credentials not configured correctly"

echo -e "${YELLOW}Destroying CDK stack...${NC}"
cd cdk || error_exit "Failed to change to CDK directory"

# Check if CDK dependencies are installed
if ! command -v cdk &> /dev/null; then
    error_exit "CDK CLI is not installed. Please run: npm install -g aws-cdk"
fi

# Activate virtual environment if it exists
if [[ -d "venv" ]]; then
    source venv/bin/activate || error_exit "Failed to activate virtual environment"
else
    error_exit "Virtual environment not found. Please run deploy.sh first"
fi

# Confirm destruction
read -p "Are you sure you want to destroy the stack? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Destruction cancelled${NC}"
    exit 0
fi

# Destroy the stack
if ! cdk destroy --force; then
    error_exit "CDK destruction failed"
fi

echo -e "${YELLOW}Cleaning up ECR repository...${NC}"
# Clean up ECR images but keep repository
if aws ecr describe-repositories --repository-names databreaker-converter &> /dev/null; then
    echo "Deleting images from ECR repository..."
    aws ecr batch-delete-image \
        --repository-name databreaker-converter \
        --image-ids "$(aws ecr list-images --repository-name databreaker-converter --query 'imageIds[*]' --output json)" \
        || echo "Warning: Failed to delete some images from ECR"
fi

echo -e "${GREEN}Stack destroyed successfully!${NC}" 