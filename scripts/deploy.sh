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


# Check if a configuration file argument was provided.
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <path-to-stack-config.json>"
    exit 1
fi

STACK_CONFIG_JSON="$1"

# Verify that the specified file exists.
if [ ! -f "$STACK_CONFIG_JSON" ]; then
    echo "Error: File $STACK_CONFIG_JSON not found."
    exit 1
fi



echo -e "${YELLOW}Checking AWS credentials...${NC}"
aws sts get-caller-identity > /dev/null 2>&1 || error_exit "AWS credentials not configured correctly"

# Create build directory
mkdir -p build

# Build Lambda package
echo -e "${YELLOW}Building Lambda package...${NC}"
./scripts/package_lambda.sh || error_exit "Failed to package Lambda function"

# Create ECR repository if it doesn't exist
if ! aws ecr describe-repositories --repository-names databreaker-converter &> /dev/null; then
    echo "Creating ECR repository..."
    if ! aws ecr create-repository --repository-name databreaker-converter; then
        error_exit "Failed to create ECR repository"
    fi
else
    echo "Repository already exists, continuing..."
fi

# Build Docker image for linux/amd64 locally and load it into Docker runtime
echo -e "${YELLOW}Building Docker image for linux/amd64 locally (using AMD64 wheels)...${NC}"
if ! docker buildx build --platform linux/amd64 --load --build-arg TARGET_ARCH=amd64 --build-arg APP_CONFIG_FILE=$STACK_CONFIG_JSON -t databreaker-converter -f ecs/Dockerfile.converter .; then
    error_exit "Docker build failed"
fi

# Login to ECR
echo -e "${YELLOW}Logging into ECR...${NC}"
if ! aws ecr get-login-password --region $AWS_DEFAULT_REGION | \
    docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com; then
    error_exit "Failed to login to ECR"
fi

# Tag and push the image to ECR
echo -e "${YELLOW}Tagging and pushing Docker image...${NC}"
if ! docker tag databreaker-converter:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/databreaker-converter:latest; then
    error_exit "Failed to tag Docker image"
fi

if ! docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/databreaker-converter:latest; then
    error_exit "Failed to push Docker image to ECR"
fi

# Deploy CDK stack
echo -e "${YELLOW}Deploying CDK stack...${NC}"
cd cdk || error_exit "Failed to change to CDK directory"

# Install Python dependencies if needed
if [[ ! -d "venv" ]]; then
    echo "Creating virtual environment..."
    python3 -m venv venv || error_exit "Failed to create virtual environment"
    source venv/bin/activate || error_exit "Failed to activate virtual environment"
    pip install -r requirements.txt || error_exit "Failed to install Python dependencies"
else
    source venv/bin/activate || error_exit "Failed to activate virtual environment"
fi

echo "Deploying CDK stack using configuration: $STACK_CONFIG_JSON"

# Deploy the stack using the provided configuration file at deployment time.
cdk deploy --all -c stackConfig="$STACK_CONFIG_JSON" 

echo -e "${GREEN}Deployment completed successfully!${NC}" 