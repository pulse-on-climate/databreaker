#!/bin/bash
set -e

echo "Registering ECS task definition..."

aws --endpoint-url=http://localhost:4566 ecs register-task-definition \
    --family dask-client \
    --requires-compatibilities FARGATE \
    --network-mode awsvpc \
    --cpu 256 \
    --memory 512 \
    --container-definitions '[{
        "name": "dask-client",
        "image": "databreaker-client:latest",
        "essential": true,
        "environment": [
            {"name": "DASK_SCHEDULER", "value": "tcp://dask-scheduler:8786"},
            {"name": "AWS_ENDPOINT_URL", "value": "http://localstack:4566"}
        ]
    }]'

echo "Creating ECS cluster..."
aws --endpoint-url=http://localhost:4566 ecs create-cluster \
    --cluster-name databreaker

echo "✅ ECS resources configured" 