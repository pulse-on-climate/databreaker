import os
import json
import boto3
import logging
import time
from datetime import datetime, date

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_dataset_config(bucket_name):
    """Get dataset configuration based on bucket name"""
    # Load from environment variable or parameter store
    dataset_configs = json.loads(os.environ['DATASET_CONFIGS'])
    return dataset_configs.get(bucket_name)

def trigger_ecs_task(event, dataset_config):
    """Trigger ECS task for conversion"""
    ecs = boto3.client('ecs')
    
    # Prepare container overrides
    container_overrides = {
        'containerOverrides': [{
            'name': dataset_config['container_name'],
            'environment': [
                {
                    'name': 'DATASET_CONFIG',
                    'value': '/app/config/oisst.json'
                },
                {
                    'name': 'SQS_QUEUE_URL',
                    'value': dataset_config['queue_url']
                }
            ]
        }]
    }
    
    # Launch ECS task
    response = ecs.run_task(
        cluster=dataset_config['cluster'],
        taskDefinition=dataset_config['task_definition'],
        launchType='FARGATE',
        networkConfiguration=dataset_config['network_config'],
        overrides=container_overrides
    )
    
    return response

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    
    # Check if the event is from SNS (S3 notifications via SNS will be wrapped)
    if "Records" in event and "Sns" in event["Records"][0]:
        sns_message = event["Records"][0]["Sns"]["Message"]
        print(f"Extracted SNS message: {sns_message}")
        try:
            # Assuming the SNS message payload is a JSON string containing an S3 event.
            event = json.loads(sns_message)
        except Exception as e:
            print(f"Error parsing SNS message: {e}")
            raise
    
    for record in event['Records']:
        # Get the S3 bucket and key from the underlying S3 event.
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        print(f"Processing file: s3://{bucket}/{key}")
        
        # Start ECS task
        try:
            ecs = boto3.client('ecs')
            
            # Get VPC configuration from environment
            subnet_ids_str = os.environ.get('SUBNET_IDS')
            if not subnet_ids_str:
                raise ValueError("SUBNET_IDS environment variable is not set")
            subnet_ids = subnet_ids_str.split(',')
            print(f"Using subnets: {subnet_ids}")
            
            security_group_id = os.environ.get('SECURITY_GROUP_IDS')
            if not security_group_id:
                raise ValueError("SECURITY_GROUP_IDS environment variable is not set")
            security_group_ids = [security_group_id]
            print(f"Using security groups: {security_group_ids}")

            response = ecs.run_task(
                cluster=os.environ['CLUSTER_NAME'],
                taskDefinition=os.environ['TASK_DEFINITION'],
                launchType='FARGATE',
                networkConfiguration={
                    'awsvpcConfiguration': {
                        'subnets': subnet_ids,
                        'securityGroups': security_group_ids,
                        'assignPublicIp': 'ENABLED'
                    }
                },
                overrides={
                    'containerOverrides': [{
                        'name': 'converter',
                        'environment': [
                            {
                                'name': 'INPUT_FILE',
                                'value': f"s3://{bucket}/{key}"
                            },
                            {
                                'name': 'SOURCE_BUCKET',
                                'value': bucket
                            },
                            {
                                'name': 'DEST_BUCKET',
                                'value': os.environ.get('DEST_BUCKET')
                            },
                            {
                                'name': 'AWS_DEFAULT_REGION',
                                'value': os.environ['AWS_DEFAULT_REGION']
                            }
                        ]
                    }]
                }
            )
            print(f"Started ECS task: {json.dumps(response, default=json_serial)}")
        except Exception as e:
            print(f"Error starting ECS task: {str(e)}")
            print(f"Environment variables:")
            print(f"SUBNET_IDS: {os.environ.get('SUBNET_IDS', 'Not set')}")
            print(f"SECURITY_GROUP_IDS: {os.environ.get('SECURITY_GROUP_IDS', 'Not set')}")
            print(f"CLUSTER_NAME: {os.environ.get('CLUSTER_NAME', 'Not set')}")
            print(f"TASK_DEFINITION: {os.environ.get('TASK_DEFINITION', 'Not set')}")
            raise

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully launched conversion tasks')
    } 