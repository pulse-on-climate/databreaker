import os
import json
import boto3
import logging
import time

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

def lambda_handler(event, context):
    """Handle S3 event and trigger conversion process"""
    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            # Get dataset-specific configuration
            dataset_config = get_dataset_config(bucket)
            if not dataset_config:
                logger.error(f"No configuration found for bucket: {bucket}")
                continue
            
            # Send message to SQS
            sqs = boto3.client('sqs')
            message_body = json.dumps({
                'source_bucket': bucket,
                'source_key': key,
                'dest_bucket': dataset_config['dest_bucket'],
                'task_id': f"convert-{int(time.time())}-{key.replace('/', '-')}"
            })
            
            # Send to SQS first
            sqs_response = sqs.send_message(
                QueueUrl=dataset_config['queue_url'],
                MessageBody=message_body,
                MessageDeduplicationId=f"{bucket}/{key}",  # Prevent duplicates
                MessageGroupId=bucket  # Group by dataset
            )
            
            # Launch dedicated ECS task for this file
            ecs = boto3.client('ecs')
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
                        },
                        {
                            'name': 'MESSAGE_ID',
                            'value': sqs_response['MessageId']
                        }
                    ]
                }]
            }
            
            # Launch task
            ecs.run_task(
                cluster=dataset_config['cluster'],
                taskDefinition=dataset_config['task_definition'],
                launchType='FARGATE',
                networkConfiguration=dataset_config['network_config'],
                overrides=container_overrides,
                tags=[
                    {
                        'key': 'Source',
                        'value': f"{bucket}/{key}"
                    },
                    {
                        'key': 'MessageId',
                        'value': sqs_response['MessageId']
                    }
                ]
            )
            
            logger.info(f"Launched conversion task for {bucket}/{key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully launched conversion tasks')
        }
        
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        raise 