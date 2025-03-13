import os
import json
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create an S3 client that makes unsigned (anonymous) requests.
s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
ecs_client = boto3.client('ecs')
ssm_client = boto3.client('ssm')

def lambda_handler(event, context):
    """
    Polls the external public S3 bucket for new files, triggers an ECS conversion task for each,
    and updates the last processed timestamp in SSM Parameter Store.
    Files are processed if their LastModified date is on or after the stored parameter date.
    """
    source_bucket = os.environ.get("SOURCE_BUCKET")
    if not source_bucket:
        logger.error("SOURCE_BUCKET environment variable is not set")
        raise Exception("SOURCE_BUCKET environment variable is not set")
    
    last_processed = get_last_processed_timestamp()
    last_processed_date = last_processed.date()  # Compare only the date part
    logger.info(f"Last processed date: {last_processed_date.isoformat()}")

    new_files = []
    max_timestamp = last_processed

    # List objects in the bucket
    logger.info("Starting to paginate S3 objects from bucket: %s", source_bucket)
    paginator = s3_client.get_paginator('list_objects_v2')
    page_count = 0
    for page in paginator.paginate(Bucket=source_bucket):
        page_count += 1
        contents = page.get('Contents', [])
        logger.info("Page %d retrieved, object count: %d", page_count, len(contents))
        for obj in contents:
            key = obj['Key']
            last_modified = obj['LastModified']
            logger.debug("Processing object: %s, last_modified: %s", key, last_modified.isoformat())
            # Compare the date portion so that files on the same day are included.
            if last_modified.date() >= last_processed_date:
                logger.debug("New object found: %s", key)
                new_files.append((key, last_modified))
                if last_modified > max_timestamp:
                    max_timestamp = last_modified
    logger.info("Finished pagination. Total pages: %d, new files: %d", page_count, len(new_files))

    logger.info(f"Found {len(new_files)} new file(s) in bucket {source_bucket}")

    # Trigger ECS conversion tasks for new files
    for key, _ in new_files:
        try:
            response = trigger_ecs_task(source_bucket, key)
            logger.info(f"Triggered ECS task for s3://{source_bucket}/{key}: {json.dumps(response, default=str)}")
        except Exception as e:
            logger.error(f"Error triggering ECS task for {key}: {e}")

    # Update SSM parameter if any new files were processed.
    if new_files:
        update_last_processed_timestamp(max_timestamp)

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {len(new_files)} new file(s).")
    }

def get_last_processed_timestamp():
    """
    Retrieve the last processed timestamp from SSM Parameter Store.
    Defaults to January 1, 1970 (UTC) if the parameter does not exist.
    Ensures the returned datetime is timezone aware.
    """
    param_name = os.environ.get('LAST_PROCESSED_PARAM', '/my-app/last_processed')
    try:
        response = ssm_client.get_parameter(Name=param_name)
        timestamp_str = response['Parameter']['Value']
        ts = datetime.fromisoformat(timestamp_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except ssm_client.exceptions.ParameterNotFound:
        # If a start timestamp override is provided, use it.
        polling_start = os.environ.get("POLLING_START_TIMESTAMP")
        if polling_start:
            ts = datetime.fromisoformat(polling_start)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    except Exception as e:
        logger.error(f"Error retrieving SSM parameter {param_name}: {e}")
        raise

def update_last_processed_timestamp(timestamp):
    """
    Update the SSM parameter with the new last processed timestamp.
    """
    param_name = os.environ.get('LAST_PROCESSED_PARAM', '/my-app/last_processed')
    timestamp_str = timestamp.isoformat()
    try:
        ssm_client.put_parameter(
            Name=param_name,
            Value=timestamp_str,
            Type='String',
            Overwrite=True
        )
        logger.info(f"Updated last processed timestamp to {timestamp_str} in SSM")
    except Exception as e:
        logger.error(f"Error updating SSM parameter {param_name}: {e}")
        raise

def trigger_ecs_task(bucket, key):
    """
    Trigger an ECS Fargate task for file conversion.
    Passes required environment variables such as INPUT_FILE, SOURCE_BUCKET, DEST_BUCKET, and AWS_DEFAULT_REGION.
    """
    subnet_ids_str = os.environ.get('SUBNET_IDS')
    if not subnet_ids_str:
        raise Exception("SUBNET_IDS environment variable is not set")
    subnet_ids = subnet_ids_str.split(',')

    security_group_id = os.environ.get('SECURITY_GROUP_IDS')
    if not security_group_id:
        raise Exception("SECURITY_GROUP_IDS environment variable is not set")
    security_group_ids = [security_group_id]

    cluster_name = os.environ.get('CLUSTER_NAME')
    if not cluster_name:
        raise Exception("CLUSTER_NAME environment variable is not set")

    task_definition = os.environ.get('TASK_DEFINITION')
    if not task_definition:
        raise Exception("TASK_DEFINITION environment variable is not set")

    dest_bucket = os.environ.get('DEST_BUCKET')
    if not dest_bucket:
        raise Exception("DEST_BUCKET environment variable is not set")

    region = os.environ.get('AWS_DEFAULT_REGION')
    if not region:
        raise Exception("AWS_DEFAULT_REGION environment variable is not set")

    # Build container overrides.
    overrides = {
        'containerOverrides': [{
            'name': 'converter',
            'environment': [
                {'name': 'INPUT_FILE', 'value': f"s3://{bucket}/{key}"},
                {'name': 'SOURCE_BUCKET', 'value': bucket},
                {'name': 'DEST_BUCKET', 'value': dest_bucket},
                {'name': 'AWS_DEFAULT_REGION', 'value': region}
            ]
        }]
    }

    response = ecs_client.run_task(
        cluster=cluster_name,
        taskDefinition=task_definition,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': subnet_ids,
                'securityGroups': security_group_ids,
                'assignPublicIp': 'ENABLED'
            }
        },
        overrides=overrides
    )
    return response
