#!/usr/bin/env python3
from distributed import Client
import boto3
import time
import json
import requests
import zipfile
from io import BytesIO

def test_conversion_flow():
    """Test the entire conversion flow"""
    # Initialize clients
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')
    lambda_client = boto3.client('lambda', endpoint_url='http://localhost:4566')
    ecs = boto3.client('ecs', endpoint_url='http://localhost:4566')
    
    # Upload test file
    print("Uploading test file...")
    s3.upload_file(
        'tests/data/sample.nc',
        'noaa-oisst-nc',
        'incoming/sample.nc'
    )
    
    # Invoke Lambda
    print("Invoking Lambda function...")
    response = lambda_client.invoke(
        FunctionName='NetCDFEventDispatcher',
        InvocationType='RequestResponse',
        Payload=json.dumps({
            'Records': [{
                's3': {
                    'bucket': {'name': 'noaa-oisst-nc'},
                    'object': {'key': 'incoming/sample.nc'}
                }
            }]
        })
    )
    
    # Parse Lambda response
    payload = json.loads(response['Payload'].read())
    if 'taskArn' not in json.loads(payload['body']):
        raise Exception(f"Lambda did not return taskArn: {payload}")
    
    task_arn = json.loads(payload['body'])['taskArn']
    print(f"ECS task started: {task_arn}")
    
    # Wait for ECS task completion
    print("Waiting for ECS task completion...")
    max_attempts = 30
    for i in range(max_attempts):
        task = ecs.describe_tasks(
            cluster='databreaker',
            tasks=[task_arn]
        )['tasks'][0]
        
        status = task['lastStatus']
        print(f"Task status: {status}")
        
        if status == 'STOPPED':
            if task.get('stoppedReason'):
                raise Exception(f"Task failed: {task['stoppedReason']}")
            break
            
        if i == max_attempts - 1:
            raise TimeoutError("Task did not complete in time")
            
        time.sleep(2)
    
    # Check for output in S3
    print("Checking for converted file...")
    response = s3.list_objects_v2(
        Bucket='noaa-oisst-zarr',
        Prefix='incoming/sample'
    )
    
    if not response.get('Contents'):
        raise Exception("No output files found")
        
    print("✅ Conversion completed successfully")

if __name__ == '__main__':
    test_conversion_flow() 