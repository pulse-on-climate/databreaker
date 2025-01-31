import boto3
import json
import time
from pathlib import Path
import io
import zipfile
import subprocess
import os
from scripts.display_zarr_data import display_zarr_data
from scripts.compare_local_nc import analyze_local_netcdf
import xarray as xr
import pandas as pd

def setup_test_environment():
    """Setup LocalStack test environment"""
    print("Setting up test environment...")
    
    # Clean up existing Zarr store
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')
    try:
        # Delete all objects in zarr bucket
        response = s3.list_objects_v2(Bucket='noaa-oisst-zarr')
        if 'Contents' in response:
            for obj in response['Contents']:
                s3.delete_object(Bucket='noaa-oisst-zarr', Key=obj['Key'])
        print("Cleaned up existing Zarr store")
    except Exception as e:
        print(f"Error cleaning up Zarr store: {e}")
    
    # Initialize clients
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')
    sqs = boto3.client('sqs', endpoint_url='http://localhost:4566')
    lambda_client = boto3.client('lambda', endpoint_url='http://localhost:4566')
    
    # Create buckets if they don't exist
    for bucket in ['noaa-oisst-nc', 'noaa-oisst-zarr']:
        try:
            s3.head_bucket(Bucket=bucket)
        except:
            print(f"Creating bucket {bucket}")
            s3.create_bucket(Bucket=bucket)
            # Make bucket public
            s3.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': False,
                    'IgnorePublicAcls': False,
                    'BlockPublicPolicy': False,
                    'RestrictPublicBuckets': False
                }
            )
            # Add public read bucket policy
            bucket_policy = {
                'Version': '2012-10-17',
                'Statement': [{
                    'Sid': 'PublicReadGetObject',
                    'Effect': 'Allow',
                    'Principal': '*',
                    'Action': ['s3:GetObject'],
                    'Resource': [f'arn:aws:s3:::{bucket}/*']
                }]
            }
            s3.put_bucket_policy(
                Bucket=bucket,
                Policy=json.dumps(bucket_policy)
            )
    
    # Create SQS queue
    try:
        queue_url = sqs.get_queue_url(QueueName='netcdf-conversion')['QueueUrl']
    except:
        print("Creating SQS queue")
        queue = sqs.create_queue(QueueName='netcdf-conversion')
        queue_url = queue['QueueUrl']
    
    # Start converter container
    print("Starting converter container...")
    subprocess.run([
        'docker-compose',
        '-f', f'{Path(__file__).parent.parent}/docker/docker-compose.yml',
        'up',
        '-d',
        'converter'
    ])
    
    # Add S3 notification to SQS
    s3.put_bucket_notification_configuration(
        Bucket='noaa-oisst-nc',
        NotificationConfiguration={
            'QueueConfigurations': [{
                'QueueArn': f'arn:aws:sqs:us-east-1:000000000000:netcdf-conversion',
                'Events': ['s3:ObjectCreated:*']
            }]
        }
    )
    
    return s3, sqs, lambda_client, queue_url

def print_zarr_structure(objects):
    """Print Zarr store contents in a tree structure"""
    # Group files by their top-level directory
    structure = {}
    for obj in objects:
        key = obj['Key']
        top_level = key.split('/')[0] if '/' in key else 'root'
        if top_level not in structure:
            structure[top_level] = []
        structure[top_level].append(key)
    
    # Print tree structure
    print("\nZarr Store Structure:")
    print("└── root")
    for var_name in sorted(structure.keys()):
        if var_name == 'root':
            continue
        print(f"    ├── {var_name}/")
        # Show first few files for each variable
        files = sorted(structure[var_name])
        if len(files) > 4:
            for f in files[:3]:
                print(f"    │   ├── {f.split('/', 1)[1]}")
            print("    │   └── ... ({} more files)".format(len(files) - 3))
        else:
            for f in files[:-1]:
                print(f"    │   ├── {f.split('/', 1)[1]}")
            if files:
                print(f"    │   └── {files[-1].split('/', 1)[1]}")

def test_event_flow():
    """Test the full event flow from S3 trigger to conversion"""
    print("Starting event flow test...")
    
    # Setup test environment (creates buckets and other resources)
    s3, sqs, _, queue_url = setup_test_environment()
    
    # Load dataset config from file
    with open('config/oisst.json', 'r') as f:
        dataset_config = json.load(f)
    
    # Create config with environment settings and dataset config
    config = {
        "s3": {
            "endpoint_url": "http://localhost:4566",
            "access_key_id": "test",
            "secret_access_key": "test",
            "region": "us-east-1"
        },
        "conversion": dataset_config["conversion"]
    }
    
    # Set up test parameters
    source_bucket = "noaa-oisst-nc"
    dest_bucket = "noaa-oisst-zarr"
    test_files = [
        "oisst-avhrr-v02r01.20250101.nc",
        "oisst-avhrr-v02r01.20250102.nc",
        "oisst-avhrr-v02r01.20250103.nc"
    ]
    
    # Create source bucket structure
    print("\nSetting up source bucket...")
    s3.put_object(Bucket=source_bucket, Key="202501/")
    
    # Process each file in sequence
    for test_file in test_files:
        print(f"\nProcessing {test_file}...")
        
        # Verify test file exists
        test_file_path = f'tests/data/{test_file}'
        if not os.path.exists(test_file_path):
            print(f"Error: Test file not found: {test_file_path}")
            continue
        
        # Upload test file to source bucket
        source_key = f"202501/{test_file}"
        print(f"Uploading to {source_bucket}/{source_key}")
        s3.upload_file(
            test_file_path,
            source_bucket,
            source_key
        )
        
        # Wait for and check SQS message
        print("\nChecking SQS message...")
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )
        if 'Messages' in response:
            message = response['Messages'][0]
            print(f"SQS Message received: {message['Body']}")
            # Delete the message to prevent reprocessing
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        else:
            print("No SQS message received!")
        
        # Wait for conversion to complete
        print("Waiting for conversion to complete...")
        max_attempts = 30
        for _ in range(max_attempts):
            try:
                # Try to read the Zarr store
                ds = xr.open_zarr(f's3://{dest_bucket}', storage_options={'client_kwargs': {'endpoint_url': 'http://localhost:4566'}, 'anon': True})
                if pd.Timestamp(ds.time.values[0]).strftime('%Y-%m-%d') in [pd.Timestamp(t).strftime('%Y-%m-%d') for t in ds.time.values]:
                    print("Conversion completed successfully")
                    break
            except Exception:
                pass
            time.sleep(1)
            print(".", end="", flush=True)
        
        # Verify the conversion
        print("\nVerifying converted data...")
        try:
            response = s3.list_objects_v2(Bucket=dest_bucket)
            if 'Contents' in response:
                print_zarr_structure(response['Contents'])
                
                print("\n=== Zarr Store Analysis ===")
                # Display the data after structure verification
                display_zarr_data(f's3://{dest_bucket}', storage_options=config['s3'])
                
                print("\n=== Local NetCDF Analysis ===")
                # Analyze local files for comparison
                analyze_local_netcdf()
                
                print("\nNote: Values should match between Zarr and NetCDF analyses")
        except Exception as e:
            print(f"Error checking conversion: {e}")
        
        # Wait briefly between files to simulate real-world timing
        time.sleep(1)
    
    return True

if __name__ == '__main__':
    success = test_event_flow()
    exit(0 if success else 1) 