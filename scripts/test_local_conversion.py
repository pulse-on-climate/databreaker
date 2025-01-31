import json
import time
import os
import boto3
from ecs.converter import convert_netcdf_to_zarr

def test_conversion():
    """Test local conversion of NetCDF to Zarr"""
    print("Starting conversion test...")
    
    test_files = [
        "oisst-avhrr-v02r01.20250101.nc",
        "oisst-avhrr-v02r01.20250102.nc",
        "oisst-avhrr-v02r01.20250103.nc"
    ]
    
    # Load dataset config from file
    with open('config/oisst.json', 'r') as f:
        dataset_config = json.load(f)
    
    # Create config with environment settings and dataset config
    config = {
        "s3": {
            "endpoint_url": "http://localstack:4566",
            "access_key_id": "test",
            "secret_access_key": "test",
            "region": "us-east-1"
        },
        "conversion": dataset_config["conversion"]
    }
    
    # Initialize S3 client
    s3 = boto3.client('s3', endpoint_url='http://localstack:4566')
    
    # Create source bucket structure
    print("\nChecking source bucket...")
    try:
        s3.head_bucket(Bucket='noaa-oisst-nc')
    except:
        print("Creating source bucket")
        s3.create_bucket(Bucket='noaa-oisst-nc')
    
    # Process each test file
    for test_file in test_files:
        # Verify test file exists
        test_file_path = f'tests/data/{test_file}'
        if not os.path.exists(test_file_path):
            print(f"Error: Test file not found: {test_file_path}")
            continue
        
        # Upload test file
        source_key = f"202501/{test_file}"
        print(f"\nUploading test file to s3://noaa-oisst-nc/{source_key}")
        s3.upload_file(
            test_file_path,
            'noaa-oisst-nc',
            source_key
        )
        
        # Convert file
        result = convert_netcdf_to_zarr(
            source_path=f's3://noaa-oisst-nc/{source_key}',
            base_dest_path='s3://noaa-oisst-zarr',
            config=config
        )
        print(f"Conversion completed: {result}")
    
    return True

if __name__ == '__main__':
    success = test_conversion()
    exit(0 if success else 1) 