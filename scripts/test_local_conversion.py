from dask_workers.converter import convert_netcdf_to_zarr

def test_conversion():
    """Test local conversion of a single file"""
    print("Starting conversion test...")
    
    # Test S3 connection
    import boto3
    s3 = boto3.client('s3', endpoint_url='http://localstack:4566')
    
    # Create test file path
    source_key = "202501/oisst-avhrr-v02r01.20250108.nc"
    source_bucket = "noaa-oisst-nc"
    dest_bucket = "noaa-oisst-zarr"
    
    print("Checking source bucket...")
    # Create folder structure and upload file
    s3.put_object(
        Bucket=source_bucket,
        Key="202501/"
    )
    
    # Upload test file
    s3.upload_file(
        'tests/data/sample.nc',  # Assuming this is your test file
        source_bucket,
        source_key
    )
    
    result = convert_netcdf_to_zarr(
        source_path=f's3://{source_bucket}/{source_key}',
        base_dest_path=f's3://{dest_bucket}'
    )
    print(f"Conversion completed: {result}")

if __name__ == '__main__':
    test_conversion() 