from dask_workers.converter import convert_netcdf_to_zarr

def test_conversion():
    """Test local conversion of a single file"""
    print("Starting conversion test...")
    
    # Test S3 connection
    import boto3
    s3 = boto3.client('s3', endpoint_url='http://localstack:4566')
    print("Checking source bucket...")
    response = s3.list_objects_v2(Bucket='noaa-oisst-nc')
    print(f"Source bucket contents: {response}")
    
    result = convert_netcdf_to_zarr(
        source_path='s3://noaa-oisst-nc/incoming/sample.nc',
        dest_path='s3://noaa-oisst-zarr/incoming/sample'
    )
    print(f"Conversion completed: {result}")

if __name__ == '__main__':
    test_conversion() 