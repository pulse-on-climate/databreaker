import json
from dask.distributed import Client

def handler(event, context):
    """Lambda handler that coordinates with Dask cluster"""
    try:
        # Connect to Dask scheduler
        client = Client("tcp://dask-scheduler:8786")
        
        # Get S3 event details
        records = event['Records']
        for record in records:
            # Extract bucket and key
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            # Submit task to Dask
            future = client.submit(
                convert_netcdf_to_zarr,
                source_path=f's3://{bucket}/{key}',
                dest_path=f's3://noaa-oisst-zarr/{key.replace(".nc", "")}',
                chunk_config={'time': 1},
                compression_config={'compressor': None},
                s3_config={}
            )
            
            # Wait for result
            result = future.result()
            return {
                'statusCode': 200,
                'body': json.dumps(result)
            }
            
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    