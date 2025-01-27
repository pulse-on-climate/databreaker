import os
import json
from dask_workers.converter import convert_netcdf_to_zarr, load_config

def main(event, context):
    """Lambda handler for NetCDF to Zarr conversion"""
    try:
        # Load config
        config = load_config()
        
        # Extract S3 event details
        records = event.get('Records', [])
        if not records:
            return {'statusCode': 400, 'body': 'No records in event'}
        
        s3_event = records[0]['s3']
        source_bucket = s3_event['bucket']['name']
        source_key = s3_event['object']['key']
        
        # Construct paths
        source_path = f's3://{source_bucket}/{source_key}'
        dest_path = f's3://{config["storage"]["dest_bucket"]}/{source_key.replace(".nc", "")}'
        
        # Convert file
        result = convert_netcdf_to_zarr(
            source_path=source_path,
            dest_path=dest_path,
            config=config
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Conversion completed',
                'result': result
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
    