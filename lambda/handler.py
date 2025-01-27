import os
import json
import boto3
from converter import convert_netcdf_to_zarr

def main(event, context):
    """Lambda handler for NetCDF to Zarr conversion"""
    try:
        # Extract S3 event details
        records = event.get('Records', [])
        if not records:
            return {'statusCode': 400, 'body': 'No records in event'}
        
        s3_event = records[0]['s3']
        bucket = s3_event['bucket']['name']
        key = s3_event['object']['key']
        
        # Convert file
        result = convert_netcdf_to_zarr(
            source_path=f's3://{bucket}/{key}',
            dest_path=f's3://noaa-oisst-zarr/{key.replace(".nc", "")}'
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
    