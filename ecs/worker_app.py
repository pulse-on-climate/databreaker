import os
import boto3
import logging
import json
import sys
from ecs.converter import convert_netcdf_to_zarr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_message(sqs_client, queue_url, message_id):
    """Process specific SQS message and exit"""
    try:
        # Receive specific message using message ID
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=900,  # 15 minutes
            WaitTimeSeconds=20
        )
        
        if 'Messages' not in response:
            logger.error("Message not found")
            return False
            
        message = response['Messages'][0]
        if message['MessageId'] != message_id:
            logger.error("Message ID mismatch")
            return False
            
        # Parse message
        body = json.loads(message['Body'])
        
        # Load dataset-specific config
        config_path = os.environ.get('DATASET_CONFIG', '/app/config/dataset_config.json')
        
        # Process the file
        result = convert_netcdf_to_zarr(
            source_path=f"s3://{body['source_bucket']}/{body['source_key']}",
            dest=f"s3://{body['dest_bucket']}",
            config_path=config_path
        )
        
        # Delete message if successful
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
        
        logger.info(f"Successfully processed {body['source_key']}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise

def process_input():
    """
    Process the input file using environment variables:
      INPUT_FILE: S3 path to the file to process
      SOURCE_BUCKET: Source S3 bucket name (for logging/debug purposes)
      DEST_BUCKET: Destination S3 bucket name for output data
      AWS_DEFAULT_REGION: The AWS region (optional, defaults to 'us-east-1')
      DATASET_CONFIG: Optional path to a config file (defaults to '/app/config/dataset_config.json')
    """
    input_file = os.environ.get('INPUT_FILE')
    source_bucket = os.environ.get('SOURCE_BUCKET')
    dest_bucket = os.environ.get('DEST_BUCKET')
    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    config_path = os.environ.get('DATASET_CONFIG', '/app/config/dataset_config.json')

    if not input_file:
        print("INPUT_FILE environment variable is not set")
        sys.exit(1)

    print(f"Processing file: {input_file}")
    logger.info(f"Processing file: {input_file}")

    try:
        result = convert_netcdf_to_zarr(
            source_path=input_file,
            dest=f"s3://{dest_bucket}",
            config_path=config_path
        )
        logger.info(f"Successfully processed {input_file}")
    except Exception as e:
        logger.error(f"Failed to process {input_file}: {str(e)}")
        sys.exit(1)

def main():
    """Process input file directly without using SQS."""
    print("SQS_QUEUE_URL not required; processing input file directly")
    process_input()

if __name__ == "__main__":
    main() 