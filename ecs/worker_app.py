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
            dest_path=f"s3://{body['dest_bucket']}",
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

def main():
    """Process single message and exit"""
    sqs = boto3.client('sqs')
    queue_url = os.environ['SQS_QUEUE_URL']
    message_id = os.environ['MESSAGE_ID']
    
    logger.info(f"Processing message {message_id}")
    
    success = process_message(sqs, queue_url, message_id)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 