import json
import boto3
import logging
from datetime import (datetime)

# Initialize SNS client
sns_client = boto3.client('sns')

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topic ARN (replace with your SNS Topic ARN)
SNS_TOPIC_ARN = 'arn:aws:sns:REGION:ACCOUNT_ID:TOPIC_NAME'  # Replace with your actual SNS topic ARN


def lambda_handler(event, context):
    try:
        # Extract the first record from the event
        record = event['Records'][0]
        s3_info = record['s3']

        # Extract relevant details
        bucket_name = s3_info['bucket']['name']
        object_key = s3_info['object']['key']
        object_size_bytes = s3_info['object'].get('size', 0)  # Get size, default to 0 if not present
        object_size_mb = object_size_bytes / (1024 * 1024)  # Convert size to MB

        # Extract the event time and format it
        event_time = record['eventTime']
        formatted_time = datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d %H:%M:%S UTC')

        # Create a human-readable message
        message = (
            f"**S3 Object Upload Notification**\n\n"
            f"**Bucket Name**: {bucket_name}\n"
            f"**File Name**: {object_key}\n"
            f"**Size**: {object_size_mb:.2f} MB\n"
            f"**Upload Time**: {formatted_time}\n"
        )

        # Log the message in CloudWatch Logs
        logger.info(f"File Uploaded: {object_key}, Size: {object_size_mb:.2f} MB, "
                    f"Time: {formatted_time}, Bucket: {bucket_name}")

        # Send the formatted message to SNS
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject='S3 Upload Notification'
        )

        print("Notification sent successfully.")

    except Exception as e:
        # Log the error in CloudWatch Logs
        logger.error(f"Error processing S3 event: {e}")
        raise e
