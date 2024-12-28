import json
from PIL import Image
import boto3
import os
from io import BytesIO
import urllib.parse

# import uuid

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    # Fetch the target bucket name from environment variable
    target_bucket_name = os.environ.get('TARGET_BUCKET')
    print(f"Target bucket: {target_bucket_name}")

    if not target_bucket_name:
        raise ValueError("Target bucket name is not set in environment variables.")

    # Log the full event for debugging purposes
    # print("Event received:", json.dumps(event, indent=4))

    try:
        # Get the bucket and object key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        # Decode the object key to handle URL encoding issues
        object_key = urllib.parse.unquote_plus(object_key)

        # Log the bucket name and object key for verification
        print(f"Source Bucket: {bucket_name}")
        print(f"Object Key: {object_key}")

        # Log a message indicating the start of the compression process
        print(f"Starting compression for object: {object_key}")

        if not object_key.startswith("resized_"):
            # Fetch the image from the S3 bucket
            print(f"Fetching object: {object_key} from bucket: {bucket_name}")
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            image_data = s3_response['Body'].read()

            # Log that image data has been successfully fetched
            print(f"Image data fetched for object: {object_key}")

            # Resize the image
            image = Image.open(BytesIO(image_data))
            width, height = image.size
            resized_image = image.resize((width // 2, height // 2))

            # Log the image resize process
            print(f"Resized image from {width}x{height} to {width // 2}x{height // 2}")

            # Save resized image to buffer
            output_buffer = BytesIO()
            resized_image.save(output_buffer, format='PNG')
            output_buffer.seek(0)

            # Log the size of the buffer
            print(f"Size of resized image buffer: {output_buffer.getbuffer().nbytes}")

            # Create a unique key to avoid collisions
            # new_object_key = f"resized_{uuid.uuid4()}_{object_key}"
            new_object_key = f"resized_{object_key}"

            # Upload the resized image to the target bucket
            print(f"Uploading resized image to {target_bucket_name}/{new_object_key}")
            response = s3_client.put_object(Bucket=target_bucket_name, Key=new_object_key, Body=output_buffer)

            # Log the response from the put_object call
            print(f"PutObject response: {response}")

        else:
            print(f"Object {object_key} is already resized. Skipping processing.")

    except s3_client.exceptions.NoSuchKey as e:
        print(f"Error: The object key '{object_key}' does not exist in the bucket '{bucket_name}'")
        raise e
    except Exception as e:
        print(f"Error processing object {object_key} from bucket {bucket_name}: {str(e)}")
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Compression Complete!')
    }
