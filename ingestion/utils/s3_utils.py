import boto3
from config.config import *

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

def upload_file_to_s3(local_file, bucket, s3_key):

    print(f"Uploading {local_file} to S3...")

    s3_client.upload_file(
        local_file,
        bucket,
        s3_key
    )

    print("Upload successful")