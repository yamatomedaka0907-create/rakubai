
import boto3
import os

s3 = boto3.client("s3")

def delete_from_s3(image_url: str):
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        return

    # URL → key 抽出
    key = image_url.split(f"{bucket}.s3.amazonaws.com/")[-1]

    s3.delete_object(Bucket=bucket, Key=key)
