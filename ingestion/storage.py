"""MinIO (S3-compatible) helpers shared by collectors, Spark driver code and loaders."""
import json
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config import settings

BUCKETS = [settings.BUCKET_RAW, settings.BUCKET_CURATED, settings.BUCKET_QUALITY, settings.BUCKET_OPS]
_MISSING = {"404", "NoSuchKey", "NotFound"}


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
    )


def init_buckets(attempts=30):
    s3 = s3_client()
    for attempt in range(1, attempts + 1):       # wait for MinIO to be ready (bounded, never forever)
        try:
            existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
            break
        except Exception as exc:
            if attempt == attempts:
                raise
            print(f"MinIO not ready yet ({exc.__class__.__name__}), retrying...")
            time.sleep(2)
    for bucket in BUCKETS:
        if bucket in existing:
            print(f"bucket '{bucket}' already exists")
        else:
            s3.create_bucket(Bucket=bucket)
            print(f"created bucket '{bucket}'")


def put_json(bucket, key, obj, s3=None):
    s3 = s3 or s3_client()
    body = json.dumps(obj, indent=2, default=str).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def get_json(bucket, key, default=None, s3=None):
    s3 = s3 or s3_client()
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in _MISSING:
            return default
        raise
    return json.loads(body)


def exists(bucket, key, s3=None):
    s3 = s3 or s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in _MISSING:
            return False
        raise


if __name__ == "__main__":
    if sys.argv[1:] == ["init"]:
        init_buckets()
    else:
        print("usage: python -m ingestion.storage init")