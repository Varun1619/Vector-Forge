from __future__ import annotations

import os

import boto3


def s3_client():
    """Return a boto3 S3 client pointed at our MinIO instance."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.environ['VF_MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ["VF_MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["VF_MINIO_SECRET_KEY"],
    )