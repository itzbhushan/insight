#!/usr/bin/env python

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import boto3

import zstandard as zst

in_file_name = Path("/mnt/RS_2019-01.zst")
out_file_name = Path("/mnt/tmp")

# read access from environment variables.
AWS_ACCESS_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

# setup boto3 client.
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


# create a test bucket.
bucket_name = "test-ycm-1"


def create_bucket(bucket_name):
    """
    Create bucket if it doesn't exist.
    """
    response = s3.list_buckets()
    buckets = {b["Name"] for b in response["Buckets"]}
    if bucket_name not in buckets:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )
        response = s3.list_buckets()
        print("Bucket successfully created. Details:", response["Buckets"])
    else:
        print(f"Bucket {bucket_name} already exists.")


def decompress_file(in_file_name, out_file_name):
    with open(in_file_name, "rb") as compressed:
        decomp = zst.ZstdDecompressor()
        with open(out_file_name, "w") as outfile:
            print(outfile.name)
            decomp.copy_stream(compressed, outfile)


create_bucket(bucket_name)
# decompress_file(in_file_name, out_file_name)

# response = s3.put_object(Bucket=bucket_name, Body="", Key="test-bucket")
# print(response['ResponseMetadata']['HTTPStatusCode'])
