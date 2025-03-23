#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 18:45:16 2025

@author: syphaxmedjkane
"""

import boto3
import json
from datetime import datetime, timezone

# Initialize the S3 client using boto3 to interact with AWS S3
s3_client = boto3.client(
    's3',
    aws_access_key_id='',  # Replace with your AWS access key
    aws_secret_access_key='',  # Replace with your AWS secret key
    region_name='eu-west-3'  # Specify the region for your S3 bucket
)

bucket_name = "flight-simulator-data-414266450797" # S3 bucket name where data is stored
prefix = "data/"  # Prefix to filter files by the "data/" folder

# List all objects in the S3 bucket with the specified prefix ("data/")
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if "Contents" in response:
    # Sort the files by their last modified date (latest first)
    files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)

    latest_file = files[0]  # Take the most recent file
    latest_file_key = latest_file["Key"]  # Get the key (path) of the latest file
    last_modified = latest_file["LastModified"].astimezone(timezone.utc)  # Convert the last modified date to UTC timezone

    print(f"Latest file found: {latest_file_key} (modified: {last_modified} UTC)")

    # Download the content of the latest file
    response = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
    content = response["Body"].read().decode("utf-8")  # Read the file content and decode it from bytes to string
    print("File content:")
    print(content)

else:
    print("No files found in S3.")  # If no files are found, print a message