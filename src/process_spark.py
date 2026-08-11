"""
upload_to_s3.py

Bronze layer ingestion.

Uploads all raw CSV files found in the local data/raw folder to the
S3 bucket (Bronze layer), preserving the original file names. This is
the entry point of the pipeline: nothing is transformed here, the data
lands in S3 exactly as it was downloaded from Kaggle.
"""

import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
from pathlib import Path

# Project root is two levels up from this file (src/ -> project root)
default_path = Path(__file__).parent.parent

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

AWS_BUCKET_NAME = "olist-datalake-nean"
AWS_REGION = "us-east-1"
LOCAL_DATA_DIR = default_path / 'data' / 'raw'
S3_PREFIX = "raw/olist"


def get_cliente():
    """Returns a boto3 S3 client (credentials picked up from the environment)."""
    return boto3.client("s3")


def upload_file():
    """
    Uploads every CSV file in the local raw data folder to S3, under
    the configured prefix, keeping the original file names.
    """
    if not os.path.exists(LOCAL_DATA_DIR):
        print(f"Error: folder '{LOCAL_DATA_DIR}' was not found.")
        return

    s3 = get_cliente()

    arquivos = list(LOCAL_DATA_DIR.glob("*.csv"))

    if not arquivos:
        print(f"No CSV files found in '{LOCAL_DATA_DIR}'.")
        return

    print(f"Starting upload of {len(arquivos)} files to S3 (bucket: {AWS_BUCKET_NAME})...")

    for file_path in arquivos:
        s3_key = f"{S3_PREFIX}/{file_path.name}"
        try:
            print(f"Uploading: {file_path.name} -> s3://{AWS_BUCKET_NAME}/{s3_key}")
            s3.upload_file(str(file_path), AWS_BUCKET_NAME, s3_key)
        except NoCredentialsError:
            print("Error: AWS credentials not found. Check your .env file!")
            break
        except ClientError as e:
            print(f"AWS error on file {file_path.name}: {e}")


if __name__ == "__main__":
    upload_file()