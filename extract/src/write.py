import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

BUCKET_NAME = os.environ['ingestion_zone_bucket']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def write_data(s3_client: str, formatted_data: list) -> json:
    
    body = json.dumps(formatted_data)
    key = f'{datetime.now()}_totesys_snapshot'

    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)
        return True
    except ClientError as c:
        logger.info(f"Boto3 ClientError: {str(c)}")
        return False