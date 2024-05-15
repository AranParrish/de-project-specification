import boto3
from botocore.exceptions import ClientError
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Main handler - event is empty."""
    try:
        s3_client = boto3.client("s3")


