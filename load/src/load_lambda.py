import pandas as pd
import logging, boto3, os, json, urllib
import awswrangler as wr
from pg8000.native import Connection, DatabaseError, InterfaceError
from botocore.exceptions import ClientError
# Load local DB credentials for initial testing purposes
from dotenv import load_dotenv
from time import sleep
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# SECRET_NAME = "dw_creds"
# REGION_NAME = "eu-west-2"
PROCESSED_ZONE_BUCKET = os.environ["processed_data_zone_bucket"]

# # Create a Secrets Manager client
# def get_dw_creds(secret, region):

#     session = boto3.session.Session()
#     client = session.client(service_name="secretsmanager", region_name=region)

#     try:
#         get_secret_value_response = client.get_secret_value(SecretId=secret)
#         secret_value = json.loads(get_secret_value_response["SecretString"])
#         return secret_value
#     except ClientError as e:
#         logger.error("Invalid secret name")

# DW_CREDS = get_dw_creds(secret=SECRET_NAME, region=REGION_NAME)

DW_CREDS = {
    "database": os.environ["DATABASE"],
    "user": os.environ["USERNAME"],
    "password": os.environ["PASSWORD"],
    "host": os.environ["HOST"],
    "port": os.environ["PORT"]
}

# Connects to the totesys database using environment variables for credentials
def connect_to_db():
    """This function will connect to the totesys database and return the connection"""
    conn_attempts = 0
    try:
        conn_attempts += 1
        conn = Connection(**DW_CREDS)
        return conn
    except DatabaseError as exc:
        logger.error(f"Database error: {str(exc)}")
    except InterfaceError:
        while conn_attempts < 3:
            try:
                logger.error(f"Connection failed, waiting 10 seconds and retrying")
                sleep(1)
                conn = Connection(**DW_CREDS)
                return conn
            except:
                conn_attempts += 1
        logger.error(f"Unable to connect to database")

# Read in parquet files (AWS Wrangler? or URLLIB?)


# Convert to JSON

# Secrets Manager for Data warehouse access

# Use someone's local psql account for initial testing

# Create initial schema

# Read JSON files into python objects


# Load python objects to psql (PG8000)

def lambda_handler(event, context):
    print(event)