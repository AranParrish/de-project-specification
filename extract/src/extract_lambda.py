import boto3
from botocore.exceptions import ClientError
import logging
import os
from pg8000.native import Connection, DatabaseError, InterfaceError
import json
from datetime import datetime
from time import sleep

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SECRET_NAME = "db_creds"
REGION_NAME = "eu-west-2"
BUCKET_NAME = os.environ["ingestion_zone_bucket"]


# Create a Secrets Manager client
def get_db_creds(secret, region):

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret)
        secret_value = json.loads(get_secret_value_response["SecretString"])
        return secret_value
    except ClientError as e:
        logger.error("Invalid secret name")


DB_CREDS = get_db_creds(secret=SECRET_NAME, region=REGION_NAME)


# Connects to the totesys database using environment variables for credentials
def connect_to_db():
    """This function will connect to the totesys database and return the connection"""
    conn_attempts = 0
    try:
        conn_attempts += 1
        conn = Connection(**DB_CREDS)
        return conn
    except DatabaseError as exc:
        logger.error(f"Database error: {str(exc)}")
    except InterfaceError:
        while conn_attempts < 3:
            try:
                logger.error(f"Connection failed, waiting 10 seconds and retrying")
                sleep(1)
                conn = Connection(**DB_CREDS)
                return conn
            except:
                conn_attempts += 1
        logger.error(f"Unable to connect to database")


# Reads all data from a specified table in the database
def read_history_data_from_any_tb(tb_name):
    valid_tb_name = [
        "sales_order",
        "design",
        "currency",
        "staff",
        "counterparty",
        "address",
        "department",
        "purchase_order",
        "payment_type",
        "payment",
        "transaction",
    ]
    if tb_name in valid_tb_name:
        conn = connect_to_db()
        history_data = conn.run(f"""SELECT * FROM {tb_name};""")
        col_headers = [col["name"] for col in conn.columns]      
        if len(history_data) == 1:
            history_data = [i.isoformat(timespec='microseconds') for i in history_data if isinstance(i, datetime)]
            return dict(zip(col_headers, history_data[0]))
        conn.close()
        history_data = [[i.isoformat(timespec='microseconds') for i in data if isinstance(i, datetime)] for data in history_data]
        return [dict(zip(col_headers, data)) for data in history_data]
    else:
        logger.error(f"{tb_name} is not a valid table name.")


# Reads the data updated within the last 20 minutes from a specified table
def read_updates_from_any_tb(tb_name):
    valid_tb_name = [
        "sales_order",
        "design",
        "currency",
        "staff",
        "counterparty",
        "address",
        "department",
        "purchase_order",
        "payment_type",
        "payment",
        "transaction",
    ]
    if tb_name in valid_tb_name:
        conn = connect_to_db()
        updates = conn.run(
            f"""SELECT * FROM {tb_name} WHERE  now() - last_updated < interval '20 minutes';"""
        )
        col_headers = [col["name"] for col in conn.columns]
        if len(updates) == 1:
            updates = [i.isoformat(timespec='microseconds') for i in updates if isinstance(i, datetime)]
            return dict(zip(col_headers, updates[0]))
        conn.close()
        updates = [[i.isoformat(timespec='microseconds') for i in data if isinstance(i, datetime)] for data in updates]
        return [dict(zip(col_headers, data)) for data in updates]
    else:
        logger.error(f"{tb_name} is not a valid table name.")


# Writes the provided data to a JSON file and uploads it to an S3 bucket
def write_data(
    s3_client: str, BUCKET_NAME: str, formatted_data: list, table: str
) -> json:
    file = json.dumps(formatted_data, default=str)
    key = f"{datetime.now().date()}/{table}-{datetime.now().time()}.json"

    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=file)
        logger.info(
            f"Data from {table} at {datetime.now()} written to S3 successfully."
        )
    except ClientError as c:
        logger.error(f"Boto3 ClientError: {str(c)}")


def lambda_handler(event, context):
    """Main handler - event is empty."""
    try:
        s3_client = boto3.client("s3")
        bucket_content = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        tables = [
            "sales_order",
            "design",
            "currency",
            "staff",
            "counterparty",
            "address",
            "department",
            "purchase_order",
            "payment_type",
            "payment",
            "transaction",
        ]

        if bucket_content["KeyCount"] == 0:
            for table in tables:
                print("reading data start")
                result = read_history_data_from_any_tb(table)

                print("reading data successful")
                write_data(s3_client, BUCKET_NAME, result, table)
        else:
            for table in tables:
                result = read_updates_from_any_tb(table)
                if result:
                    write_data(s3_client, BUCKET_NAME, result, table)
                else:
                    logger.info(f"{table} has no new data at {datetime.now()}.")

    except ClientError as e:
        logger.error(f"Error InvalidClientTokenId: {e}")
