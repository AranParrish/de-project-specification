import boto3
from botocore.exceptions import ClientError
import logging
import os
from dotenv import load_dotenv
from pg8000.native import Connection
import json
from datetime import datetime

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Connects to the totesys database using environment variables for credentials
def connect_to_db(): 
    """This function will connect to the totesys database and return the connection"""
    secret_name = "db_creds"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    print(secret)
    print(type(secret))
    # Your code goes here.
    secret = json.loads(secret)
    username = secret['username']
    password = secret['password']
    database = secret['dbname']
    host = secret['host']
    port = secret['port']
    return Connection(username, password = password, database = database, host = host, port = port)

# Reads all data from a specified table in the database
def read_history_data_from_any_tb(tb_name): 
    valid_tb_name = ['sales_order', 'design', 'currency', 'staff', 'counterparty',
    'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction' ]
    if tb_name in valid_tb_name:
        conn = connect_to_db()
        history_data = conn.run(f"""SELECT * FROM {tb_name};""")
        col_headers = [col["name"] for col in conn.columns]
        conn.close()
        if len(history_data) == 1:
            return dict(zip(col_headers, history_data[0]))
        return [dict(zip(col_headers, data)) for data in history_data]
    else:
        return f"{tb_name} is not a valid table name."
    
# Reads the data updated within the last 20 minutes from a specified table
def read_updates_from_any_tb(tb_name): 
    valid_tb_name = ['sales_order', 'design', 'currency', 'staff', 'counterparty',
    'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction' ]
    if tb_name in valid_tb_name:
        conn = connect_to_db()
        updates = conn.run(f"""SELECT * FROM {tb_name} WHERE  now() - last_updated < interval '20 minutes';""")
        col_headers = [col["name"] for col in conn.columns]
        conn.close()
        if len(updates) == 1:
            return dict(zip(col_headers, updates[0]))
        return [dict(zip(col_headers, data)) for data in updates]
    else:
        return f"{tb_name} is not a valid table name."

# Writes the provided data to a JSON file and uploads it to an S3 bucket
def write_data(s3_client: str, BUCKET_NAME: str, formatted_data: list, table: str) -> json: 
        file = json.dumps(formatted_data, default=str)
        key = f'{datetime.now().date()}/{table}-{datetime.now().time()}.json'

        try:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=file)
            logger.info(f'Data from {table} at {datetime.now()} written to S3 successfully.')
            return True
        except ClientError as c:
            logger.error(f"Boto3 ClientError: {str(c)}")
            return False



def lambda_handler(event, context):
    """Main handler - event is empty."""

    try:
        #BUCKET_NAME = os.environ['ingestion_zone_bucket']
        BUCKET_NAME = 'de-heritage-ingestion'
        s3_client = boto3.client("s3")
        bucket_content = s3_client.list_objects_v2(Bucket = BUCKET_NAME)
        tables = ['sales_order', 'design', 'currency', 'staff', 'counterparty',
        'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction' ]
        print(bucket_content)
        if bucket_content['KeyCount'] == 0:
            for table in tables:
                print("reading data start")
                result = read_history_data_from_any_tb(table)
                
                print("reading data successful")
                write_data(s3_client,BUCKET_NAME, result, table)
        else:
            for table in tables:
                result = read_updates_from_any_tb(table)
                if result:
                    write_data(s3_client, BUCKET_NAME, result, table)
                else:
                    logger.info(f'{table} has no new data at {datetime.now()}.')
    
    except ClientError as e:
        logger.error(e)
                    
        






        


