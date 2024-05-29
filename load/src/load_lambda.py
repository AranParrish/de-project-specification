import pandas as pd
import logging, boto3, os, json, urllib, re
import awswrangler as wr
from pg8000.native import Connection, DatabaseError, InterfaceError
from botocore.exceptions import ClientError
from time import sleep


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SECRET_NAME = "dw_creds"
REGION_NAME = "eu-west-2"
PROCESSED_ZONE_BUCKET = os.environ["processed_data_zone_bucket"]

# Create a Secrets Manager client
def get_dw_creds(secret, region):

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret)
        secret_value = json.loads(get_secret_value_response["SecretString"])
        return secret_value
    except ClientError as e:
        logger.error("Invalid secret name")

DW_CREDS = get_dw_creds(secret=SECRET_NAME, region=REGION_NAME)


# Connects to the totesys database using environment variables for credentials
def connect_to_db():
    """This function will connect to the totesys database and return the connection"""
    conn_attempts = 0
    try:
        conn_attempts += 1
        conn = Connection(user=DW_CREDS['user'],
                          password=DW_CREDS['password'],
                          port=DW_CREDS['port'],
                          host=DW_CREDS['host'],
                          database=DW_CREDS['database'])
        return conn
    except DatabaseError as exc:
        logger.error(f"Database error: {str(exc)}")
    except InterfaceError:
        while conn_attempts < 3:
            try:
                logger.error(f"Connection failed, waiting 10 seconds and retrying")
                sleep(1)
                conn = Connection(user=DW_CREDS['user'],
                                password=DW_CREDS['password'],
                                port=DW_CREDS['port'],
                                host=DW_CREDS['host'],
                                database=DW_CREDS['database'])
                return conn
            except:
                conn_attempts += 1
        logger.error(f"Unable to connect to database")


# Read in parquet files -AWS Wrangler
# write data to data warehouse
def get_file_and_write_to_db(table_name, object_key):
    con = None
    try:
        # read parquet data from s3 
        df = wr.s3.read_parquet(path=f's3://{PROCESSED_ZONE_BUCKET}/{object_key}')
        # write data to warehouse
        con = connect_to_db()
        wr.postgresql.to_sql(
            df=df,
            table=table_name,
            schema=DW_CREDS["schema"],
            mode="append"
        )
    except Exception:
        logger.error("ERROR")
    finally:
        if con:
            con.close()


def lambda_handler(event, context):
    try:
        client = boto3.client('s3')
        pattern = re.compile(r"(['/'])([a-z-]+)")
        # if event['Records'][0]['s3']['bucket']['name'] == PROCESSED_ZONE_BUCKET:
        if event:
            key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
            match = pattern.search(key)
            table_name = match.group(2)[:-1]
            get_file_and_write_to_db(table_name=table_name, object_key=key)
        else:
            # insert all the files in processed bucket
            response = client.list_objects_v2(Bucket=PROCESSED_ZONE_BUCKET)
            for key in response['Contents']['Key']:
                match = pattern.search(key)
                table_name = match.group(2)[:-1]
                get_file_and_write_to_db(table_name=table_name, object_key= key)
    
    except KeyError as k:
        logger.error(f"Error retrieving data, {k}")
    except ClientError as c:
        if c.response["Error"]["Code"] == "NoSuchKey":
            logger.error(f"No such key: {c}")
        elif c.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(f"No such bucket: {c}")
        else:
            logger.error(f"Error InvalidClientTokenId: {c}")
    except UnicodeDecodeError as e:
        logger.error(f'Unable to decode the file: {e}')
    except Exception as e:
        logger.error(e)
        raise RuntimeError  
    