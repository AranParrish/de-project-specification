import pandas as pd
import logging, boto3, os, json
import awswrangler as wr
from pg8000.native import Connection, DatabaseError, InterfaceError, identifier
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

# Connects to the data warehouse using secrets manager credentials
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


# check data in data warehouse
def check_exist_data(table_name):
    con = connect_to_db()
    query = con.run(f"SELECT * FROM {identifier(DW_CREDS['schema'])}.{identifier(table_name)}")
    if len(query) == 0:
        return False
    else: 
        return True


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
    print(event)