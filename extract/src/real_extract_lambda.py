import boto3
from botocore.exceptions import ClientError
import logging
import os
from dotenv import load_dotenv
from pg8000.native import Connection
import json


load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# can set the environment variables in the aws_lambda_function resource in lambda.tf except for password 
def connect_to_db():   
    """This function will connect to the totesys database and return the connection"""
    username = os.environ['USERNAME']
    password = os.environ['PASSWORD']
    database = os.environ['DATABASE']
    host = os.environ['HOST']
    port = os.environ['PORT']
    return Connection(username, password = password, database = database, host = host, port = port)

# function that reads entire contents of table at current time

def read_history_data_from_any_tb(tb_name):
    valid_tb_name = ['sales_order', 'design', 'currency', 'staff', 'counterparty', 'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction' ]
    if tb_name in valid_tb_name:
        conn = connect_to_db()
        history_data = conn.run(f"""SELECT * FROM {tb_name};""")
        col_headers = [col["name"] for col in db.columns]
        if len(result) == 1:
            return dict(zip(col_headers, history_data[0]))
        return [dict(zip(col_headers, data)) for data in history_data]
    else:
        return f"{tb_name} is not a valid table name."

def read_updates_from_any_tb(tb_name):
    valid_tb_name = ['sales_order', 'design', 'currency', 'staff', 'counterparty', 'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction' ]
    if tb_name in valid_tb_name:
        conn = connect_to_db()
        updates = conn.run(f"""SELECT * FROM {tb_name} WHERE  now() - last_updated < interval '20 minutes';""")
        col_headers = [col["name"] for col in db.columns]
        if len(result) == 1:
            return dict(zip(col_headers, updates[0]))
        return [dict(zip(col_headers, data)) for data in updates]
    else:
        return f"{tb_name} is not a valid table name."



def lambda_handler(event, context):
    """Main handler - event is empty."""
    try:
        s3_client = boto3.client("s3")
        tables = ['sales_order', 'design', 'currency', 'staff', 'counterparty', 'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction' ]
        for table in tables:
            result = read_history_data_from_any_tb(table)
            with open(f'{table}.json', 'w') as file:
                json.dump(result, file)

        






        


