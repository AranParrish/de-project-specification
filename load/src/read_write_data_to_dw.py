import pandas as pd
import logging, boto3, os, json, urllib, re
import awswrangler as wr
from pg8000.native import Connection, DatabaseError, InterfaceError
from botocore.exceptions import ClientError
from time import sleep
#from load.src.load_lambda import connect_to_db

def read_data():
    df = pd.read_parquet(path="load/data/fact_sales_order-15_36_42.731009.parquet")
    print(df.head(20))
    
    
read_data()
    

