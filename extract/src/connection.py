import os
import logging
from pg8000.native import Connection
from dotenv import load_dotenv
load_dotenv()

username = os.environ['USERNAME']
password = os.environ['PASSWORD']
database = os.environ['DATABASE']
host = os.environ['HOST']
port = os.environ['PORT']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def connect_to_db():   
    """This function will connect to the totesys database and return the connection"""
    conn = None
    try:
        conn = Connection(username, password = password, database = database, host = host, port = port)
        return conn
    except:
        if not conn:
           logger.error("contact to Manager")


