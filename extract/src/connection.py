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
    """This function will connect to the totesys database and return the
    connection"""
    conn = None
    conn = Connection(username=username, password=password,
                      database=database, host=host, port=port)
    if conn:
        return conn
    else:
        logger.error("contact to Manager")
