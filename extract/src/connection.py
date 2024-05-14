from pg8000.native import Connection
import os
from dotenv import load_dotenv
load_dotenv()

username = os.environ['USERNAME']
password = os.environ['PASSWORD']
database = os.environ['DATABASE']
host = os.environ['HOST']
port = os.environ['PORT']

def connect_to_db():   
    """This function will connect to the totesys database and return the connection"""
    return Connection(username, password = password, database = database, host = host, port = port)



