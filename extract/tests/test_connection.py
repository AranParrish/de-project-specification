from extract.src.connection import connect_to_db
from pg8000.native import Connection


def test_func_returns_connection():
    assert isinstance(connect_to_db(), Connection)
