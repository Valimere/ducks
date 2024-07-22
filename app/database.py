import duckdb
from config import Config

def get_connection():
    conn = duckdb.connect(Config.DUCKDB_PATH)
    return conn
