import os

class Config:
    DUCKDB_PATH = os.getenv('DUCKDB_PATH', '/app/data/db.duckdb')
