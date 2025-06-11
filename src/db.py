import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_database_url():
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_DATABASE')
    return f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def connect_db():
    return psycopg2.connect(get_database_url())