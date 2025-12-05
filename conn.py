from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import urllib
from tenacity import retry, wait_fixed, stop_after_attempt

load_dotenv()

@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
def create_engine_connection(connection_string):
    try:
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        connection = engine.connect()
        print("Engine connection successful")
        return connection
    except Exception as e:
        print("Error creating engine connection: ", e)
        return None
    

def get_database_remote():
    server = os.getenv('REMOTE_HOST')
    database = os.getenv('REMOTE_DB')
    username =  os.getenv('REMOTE_USER')
    password = os.getenv('REMOTE_PASSWORD')

    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )

    try:
        return create_engine_connection(f"mssql+pyodbc:///?odbc_connect={params}")

    except SQLAlchemyError as e:
        print("Error connecting to database: ", e)
        return None

if __name__ == "__main__":
    remote_conn = get_database_remote()
    if remote_conn:
        with remote_conn as conn:
            result = remote_conn.execute(text("SELECT GETDATE();")).scalar()
            print(result)
        print("Remote database connection established.")
    else:
        print("Failed to establish remote database connection.")