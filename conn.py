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
    
def get_database_local():
    server = os.getenv('LOCAL_HOST')
    database = os.getenv('LOCAL_DB')
    username =  os.getenv('LOCAL_USER')
    password = os.getenv('LOCAL_PASSWORD')

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
    
def try_connection(connection):
    try:
        with connection as conn:
            result = conn.execute(text("SELECT GETDATE();")).scalar()
            print(result)
        return True
    except SQLAlchemyError as e:
        print(e)

def main():
    remote_conn = get_database_remote()
    local_conn = get_database_local()
    if try_connection(remote_conn) and try_connection(local_conn):
        print("Successfully!")

    else:
        print("Error connecting to database")
        raise SQLAlchemyError
    
    return remote_conn, local_conn

if __name__ == "__main__":
    main()