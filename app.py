import pyodbc
from datetime import datetime
import time
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from tenacity import retry, wait_fixed, stop_after_attempt
from sqlalchemy.exc import SQLAlchemyError
import urllib

load_dotenv()

@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
def create_engine_connection(connection_string):
    try:
        engine = create_engine(connection_string)
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
    server =  os.getenv('LOCAL_HOST')
    database = os.getenv('LOCAL_DB')
    username = os.getenv('LOCAL_USER')
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

def transfer_data():
    try:
        local = get_database_local()
        remote = get_database_remote()
        result = None
        with remote.begin():
            local_result = local.execute(text("EXEC HOVISYS.dbo.SALES"))
            result = local_result.fetchall()
        
        if not result:
            print("No data to transfer")
            return
        
        for row in result:
            with remote.begin():
                remote.execute(
                    text("""
                        INSERT INTO [Ventas_RD].[dbo].[Ventas_SubwaySambilHoras]
                        (
                            NUMSERIE_RNC,
                            FECHA,
                            HORA,
                            TOTALARTICULOS,
                            TOTALTRANSVENTA,
                            TASA,
                            TOTALBRUTO,
                            TOTALIMPUESTOS,
                            TOTALNETO
                        )
                        VALUES (
                            :NUMSERIE_RNC,
                            :FECHA,
                            :HORA,
                            :TOTALARTICULOS,
                            :TOTALTRANSVENTA,
                            :TASA,
                            :TOTALBRUTO,
                            :TOTALIMPUESTOS,
                            :TOTALNETO
                        )
                    """),
                    {
                        "NUMSERIE_RNC": row.NUMSERIE_RNC,
                        "FECHA": row.FECHA,
                        "HORA": row.HORA,
                        "TOTALARTICULOS": row.TOTALARTICULOS,
                        "TOTALTRANSVENTA": row.TOTALTRANSVENTA,
                        "TASA": row.TASA,
                        "TOTALBRUTO": row.TOTALBRUTO,
                        "TOTALIMPUESTOS": row.TOTALIMPUESTOS,
                        "TOTALNETO": row.TOTALNETO,
                    }
                )
                remote.commit()
        print("Data transfer complete")
    except pyodbc.Error as e:
        print("Error during data transfer: ", e)


def store_open():
    from datetime import time
    now = datetime.now().time()
    openAt = time(7,0,0)
    closeAt = time(22,0,0)
    return openAt <= now <= closeAt


def test_connections():
    print(pyodbc.drivers())
    remote_server = get_database_remote()
    local_server = get_database_local()
    if remote_server and local_server:
        remote_server.close()
        local_server.close()
        return True
    return False

if __name__ == "__main__":
    while True:
        try:
            if store_open():
                transfer_data()
        except Exception as e:
            print("An error occurred: ", e)       
        time.sleep(3600)  # Esperar 1 hora para la siguiente transferencia
