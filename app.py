import pyodbc
from datetime import datetime
import logging
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from tenacity import retry, wait_fixed, stop_after_attempt
from sqlalchemy.exc import SQLAlchemyError
import urllib

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_ventas.log'),
        logging.StreamHandler()
    ]
)

class Sync:
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
    def create_engine_connection(self, connection_string):
        try:
            engine = create_engine(connection_string)
            connection = engine.connect()
            print("Engine connection successful")
            return connection
        except Exception as e:
            print("Error creating engine connection: ", e)
            return None

    def get_database_remote(self):
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
            return self.create_engine_connection(f"mssql+pyodbc:///?odbc_connect={params}")

        except SQLAlchemyError as e:
            print("Error connecting to database: ", e)
            return None


    def get_database_local(self):
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
            return self.create_engine_connection(f"mssql+pyodbc:///?odbc_connect={params}")

        except SQLAlchemyError as e:
            print("Error connecting to database: ", e)
            return None

    def sync(self):
        try:
            local = self.get_database_local()
            remote = self.get_database_remote()

            if not local or not remote:
                raise Exception("Database connections could not be established")

            result = None
            with remote.begin():
                local_result = local.execute(text("EXEC HOVISYS.dbo.SALES"))
                result = local_result.fetchall()
            
            if not result:
                raise Exception("No data to transfer")
            
            for row in result:
                with remote.begin():
                    remote_result = remote.execute(
                        text("""
                            INSERT INTO [Ventas_RD].[dbo].[Ventas_SubwaySambilHoras]
                            (
                                NUMSERIE_RNC,FECHA,HORA,TOTALARTICULOS,TOTALTRANSVENTA,TASA,TOTALBRUTO,TOTALIMPUESTOS,TOTALNETO
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
                    if remote_result.rowcount != 0:
                        remote.execute(text(
                            "INSERT INTO [Ventas_RD].[dbo].[sync_control] (estado, mensaje)"
                            "VALUES ('SUCCESS', 'Data synchronized successfully')"
                        ))
                    else :
                        remote.execute(text(
                            "INSERT INTO [Ventas_RD].[dbo].[sync_control] (estado, mensaje)"
                            "VALUES ('FAILURE', 'Data synchronization failed')"
                        ))
                    remote.commit()
            print("Data transfer complete")
        except pyodbc.Error as e:
            print("Error during data transfer: ", e)
        
        finally:
            if local:
                local.close()
            if remote:
                remote.close()

    def store_open(self):
        from datetime import time
        now = datetime.now().time()
        openAt = time(6,0,0)
        closeAt = time(23,0,0)
        return openAt <= now <= closeAt


def main():
    """Función principal"""
    sincronizador = Sync()
    try:
        sincronizador.sync()
    except Exception as e:
        logging.error(f"Error crítico: {e}")
        exit(1)

if __name__ == "__main__":
    main()