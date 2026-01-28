import pyodbc
from datetime import datetime
import logging
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from tenacity import retry, wait_fixed, stop_after_attempt
from sqlalchemy.exc import SQLAlchemyError
import urllib
from conn import main

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
    def __init__(self):
        self.remote, self.local = main()

    def sync(self):
        try:
            if not self.local or not self.remote:
                raise Exception("Database connections could not be established")

            result = None
            with self.local.begin() as l:
                local_result = l.execute(text("EXEC HOVISYS.dbo.SALES"))
                result = local_result.fetchall()
            
            if not result:
                raise Exception("No data to transfer")
            
            for row in result:
                with self.remote.begin() as r:
                    remote_result = r.execute(
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
                        r.execute(text(
                            "INSERT INTO [Ventas_RD].[dbo].[sync_control] (estado, mensaje)"
                            "VALUES ('SUCCESS', 'Data synchronized successfully')"
                        ))
                    else :
                        r.execute(text(
                            "INSERT INTO [Ventas_RD].[dbo].[sync_control] (estado, mensaje)"
                            "VALUES ('FAILURE', 'Data synchronization failed')"
                        ))
                    r.commit()
            print("Data transfer complete")
        except pyodbc.Error as e:
            print("Error during data transfer: ", e)

    def store_open(self):
        from datetime import time
        now = datetime.now().time()
        openAt = time(6,0,0)
        closeAt = time(23,0,0)
        return openAt <= now <= closeAt


def exec():
    """Función principal"""
    sincronizador = Sync()
    try:
        sincronizador.sync()
    except Exception as e:
        logging.error(f"Error: {e}")
        exit(1)

if __name__ == "__main__":
    exec()