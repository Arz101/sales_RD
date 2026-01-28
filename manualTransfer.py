import pyodbc
from conn import main
from sqlalchemy import text

class Transafer:
    def __init__(self):
        self.remote, self.local = main()
        self.startDate = input("start date: ")
        self.endDate = input("end date: ")

    def sync(self):
        try:
            if not self.local or not self.remote:
                raise Exception("Database connections could not be established")

            result = None
            with self.remote.begin():
                local_result = self.local.execute(text(f"EXEC HOVISYS.dbo.SALES {self.startDate}, {self.endDate}"))
                result = local_result.fetchall()
            
            if not result:
                raise Exception("No data to transfer")
            
            for row in result:
                with self.remote.begin():
                    remote_result = self.remote.execute(
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
                        self.remote.execute(text(
                            "INSERT INTO [Ventas_RD].[dbo].[sync_control] (estado, mensaje)"
                            "VALUES ('SUCCESS', 'Data synchronized successfully')"
                        ))
                    else :
                        self.remote.execute(text(
                            "INSERT INTO [Ventas_RD].[dbo].[sync_control] (estado, mensaje)"
                            "VALUES ('FAILURE', 'Data synchronization failed')"
                        ))
                    self.remote.commit()
            print("Data transfer complete")
        except pyodbc.Error as e:
            print("Error during data transfer: ", e)
        
        finally:
            if self.local:
                self.local.close()
            if self.remote:
                self.remote.close()

def exec():
    try:
        t = Transafer()
        t.sync()
    except Exception as e:
        print("Critical Error: ", e)

if __name__ == '__main__':
    exec()    