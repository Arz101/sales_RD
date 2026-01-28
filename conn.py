from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from tenacity import retry, wait_fixed, stop_after_attempt
import os
import urllib
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

# -----------------------
# Engine factory (retry)
# -----------------------
@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
def create_engine_safe(connection_string: str):
    logger.info("Creating engine...")
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=5,
        max_overflow=10,
        future=True
    )

    # test de conexión
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    logger.info("Engine ready")
    return engine


def build_connection_string(prefix: str) -> str:
    server = os.getenv(f"{prefix}_HOST")
    database = os.getenv(f"{prefix}_DB")
    username = os.getenv(f"{prefix}_USER")
    password = os.getenv(f"{prefix}_PASSWORD")

    if not all([server, database, username, password]):
        raise ValueError(f"Missing env vars for {prefix}")

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

    return f"mssql+pyodbc:///?odbc_connect={params}"


def get_engine_remote():
    return create_engine_safe(build_connection_string("REMOTE"))


def get_engine_local():
    return create_engine_safe(build_connection_string("LOCAL"))

def main():
    remote_engine = get_engine_remote()
    local_engine = get_engine_local()

    logger.info("Connections established successfully")

    return remote_engine, local_engine

if __name__ == "__main__":
    remote_engine, local_engine = main()

    with remote_engine.begin() as conn:
        result = conn.execute(text("SELECT GETDATE()")).scalar()
        print("REMOTE:", result)

    with local_engine.begin() as conn:
        result = conn.execute(text("SELECT GETDATE()")).scalar()
        print("LOCAL:", result)
