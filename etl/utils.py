# Funciones utilitarias y helpers para el ETL
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_db_connection():
    """
    Crea y retorna un SQLAlchemy engine para conectarse a SQL Server usando parámetros del .env
    """
    server = os.getenv("DB_SERVER").strip('"')
    db = os.getenv("DB_NAME").strip('"')
    user = os.getenv("DB_USER").strip('"')
    password = os.getenv("DB_PASSWORD").strip('"')
    driver = os.getenv("DB_DRIVER").strip('"')
    connection_string = (
        f"mssql+pyodbc://{user}:{password}@{server}/{db}?driver={driver.replace(' ', '+')}"
    )
    engine = create_engine(connection_string)
    return engine
