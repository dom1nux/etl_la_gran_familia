# Funciones utilitarias y helpers para el ETL
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

def get_db_connection(db_env_prefix="DB_"):
    """
    Crea y retorna un SQLAlchemy engine para conectarse a SQL Server usando parámetros del .env
    db_env_prefix: prefijo de las variables de entorno (por defecto 'DB_')
    Lanza ValueError si falta alguna variable necesaria.
    Lanza RuntimeError con mensaje claro si falla la conexión.
    """
    required_vars = ["SERVER", "NAME", "USER", "PASSWORD", "DRIVER"]
    config = {}
    for var in required_vars:
        env_var = f"{db_env_prefix}{var}"
        value = os.getenv(env_var)
        if value is None:
            raise ValueError(f"Falta la variable de entorno: {env_var}")
        config[var.lower()] = value.strip('"')
    connection_string = f"mssql+pyodbc://{config['user']}:{config['password']}@{config['server']}/{config['name']}?driver={config['driver'].replace(' ', '+')}"
    try:
        engine = create_engine(connection_string)
        # Probar conexión inmediatamente para dar feedback temprano
        with engine.connect() as conn:
            pass
        return engine
    except SQLAlchemyError as e:
        raise RuntimeError(
            f"No se pudo conectar a la base de datos '{config['name']}' en el servidor '{config['server']}' con el usuario '{config['user']}'.\n"
            f"Error original: {e}\n"
            "Verifica que el nombre de la base de datos, usuario, contraseña y permisos sean correctos."
        )
