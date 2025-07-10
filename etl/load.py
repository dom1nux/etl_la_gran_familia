# Este módulo contendrá funciones para cargar los datos transformados a la base de datos
import pandas as pd

def load_dataframe_to_sql(df: pd.DataFrame, table_name: str, engine, if_exists: str = 'replace'):
    """
    Carga un DataFrame a una tabla SQL Server usando SQLAlchemy.
    Args:
        df: DataFrame de pandas a cargar.
        table_name: Nombre de la tabla destino en la base de datos.
        engine: SQLAlchemy engine de la base de datos destino.
        if_exists: 'append' para agregar, 'replace' para reemplazar la tabla.
    """
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
