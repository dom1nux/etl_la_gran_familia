# Este módulo contendrá funciones para extraer datos de diversas fuentes

import pandas as pd
import os

def extract_table(engine, table_name, columns=None, where=None):
    """
    Extrae datos de una tabla específica.
    columns: lista de columnas a extraer (None para todas)
    where: condición opcional (string SQL)
    """
    cols = ', '.join(columns) if columns else '*'
    query = f"SELECT {cols} FROM {table_name}"
    if where:
        query += f" WHERE {where}"
    return pd.read_sql(query, engine)

def extract_from_sql_file(engine, sql_filename):
    """
    Ejecuta un query SQL almacenado en un archivo dentro de la carpeta 'queries'.
    """
    queries_dir = os.path.join(os.path.dirname(__file__), '..', 'queries')
    sql_path = os.path.join(queries_dir, sql_filename)
    with open(sql_path, 'r', encoding='utf-8') as f:
        query = f.read()
    return pd.read_sql(query, engine)
