# Este módulo contendrá funciones para transformar los datos extraídos

from sqlalchemy import text

def clean_mart_tables(engine):
    """
    Elimina los datos existentes en las tablas del mart para facilitar la recarga.
    """
    print("Eliminando datos existentes en el mart...")
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM DIM.Hechos_Venta"))
        conn.execute(text("DELETE FROM DIM.Tiempo"))
        conn.execute(text("DELETE FROM DIM.FormaPago"))
        conn.execute(text("DELETE FROM DIM.Proyecto"))
        conn.execute(text("DELETE FROM DIM.Empleado"))
        conn.execute(text("DELETE FROM DIM.Inmueble"))
        conn.execute(text("DELETE FROM DIM.Cliente"))
        conn.commit()
