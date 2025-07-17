# Este módulo contendrá funciones para transformar los datos extraídos

from sqlalchemy import text

def clean_mart_tables(engine):
    """
    Elimina los datos existentes en las tablas del mart respetando el orden de foreign keys.
    Primero elimina hechos, luego dimensiones.
    """
    print("Eliminando datos existentes en el mart (respetando foreign keys)...")
    with engine.connect() as conn:
        # Primero eliminar hechos (tablas que referencian a otras)
        conn.execute(text("DELETE FROM DIM.Hechos_Venta"))
        
        # Luego eliminar dimensiones (en orden que respete las FK entre dimensiones)
        conn.execute(text("DELETE FROM DIM.Tiempo"))
        conn.execute(text("DELETE FROM DIM.FormaPago"))
        conn.execute(text("DELETE FROM DIM.Inmueble"))  # Referencia a Proyecto
        conn.execute(text("DELETE FROM DIM.Proyecto"))
        conn.execute(text("DELETE FROM DIM.Empleado"))
        conn.execute(text("DELETE FROM DIM.Cliente"))
        
        # Reiniciar los contadores de IDENTITY si existen
        try:
            conn.execute(text("DBCC CHECKIDENT ('DIM.Cliente', RESEED, 0)"))
            conn.execute(text("DBCC CHECKIDENT ('DIM.Empleado', RESEED, 0)"))
            conn.execute(text("DBCC CHECKIDENT ('DIM.Proyecto', RESEED, 0)"))
            conn.execute(text("DBCC CHECKIDENT ('DIM.Inmueble', RESEED, 0)"))
            conn.execute(text("DBCC CHECKIDENT ('DIM.FormaPago', RESEED, 0)"))
            conn.execute(text("DBCC CHECKIDENT ('DIM.Tiempo', RESEED, 0)"))
        except Exception as e:
            print(f"Nota: No se pudieron reiniciar algunos contadores IDENTITY: {e}")
        
        conn.commit()
    print("Limpieza completada.")
