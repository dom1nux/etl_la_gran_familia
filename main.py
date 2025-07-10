from etl.utils import get_db_connection
from etl.extract import extract_table
from etl.extract import extract_from_sql_file
from etl.transform import clean_mart_tables
from etl.load import load_dataframe_to_sql


def main():
    # Ejemplo de flujo ETL
    print("Iniciando flujo ETL...")
    # 1. Extraer datos de la tabla Cliente
    engine = get_db_connection("DB_")

    # Extracción de Cliente
    df_cliente = extract_table(
        engine,
        "Cliente",
        columns=["ClienteID", "Nombre", "Apellido", "Contacto", "Email"],
    )
    print("Primeros registros extraídos de Cliente:")
    print(df_cliente.head())

    # Renombrar columnas para coincidir con DIM_Cliente
    df_cliente = df_cliente.rename(
        columns={
            "ClienteID": "idcliente",
            "Nombre": "nombrecliente",
            "Apellido": "apellidocliente",
            "Contacto": "contacto",
            "Email": "email",
        }
    )
    print("Primeros registros de Cliente listos para el mart:")
    print(df_cliente.head())

    # Extracción de Inmueble
    df_inmueble = extract_table(
        engine,
        "Inmueble",
        columns=[
            "InmuebleID",
            "Tipo",
            "Direccion",
            "Area",
            "Estado",
            "Precio",
            "ProyectoID",
        ],
    )
    print("Primeros registros extraídos de Inmueble:")
    print(df_inmueble.head())

    # Renombrar columnas para coincidir con DIM_Inmueble
    df_inmueble = df_inmueble.rename(
        columns={
            "InmuebleID": "idinmueble",
            "Tipo": "tipoinmueble",
            "Direccion": "direccion",
            "Area": "area",
            "Estado": "estadoinmueble",
            "Precio": "precio",
            "ProyectoID": "idproyecto",
        }
    )
    print("Primeros registros de Inmueble listos para el mart:")
    print(df_inmueble.head())

    # Extracción de Empleado
    df_empleado = extract_table(
        engine, "Empleado", columns=["EmpleadoID", "Nombre", "Cargo", "Apellido"]
    )
    print("Primeros registros extraídos de Empleado:")
    print(df_empleado.head())

    # Renombrar columnas para coincidir con DIM_Empleado
    df_empleado = df_empleado.rename(
        columns={
            "EmpleadoID": "idempleado",
            "Nombre": "nombreempleado",
            "Cargo": "cargo",
            "Apellido": "apellidoempleado",
        }
    )
    print("Primeros registros de Empleado listos para el mart:")
    print(df_empleado.head())

    # Extracción de Proyecto para futura carga en DIM_Proyecto
    df_proyecto = extract_table(
        engine,
        "Proyecto",
        columns=[
            "ProyectoID",
            "Nombre",
            "Tipo",
            "Ubicacion",
            "Estado",
            "FechaInicio",
            "FechaFinEstimada",
            "Presupuesto",
        ],
    )
    print("Primeros registros extraídos de Proyecto:")
    print(df_proyecto.head())

    # Renombrar columnas para coincidir con DIM_Proyecto
    df_proyecto = df_proyecto.rename(
        columns={
            "ProyectoID": "idproyecto",
            "Nombre": "nombreproyecto",
            "Tipo": "tipoproyecto",
            "Ubicacion": "ubicacion",
            "Estado": "estadoproyecto",
            "FechaInicio": "fechainicio",
            "FechaFinEstimada": "fechafinestimada",
            "Presupuesto": "presupuesto",
        }
    )
    print("Primeros registros de Proyecto listos para el mart:")
    print(df_proyecto.head())

    # Extracción de Forma de Pago para DIM_FormaPago usando query SQL
    df_formapago = extract_from_sql_file(engine, "dim_formapago.sql")
    print("Primeros registros de FormaPago listos para el mart:")
    print(df_formapago.head())
    print("Columnas de df_formapago:", df_formapago.columns.tolist())

    # Extracción de Tiempo para DIM_Tiempo usando query SQL
    df_tiempo = extract_from_sql_file(engine, "dim_tiempo.sql")
    print("Primeros registros de Tiempo listos para el mart:")
    print(df_tiempo.head())

    # --- Carga de dimensiones al mart ---
    mart_engine = get_db_connection("MART_")
    print("Cargando dimensiones al mart...")

    # --- Limpieza de tablas en el mart antes de la carga ---
    clean_mart_tables(mart_engine)

    load_dataframe_to_sql(df_cliente, "DIM_Cliente", mart_engine)
    load_dataframe_to_sql(df_inmueble, "DIM_Inmueble", mart_engine)
    load_dataframe_to_sql(df_empleado, "DIM_Empleado", mart_engine)
    load_dataframe_to_sql(df_proyecto, "DIM_Proyecto", mart_engine)
    load_dataframe_to_sql(df_formapago, "DIM_FormaPago", mart_engine)
    load_dataframe_to_sql(df_tiempo, "DIM_Tiempo", mart_engine)

    # Extracción de Hechos Venta usando query SQL
    print("Extrayendo Hechos_Venta...")
    df_hechos_venta = extract_from_sql_file(engine, "hechos_venta.sql")
    print("Primeros registros de Hechos_Venta listos para el mart (query SQL):")
    print(df_hechos_venta.head())

    # --- Carga de hechos al mart ---
    print("Cargando hechos al mart...")
    load_dataframe_to_sql(df_hechos_venta, "Hechos_Venta", mart_engine)

    print("Extracción, transformación y carga finalizadas.")


if __name__ == "__main__":
    main()
