from etl.utils import get_db_connection
from etl.extract import extract_table
from etl.extract import extract_from_sql_file
from etl.transform import clean_mart_tables
from etl.load import load_dataframe_to_sql
import pandas as pd


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

    # Eliminar columnas ID (IDENTITY) antes de insertar para que SQL Server las genere automáticamente
    df_cliente_clean = df_cliente.drop(columns=['idcliente'])
    df_inmueble_clean = df_inmueble.drop(columns=['idinmueble'])  
    df_empleado_clean = df_empleado.drop(columns=['idempleado'])
    df_proyecto_clean = df_proyecto.drop(columns=['idproyecto'])
    
    # Para dimensiones calculadas, también eliminar IDs si existen
    if 'idformapago' in df_formapago.columns:
        df_formapago_clean = df_formapago.drop(columns=['idformapago'])
    else:
        df_formapago_clean = df_formapago
        
    if 'idtiempo' in df_tiempo.columns:
        df_tiempo_clean = df_tiempo.drop(columns=['idtiempo'])
    else:
        df_tiempo_clean = df_tiempo

    load_dataframe_to_sql(df_cliente_clean, "Cliente", mart_engine)
    load_dataframe_to_sql(df_inmueble_clean, "Inmueble", mart_engine)
    load_dataframe_to_sql(df_empleado_clean, "Empleado", mart_engine)
    load_dataframe_to_sql(df_proyecto_clean, "Proyecto", mart_engine)
    load_dataframe_to_sql(df_formapago_clean, "FormaPago", mart_engine)
    load_dataframe_to_sql(df_tiempo_clean, "Tiempo", mart_engine)

    # --- Creación de mapas de IDs desde el Mart ---
    print("Creando mapas de IDs desde el datamart...")
    map_cliente = pd.read_sql("SELECT idcliente, nombrecliente, apellidocliente FROM DIM.Cliente", mart_engine)
    map_empleado = pd.read_sql("SELECT idempleado, nombreempleado, apellidoempleado FROM DIM.Empleado", mart_engine)
    map_proyecto = pd.read_sql("SELECT idproyecto, nombreproyecto FROM DIM.Proyecto", mart_engine)
    map_inmueble = pd.read_sql("SELECT idinmueble, direccion FROM DIM.Inmueble", mart_engine) # Usando direccion como clave de negocio
    map_formapago = pd.read_sql("SELECT idformapago, formapago FROM DIM.FormaPago", mart_engine)
    map_tiempo = pd.read_sql("SELECT idtiempo, fechaventa FROM DIM.Tiempo", mart_engine)

    # --- Limpieza de mapas para evitar productos cartesianos ---
    map_formapago = map_formapago.drop_duplicates(subset=['formapago'])
    map_tiempo = map_tiempo.drop_duplicates(subset=['fechaventa'])

    # Extracción de Hechos Venta usando query SQL
    print("Extrayendo Hechos_Venta...")
    df_hechos_venta = extract_from_sql_file(engine, "hechos_venta.sql")
    print("Primeros registros de Hechos_Venta listos para el mart (query SQL):")
    print(df_hechos_venta.head())

    # Para la tabla de hechos, eliminar IDs que serán foreign keys generados automáticamente
    # Solo mantener los valores originales de OLTP que se usarán para hacer el mapeo después
    hechos_columns_to_keep = ['fecha', 'formapago', 'idempleado', 'idproyecto', 'idinmueble', 'idcliente', 'montototal', 'montocosto', 'montoganancia']
    if 'presupuesto' in df_hechos_venta.columns:
        hechos_columns_to_keep.append('presupuesto')
    
    # Agregar columnas necesarias para el mapeo
    business_key_columns = ['nombrecliente', 'apellidocliente', 'nombreempleado', 'apellidoempleado', 'nombreproyecto', 'direccion']
    for col in business_key_columns:
        if col in df_hechos_venta.columns:
            hechos_columns_to_keep.append(col)
    
    df_hechos_venta_clean = df_hechos_venta[hechos_columns_to_keep]

    # --- Mapeo de IDs para la tabla de hechos ---
    print("Mapeando IDs de OLTP a IDs del datamart para la tabla de hechos...")
    
    # Directamente usar los datos de hechos con las claves de negocio ya incluidas
    df_hechos_to_map = df_hechos_venta_clean.copy()

    # 2. Mapear usando las claves de negocio y los mapas de IDs del mart
    df_hechos_mapped = df_hechos_to_map.merge(map_cliente, on=['nombrecliente', 'apellidocliente'], how='left')
    df_hechos_mapped = df_hechos_mapped.merge(map_empleado, on=['nombreempleado', 'apellidoempleado'], how='left')
    df_hechos_mapped = df_hechos_mapped.merge(map_proyecto, on=['nombreproyecto'], how='left')
    df_hechos_mapped = df_hechos_mapped.merge(map_inmueble, on=['direccion'], how='left')
    df_hechos_mapped = df_hechos_mapped.merge(map_formapago, on='formapago', how='left')
    
    # Mapeo especial para tiempo por fecha
    df_hechos_mapped['fecha'] = pd.to_datetime(df_hechos_mapped['fecha'])
    map_tiempo['fechaventa'] = pd.to_datetime(map_tiempo['fechaventa'])
    df_hechos_mapped = df_hechos_mapped.merge(map_tiempo, left_on='fecha', right_on='fechaventa', how='left')

    # Debug: Mostrar las columnas disponibles después del merge
    print("Columnas disponibles después del mapeo:")
    print(df_hechos_mapped.columns.tolist())
    
    # 3. Seleccionar solo las columnas finales para la tabla de hechos
    # Usar los IDs del mart que fueron agregados por los merge
    hechos_final_columns = ['idtiempo', 'idempleado', 'idproyecto', 'idinmueble', 'idformapago', 'idcliente', 'montototal', 'montocosto', 'montoganancia']
    if 'presupuesto' in df_hechos_mapped.columns:
        hechos_final_columns.append('presupuesto')
    
    # Verificar que las columnas existen antes de seleccionarlas
    available_columns = [col for col in hechos_final_columns if col in df_hechos_mapped.columns]
    missing_columns = [col for col in hechos_final_columns if col not in df_hechos_mapped.columns]
    
    if missing_columns:
        print(f"Columnas faltantes: {missing_columns}")
        print("Intentando usar nombres alternativos...")
        
        # Mapear nombres alternativos si es necesario
        column_mapping = {}
        for col in missing_columns:
            if col + '_x' in df_hechos_mapped.columns:
                column_mapping[col + '_x'] = col
            elif col + '_y' in df_hechos_mapped.columns:
                column_mapping[col + '_y'] = col
        
        if column_mapping:
            df_hechos_mapped = df_hechos_mapped.rename(columns=column_mapping)
            available_columns = [col for col in hechos_final_columns if col in df_hechos_mapped.columns]
    
    df_hechos_venta_final = df_hechos_mapped[available_columns].dropna()
    
    # Eliminar duplicados basados en las claves primarias de la tabla de hechos
    df_hechos_venta_final = df_hechos_venta_final.drop_duplicates(subset=['idtiempo', 'idempleado', 'idproyecto', 'idinmueble', 'idformapago', 'idcliente'])

    # 4. Las columnas ya tienen los nombres correctos para la tabla de hechos del mart
    # No es necesario renombrar columnas
    
    print(f"Registros después del mapeo: {len(df_hechos_venta_final)}")
    print("Primeros registros mapeados:")
    print(df_hechos_venta_final.head())

    # --- Carga de hechos al mart ---
    print("Cargando hechos al mart...")
    load_dataframe_to_sql(df_hechos_venta_final, "Hechos_Venta", mart_engine)

    print("Extracción, transformación y carga finalizadas.")


if __name__ == "__main__":
    main()
