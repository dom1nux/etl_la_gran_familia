from etl.utils import get_db_connection
from etl.extract import extract_table
from etl.extract import extract_from_sql_file
from etl.transform import clean_mart_tables
from etl.load import load_dataframe_to_sql
import pandas as pd


def main():
    """
    Proceso ETL completo para cargar datos desde la base OLTP hacia el Data Mart.
    
    Flujo:
    1. EXTRACT: Extrae datos de las tablas transaccionales (OLTP)
    2. TRANSFORM: Transforma y mapea los datos para el esquema dimensional
    3. LOAD: Carga las dimensiones y tabla de hechos al Data Mart
    """
    print("Iniciando flujo ETL...")
    
    # ===== FASE 1: CONEXIÓN Y EXTRACCIÓN DE DATOS OLTP =====
    engine = get_db_connection("DB_")

    # --- Extracción de dimensiones desde tablas OLTP ---
    
    # Extracción de Cliente
    df_cliente = extract_table(
        engine,
        "Cliente",
        columns=["ClienteID", "Nombre", "Apellido", "Contacto", "Email"],
    )
    print("Primeros registros extraídos de Cliente:")
    print(df_cliente.head())

    # Transformación: Renombrar columnas para coincidir con esquema dimensional
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

    # Transformación: Renombrar columnas para esquema dimensional
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

    # Transformación: Renombrar columnas para esquema dimensional
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

    # Extracción de Proyecto
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

    # Transformación: Renombrar columnas para esquema dimensional
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

    # --- Extracción de dimensiones calculadas mediante queries SQL ---
    
    # Extracción de Forma de Pago: Datos únicos extraídos de transacciones
    df_formapago = extract_from_sql_file(engine, "dim_formapago.sql")
    print("Primeros registros de FormaPago listos para el mart:")
    print(df_formapago.head())
    print("Columnas de df_formapago:", df_formapago.columns.tolist())

    # Extracción de Tiempo: Dimensión temporal basada en fechas de ventas
    df_tiempo = extract_from_sql_file(engine, "dim_tiempo.sql")
    print("Primeros registros de Tiempo listos para el mart:")
    print(df_tiempo.head())

    # ===== FASE 2: CARGA DE DIMENSIONES AL DATA MART =====
    mart_engine = get_db_connection("MART_")
    print("Cargando dimensiones al mart...")

    # Limpieza previa: Eliminar datos existentes respetando foreign keys
    clean_mart_tables(mart_engine)

    # Preparación de datos: Eliminar IDs IDENTITY para regeneración automática
    df_cliente_clean = df_cliente.drop(columns=['idcliente'])
    df_inmueble_clean = df_inmueble.drop(columns=['idinmueble'])  
    df_empleado_clean = df_empleado.drop(columns=['idempleado'])
    df_proyecto_clean = df_proyecto.drop(columns=['idproyecto'])
    
    # Limpieza condicional para dimensiones calculadas
    if 'idformapago' in df_formapago.columns:
        df_formapago_clean = df_formapago.drop(columns=['idformapago'])
    else:
        df_formapago_clean = df_formapago
        
    if 'idtiempo' in df_tiempo.columns:
        df_tiempo_clean = df_tiempo.drop(columns=['idtiempo'])
    else:
        df_tiempo_clean = df_tiempo

    # Carga de todas las dimensiones al mart
    load_dataframe_to_sql(df_cliente_clean, "Cliente", mart_engine)
    load_dataframe_to_sql(df_inmueble_clean, "Inmueble", mart_engine)
    load_dataframe_to_sql(df_empleado_clean, "Empleado", mart_engine)
    load_dataframe_to_sql(df_proyecto_clean, "Proyecto", mart_engine)
    load_dataframe_to_sql(df_formapago_clean, "FormaPago", mart_engine)
    load_dataframe_to_sql(df_tiempo_clean, "Tiempo", mart_engine)

    # ===== FASE 3: MAPEO DE IDs PARA TABLA DE HECHOS =====
    
    # Creación de mapas: Obtener IDs generados automáticamente del mart
    print("Creando mapas de IDs desde el datamart...")
    map_cliente = pd.read_sql("SELECT idcliente, nombrecliente, apellidocliente FROM DIM.Cliente", mart_engine)
    map_empleado = pd.read_sql("SELECT idempleado, nombreempleado, apellidoempleado FROM DIM.Empleado", mart_engine)
    map_proyecto = pd.read_sql("SELECT idproyecto, nombreproyecto FROM DIM.Proyecto", mart_engine)
    map_inmueble = pd.read_sql("SELECT idinmueble, direccion FROM DIM.Inmueble", mart_engine) # Direccion como clave de negocio
    map_formapago = pd.read_sql("SELECT idformapago, formapago FROM DIM.FormaPago", mart_engine)
    map_tiempo = pd.read_sql("SELECT idtiempo, fechaventa FROM DIM.Tiempo", mart_engine)

    # Limpieza de duplicados en dimensiones para evitar productos cartesianos
    map_formapago = map_formapago.drop_duplicates(subset=['formapago'])
    map_tiempo = map_tiempo.drop_duplicates(subset=['fechaventa'])

    # ===== FASE 4: EXTRACCIÓN Y TRANSFORMACIÓN DE HECHOS =====
    
    # Extracción de hechos: Query compleja que calcula métricas de negocio
    print("Extrayendo Hechos_Venta...")
    df_hechos_venta = extract_from_sql_file(engine, "hechos_venta.sql")
    print("Primeros registros de Hechos_Venta listos para el mart (query SQL):")
    print(df_hechos_venta.head())

    # Preparación de datos para mapeo: Seleccionar columnas relevantes
    hechos_columns_to_keep = ['fecha', 'formapago', 'idempleado', 'idproyecto', 'idinmueble', 'idcliente', 'montototal', 'montocosto', 'montoganancia']
    if 'presupuesto' in df_hechos_venta.columns:
        hechos_columns_to_keep.append('presupuesto')
    
    # Incluir claves de negocio necesarias para el mapeo de IDs
    business_key_columns = ['nombrecliente', 'apellidocliente', 'nombreempleado', 'apellidoempleado', 'nombreproyecto', 'direccion']
    for col in business_key_columns:
        if col in df_hechos_venta.columns:
            hechos_columns_to_keep.append(col)
    
    df_hechos_venta_clean = df_hechos_venta[hechos_columns_to_keep]

    # ===== FASE 5: MAPEO DE IDs OLTP A IDs DEL MART =====
    print("Mapeando IDs de OLTP a IDs del datamart para la tabla de hechos...")
    
    # Usar datos de hechos con claves de negocio incluidas
    df_hechos_to_map = df_hechos_venta_clean.copy()

    # Mapeo secuencial usando claves de negocio de cada dimensión
    df_hechos_mapped = df_hechos_to_map.merge(map_cliente, on=['nombrecliente', 'apellidocliente'], how='left')
    df_hechos_mapped = df_hechos_mapped.merge(map_empleado, on=['nombreempleado', 'apellidoempleado'], how='left')
    df_hechos_mapped = df_hechos_mapped.merge(map_proyecto, on=['nombreproyecto'], how='left')
    df_hechos_mapped = df_hechos_mapped.merge(map_inmueble, on=['direccion'], how='left')
    df_hechos_mapped = df_hechos_mapped.merge(map_formapago, on='formapago', how='left')
    
    # Mapeo especial para dimensión tiempo: Conversión de tipos de fecha
    df_hechos_mapped['fecha'] = pd.to_datetime(df_hechos_mapped['fecha'])
    map_tiempo['fechaventa'] = pd.to_datetime(map_tiempo['fechaventa'])
    df_hechos_mapped = df_hechos_mapped.merge(map_tiempo, left_on='fecha', right_on='fechaventa', how='left')

    # Debug: Verificar columnas disponibles después del mapeo
    print("Columnas disponibles después del mapeo:")
    print(df_hechos_mapped.columns.tolist())
    
    # ===== FASE 6: PREPARACIÓN FINAL DE DATOS PARA CARGA =====
    
    # Selección de columnas finales para la tabla de hechos
    hechos_final_columns = ['idtiempo', 'idempleado', 'idproyecto', 'idinmueble', 'idformapago', 'idcliente', 'montototal', 'montocosto', 'montoganancia']
    if 'presupuesto' in df_hechos_mapped.columns:
        hechos_final_columns.append('presupuesto')
    
    # Validación de columnas: Verificar existencia y manejar sufijos de pandas
    available_columns = [col for col in hechos_final_columns if col in df_hechos_mapped.columns]
    missing_columns = [col for col in hechos_final_columns if col not in df_hechos_mapped.columns]
    
    if missing_columns:
        print(f"Columnas faltantes: {missing_columns}")
        print("Intentando usar nombres alternativos...")
        
        # Mapeo automático de sufijos generados por pandas merge
        column_mapping = {}
        for col in missing_columns:
            if col + '_x' in df_hechos_mapped.columns:
                column_mapping[col + '_x'] = col
            elif col + '_y' in df_hechos_mapped.columns:
                column_mapping[col + '_y'] = col
        
        if column_mapping:
            df_hechos_mapped = df_hechos_mapped.rename(columns=column_mapping)
            available_columns = [col for col in hechos_final_columns if col in df_hechos_mapped.columns]
    
    # Preparación final: Eliminar registros incompletos y duplicados
    df_hechos_venta_final = df_hechos_mapped[available_columns].dropna()
    df_hechos_venta_final = df_hechos_venta_final.drop_duplicates(subset=['idtiempo', 'idempleado', 'idproyecto', 'idinmueble', 'idformapago', 'idcliente'])

    print(f"Registros después del mapeo: {len(df_hechos_venta_final)}")
    print("Primeros registros mapeados:")
    print(df_hechos_venta_final.head())

    # ===== FASE 7: CARGA FINAL DE HECHOS AL MART =====
    print("Cargando hechos al mart...")
    load_dataframe_to_sql(df_hechos_venta_final, "Hechos_Venta", mart_engine)

    print("Extracción, transformación y carga finalizadas.")


if __name__ == "__main__":
    main()
