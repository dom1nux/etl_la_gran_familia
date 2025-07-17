# ETL La Gran Familia 🏢

Pipeline ETL completo y modular en Python para extraer, transformar y cargar datos desde una base OLTP SQL Server hacia un Data Mart dimensional. Este proyecto implementa un esquema estrella optimizado para análisis de ventas inmobiliarias.

## 🎯 Características Principales

- **Arquitectura Modular**: Separación clara entre extracción, transformación y carga
- **Esquema Dimensional**: Implementa modelo estrella con dimensiones y tabla de hechos
- **Mapeo Automático de IDs**: Convierte automáticamente IDs de OLTP a IDs del Data Mart
- **Gestión de Dependencias Moderna**: Utiliza `uv` para manejo rápido de entornos virtuales
- **Calculo de Métricas**: Computa costos, ganancias y presupuestos a nivel de proyecto
- **Validación de Datos**: Elimina duplicados y valida integridad referencial

## 📁 Estructura del Proyecto

```
etl_la_gran_familia/
├── etl/                    # Módulos ETL principales
│   ├── __init__.py
│   ├── extract.py         # Extracción de datos desde OLTP
│   ├── transform.py       # Transformaciones y limpieza
│   ├── load.py           # Carga hacia Data Mart
│   └── utils.py          # Utilidades de conexión
├── queries/               # Scripts SQL especializados
│   ├── dim_formapago.sql # Dimensión forma de pago
│   ├── dim_tiempo.sql    # Dimensión temporal
│   ├── hechos_venta.sql  # Tabla de hechos con métricas
│   └── select_clientes.sql
├── data/                  # Scripts de definición de BD
│   ├── GranFamiliaDB.sql    # Base OLTP
│   ├── GranFamiliaMart.sql  # Data Mart dimensional
│   └── GranFamilia.bacpac   # Backup de datos
├── tests/                 # Pruebas unitarias
│   ├── __init__.py
│   └── test_etl.py
├── main.py               # Orquestador principal ETL
├── pyproject.toml        # Configuración del proyecto
├── uv.lock              # Lock file de dependencias
└── README.md            # Este archivo
```

## 🛠️ Tecnologías Utilizadas

- **Python 3.13+**: Lenguaje principal
- **Pandas**: Manipulación y análisis de datos
- **SQLAlchemy**: ORM y manejo de conexiones
- **PyODBC**: Driver para SQL Server
- **uv**: Gestión moderna de entornos virtuales
- **SQL Server**: Base de datos OLTP y Data Mart
- **unittest**: Framework de pruebas

## 🚀 Instalación Rápida

### 1. Instalar uv (si no lo tienes)
```bash
# Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Clonar y configurar el proyecto
```bash
git clone <url-del-repositorio>
cd etl_la_gran_familia

# Crear entorno virtual e instalar dependencias
uv sync

# Configurar variables de entorno
cp .env.template .env
# Edita .env con tus credenciales de SQL Server
```

### 3. Configurar bases de datos
```sql
-- Ejecutar en SQL Server Management Studio
-- 1. Crear base OLTP
sqlcmd -S tu_servidor -i data/GranFamiliaDB.sql

-- 2. Crear Data Mart
sqlcmd -S tu_servidor -i data/GranFamiliaMart.sql

-- 3. (Opcional) Restaurar datos de ejemplo
-- Restaurar GranFamilia.bacpac usando SSMS
```

## 🎮 Uso del Sistema

### Ejecución Principal
```bash
# Ejecutar pipeline ETL completo
uv run main.py
```

### Comandos Útiles con uv
```bash
# Agregar nueva dependencia
uv add pandas sqlalchemy

# Eliminar dependencia
uv remove package_name

# Ejecutar comando específico
uv run python -m unittest tests.test_etl

# Verificar dependencias
uv tree
```

## 🔄 Flujo ETL Detallado

### Fase 1: Extracción (EXTRACT)
- Conexión a base OLTP SQL Server
- Extracción de tablas: Cliente, Inmueble, Empleado, Proyecto
- Generación de dimensiones calculadas: FormaPago, Tiempo
- Transformación de nombres de columnas

### Fase 2: Carga de Dimensiones (LOAD)
- Limpieza de tablas existentes (respetando FKs)
- Eliminación de IDs IDENTITY para regeneración automática
- Carga de todas las dimensiones al Data Mart

### Fase 3: Mapeo de IDs
- Creación de mapas ID_OLTP → ID_MART
- Eliminación de duplicados en dimensiones
- Preparación para mapeo de tabla de hechos

### Fase 4: Procesamiento de Hechos
- Extracción usando query compleja con métricas calculadas
- Cálculo de costos, ganancias y presupuestos por proyecto
- Inclusión de claves de negocio para mapeo

### Fase 5: Mapeo Dimensional
- Conversión de IDs OLTP a IDs del Data Mart
- Mapeo secuencial usando claves de negocio
- Manejo especial de dimensión temporal

### Fase 6: Validación y Limpieza
- Eliminación de registros incompletos
- Remoción de duplicados por clave primaria compuesta
- Validación de integridad referencial

### Fase 7: Carga Final
- Inserción de tabla de hechos en Data Mart
- Validación de carga exitosa

## 📊 Esquema del Data Mart

### Dimensiones
- **DIM.Cliente**: Información de clientes
- **DIM.Empleado**: Datos de empleados/vendedores
- **DIM.Proyecto**: Detalles de proyectos inmobiliarios
- **DIM.Inmueble**: Propiedades y características
- **DIM.FormaPago**: Métodos de pago
- **DIM.Tiempo**: Dimensión temporal con jerarquías

### Tabla de Hechos
- **DIM.Hechos_Venta**: Transacciones con métricas calculadas
  - Montos: total, costo, ganancia
  - Presupuestos por proyecto
  - Referencias a todas las dimensiones

## 🧪 Pruebas

### Ejecutar todas las pruebas
```bash
uv run -m unittest discover tests
```

### Pruebas específicas
```bash
# Prueba de conexión
uv run -m unittest tests.test_etl.TestETL.test_connection

# Prueba de extracción
uv run -m unittest tests.test_etl.TestETL.test_extract_cliente

# Prueba de transformación
uv run -m unittest tests.test_etl.TestETL.test_transform_data
```

## 📈 Métricas y KPIs

El sistema calcula automáticamente:
- **Costo por Proyecto**: Suma de costos hasta fecha de venta
- **Ganancia por Venta**: Monto total - costos del proyecto
- **Presupuesto vs Ejecutado**: Comparación presupuestal
- **Análisis Temporal**: Ventas por período

## 🛡️ Consideraciones de Seguridad

- Variables de entorno para credenciales
- Conexiones seguras con SQL Server
- Validación de datos de entrada
- Manejo de errores robusto

## 🔧 Configuración Avanzada

### Variables de Entorno (.env)
```env
# Base OLTP
DB_SERVER=tu_servidor
DB_DATABASE=GranFamilia
DB_USERNAME=tu_usuario
DB_PASSWORD=tu_password

# Data Mart
MART_SERVER=tu_servidor
MART_DATABASE=GranFamiliaMart
MART_USERNAME=tu_usuario
MART_PASSWORD=tu_password
```

### Personalización del Pipeline
El sistema es altamente configurable:
- Agregar nuevas dimensiones editando `main.py`
- Crear queries personalizadas en `queries/`
- Implementar transformaciones en `etl/transform.py`
- Extender validaciones en `etl/load.py`

## 🐛 Solución de Problemas

### Errores Comunes
1. **Error de conexión**: Verificar credenciales en `.env`
2. **Duplicados en hechos**: El sistema elimina automáticamente duplicados
3. **Columnas faltantes**: Verificar estructura de bases de datos
4. **Mapeo de IDs**: Asegurar que dimensiones se carguen primero

### Logs y Debugging
El sistema incluye mensajes informativos detallados:
- Conteos de registros por fase
- Validación de columnas disponibles
- Estadísticas de mapeo y carga

## 📜 Licencia

Este proyecto está bajo la Licencia AGPL v3. Ver archivo `LICENSE` para más detalles.

---

**Nota**: Este proyecto es parte de un ejercicio académico para demostrar implementación de pipelines ETL con Python y SQL Server.
