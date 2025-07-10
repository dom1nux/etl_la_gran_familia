# ETL La Gran Familia

Este proyecto implementa un pipeline ETL robusto y modular en Python para extraer, transformar y cargar datos desde una base OLTP SQL Server (GranFamilia) hacia un data mart (GranFamiliaMart).

## Estructura del Proyecto

- `etl/` — Módulos de extracción, transformación, carga y utilidades.
- `queries/` — Scripts SQL para extracción y transformación.
- `data/` — Scripts de definición de base de datos.
- `tests/` — Pruebas unitarias.
- `main.py` — Orquestador principal del ETL.
- `.env` y `.env.template` — Parámetros de conexión a bases de datos.

## Requisitos

- Python 3.9+
- SQL Server (OLTP y Data Mart)
- `uv` para gestión de entornos virtuales y dependencias

## Instalación

1. **Instala uv** (si no lo tienes):
   ```sh
   curl -LsSf https://astral.sh/uv/install.sh | sh
   # O en Windows:
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

2. **Clona el repositorio**:
   ```sh
   git clone <url-del-repositorio>
   cd etl_la_gran_familia
   ```

3. **Configura el entorno virtual y las dependencias** con uv:
   ```sh
   uv sync
   ```

4. **Configura las variables de entorno**:
   ```sh
   cp .env.template .env
   # Edita .env con los datos de conexión a tus bases de datos
   ```

## Gestión de Dependencias con uv

Este proyecto utiliza `uv` para la gestión moderna de entornos virtuales y dependencias de Python:

- **Archivo `pyproject.toml`**: Define las dependencias del proyecto y metadatos.
- **Entorno virtual automático**: `uv` crea y gestiona automáticamente el entorno virtual.
- **Resolución rápida**: Instalación y resolución de dependencias más rápida que pip.
- **Lock file**: Garantiza reproducibilidad con versiones exactas de dependencias.

Comandos útiles de uv:
```sh
uv add <paquete>           # Agregar nueva dependencia
uv remove <paquete>        # Eliminar dependencia
uv sync                    # Sincronizar dependencias
uv run <comando>           # Ejecutar comando en el entorno virtual
```

## Uso

1. **Activa el entorno virtual** (si no está activo):
   ```sh
   uv run
   ```

2. **Asegúrate de que las bases de datos estén accesibles** y los scripts de definición ejecutados.

3. **Ejecuta el pipeline ETL**:
    ```sh
   uv run python main.py
    ```

4. **El proceso**:
   - Extrae datos de tablas y vistas OLTP usando SQL y pandas.
   - Realiza transformaciones y renombrado de columnas para el data mart.
   - Limpia las tablas destino antes de cargar.
   - Carga dimensiones y hechos en el data mart.
   - Muestra ejemplos de los datos cargados por consola.

## Pruebas

Las pruebas unitarias están implementadas con `unittest` y pueden ejecutarse con:
```sh
uv run -m unittest discover tests
```

Para ejecutar una prueba específica:
```sh
uv run -m unittest tests.test_etl.TestETL.test_connection
```

## Notas
- El cálculo de montocosto y montoganancia se realiza a nivel de proyecto, según la estructura de los datos.
- Los datos de ejemplo pueden arrojar resultados negativos si no reflejan la lógica real del negocio.
- El pipeline es modular y fácilmente extensible para nuevas fuentes, transformaciones o destinos.

## Contacto
Para dudas o mejoras, abre un issue o contacta al autor.
