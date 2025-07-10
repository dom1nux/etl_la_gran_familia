# Pruebas unitarias para el flujo ETL
import unittest
from etl.utils import get_db_connection
from etl.extract import extract_table, extract_from_sql_file
from sqlalchemy import text

class TestDBConnection(unittest.TestCase):
    def test_source_connection(self):
        """
        Prueba que se puede establecer una conexión a la base de datos fuente (GranFamilia).
        """
        engine = get_db_connection("DB_")
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 AS test"))
                value = result.scalar()
                self.assertEqual(value, 1)
        except Exception as e:
            self.fail(f"No se pudo conectar a la base de datos fuente: {e}")

    def test_mart_connection(self):
        """
        Prueba que se puede establecer una conexión a la base de datos mart (MartFamilia).
        """
        engine = get_db_connection("MART_")
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 AS test"))
                value = result.scalar()
                self.assertEqual(value, 1)
        except Exception as e:
            self.fail(f"No se pudo conectar a la base de datos mart: {e}")

class TestExtract(unittest.TestCase):
    def test_extract_table(self):
        """
        Prueba la extracción de datos de una tabla simple.
        """
        engine = get_db_connection("DB_")
        df = extract_table(engine, "Cliente", columns=["ClienteID", "Nombre"])
        self.assertIsNotNone(df)
        self.assertIn("ClienteID", df.columns)
        self.assertIn("Nombre", df.columns)

    def test_extract_from_sql_file(self):
        """
        Prueba la extracción de datos usando un archivo SQL.
        """
        engine = get_db_connection("DB_")
        # Debe existir queries/select_clientes.sql con un SELECT válido
        df = extract_from_sql_file(engine, "select_clientes.sql")
        self.assertIsNotNone(df)
        self.assertIn("ClienteID", df.columns)
        self.assertIn("Nombre", df.columns)

if __name__ == "__main__":
    unittest.main()
