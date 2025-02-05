import sys
import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from data.database import connect_db, load_dataframes

class TestDatabase(unittest.TestCase):

    @patch("data.database.pymysql.connect")
    def test_connect_db(self, mock_connect):
        """Verifica que la conexión a la base de datos se establece correctamente"""
        mock_connect.return_value = MagicMock()
        conn = connect_db()
        self.assertIsNotNone(conn)

    @patch("data.database.pd.read_sql")
    @patch("data.database.pymysql.connect")
    def test_load_dataframes(self, mock_connect, mock_read_sql):
        """Verifica que las tablas se cargan correctamente en DataFrames"""
        mock_connect.return_value = MagicMock()
        mock_read_sql.side_effect = [
            pd.DataFrame({"id_reg": [1], "xlink_name": ["sensor_A"], "normalized_name": ["sensor_a"], "codigo_estacion": ["E001"]}),
            pd.DataFrame({"codigo_estacion": ["E001"], "path_s3": ["s3://mi-bucket/estacion_1/"]})
        ]
        
        df_diccionario, df_s3_paths = load_dataframes()
        self.assertFalse(df_diccionario.empty)
        self.assertFalse(df_s3_paths.empty)
        self.assertEqual(df_diccionario.iloc[0]["codigo_estacion"], "E001")
        self.assertEqual(df_s3_paths.iloc[0]["path_s3"], "s3://mi-bucket/estacion_1/")

if __name__ == "__main__":
    unittest.main()
