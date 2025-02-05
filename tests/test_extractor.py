import unittest
from unittest.mock import patch, MagicMock
import io
import boto3
import pandas as pd
from ftp.extractor import extract_info, process_files

class TestExtractor(unittest.TestCase):

    def test_extract_info_valid_filename(self):
        """Verifica que la función extract_info extrae correctamente xlink_name y timestamp"""
        file_name, xlink_name, timestamp = extract_info("ruta/sensorA_20250125123045.csv")
        self.assertEqual(file_name, "sensorA_20250125123045.csv")
        self.assertEqual(xlink_name, "sensorA")
        self.assertEqual(timestamp.strftime('%Y%m%d%H%M%S'), "20250125123045")

    def test_extract_info_invalid_filename(self):
        """Verifica que extract_info devuelve 'Unknown' si el formato no es válido"""
        file_name, xlink_name, timestamp = extract_info("ruta/archivo_invalido.csv")
        self.assertEqual(xlink_name, "Unknown")
        self.assertEqual(timestamp, "Unknown")

    @patch("ftp.extractor.connect_ftp")
    @patch("ftp.extractor.load_dataframes")
    @patch("ftp.extractor.s3_client.upload_fileobj")
    def test_process_files(self, mock_s3_upload, mock_load_dataframes, mock_connect_ftp):
        """Verifica que los archivos se procesan y se suben correctamente a S3"""
        # Mock FTP con archivos simulados
        mock_ftp = MagicMock()
        mock_ftp.nlst.return_value = ["sensorA_20250125123045.csv"]
        mock_connect_ftp.return_value = mock_ftp
        
        # Mock DataFrames
        mock_load_dataframes.return_value = (
            pd.DataFrame({"xlink_name": ["sensorA"], "codigo_estacion": ["E001"]}),
            pd.DataFrame({"codigo_estacion": ["E001"], "path_s3": ["s3://mi-bucket/estacion_1/"]})
        )
        
        process_files()

        # Validar que se subió un archivo a S3
        mock_s3_upload.assert_called_once()

if __name__ == "__main__":
    unittest.main()
