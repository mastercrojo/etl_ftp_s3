import os
from dotenv import load_dotenv

# Cargar variables desde el archivo .env
load_dotenv()

# Configuración de la base de datos
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Configuración del FTP
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASSWORD = os.getenv("FTP_PASSWORD")
FTP_DIR = os.getenv("FTP_DIR", "/")  # Directorio raíz por defecto

# Configuración de AWS S3
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
