import pymysql
import pandas as pd
from config.settings import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME

def connect_db():
    """Conectar a la base de datos MySQL"""
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)

def load_dataframes():
    """Carga las tablas diccionario y s3_paths_stations en DataFrames"""
    connection = connect_db()
    try:
        df_diccionario = pd.read_sql("SELECT * FROM diccionario", connection)
        df_s3_paths = pd.read_sql("SELECT * FROM s3_paths_stations", connection)
        return df_diccionario, df_s3_paths
    finally:
        connection.close()
