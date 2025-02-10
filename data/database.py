from sqlalchemy import create_engine
import pandas as pd
from config.settings import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME

def connect_db():
    """Conectar a la base de datos MySQL con SQLAlchemy"""
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(db_url)
    return engine

def load_dataframes():
    """Carga las tablas diccionario y s3_paths_stations en DataFrames usando SQLAlchemy"""
    engine = connect_db()
    with engine.connect() as connection:
        df_diccionario = pd.read_sql("SELECT * FROM diccionario", connection)
        df_s3_paths = pd.read_sql("SELECT * FROM s3_paths_stations", connection)
    return df_diccionario, df_s3_paths
