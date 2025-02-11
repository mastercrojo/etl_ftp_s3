from sqlalchemy import create_engine
import pandas as pd
from config.settings import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
from sqlalchemy.orm import sessionmaker
from models.model import Diccionario, S3PathsStations, FTPArchivosProcesados
import logging
from datetime import datetime

def connect_db():
    """Conectar a la base de datos MySQL con SQLAlchemy"""
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return engine, Session

def load_dataframes(session):
    """Carga las tablas diccionario y s3_paths_stations en DataFrames usando SQLAlchemy ORM"""
    # engine, Session = connect_db()  # Conectar a la base de datos
    # session = Session()

    try:
        # Consultar datos de las tablas usando los modelos ORM
        diccionario_data = session.query(Diccionario).all()
        s3_paths_data = session.query(S3PathsStations).all()

        # Convertir los resultados en DataFrames de pandas
        df_diccionario = pd.DataFrame(
            [
                {
                    "id_reg": row.id_reg,
                    "xlink_name": row.xlink_name,
                    "normalized_name": row.normalized_name,
                    "codigo_estacion": row.codigo_estacion,
                    "timestamp_create": row.timestamp_create,
                }
                for row in diccionario_data
            ]
        )

        df_s3_paths = pd.DataFrame(
            [
                {
                    "codigo_estacion": row.codigo_estacion,
                    "path_s3": row.path_s3,
                    "timestamp_create": row.timestamp_create,
                }
                for row in s3_paths_data
            ]
        )

        return df_diccionario, df_s3_paths
    except Exception as e:
        logging.error(f"Error al cargar las tablas en DataFrames: {str(e)}")
        raise e
    finally:
        session.close()


def get_archivos_procesados(session):
    """Obtener nombres de archivos ya procesados desde la base de datos"""
    archivos = session.query(FTPArchivosProcesados.nombre_archivo).all()
    return set(archivo[0] for archivo in archivos)


def register_archivo_procesado(session, nombre_archivo, codigo_estacion, fecha_archivo):
    """Registrar un archivo procesado en la base de datos"""
    try:
        nuevo_archivo = FTPArchivosProcesados(
            nombre_archivo=nombre_archivo,
            fecha_archivo=fecha_archivo,
            codigo_estacion=codigo_estacion,
            timestamp_procesado=datetime.now(),
        )
        session.add(nuevo_archivo)
        session.commit()
        logging.info(f"Archivo registrado en la base de datos: {fecha_archivo} {nombre_archivo} ({codigo_estacion})")
        return(True)
    except Exception as e:
        session.rollback()
        logging.error(f"Error al registrar archivo en la base de datos: {nombre_archivo}. Detalles: {str(e)}")
        raise e
        