from sqlalchemy import Column, Integer, String, Date, TIMESTAMP, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FTPArchivosProcesados(Base):
    __tablename__ = "ftp_archivos_procesados"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre_archivo = Column(String(255), nullable=False, unique=True)
    fecha_archivo = Column(Date, nullable=False)
    codigo_estacion = Column(String(255), nullable=False)
    timestamp_procesado = Column(TIMESTAMP, nullable=False)

    __table_args__ = (
        UniqueConstraint("nombre_archivo", name="idx_unique_nombre_archivo"),
    )


class Diccionario(Base):
    __tablename__ = "diccionario"

    id_reg = Column(Integer, primary_key=True, autoincrement=True)
    xlink_name = Column(String(255), nullable=False)
    normalized_name = Column(String(255), nullable=False)
    codigo_estacion = Column(String(255), nullable=False)
    timestamp_create = Column(TIMESTAMP, nullable=False)

class S3PathsStations(Base):
    __tablename__ = "s3_paths_stations"

    codigo_estacion = Column(String(255), primary_key=True, nullable=False)
    path_s3 = Column(String(255), nullable=False)
    timestamp_create = Column(TIMESTAMP, nullable=False)