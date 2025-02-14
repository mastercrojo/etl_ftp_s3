import re
import io
import logging
import ftplib  # Para capturar errores de FTP
from datetime import datetime , timedelta
import pandas as pd
import boto3
from tqdm import tqdm
from botocore.exceptions import ClientError
from ftp.ftp_client import connect_ftp
from data.database import load_dataframes, get_archivos_procesados, register_archivo_procesado, connect_db
from config.settings import  AWS_DEFAULT_REGION, AWS_S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# Configurar logging
logging.basicConfig(
    filename="etl_ftp_s3.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Cliente S3
def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_DEFAULT_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

# Cliente S3
s3_client = get_s3_client()

def extract_info(path):
    """Extrae información de un archivo con formato `nombre_yyyymmddhhmmss.csv`"""
    file_name = path.split("/")[-1]
    match = re.match(r"(.+)_([0-9]{14})\.csv", file_name)
    if match:
        xlink_name, str_timestamp = match.groups()
        timestamp = datetime.strptime(str_timestamp, '%Y%m%d%H%M%S')
    else:
        xlink_name, timestamp = "Unknown", "Unknown"
    return file_name, xlink_name, timestamp

def check_and_create_s3_path(bucket, path):
    """
    Verifica si una ruta existe en S3. Si no existe, la crea.
    """
    try:
        # Intentamos listar objetos en la ruta para ver si existe
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
        if "Contents" not in response:  # Si no hay contenido en la ruta, se debe crear
            # print(f"La ruta {path} no existe en S3. Creándola ahora...")
            s3_client.put_object(Bucket=bucket, Key=(path + "/"))  # Crea la carpeta vacía
    except Exception as e:
        print(f"Error al verificar/crear la ruta {path} en S3: {str(e)}")
        logging.warning(f"Error al verificar/crear la ruta {path} en S3: {str(e)}")

def file_exists_in_s3(bucket_name, s3_key):
    """Verifica si un archivo ya existe en S3"""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False  # El archivo no existe en S3
        else:
            raise  # Otro error, lo lanzamos para depuración

def generar_rango_meses(num_meses):
    """
    Genera una lista de meses en formato YYYYMM, considerando el mes actual y los anteriores.
    
    Args:
        num_meses (int): Número de meses a incluir en el rango (incluyendo el mes actual).
    
    Returns:
        List[str]: Lista de meses en formato YYYYMM.
    """
    hoy = datetime.now()
    meses = [
        (hoy - timedelta(days=30 * i)).strftime('%Y%m')  # Restar meses en pasos de ~30 días
        for i in range(num_meses)
    ]
    return meses

def process_files():
    try:
        """Descarga archivos del FTP, extrae información y determina la ruta en S3"""
        logging.info("Iniciando proceso de descarga y carga de archivos en S3...")
        ftp = connect_ftp()

         # Conexión a la base de datos
        engine, Session = connect_db()
        session = Session()
        
        df_diccionario, df_s3_paths = load_dataframes(session)
        
        # Obtener lista de archivos ya procesados
        archivos_procesados = get_archivos_procesados(session)

        # Si la base de datos está vacía (primera ejecución)
        primera_ejecucion = len(archivos_procesados) == 0
        
        if primera_ejecucion:
            logging.info("Primera ejecución detectada: todos los archivos serán procesados.")

        # Listar archivos en el FTP
        archivos_ftp = ftp.nlst()

        # Generar rango de meses (por ejemplo, 2 meses: actual y anterior)
        rango_meses = generar_rango_meses(num_meses=2)
        
        # Filtrar archivos        
        if not primera_ejecucion:
            archivos_a_procesar = [archivo for archivo in archivos_ftp if archivo not in archivos_procesados and any(mes in archivo for mes in rango_meses)]
            logging.info(f"Caso normal, ya existen archivos procesados.")
        else:
            archivos_a_procesar = [archivo for archivo in archivos_ftp if any(mes in archivo for mes in rango_meses)]
            logging.info(f"Se gatilla la condicion inicial para la primera ejecución, se procesarán los archivos de enero y febrero de 2025.")

        logging.info(f"Se encontraron {len(archivos_a_procesar)} archivos que coinciden con el filtro en el FTP.")
        # Registrar marca de tiempo
        start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\nTiempo en acceder al FTP: {start_timestamp}\n")        
        
        if not archivos_a_procesar:
            logging.info("No hay archivos que coincidan con el filtro en el FTP.")
            print("No hay archivos que coincidan con el filtro.")
            return

        for archivo in tqdm(archivos_a_procesar, desc="Procesando archivos", unit="archivo"):
            try:
                file_name, xlink_name, timestamp = extract_info(archivo)
                
                # Registrar en log
                logging.info(f"Procesando archivo: {file_name}")

                if timestamp is None or timestamp =="Unknown":  # Si no se pudo extraer timestamp, continuar con el siguiente archivo
                    logging.warning(f"Archivo {file_name} tiene formato inválido. Se omite.")
                    continue

                # Obtener YYYY, MM, DD
                YYYY = timestamp.strftime('%Y')
                MM = timestamp.strftime('%m')
                DD = timestamp.strftime('%d')
                
                fecha_archivo = timestamp.strftime('%Y-%m-%d')

                # Buscar codigo_estacion en df_diccionario
                estacion_data = df_diccionario[df_diccionario["xlink_name"] == xlink_name]
                if estacion_data.empty:
                    # print(f"No se encontró código estación para {xlink_name}. Archivo omitido.")
                    logging.warning(f"No se encontró código estación para {xlink_name}. Archivo omitido.")
                    continue
                
                codigo_estacion = estacion_data.iloc[0]["codigo_estacion"]
                
                # Buscar path_s3 en df_s3_paths
                path_data = df_s3_paths[df_s3_paths["codigo_estacion"] == codigo_estacion]
                if path_data.empty:
                    # print(f"No se encontró path_s3 para estación {codigo_estacion}")
                    logging.warning(f"No se encontró path_s3 para estación {codigo_estacion}")
                    continue
                
                path_s3 = path_data.iloc[0]["path_s3"]
                
                # Construir la ruta destino en S3
                path_end = f"{path_s3}/datos/{YYYY}/{MM}/{DD}"

                # Verificar si la ruta existe y crearla si es necesario
                check_and_create_s3_path(AWS_S3_BUCKET, path_end)

                # Definir la ruta de destino en S3
                s3_key = f"{path_end}/{file_name}"

                # Verificar si el archivo ya existe en S3
                if file_exists_in_s3(AWS_S3_BUCKET, s3_key):
                    logging.info(f"Archivo ya existe en S3: {s3_key}. Omitiendo...")
                    if not register_archivo_procesado(session, file_name, codigo_estacion, fecha_archivo):
                        logging.warning(f"Error al almacenenar en MySql el archivo: {file_name}")  
                    continue  # Pasar al siguiente archivo
                
                # Descargar archivo desde FTP
                with io.BytesIO() as file_stream:
                    ftp.retrbinary(f"RETR {archivo}", file_stream.write)
                    file_stream.seek(0)  # Reiniciar el puntero
                    
                    # Subir archivo a S3
                    s3_client.upload_fileobj(file_stream, AWS_S3_BUCKET, s3_key)
                    logging.warning(f"Archivo {file_name} subido a S3: {s3_key}")
                
                # Registrar archivo como procesado
                register_archivo_procesado(session, file_name, codigo_estacion, fecha_archivo)
            
            except Exception as file_error:
                logging.warning(f"Error al procesar {archivo}: {str(file_error)}")
                print(f"⚠️ Error al procesar {archivo}, continuando con el siguiente archivo...")

    except ftplib.all_errors as ftp_error:
        logging.critical(f"Error crítico en la conexión FTP: {str(ftp_error)}")
        raise ftp_error  # Detiene el proceso solo si el FTP falla

    finally:
        ftp.quit()
        print("\n✅ Proceso finalizado correctamente.")
