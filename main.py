import time
from datetime import datetime
from ftp.extractor import process_files

if __name__ == "__main__":
    # Registrar tiempo de inicio
    start_time = time.time()
    start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n🚀 Inicio de ejecución: {start_timestamp}\n")

    # Ejecutar la función principal
    process_files()

    # Registrar tiempo de finalización
    end_time = time.time()
    end_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elapsed_time = end_time - start_time  # Tiempo total en segundos

    print(f"\n✅ Fin de ejecución: {end_timestamp}")
    print(f"⏱️ Tiempo total de ejecución: {elapsed_time:.2f} segundos\n")
