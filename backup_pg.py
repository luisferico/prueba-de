import subprocess
import os
from datetime import datetime

# Configuración de la base de datos
DB_NAME = 'postcodes_db'
DB_USER = 'postgres'
DB_HOST = 'localhost'
DB_PORT = '5432'
BACKUP_DIR = './database-backups'

# Crear directorio de backups si no existe
os.makedirs(BACKUP_DIR, exist_ok=True)

# Generar nombre de archivo con fecha y hora
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_filename = f'{DB_NAME}_backup_{timestamp}.sql'
backup_path = os.path.join(BACKUP_DIR, backup_filename)

# Comando de pg_dump usando docker exec
pg_dump_command = [
    'docker', 'exec', 
    'prueba-de-postcodes-db-1',
    'pg_dump', 
    '-U', DB_USER,
    '-d', DB_NAME,
    '-h', DB_HOST,
    '-p', DB_PORT
]

# Redirigir la salida a un archivo local
try:
    with open(backup_path, 'w') as backup_file:
        print(f"Iniciando backup de {DB_NAME}...")
        subprocess.run(pg_dump_command, stdout=backup_file, check=True)
    
    # Comprimir el backup
    subprocess.run(['gzip', backup_path], check=True)
    
    # Información del backup
    compressed_path = f'{backup_path}.gz'
    file_size = os.path.getsize(compressed_path) / (1024 * 1024)  # Tamaño en MB
    
    print(f"Backup completado: {compressed_path}")
    print(f"Tamaño del archivo: {file_size:.2f} MB")

except subprocess.CalledProcessError as e:
    print(f"Error al crear el backup: {e}")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")
    