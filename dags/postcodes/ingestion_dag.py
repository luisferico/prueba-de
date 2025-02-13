from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import os
import polars as pl
from pathlib import Path
from utils.validators import validate_coordinates
from utils.db_operations import load_to_database

# Configuración por defecto del DAG
default_args = {
    'owner': 'Luis Rico',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Variables para las rutas
DATA_PATH = '/opt/airflow/data'
CSV_FILE_PATTERN = 'postcodes_geo*.csv'

def _get_next_file(**context):
    """Obtener el próximo archivo a procesar"""
    
    files = [f for f in os.listdir(DATA_PATH) 
             if f.startswith('postcodes_geo') and f.endswith('.csv')]
    
    if not files:
        print(f"No matching CSV files found in {DATA_PATH}")
        return None
    
    # Verificar en la base de datos qué archivos ya han sido procesados
    pg_hook = PostgresHook(postgres_conn_id='postcodes_db')
    processed_files = pg_hook.get_pandas_df(
        "SELECT DISTINCT file_path FROM raw_coordinates"
    )
    processed_paths = processed_files['file_path'].tolist() if not processed_files.empty else []
    
    unprocessed_files = [f for f in files if os.path.join(DATA_PATH, f) not in processed_paths]
    
    if not unprocessed_files:
        print("No new files to process")
        return None
    
    file_name = sorted(unprocessed_files, key=lambda x: os.path.getmtime(os.path.join(DATA_PATH, x)))[0]
    file_path = os.path.join(DATA_PATH, file_name)
    
    print(f"Found file to process: {file_path}")
    return file_path

def _validate_csv(**context):
    """Validar el archivo CSV"""

    file_path = context['task_instance'].xcom_pull(task_ids='get_next_file')
    
    if not file_path:
        print("No file to process")
        return False
    
    print(f"Validating file: {file_path}")
    
    df = pl.read_csv(file_path)
    
    validation_results = validate_coordinates(df)
    
    if validation_results['invalid_records'] > 0:
        context['task_instance'].xcom_push(
            key='validation_summary',
            value=validation_results
        )
    
    return validation_results['valid_records'] > 0

def _transform_data(**context):
    """Transformar y limpiar los datos antes de cargarlos"""

    file_path = context['task_instance'].xcom_pull(task_ids='get_next_file')
    
    if not file_path:
        print("No file to process")
        return None
    
    print(f"Transforming data from: {file_path}")
    
    df = pl.read_csv(file_path)
    
    df_transformed = (df.rename({
            'lat': 'latitude',
            'lon': 'longitude'
        })
        .with_columns([
            pl.col('latitude').cast(pl.Float64),
            pl.col('longitude').cast(pl.Float64)
        ])
        .unique()
    )

    transformed_records = len(df_transformed)
    original_records = len(df)
    duplicates_removed = original_records - transformed_records
    
    print(f"Original records: {original_records}")
    print(f"Transformed records: {transformed_records}")
    print(f"Duplicates removed: {duplicates_removed}")
    
    temp_file = file_path.replace('geo', 'transformed')
    df_transformed.write_csv(temp_file)
    
    context['task_instance'].xcom_push(
        key='transformation_metrics',
        value={
            'original_records': original_records,
            'transformed_records': transformed_records,
            'duplicates_removed': duplicates_removed,
            'temp_file': temp_file
        }
    )
    
    return temp_file

def _load_to_raw_coordinates(**context):
    """Cargar datos transformados a la base de datos"""
    
    transformation_metrics = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformation_metrics'
    )
    
    if not transformation_metrics:
        print("No transformation metrics found")
        return 0
    
    file_path = transformation_metrics['temp_file']
    original_file = context['task_instance'].xcom_pull(task_ids='get_next_file')
    batch_id = f"{context['ds']}_{str(context['dag_run'].id)}"
    
    records_loaded = load_to_database(file_path, batch_id, original_file)
    
    if os.path.exists(file_path):
        os.remove(file_path)
    
    return f"Loaded {records_loaded} records"

with DAG(
    '01_postcodes_ingestion',
    default_args=default_args,
    description='Ingest postcodes data from CSV',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 11),
    catchup=False,
    tags=['postcodes'],
    is_paused_upon_creation=False
) as dag:
    
    wait_for_csv = FileSensor(
        task_id='wait_for_csv',
        filepath=f"{DATA_PATH}/{CSV_FILE_PATTERN}",
        poke_interval=30,  # Revisar cada 30 segundos
        timeout=60 * 60,   # Timeout después de 1 hora
        mode='poke'
    )
    
    get_next_file = PythonOperator(
        task_id='get_next_file',
        python_callable=_get_next_file
    )
    
    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=_validate_csv
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=_transform_data
    )
    
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=_load_to_raw_coordinates
    )
    
    trigger_enrichment = TriggerDagRunOperator(
        task_id='trigger_enrichment',
        trigger_dag_id='02_postcode_enrichment',
        trigger_rule='all_success'
    )
    
    wait_for_csv >> get_next_file >> validate_data >> transform_data >> load_data
    load_data >> trigger_enrichment