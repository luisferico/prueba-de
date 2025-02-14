from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.api_client import PostcodesAPIClient
from utils.db_operations import (
    get_unprocessed_coordinates,
    insert_postcode_details,
    create_coordinate_postcode_relations,
    log_errors,
    mark_coordinates_as_processed
)

default_args = {
    'owner': 'Luis Rico',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def _get_unprocessed_coordinates(**context):
    """Obtener coordenadas no procesadas en lotes"""
    
    batch_size = 2000
    coordinates = get_unprocessed_coordinates(batch_size)
    
    if not coordinates:
        print("No coordinates to process")
        return None
    
    print(f"Found {len(coordinates)} coordinates to process")
    return coordinates

def _enrich_coordinates(**context):
    """Procesar la respuesta de la API y guardar en la base de datos"""
    
    coordinates = context['task_instance'].xcom_pull(task_ids='get_coordinates')
    if not coordinates:
        print("No coordinates to process")
        return None
    
    max_workers = 10
    api_results = PostcodesAPIClient.get_postcodes(coordinates, max_workers)
    
    if api_results.successes:
        
        relations = []
        processed_ids = []
        
        all_postcodes = []
        for result in api_results.successes:
            all_postcodes.extend(result['result'])
        
        postcode_id_map = insert_postcode_details(all_postcodes)
        
        # Crear las relaciones
        for result in api_results.successes:
            coordinate_id = result['coordinate_id']
            processed_ids.append(coordinate_id)
            
            for postcode in result['result']:
                postcode_id = postcode_id_map.get(postcode['postcode'])
                if postcode_id:
                    relations.append({
                        'coordinate_id': coordinate_id,
                        'postcode_id': postcode_id,
                        'distance': postcode['distance']
                    })
        
        if relations:
            create_coordinate_postcode_relations(relations)
        
        mark_coordinates_as_processed(processed_ids)
        
        print(f"Successfully processed {len(processed_ids)} coordinates")
    
    
    if api_results.errors:
        log_errors(api_results.errors)
        
        # Marcar coordenadas con error como procesadas
        error_ids = [error['coordinate_id'] for error in api_results.errors]
        mark_coordinates_as_processed(error_ids)
        
        print(f"Found {len(api_results.errors)} errors")
    
    return {
        'success_count': len(api_results.successes),
        'error_count': len(api_results.errors)
    }

def _should_continue(**context):
    """Verificar si hay más coordenadas para procesar"""
    coordinates = get_unprocessed_coordinates(1)  # Solo verificamos si hay al menos una
    if bool(coordinates):
        return 'trigger_next_batch'
    else:
        return 'dummy_end'

with DAG(
    '02_postcode_enrichment',
    default_args=default_args,
    description='Enrich coordinates with postcode data',
    schedule_interval=None,
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['postcodes'],
    max_active_runs=1
) as dag:
    
    get_coordinates = PythonOperator(
        task_id='get_coordinates',
        python_callable=_get_unprocessed_coordinates
    )
    
    enrich_coordinates = PythonOperator(
        task_id='enrich_coordinates',
        python_callable=_enrich_coordinates
    )
    
    check_more_coordinates = BranchPythonOperator(
        task_id='check_more_coordinates',
        python_callable=_should_continue
    )
    
    # Si hay más coordenadas, se ejecuta un nuevo run del DAG
    trigger_next_batch = TriggerDagRunOperator(
        task_id='trigger_next_batch',
        trigger_dag_id='02_postcode_enrichment',
        trigger_rule='all_success'
    )
    
    dummy_end = EmptyOperator(
        task_id='dummy_end'
    )
    
    get_coordinates >> enrich_coordinates >> check_more_coordinates >> [trigger_next_batch, dummy_end]
 