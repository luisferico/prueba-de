from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.utils.session import provide_session

default_args = {
    'owner': 'Luis Rico',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1
}

@provide_session
def create_connections(session=None):
    """Crear o actualizar las conexiones necesarias"""
    
    # Conexión para el sistema de archivos
    fs_conn = Connection(
        conn_id='fs_default',
        conn_type='fs',
        extra='{"path": "/opt/airflow/data"}'
    )
    
    # Conexión para la base de datos de postcodes
    db_conn = Connection(
        conn_id='postcodes_db',
        conn_type='postgres',
        host='postcodes-db',
        schema='postcodes_db',
        login='postgres',
        password='postgres',
        port=5432
    )
    
    # Lista de conexiones
    connections = [fs_conn, db_conn]
    
    for conn in connections:
        if session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
            session.query(Connection).filter(Connection.conn_id == conn.conn_id).delete()
        session.add(conn)
    
    session.commit()

with DAG(
    '00_setup_connections',
    default_args=default_args,
    description='Setup required connections',
    schedule_interval='@once',
    catchup=False,
    tags=['setup']
) as dag:

    create_conn = PythonOperator(
        task_id='create_connections',
        python_callable=create_connections
    )

    create_conn
    