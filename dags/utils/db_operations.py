import polars as pl
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_to_database(transformed_file: str, batch_id: str, original_file: str) -> int:
    """
    Cargar datos transformados a la tabla raw_coordinates
    """
    df = pl.read_csv(transformed_file)
    
    batch_id = int(batch_id.split('T')[0])
    
    pg_hook = PostgresHook(postgres_conn_id='postcodes_db')
    
    records = df.with_columns([
        pl.lit(batch_id).alias('batch_id'),
        pl.lit(False).alias('processed'),
        pl.lit(original_file).alias('file_path')
    ])
    
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        values = [
            f"({row['latitude']}, {row['longitude']}, {row['batch_id']}, {row['processed']}, '{row['file_path']}')"
            for row in records.iter_rows(named=True)
        ]
        
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            insert_query = f"""
                INSERT INTO raw_coordinates (latitude, longitude, batch_id, processed, file_path)
                VALUES {','.join(batch)}
            """
            cursor.execute(insert_query)
        
        conn.commit()
        return len(records)
    
    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {str(e)}")
        raise e
    
    finally:
        cursor.close()
        conn.close()
