import polars as pl
from typing import List, Dict, Any
from airflow.providers.postgres.hooks.postgres import PostgresHook


def bulk_insert(table_name: str, records: List[Dict[str, Any]], columns: List[str], 
                unique_constraint: str = None) -> List[int]:
    """
    Ejecuta inserción en lote para cualquier tabla
    
    Args:
        table_name: Nombre de la tabla
        records: Lista de diccionarios con los datos a insertar
        columns: Lista de columnas a insertar
        unique_constraint: Columna con restricción única para manejo de conflictos
    Returns:
        Lista de IDs insertados/actualizados
    """
    if not records:
        return []
        
    pg_hook = PostgresHook(postgres_conn_id='postcodes_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Crear placeholders para el query
        placeholders = ', '.join(['%s' for _ in columns])
        columns_str = ', '.join(columns)
        
        # Preparar los valores para inserción
        values = []
        for record in records:
            record_values = [record.get(col) for col in columns]
            values.append(
                cursor.mogrify(f"({placeholders})", record_values).decode('utf-8')
            )
        
        # Construir query base
        base_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES {','.join(values)}
        """
        
        # Agregar manejo de conflictos si es necesario
        if unique_constraint:
            base_query += f" ON CONFLICT ({unique_constraint}) DO NOTHING"
            
        base_query += " RETURNING id"
        
        # Ejecutar query
        cursor.execute(base_query)
        inserted_ids = [row[0] for row in cursor.fetchall()]
        
        conn.commit()
        return inserted_ids
    
    except Exception as e:
        conn.rollback()
        print(f"Error in bulk insert: {str(e)}")
        raise e
    
    finally:
        cursor.close()
        conn.close()

def load_to_database(transformed_file: str, batch_id: str, original_file: str) -> int:
    """Cargar datos del CSV a raw_coordinates"""
    df = pl.read_csv(transformed_file)
    
    records = df.with_columns([
        pl.lit(batch_id).alias('batch_id'),
        pl.lit(False).alias('processed'),
        pl.lit(original_file).alias('file_path')
    ]).to_dicts()
    
    columns = ['latitude', 'longitude', 'batch_id', 'processed', 'file_path']
    
    return bulk_insert('raw_coordinates', records, columns)

def get_unprocessed_coordinates(batch_size: int = 100) -> List[Dict]:
    """Obtener lote de coordenadas no procesadas"""
    pg_hook = PostgresHook(postgres_conn_id='postcodes_db')
    
    coordinates = pg_hook.get_records("""
        SELECT id, latitude, longitude 
        FROM raw_coordinates 
        WHERE processed = false 
        LIMIT %s
    """, parameters=[batch_size])
    
    return [
        {'id': coord[0], 'latitude': coord[1], 'longitude': coord[2]}
        for coord in coordinates
    ] if coordinates else []

def get_existing_postcodes(postcodes: List[str]) -> Dict[str, int]:
    """Obtener los IDs de postcodes que ya existen"""
    if not postcodes:
        return {}
    
    pg_hook = PostgresHook(postgres_conn_id='postcodes_db')
    
    # Convertir lista a string para la query
    postcode_list = "','".join(postcodes)
    
    results = pg_hook.get_records(f"""
        SELECT postcode, id 
        FROM postcode_details 
        WHERE postcode IN ('{postcode_list}')
    """)
    
    return {postcode: id for postcode, id in results} if results else {}

def insert_postcode_details(postcodes: List[Dict]) -> List[int]:
    """Insertar múltiples códigos postales"""
    existing_postcodes = get_existing_postcodes([p['postcode'] for p in postcodes])
    
    new_postcodes = [
        p for p in postcodes 
        if p['postcode'] not in existing_postcodes
    ]
    
    columns = [
        'postcode', 'quality', 'eastings', 'northings', 'country',
        'nhs_ha', 'longitude', 'latitude', 'european_electoral_region',
        'primary_care_trust', 'region', 'lsoa', 'msoa', 'incode',
        'outcode', 'parliamentary_constituency', 'admin_district',
        'parish', 'admin_county', 'date_of_introduction', 'admin_ward',
        'ced', 'ccg', 'nuts', 'pfa'
    ]
    
    if new_postcodes:
        new_ids = bulk_insert('postcode_details', new_postcodes, columns, 'postcode')
        for postcode, id in zip([p['postcode'] for p in new_postcodes], new_ids):
            existing_postcodes[postcode] = id
    
    return existing_postcodes

def create_coordinate_postcode_relations(relations: List[Dict]) -> int:
    """Crear múltiples relaciones coordenada-postcode"""
    columns = ['coordinate_id', 'postcode_id', 'distance']
    
    return bulk_insert('coordinates_postcodes', relations, columns)

def log_errors(errors: List[Dict]) -> int:
    """Registrar múltiples errores"""
    columns = ['coordinate_id', 'error_type', 'error_message']
    
    return bulk_insert('error_logs', errors, columns)

def mark_coordinates_as_processed(coordinate_ids: List[int]):
    """Marcar múltiples coordenadas como procesadas"""
    if not coordinate_ids:
        return
        
    pg_hook = PostgresHook(postgres_conn_id='postcodes_db')
    
    ids_str = ','.join(str(id) for id in coordinate_ids)
    pg_hook.run(f"""
        UPDATE raw_coordinates 
        SET processed = true 
        WHERE id IN ({ids_str})
    """)
    