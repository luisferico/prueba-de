import polars as pl
from typing import Dict

def validate_coordinates(df: pl.DataFrame) -> Dict:
    """
    Validar las coordenadas del DataFrame
    """
    total_records = len(df)
    invalid_records = 0
    errors = []

    # Validar presencia de columnas requeridas
    required_columns = ['lat', 'lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return {
            'valid_records': 0,
            'invalid_records': total_records,
            'errors': [f"Missing required columns: {missing_columns}"]
        }

    # Validar rangos de coordenadas
    invalid_lat = df.filter(
        (pl.col('lat') < -90) | 
        (pl.col('lat') > 90)
    ).height

    invalid_lon = df.filter(
        (pl.col('lon') < -180) | 
        (pl.col('lon') > 180)
    ).height

    invalid_records = invalid_lat + invalid_lon
    
    if invalid_records > 0:
        errors.append(f"Found {invalid_records} records with invalid coordinates")

    return {
        'total_records': total_records,
        'valid_records': total_records - invalid_records,
        'invalid_records': invalid_records,
        'errors': errors
    }