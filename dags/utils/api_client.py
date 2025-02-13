import time
import requests
from threading import Lock
from typing import List, Dict, NamedTuple
from concurrent.futures import ThreadPoolExecutor, as_completed

class APIResult(NamedTuple):
    successes: List[Dict]
    errors: List[Dict]

class PostcodesAPIClient:
    def __init__(self):
        self.base_url = 'https://api.postcodes.io'
        self.session = requests.Session()
        self.timeout = 30
        self.max_requests_per_minute = 2000
        self.request_interval = 60.0 / self.max_requests_per_minute
        self.last_request_time = 0
        self.lock = Lock()

    def _rate_limit(self):
        """Rate limit simple"""
        with self.lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            time_to_wait = self.request_interval - time_since_last_request
            
            if time_to_wait > 0:
                time.sleep(time_to_wait)
            
            self.last_request_time = time.time()

    def _get_postcode_for_coordinate(self, coord: Dict) -> Dict:
        """Procesar una coordenada"""
        self._rate_limit()
        
        try:
            response = self.session.get(
                    f"{self.base_url}/postcodes?lon={coord['longitude']}&lat={coord['latitude']}",
                    timeout=self.timeout
                )
            
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 200 and data['result']:
                return {
                    'coordinate_id': coord['id'],
                    'result': data['result'],
                    'success': True
                }
            else:
                return {
                    'coordinate_id': coord['id'],
                    'error_type': 'NO_RESULTS',
                    'error_message': f"No postcodes found for coordinates: lat={coord['latitude']}, lon={coord['longitude']}",
                    'success': False
                }
                
        except requests.exceptions.RequestException as e:
            return {
                'coordinate_id': coord['id'],
                'error_type': 'API_ERROR',
                'error_message': str(e),
                'success': False
            }
        except Exception as e:
            return {
                'coordinate_id': coord['id'],
                'error_type': 'PROCESSING_ERROR',
                'error_message': str(e),
                'success': False
            }
    
    def get_nearest_postcodes(self, coordinates: List[Dict], max_workers: int = 10) -> APIResult:
        """
        Obtener los códigos postales usando múltiples hilos
        
        Args:
            coordinates: Lista de coordenadas
            max_workers: Número máximo de hilos
        """
        successes = []
        errors = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Crear futures para todas las coordenadas
            future_to_coord = {
                executor.submit(self._get_postcode_for_coordinate, coord): coord 
                for coord in coordinates
            }
            
            # Procesar resultados según van completándose
            for future in as_completed(future_to_coord):
                result = future.result()
                
                if result['success']:
                    successes.append({
                        'coordinate_id': result['coordinate_id'],
                        'result': result['result']
                    })
                else:
                    errors.append({
                        'coordinate_id': result['coordinate_id'],
                        'error_type': result['error_type'],
                        'error_message': result['error_message']
                    })
        
        return APIResult(successes=successes, errors=errors)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    @classmethod
    def get_postcodes(cls, coordinates: List[Dict], max_workers: int = 10) -> APIResult:
        """Método para usar el cliente en el context"""
        with cls() as client:
            return client.get_nearest_postcodes(coordinates, max_workers)
       