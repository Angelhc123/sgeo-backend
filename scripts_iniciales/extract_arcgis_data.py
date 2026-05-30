import requests
import json
import os

def extract_arcgis_data():
    # Diccionario con los servicios de Hurto y Robo
    servicios = {
        "HURTO": "https://services6.arcgis.com/lMIZrqiJkpM748BR/arcgis/rest/services/HURTO_IDSUBTIPO_10501/FeatureServer/2/query",
        "ROBO": "https://services6.arcgis.com/lMIZrqiJkpM748BR/arcgis/rest/services/ROBO_IDSUBTIPO_10502/FeatureServer/3/query"
    }

    all_features = []
    
    for delito, base_url in servicios.items():
        print(f"\nIniciando la extracción de datos de {delito}...")
        
        # Parámetros base para la consulta
        params = {
            "where": "departamento_hecho = 'TACNA'",
            "outFields": "*",
            "outSR": 4326, # WGS84 para coordenadas de latitud/longitud
            "f": "json",
            "returnGeometry": "false", 
            "resultOffset": 0,
            "resultRecordCount": 2000 
        }

        while True:
            try:
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                data = response.json()
                
                features = data.get("features", [])
                if not features:
                    break
                    
                all_features.extend(features)
                
                if data.get("exceededTransferLimit"):
                    params["resultOffset"] += len(features)
                    print(f"[{delito}] Extraídos {params['resultOffset']} registros temporalmente...")
                else:
                    print(f"[{delito}] Extracción completa para este servicio.")
                    break
                    
            except Exception as e:
                print(f"Error durante la extracción de {delito}: {e}")
                break

    # Guardar en archivo unificado
    output_path = os.path.join(os.path.dirname(__file__), "datos_historicos_tacna.json")
    
    # Extraer los atributos para tener una lista plana de diccionarios
    clean_data = [feature["attributes"] for feature in all_features]
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(clean_data, f, ensure_ascii=False, indent=2)
        
    print(f"Extracción completada. {len(clean_data)} registros guardados en {output_path}")

if __name__ == "__main__":
    extract_arcgis_data()
