import os
import json
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar configuración desde .env
load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")

def import_arcgis_data():
    if not MONGO_URL:
        print("❌ Error: Faltan las credenciales MONGO_URL en el archivo .env")
        return

    client = MongoClient(MONGO_URL)
    db = client['geocrimen_tacna']
    coleccion_sidpol = db['historial_delitos']

    # Ruta del archivo JSON
    file_path = os.path.join(os.path.dirname(__file__), "datos_historicos_tacna.json")
    
    if not os.path.exists(file_path):
        print(f"❌ Error: No se encontró el archivo {file_path}")
        return

    # 🧹 LIMPIEZA PREVIA PARA EVITAR DUPLICADOS
    # Borramos todos los registros que vinieron de ArcGIS previamente antes de insertar los nuevos.
    # Así no borramos los reportes que hayan hecho los ciudadanos ("fuente": "ciudadano").
    borrados = coleccion_sidpol.delete_many({"fuente": "arcgis_sidpol"})
    print(f"🧹 Se borraron {borrados.deleted_count} registros antiguos de ArcGIS para evitar duplicados.")

    print("Abriendo archivo de datos...")
    with open(file_path, "r", encoding="utf-8") as f:
        datos_arcgis = json.load(f)

    print(f"Se encontraron {len(datos_arcgis)} registros. Procesando...")

    nuevos_incidentes = []
    errores = 0

    for registro in datos_arcgis:
        try:
            # Extraer y transformar coordenadas (ArcGIS REST devuelve WGS84 gracias al outSR=4326)
            lat = registro.get("lat_hecho")
            lon = registro.get("long_hecho")

            # Evitar registros con coordenadas nulas o en 0,0
            if not lat or not lon or (lat == 0 and lon == 0):
                continue

            # Transformar fechas (ArcGIS devuelve timestamps en milisegundos)
            fecha_ms = registro.get("fecha_hora_hecho")
            if fecha_ms:
                fecha_hecho = datetime.fromtimestamp(fecha_ms / 1000.0)
            else:
                fecha_hecho = datetime.utcnow()

            # Estandarizar solo las columnas útiles y descartar basuras internas (OBJECTID, códigos macroregionales, etc)
            incidente_historico = {
                "ubicacion": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)]
                },
                "direccion_hecho": registro.get("direccion_hecho"),
                "tipo_via": registro.get("tipo_via_hecho"),
                "departamento_hecho": registro.get("departamento_hecho"),
                "provincia_hecho": registro.get("provincia_hecho"),
                "distrito_hecho": registro.get("distrito_hecho"),
                "ubigeo": registro.get("ubigeo_hecho_delito"),
                "fecha_hecho": fecha_hecho,
                "turno_hecho": registro.get("turno_hecho"),
                "tipo_hecho": registro.get("tipo_hecho"),
                "subtipo_hecho": registro.get("subtipo_hecho"),
                "modalidad_hecho": registro.get("modalidad_hecho"),
                "estado_coord": registro.get("ESTADO_COORD"),
                "fuente": "arcgis_sidpol",
                "creado_en": datetime.utcnow()
            }
            
            nuevos_incidentes.append(incidente_historico)

        except Exception as e:
            errores += 1
            # Para depurar puedes usar: print(f"Error procesando registro: {e}")
            continue

    if nuevos_incidentes:
        print(f"Insertando {len(nuevos_incidentes)} incidentes históricos en la base de datos...")
        # InsertMany es eficiente para grandes cantidades de datos
        resultado = coleccion_sidpol.insert_many(nuevos_incidentes)
        print(f"✅ Se insertaron {len(resultado.inserted_ids)} registros correctamente.")
    else:
        print("⚠️ No se encontraron registros válidos para insertar.")

    if errores > 0:
        print(f"⚠️ Se omitieron {errores} registros por errores de formato.")

if __name__ == "__main__":
    import_arcgis_data()
