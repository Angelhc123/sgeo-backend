import os
import json
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")

PERU = timezone(timedelta(hours=-5))


def _parse_fecha(valor) -> datetime:
    """Convierte timestamp ms (ArcGIS) o string ISO a datetime UTC."""
    if valor is None:
        return datetime.utcnow()
    if isinstance(valor, (int, float)) and valor > 1e9:
        return datetime.fromtimestamp(valor / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    if isinstance(valor, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(valor, fmt)
            except ValueError:
                pass
    return datetime.utcnow()


def import_arcgis_data():
    if not MONGO_URL:
        print("❌ Falta MONGO_URL en .env")
        return

    client = MongoClient(MONGO_URL)
    db = client["geocrimen_tacna"]
    coleccion = db["historial_delitos"]

    file_path = os.path.join(os.path.dirname(__file__), "datos_historicos_tacna.json")
    if not os.path.exists(file_path):
        print(f"❌ No se encontró {file_path}")
        return

    # Borrar solo los datos de ArcGIS anteriores (no toca reportes ciudadanos)
    borrados = coleccion.delete_many({"fuente": "arcgis_sidpol"})
    print(f"🧹 {borrados.deleted_count} registros ArcGIS anteriores eliminados.")

    with open(file_path, "r", encoding="utf-8") as f:
        datos = json.load(f)

    print(f"📂 {len(datos)} registros leídos. Procesando...")

    nuevos = []
    omitidos = 0

    for r in datos:
        try:
            lat = r.get("lat_hecho")
            lon = r.get("long_hecho")

            # Descartar registros sin coordenadas válidas
            if not lat or not lon or (abs(lat) < 0.001 and abs(lon) < 0.001):
                omitidos += 1
                continue

            fecha_hecho = _parse_fecha(r.get("fecha_hora_hecho"))

            # estado_coord: derivado de si tiene coordenadas (campo ausente en SIDPOL_DELITOS_TOTAL)
            estado_coord_raw = r.get("ESTADO_COORD") or r.get("estado_coord")
            if estado_coord_raw:
                estado_coord = str(estado_coord_raw).strip()
            else:
                estado_coord = "CON COORDENADA"  # ya filtramos lat/lon arriba

            incidente = {
                "ubicacion": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)],
                },
                "direccion_hecho": r.get("direccion_hecho"),
                "tipo_via": r.get("tipo_via_hecho"),
                "departamento_hecho": r.get("departamento_hecho"),
                "provincia_hecho": r.get("provincia_hecho"),
                "distrito_hecho": r.get("distrito_hecho"),
                "ubigeo": r.get("ubigeo_hecho_delito"),
                "fecha_hecho": fecha_hecho,
                "turno_hecho": r.get("turno_hecho"),
                "tipo_hecho": r.get("tipo_hecho"),
                "subtipo_hecho": r.get("subtipo_hecho"),
                "modalidad_hecho": r.get("modalidad_hecho"),
                "estado_coord": estado_coord,
                "fuente": "arcgis_sidpol",
                "creado_en": datetime.utcnow(),
                # Campos numéricos directos (útiles para agregaciones futuras)
                "anio": r.get("año_hecho") or r.get("a�o_hecho"),
                "mes": r.get("mes_hecho"),
                "dia": r.get("dia_hecho"),
            }
            nuevos.append(incidente)

        except Exception as e:
            omitidos += 1
            continue

    if nuevos:
        resultado = coleccion.insert_many(nuevos)
        print(f"✅ {len(resultado.inserted_ids)} registros insertados correctamente.")
    else:
        print("⚠️  No se encontraron registros válidos para insertar.")

    if omitidos:
        print(f"ℹ️  {omitidos} registros omitidos (sin coordenadas o con error).")


if __name__ == "__main__":
    import_arcgis_data()
