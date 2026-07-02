import requests
import json
import os
from datetime import datetime, timezone, timedelta

# ──────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# Servicio unificado (HURTO + ROBO + todos los subtipos)
# Última actualización en ArcGIS: mayo 2026 (4 969 registros Tacna)
# ──────────────────────────────────────────────────────────────────────
SERVICIO_URL = (
    "https://services6.arcgis.com/lMIZrqiJkpM748BR"
    "/arcgis/rest/services/SIDPOL_DELITOS_TOTAL/FeatureServer/1/query"
)

ANO_OBJETIVO = 2026   # Cambia a 2025 si el año actual no tiene datos suficientes
MES_OBJETIVO = None   # None = todos los meses del año; ej: 5 = solo mayo


def extract_arcgis_data(anio: int = ANO_OBJETIVO, mes: int | None = MES_OBJETIVO):
    peru = timezone(timedelta(hours=-5))
    ahora_peru = datetime.now(peru)

    desc = f"año {anio}" + (f" mes {mes}" if mes else "")
    print(f"\n📅 Extrayendo datos SIDPOL — {desc} (Tacna)")

    # Construir cláusula WHERE
    where = f"departamento_hecho = 'TACNA' AND año_hecho = {anio}"
    if mes:
        where += f" AND mes_hecho = {mes}"

    params = {
        "where": where,
        "outFields": (
            "fecha_hora_hecho,año_hecho,mes_hecho,dia_hecho,"
            "lat_hecho,long_hecho,tipo_hecho,subtipo_hecho,modalidad_hecho,"
            "departamento_hecho,provincia_hecho,distrito_hecho,ubigeo_hecho_delito,"
            "turno_hecho,direccion_hecho,tipo_via_hecho,ESTADO_COORD"
        ),
        "outSR": 4326,
        "f": "json",
        "returnGeometry": "false",
        "resultOffset": 0,
        "resultRecordCount": 2000,
    }

    all_features = []
    offset = 0

    while True:
        params["resultOffset"] = offset
        try:
            resp = requests.get(SERVICIO_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                print(f"❌ Error ArcGIS: {data['error']}")
                break

            features = data.get("features", [])
            if not features:
                break

            all_features.extend(features)
            offset += len(features)

            if data.get("exceededTransferLimit"):
                print(f"   {offset} registros extraídos, continuando...")
            else:
                break

        except Exception as e:
            print(f"❌ Error de conexión: {e}")
            break

    clean_data = [f["attributes"] for f in all_features]

    if not clean_data:
        print(f"\n⚠️  ArcGIS no devolvió registros para {desc}.")
        print("   Opciones:")
        print(f"   • Cambia ANO_OBJETIVO = {anio - 1} si ese año aún no está publicado.")
        print("   • Verifica la URL en el navegador para confirmar que el servicio responde.")
        return

    # Distribución por mes (hora Perú)
    from collections import Counter
    dist = Counter()
    for r in clean_data:
        raw = r.get("fecha_hora_hecho") or r.get("año_hecho")
        if isinstance(raw, (int, float)) and raw > 1e9:
            d = datetime.fromtimestamp(raw / 1000, tz=peru)
            dist[d.strftime("%Y-%m")] += 1
        elif r.get("año_hecho") and r.get("mes_hecho"):
            dist[f"{r['año_hecho']:04d}-{r['mes_hecho']:02d}"] += 1

    if dist:
        print(f"\n📊 Distribución por mes: {dict(sorted(dist.items()))}")

    output_path = os.path.join(os.path.dirname(__file__), "datos_historicos_tacna.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(clean_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {len(clean_data)} registros guardados en datos_historicos_tacna.json")
    print("   Siguiente: ejecuta  python import_arcgis_data.py")


if __name__ == "__main__":
    extract_arcgis_data()
