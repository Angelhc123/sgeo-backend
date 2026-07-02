import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
from config.database import db
from utils.string_helpers import limpiar_distrito

try:
    from predictive_context_engine import calcular_tendencia_real
    _HAS_PREDICTIVE = True
except ImportError:
    _HAS_PREDICTIVE = False


def _detectar_ultimo_mes_sidpol() -> tuple[int, int]:
    """Retorna (año, mes) del mes más reciente con datos SIDPOL en historial_delitos."""
    try:
        pipeline = [
            {"$match": {"fuente": "arcgis_sidpol", "fecha_hecho": {"$exists": True}}},
            {"$group": {"_id": None, "max_fecha": {"$max": "$fecha_hecho"}}},
        ]
        resultado = list(db.historial_delitos.aggregate(pipeline))
        if resultado and resultado[0].get("max_fecha"):
            d = resultado[0]["max_fecha"]
            return d.year, d.month
    except Exception as e:
        print("Aviso: no se pudo detectar ultimo mes SIDPOL:", e)
    return datetime.utcnow().year, datetime.utcnow().month


def ejecutar_ia_zonas_riesgo():
    hoy = datetime.utcnow()
    print("🧠 Iniciando Motor IA de Zonas de Riesgo...")

    # ── Detectar el mes más reciente con datos disponibles ──────────────
    anio_periodo, mes_periodo = _detectar_ultimo_mes_sidpol()
    inicio_mes = datetime(anio_periodo, mes_periodo, 1)
    fin_mes = (
        datetime(anio_periodo + 1, 1, 1)
        if mes_periodo == 12
        else datetime(anio_periodo, mes_periodo + 1, 1)
    )
    # Incluir también reportes de ciudadanos confirmados de los últimos 60 días
    hace_60d = hoy - timedelta(days=60)

    print(f"📅 Periodo SIDPOL: {mes_periodo:02d}/{anio_periodo}  |  Reportes ciudadanos: últimos 60 días")

    # ── Cargar puntos con coordenadas del periodo ────────────────────────
    puntos_reales = []

    incidentes_cursor = db.historial_delitos.find(
        {
            "ubicacion": {"$exists": True},
            "estado_coord": {"$ne": "SIN COORDENADA"},
            "provincia_hecho": "TACNA",  # solo provincia de Tacna
            "$or": [
                # SIDPOL: solo el mes más reciente disponible
                {
                    "fuente": "arcgis_sidpol",
                    "fecha_hecho": {"$gte": inicio_mes, "$lt": fin_mes},
                },
                # Ciudadanos: últimos 60 días (para enriquecer con datos en tiempo real)
                {
                    "fuente": "ciudadano",
                    "fecha_hecho": {"$gte": hace_60d},
                },
            ],
        },
        {"ubicacion": 1, "subtipo_hecho": 1},
    )

    for inc in incidentes_cursor:
        coords = inc.get("ubicacion", {}).get("coordinates", [])
        if len(coords) == 2:
            puntos_reales.append({
                "lng": coords[0],
                "lat": coords[1],
                "subtipo_hecho": inc.get("subtipo_hecho", "DESCONOCIDO"),
            })

    print(f"   Puntos cargados: {len(puntos_reales)}")

    nuevas_zonas = []

    # ── DBSCAN Micro-Espacial ────────────────────────────────────────────
    if len(puntos_reales) >= 5:
        df = pd.DataFrame(puntos_reales)
        coords_rad = np.radians(df[["lat", "lng"]].values)

        epsilon = 0.15 / 6371.0   # ~150 metros
        min_samples = 5

        dbscan = DBSCAN(
            eps=epsilon, min_samples=min_samples,
            algorithm="ball_tree", metric="haversine"
        )
        df["cluster"] = dbscan.fit_predict(coords_rad)
        clusters = df[df["cluster"] != -1]

        for cluster_id, grupo in clusters.groupby("cluster"):
            total_ml = len(grupo)
            centro_lat = grupo["lat"].mean()
            centro_lng = grupo["lng"].mean()
            delito_ml = grupo["subtipo_hecho"].mode()[0]

            if total_ml >= 50:   nivel_riesgo = "critico"
            elif total_ml >= 25: nivel_riesgo = "alto"
            elif total_ml >= 10: nivel_riesgo = "medio"
            else:                nivel_riesgo = "bajo"

            if _HAS_PREDICTIVE:
                try:
                    tendencia_calc = calcular_tendencia_real(
                        db, float(centro_lat), float(centro_lng), radio_m=500
                    )
                except Exception:
                    tendencia_calc = "estable"
            else:
                tendencia_calc = "estable"

            nuevas_zonas.append({
                "centroide": {
                    "type": "Point",
                    "coordinates": [float(centro_lng), float(centro_lat)],
                },
                "radio_metros": int(max(150, min(350, 100 + (total_ml * 5)))),
                "distrito": "Zona Caliente Detectada",
                "nivel_riesgo": nivel_riesgo,
                "total_incidentes": int(total_ml),
                "delito_predominante": delito_ml,
                "tendencia": tendencia_calc,
                "calculado_en": hoy,
                "origen": "APP_HOTSPOT_ML",
                # Periodo fuente — usado por el mapa ciudadano para auto-filtrar
                "anio_periodo": anio_periodo,
                "mes_periodo": mes_periodo,
            })

    # ── Guardar atómicamente ─────────────────────────────────────────────
    if nuevas_zonas:
        try:
            db.zonas_riesgo.delete_many({})
            db.zonas_riesgo.insert_many(nuevas_zonas)
            print(f"✅ {len(nuevas_zonas)} zonas guardadas ({mes_periodo:02d}/{anio_periodo}).")

            # Cooldown: notificar máximo una vez cada 24 horas
            try:
                from firebase_service import send_push_notification
                ultimo = db.config.find_one({"key": "last_map_update_notification"})
                horas_desde_ultimo = (
                    (hoy - ultimo["sent_at"]).total_seconds() / 3600
                    if ultimo and "sent_at" in ultimo
                    else 9999
                )
                if horas_desde_ultimo >= 24:
                    send_push_notification(
                        title="🗺️ Mapa de Zonas Actualizado",
                        body="La inteligencia artificial recalculó los puntos calientes en Tacna.",
                        tipo_alerta="update",
                        topic="alertas_ciudadanos",
                    )
                    db.config.update_one(
                        {"key": "last_map_update_notification"},
                        {"$set": {"sent_at": hoy}},
                        upsert=True,
                    )
            except Exception as e:
                print("No se pudo enviar notificacion de actualizacion:", e)

        except Exception as e:
            print("❌ Error guardando zonas:", e)
    else:
        print("ℹ️  No hay datos suficientes para generar zonas.")
        print(f"   Verifica que historial_delitos tiene registros de {mes_periodo:02d}/{anio_periodo}.")


if __name__ == "__main__":
    ejecutar_ia_zonas_riesgo()
