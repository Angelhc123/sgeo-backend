import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.cluster import DBSCAN
from config.database import db
from utils.string_helpers import limpiar_distrito

# Importar calculador de tendencia real del motor predictivo
try:
    from predictive_context_engine import calcular_tendencia_real
    _HAS_PREDICTIVE = True
except ImportError:
    _HAS_PREDICTIVE = False

def ejecutar_ia_zonas_riesgo():
    hoy = datetime.utcnow()

    print("🧠 Iniciando Procesamiento Analítico Espacial Estricto usando el Historial de Delitos...")
    
    nuevas_zonas = []

    # =========================================================================
    # ANÁLISIS MICRO-ESPACIAL (Machine Learning DBSCAN para encontrar Hotspots)
    # Ignora totalmente divisiones distritales y se enfoca solo donde OCURREN delitos
    # =========================================================================
    # Filtramos la data ignorando intencionalmente las que tienen "SIN COORDENADA"
    puntos_reales = []
    
    # Limitamos el análisis a incidentes que posean coordenadas reales
    incidentes_cursor = db.historial_delitos.find(
        {
            "ubicacion": {"$exists": True},
            "estado_coord": {"$ne": "SIN COORDENADA"}
        }, 
        {"ubicacion": 1, "subtipo_hecho": 1}
    )
    
    for inc in incidentes_cursor:
        coords = inc.get("ubicacion", {}).get("coordinates", [])
        if len(coords) == 2:
            puntos_reales.append({
                "lng": coords[0],
                "lat": coords[1],
                "subtipo_hecho": inc.get("subtipo_hecho", "DESCONOCIDO")
            })
    
    # Configuramos mínimo de reportes cercanos para formar un clúster. 
    if len(puntos_reales) >= 5:
        df = pd.DataFrame(puntos_reales)
        coords_rad = np.radians(df[['lat', 'lng']].values)
        
        # eps = ~150 metros de búsqueda en radianes (Evita el "efecto cadena" interconectando toda la ciudad)
        epsilon = 0.15 / 6371.0 
        min_samples = 5 
        
        dbscan = DBSCAN(eps=epsilon, min_samples=min_samples, algorithm='ball_tree', metric='haversine')
        df['cluster'] = dbscan.fit_predict(coords_rad)

        clusters = df[df['cluster'] != -1]
        
        for cluster_id, grupo in clusters.groupby('cluster'):
            total_ml = len(grupo)
            
            # Centro matemático exacto de los reportes unificados
            centro_lat = grupo['lat'].mean()
            centro_lng = grupo['lng'].mean()
            delito_ml = grupo['subtipo_hecho'].mode()[0]

            if total_ml >= 50: nivel_riesgo = "critico"
            elif total_ml >= 25: nivel_riesgo = "alto"
            elif total_ml >= 10: nivel_riesgo = "medio"
            else: nivel_riesgo = "bajo"

            # Calcular tendencia real en vez de hardcodear
            if _HAS_PREDICTIVE:
                try:
                    tendencia_calc = calcular_tendencia_real(db, float(centro_lat), float(centro_lng), radio_m=500)
                except Exception:
                    tendencia_calc = "estable"
            else:
                tendencia_calc = "estable"

            nuevas_zonas.append({
                "centroide": {
                    "type": "Point",
                    "coordinates": [float(centro_lng), float(centro_lat)]
                },
                "radio_metros": int(max(150, min(350, 100 + (total_ml * 5)))), 
                "distrito": "Zona Caliente Detectada",
                "nivel_riesgo": nivel_riesgo,
                "total_incidentes": int(total_ml),
                "delito_predominante": delito_ml,
                "tendencia": tendencia_calc,
                "calculado_en": hoy,
                "origen": "APP_HOTSPOT_ML"
            })

    # Guardado de la data fusionada de forma atómica
    if nuevas_zonas:
        try:
            # Primero borramos, luego insertamos (simulando transacción simple)
            db.zonas_riesgo.delete_many({})
            db.zonas_riesgo.insert_many(nuevas_zonas)
            
            print(f"✅ Análisis completado. Guardados {len(nuevas_zonas)} Hotspots generados por Machine Learning.")
            for z in nuevas_zonas[:15]:  # Imprimimos solo las 15 primeras para no saturar la consola
                print(f"   🚨 {z['distrito']} | {z['total_incidentes']} casos | Riesgo: {z['nivel_riesgo'].upper()} | {z['delito_predominante']}")
            if len(nuevas_zonas) > 15: print("   ...")
                
            try:
                from firebase_service import send_push_notification
                send_push_notification(
                    title="🗺️ Mapa de Zonas Actualizado",
                    body="La inteligencia artificial acaba de recalcular los puntos calientes en Tacna en base a la nueva data histórica confirmada.",
                    tipo_alerta="update",
                    topic="alertas_ciudadanos"
                )
            except Exception as e:
                print("No se pudo enviar la alerta de actualización de mapas:", e)
        except Exception as e:
            print("❌ Error fatal al guardar nuevas zonas de riesgo, base de datos de zonas mantenida o corrupta: ", e)
    else:
        print("ℹ️ No hay datos suficientes para generar zonas.")

if __name__ == "__main__":
    ejecutar_ia_zonas_riesgo()