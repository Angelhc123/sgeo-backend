import os
import numpy as np
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from sklearn.cluster import DBSCAN
import difflib

# Importar calculador de tendencia real del motor predictivo
try:
    from predictive_context_engine import calcular_tendencia_real
    _HAS_PREDICTIVE = True
except ImportError:
    _HAS_PREDICTIVE = False

load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")

# Coordenadas maestras de los distritos de Tacna para la fase Macro (Alineadas con centros poblados reales)
COORDENADAS_DISTRITOS = {
    "TACNA": {"lat": -18.0146, "lng": -70.2536},
    "ALTO DE LA ALIANZA": {"lat": -17.9922, "lng": -70.2436},
    "CIUDAD NUEVA": {"lat": -17.9790, "lng": -70.2380},
    "CORONEL GREGORIO ALBARRACIN LANCHIPA": {"lat": -18.0463, "lng": -70.2520},
    "POCOLLAY": {"lat": -17.9961, "lng": -70.2185},
    "CALANA": {"lat": -17.9422, "lng": -70.1834},
    "PACHIA": {"lat": -17.8925, "lng": -70.1558},
    "SAMA": {"lat": -17.8441, "lng": -70.6273},
    "LA YARADA LOS PALOS": {"lat": -18.1755, "lng": -70.4735},
}

def limpiar_distrito(nombre):
    """Normaliza nombres de distritos usando coincidencias difusas (fuzzy matching)"""
    if not nombre:
        return "TACNA"
        
    nombre = str(nombre).upper().strip()
    
    trans = str.maketrans("ÁÉÍÓÚÄËÏÖÜ", "AEIOUAEIOU")
    nombre = nombre.translate(trans)
    
    distritos_validos = list(COORDENADAS_DISTRITOS.keys())
    
    if nombre in distritos_validos:
        return nombre
        
    # Atajos manuales
    if "GREGORIO" in nombre or "ALBARRACI" in nombre:
        return "CORONEL GREGORIO ALBARRACIN LANCHIPA"
    if "YARADA" in nombre or "PALOS" in nombre:
        return "LA YARADA LOS PALOS"
    if "ALIANZA" in nombre:
        return "ALTO DE LA ALIANZA"
        
    # Auto-corrector (Fuzzy Matching)
    coincidencias = difflib.get_close_matches(nombre, distritos_validos, n=1, cutoff=0.65)
    if coincidencias:
        return coincidencias[0]
        
    return "TACNA"

def ejecutar_ia_zonas_riesgo():
    if not MONGO_URL:
        print("❌ Error: Faltan credenciales MONGO_URL")
        return

    client = MongoClient(MONGO_URL)
    db = client['geocrimen_tacna']
    hoy = datetime.utcnow()

    print("🧠 Iniciando Procesamiento Analítico Espacial Estricto usando el Historial de Delitos...")
    
    # Limpiamos las zonas de riesgo previas para recalcularlas todas
    db.zonas_riesgo.delete_many({})
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
        {"ubicacion": 1, "sub_tipo": 1}
    )
    
    for inc in incidentes_cursor:
        coords = inc.get("ubicacion", {}).get("coordinates", [])
        if len(coords) == 2:
            puntos_reales.append({
                "lng": coords[0],
                "lat": coords[1],
                "sub_tipo": inc.get("sub_tipo", "DESCONOCIDO")
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
            delito_ml = grupo['sub_tipo'].mode()[0]

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

    # Guardado de la data fusionada
    if nuevas_zonas:
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
    else:
        print("ℹ️ No hay datos suficientes para generar zonas.")

if __name__ == "__main__":
    ejecutar_ia_zonas_riesgo()
