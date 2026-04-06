import os
import numpy as np
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from sklearn.cluster import DBSCAN

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

def ejecutar_ia_zonas_riesgo():
    if not MONGO_URL:
        print("❌ Error: Faltan credenciales MONGO_URL")
        return

    client = MongoClient(MONGO_URL)
    db = client['geocrimen_tacna']
    hoy = datetime.utcnow()

    print("🧠 Iniciando Procesamiento Analítico Espacial (Híbrido)...")
    
    # Limpiamos las zonas de riesgo previas para recalcularlas todas
    db.zonas_riesgo.delete_many({})
    nuevas_zonas = []

    # =========================================================================
    # FASE 1: ANÁLISIS MACRO (Basado en SIDPOL y UNIDAD DE FLAGRANCIA)
    # =========================================================================
    estadisticas_sidpol = list(db.estadisticas_sidpol.find({}))
    estadisticas_flagrancia = list(db.estadisticas_flagrancia.find({}))
    
    agrupacion_distrital = {}
    
    # ---- 1.1 Procesar datos de SIDPOL ----
    for est in estadisticas_sidpol:
        distrito = str(est.get("distrito", "TACNA")).upper().strip()
        if distrito not in agrupacion_distrital:
            agrupacion_distrital[distrito] = {"total_delitos": 0, "tipos": []}
            
        agrupacion_distrital[distrito]["total_delitos"] += est.get("cantidad", 0)
        agrupacion_distrital[distrito]["tipos"].append(str(est.get("sub_tipo", "DESCONOCIDO")))

    # ---- 1.2 Procesar datos de FLAGRANCIA ----
    for est in estadisticas_flagrancia:
        distrito = str(est.get("distrito", "TACNA")).upper().strip()
        if distrito not in agrupacion_distrital:
            agrupacion_distrital[distrito] = {"total_delitos": 0, "tipos": []}
            
        # Flagrancia a veces trae la cantidad o es 1 por caso
        cantidad = est.get("cantidad", 1)
        agrupacion_distrital[distrito]["total_delitos"] += cantidad
        agrupacion_distrital[distrito]["tipos"].append(str(est.get("delito", "DESCONOCIDO")))

    for distrito, data in agrupacion_distrital.items():
        total = data["total_delitos"]
        if total == 0: continue
        
        # Moda matemática para hallar el delito más concurrente
        delito_principal = max(set(data["tipos"]), key=data["tipos"].count) if data["tipos"] else "DESCONOCIDO"
        
        if total > 50: nivel_riesgo = "critico"
        elif total > 20: nivel_riesgo = "alto"
        elif total > 5: nivel_riesgo = "medio"
        else: nivel_riesgo = "bajo"
            
        centro = COORDENADAS_DISTRITOS.get(distrito, COORDENADAS_DISTRITOS["TACNA"])

        nuevas_zonas.append({
            "centroide": {
                "type": "Point",
                "coordinates": [centro["lng"], centro["lat"]]
            },
            # Las zonas MACRO son distritales, radios de 1 a 2 kilómetros adaptativos
            "radio_metros": int(1000 + (total * 5)), 
            "distrito": distrito,
            "nivel_riesgo": nivel_riesgo,
            "total_incidentes": total,
            "delito_predominante": delito_principal,
            "tendencia": "estable",
            "calculado_en": hoy,
            "origen": "ESTADISTICAS_GUBERNAMENTALES"
        })


    # =========================================================================
    # FASE 2: ANÁLISIS MICRO (Machine Learning DBSCAN sobre los Incidentes de la APP)
    # =========================================================================
    incidentes_cursor = db.incidentes.find({"ubicacion": {"$exists": True}})
    puntos_reales = []
    
    for inc in incidentes_cursor:
        coords = inc.get("ubicacion", {}).get("coordinates", [])
        if len(coords) == 2:
            puntos_reales.append({
                "lng": coords[0],
                "lat": coords[1],
                "sub_tipo": inc.get("sub_tipo", "DESCONOCIDO")
            })
    
    # DBSCAN solo necesita operar si existen suficientes reportes geolocalizados
    # por usuarios/policías. Configuramos mínimo 3 reportes cercanos para formar un clúster.
    if len(puntos_reales) >= 3:
        df = pd.DataFrame(puntos_reales)
        coords_rad = np.radians(df[['lat', 'lng']].values)
        
        # eps = ~400 metros de búsqueda en radianes 
        epsilon = 0.4 / 6371.0 
        min_samples = 3 
        
        dbscan = DBSCAN(eps=epsilon, min_samples=min_samples, algorithm='ball_tree', metric='haversine')
        df['cluster'] = dbscan.fit_predict(coords_rad)

        clusters = df[df['cluster'] != -1]
        
        for cluster_id, grupo in clusters.groupby('cluster'):
            total_ml = len(grupo)
            
            # Centro matemático exacto de los reportes unificados
            centro_lat = grupo['lat'].mean()
            centro_lng = grupo['lng'].mean()
            delito_ml = grupo['sub_tipo'].mode()[0]

            if total_ml >= 10: nivel_riesgo = "critico"
            elif total_ml >= 6: nivel_riesgo = "alto"
            elif total_ml >= 3: nivel_riesgo = "medio"
            else: nivel_riesgo = "bajo"

            nuevas_zonas.append({
                "centroide": {
                    "type": "Point",
                    "coordinates": [float(centro_lng), float(centro_lat)]
                },
                # Las zonas MICRO (Hotspots) son alertas específicas, radios de 150 a 400 metros
                "radio_metros": int(max(200, (total_ml * 30))), 
                "distrito": f"Punto Crítico Detectado (App) #{cluster_id}",
                "nivel_riesgo": nivel_riesgo,
                "total_incidentes": int(total_ml),
                "delito_predominante": delito_ml,
                "tendencia": "subiendo",
                "calculado_en": hoy,
                "origen": "APP_INCIDENTES"
            })

    # Guardado de la data fusionada
    if nuevas_zonas:
        db.zonas_riesgo.insert_many(nuevas_zonas)
        zonas_macro = len(agrupacion_distrital)
        zonas_micro = len(nuevas_zonas) - zonas_macro
        
        print(f"✅ Análisis completado. Guardadas {zonas_macro} Zonas (SIDPOL+FLAGRANCIA) y {zonas_micro} Hotspots detectados de la App.")
        for z in nuevas_zonas:
            icono = "🏢" if z["origen"] == "ESTADISTICAS_GUBERNAMENTALES" else "🚨"
            print(f"   {icono} {z['distrito']} | {z['total_incidentes']} casos | Riesgo: {z['nivel_riesgo'].upper()} | {z['delito_predominante']}")
    else:
        print("ℹ️ No hay datos suficientes (ni SIDPOL, ni Flagrancia, ni en la App) para generar zonas.")

if __name__ == "__main__":
    ejecutar_ia_zonas_riesgo()
