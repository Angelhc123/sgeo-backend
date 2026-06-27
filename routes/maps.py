from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Optional
from config.database import db
import time
from motor_ia_zonas_riesgo import ejecutar_ia_zonas_riesgo

router = APIRouter(prefix="/api/map", tags=["maps"])

# Caché simple en memoria (TODO: migrar a Redis para multi-worker).
# ADVERTENCIA: este dict vive en el proceso de un solo worker. Si en algun momento se
# despliega con mas de un worker/replica (hoy el Procfile usa uvicorn sin --workers,
# es decir 1 solo proceso), cada instancia tendra su propia copia y los usuarios podrian
# ver datos cacheados distintos entre si.
_cache_store = {}

def _get_cached(key: str):
    if key in _cache_store:
        data, ts = _cache_store[key]
        if (time.time() - ts) < 60:
            return data
    return None

def _set_cache(key: str, data):
    _cache_store[key] = (data, time.time())

@router.post("/generar_zonas_ia")
def desencadenar_ia_zonas(background_tasks: BackgroundTasks):
    """
    Ruta administrativa silenciosa. 
    Lanza el motor matematico sin trabar la respuesta del servidor.
    Se llamar automaticamente cada vez que un policia apruebe un nuevo incidente.
    """
    background_tasks.add_task(ejecutar_ia_zonas_riesgo)
    return {"status": "success", "mensaje": "IA iniciada en segundo plano."}

@router.get("/zonas_riesgo")
def obtner_zonas_riesgo():
    cached = _get_cached("zonas_riesgo")
    if cached is not None:
        return {"status": "success", "zonas": cached, "cached": True}
        
    try:
        zonas = list(db.zonas_riesgo.find({}))
        for zona in zonas:
            zona["_id"] = str(zona["_id"])
            if "calculado_en" in zona:
                zona["calculado_en"] = zona["calculado_en"].isoformat()       
            if "periodo_analizado" in zona:
                if "desde" in zona["periodo_analizado"]:
                    zona["periodo_analizado"]["desde"] = zona["periodo_analizado"]["desde"].isoformat()
                if "hasta" in zona["periodo_analizado"]:
                    zona["periodo_analizado"]["hasta"] = zona["periodo_analizado"]["hasta"].isoformat()
        
        _set_cache("zonas_riesgo", zonas)
        
        return {"status": "success", "zonas": zonas, "cached": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error obteniendo zonas de riesgo: " + str(e))

@router.get("/puntos_exactos")
def obtener_puntos_exactos():
    """
    Devuelve SOLO los reportes que hayan sido CONFIRMADOS por la policia.
    Esto evita falsos positivos y sesgos en el mapa de calor de la IA.
    """
    try:
        # Filtro muy importante: {"estado": "confirmado"}
        reportes = list(db.reportes_ciudadano.find(
            {"estado": "confirmado"}, 
            {"_id": 1, "subtipo_hecho": 1, "ubicacion": 1, "estado": 1, "fecha_hora_hecho": 1}
        ))
        for rep in reportes:
            rep["_id"] = str(rep["_id"])
            if "fecha_hora_hecho" in rep and rep["fecha_hora_hecho"]:
                try:
                    rep["fecha_hora_hecho"] = rep["fecha_hora_hecho"].isoformat()
                except:
                    rep["fecha_hora_hecho"] = str(rep["fecha_hora_hecho"])
        
        return {"status": "success", "puntos": reportes}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error obteniendo los puntos: " + str(e))

@router.get("/historial_puntos")
def obtener_historial_puntos():
    """
    Devuelve todos los puntos del historial de delitos (ArcGIS + Ciudadanos confirmados) 
    para mostrarlos en el mapa cuando el usuario hace zoom a detalle.
    """
    try:
        # Traemos solo los campos necesarios para el mapa
        puntos = list(db.historial_delitos.find(
            {"estado_coord": {"$ne": "SIN COORDENADA"}},
            {"_id": 1, "subtipo_hecho": 1, "ubicacion": 1, "fuente": 1, "fecha_hecho": 1, "modalidad_hecho": 1, "turno_hecho": 1}
        ))
        for p in puntos:
            p["_id"] = str(p["_id"])
            if "fecha_hecho" in p and p["fecha_hecho"] is not None:
                try:
                    p["fecha_hecho"] = p["fecha_hecho"].isoformat()
                except:
                    p["fecha_hecho"] = str(p["fecha_hecho"])
        
        return {"status": "success", "puntos": puntos}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error obteniendo historial: " + str(e))
