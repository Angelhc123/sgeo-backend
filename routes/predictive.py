"""
=============================================================================
SGEO — Endpoints Predictivos FastAPI
=============================================================================
Router modular con 5 endpoints async para el módulo de seguridad contextual.
Se integra al main.py existente via app.include_router().
=============================================================================
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import time
from config.database import db as _db
from predictive_context_engine import (
    TemporalAnalyzer,
    SafetyScoreCalculator,
    InsightGenerator,
    SafeHoursCalculator,
)

router = APIRouter(prefix="/api/predictive", tags=["Predictive Intelligence"])

# ── Caché inteligente con TTL ──
# TODO: migrar a Redis para multi-worker.
# ADVERTENCIA: igual que en routes/maps.py, este dict vive en el proceso de un solo worker.
# Hoy el Procfile corre uvicorn sin --workers (1 proceso), pero si se escala horizontalmente
# (mas replicas en Railway) cada una mantendria su propia copia de esta cache.
_cache_store = {}
CACHE_TTL = {
    "safety_score": 30,        # 30 segundos (datos en tiempo real)
    "temporal_analysis": 300,   # 5 minutos
    "context_insights": 60,     # 1 minuto
    "risk_forecast": 600,       # 10 minutos
    "safe_hours": 600,          # 10 minutos
}


def _get_cached(key: str):
    """Retorna datos cacheados si no han expirado."""
    if key in _cache_store:
        data, ts = _cache_store[key]
        ttl = CACHE_TTL.get(key.split(":")[0], 60)
        if (time.time() - ts) < ttl:
            return data
    return None


def _set_cache(key: str, data):
    """Almacena datos en caché."""
    _cache_store[key] = (data, time.time())


# ═══════════════════════════════════════════════════════════════════════════
#  GET /api/predictive/safety_score
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/safety_score")
async def get_safety_score(
    lat: float = Query(..., description="Latitud del usuario", ge=-90, le=90),
    lng: float = Query(..., description="Longitud del usuario", ge=-180, le=180),
    hora: Optional[int] = Query(None, description="Hora del día (0-23). Si es null usa hora actual UTC", ge=0, le=23),
):
    """
    Calcula el Safety Score dinámico (0-100) para una ubicación y hora.
    
    Interpretación:
    - 80-100: Seguro (verde)
    - 50-79: Precaución (amarillo)  
    - 0-49: Alto Riesgo (rojo)
    """
    cache_key = f"safety_score:{round(lat,3)}:{round(lng,3)}:{hora}"
    cached = _get_cached(cache_key)
    if cached:
        return {"status": "success", "cached": True, **cached}

    try:
        result = SafetyScoreCalculator.calculate(_db, lat, lng, hora)
        _set_cache(cache_key, result)
        return {"status": "success", "cached": False, **result}
    except Exception as e:
        print(f"Error safety_score: {e}")
        raise HTTPException(status_code=500, detail=f"Error calculando safety score: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════
#  GET /api/predictive/temporal_analysis
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/temporal_analysis")
async def get_temporal_analysis(
    distrito: Optional[str] = Query(None, description="Filtrar por distrito"),
    dias: int = Query(365, description="Días de análisis hacia atrás", ge=7, le=1095),
):
    """
    Análisis temporal completo: distribución por hora, día de semana, turno y tendencia.
    """
    cache_key = f"temporal_analysis:{distrito}:{dias}"
    cached = _get_cached(cache_key)
    if cached:
        return {"status": "success", "cached": True, **cached}

    try:
        result = {
            "por_hora": TemporalAnalyzer.analyze_by_hour(_db, distrito, dias),
            "por_dia_semana": TemporalAnalyzer.analyze_by_day_of_week(_db, distrito, dias),
            "por_turno": TemporalAnalyzer.analyze_by_turno(_db, distrito, dias),
            "tendencia": TemporalAnalyzer.calculate_trend(_db, distrito),
        }
        _set_cache(cache_key, result)
        return {"status": "success", "cached": False, **result}
    except Exception as e:
        print(f"Error temporal_analysis: {e}")
        raise HTTPException(status_code=500, detail=f"Error en análisis temporal: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════
#  GET /api/predictive/context_insights
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/context_insights")
async def get_context_insights(
    lat: float = Query(..., description="Latitud", ge=-90, le=90),
    lng: float = Query(..., description="Longitud", ge=-180, le=180),
    hora: Optional[int] = Query(None, description="Hora (0-23)", ge=0, le=23),
):
    """
    Genera insights automáticos personalizados para la ubicación y hora.
    Retorna hasta 6 recomendaciones contextuales basadas en data real.
    """
    cache_key = f"context_insights:{round(lat,3)}:{round(lng,3)}:{hora}"
    cached = _get_cached(cache_key)
    if cached:
        return {"status": "success", "cached": True, "insights": cached}

    try:
        insights = InsightGenerator.generate(_db, lat, lng, hora)
        _set_cache(cache_key, insights)
        return {"status": "success", "cached": False, "insights": insights}
    except Exception as e:
        print(f"Error context_insights: {e}")
        raise HTTPException(status_code=500, detail=f"Error generando insights: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════
#  GET /api/predictive/risk_forecast
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/risk_forecast")
async def get_risk_forecast(
    distrito: Optional[str] = Query(None, description="Distrito para pronóstico"),
):
    """
    Pronóstico de riesgo: tendencia, score por turno y proyección por distrito.
    """
    cache_key = f"risk_forecast:{distrito}"
    cached = _get_cached(cache_key)
    if cached:
        return {"status": "success", "cached": True, **cached}

    try:
        tendencia = TemporalAnalyzer.calculate_trend(_db, distrito)
        por_turno = TemporalAnalyzer.analyze_by_turno(_db, distrito)

        # Score de riesgo por turno
        turno_risk = []
        total_t = sum(t["total_incidentes"] for t in por_turno)
        for t in por_turno:
            pct = (t["total_incidentes"] / max(total_t, 1)) * 100
            turno_risk.append({
                "turno": t["turno"],
                "total_incidentes": t["total_incidentes"],
                "porcentaje": round(pct, 1),
                "nivel_riesgo": "alto" if pct > 35 else "medio" if pct > 20 else "bajo"
            })

        result = {
            "tendencia": tendencia,
            "riesgo_por_turno": turno_risk,
            "distrito_analizado": distrito or "todos",
        }
        _set_cache(cache_key, result)
        return {"status": "success", "cached": False, **result}
    except Exception as e:
        print(f"Error risk_forecast: {e}")
        raise HTTPException(status_code=500, detail=f"Error en pronóstico: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════
#  GET /api/predictive/safe_hours
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/safe_hours")
async def get_safe_hours(
    distrito: Optional[str] = Query(None, description="Distrito para análisis"),
):
    """
    Determina las franjas horarias más seguras y peligrosas para transitar.
    Retorna distribución completa por hora y recomendación textual.
    """
    cache_key = f"safe_hours:{distrito}"
    cached = _get_cached(cache_key)
    if cached:
        return {"status": "success", "cached": True, **cached}
    
    try:
        result = SafeHoursCalculator.calculate(_db, distrito)
        _set_cache(cache_key, result)
        return {"status": "success", "cached": False, **result}
    except Exception as e:
        print(f"Error safe_hours: {e}")
        raise HTTPException(status_code=500, detail=f"Error calculando safe hours: {str(e)}")
