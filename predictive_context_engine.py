"""
=============================================================================
SGEO — Motor Predictivo de Seguridad Contextual
=============================================================================
Módulo de análisis temporal, scoring dinámico, insights automáticos y
geointelligence avanzada. Opera sobre las colecciones existentes de MongoDB
sin modificar la estructura de datos ni los pipelines ETL previos.

Responsabilidades:
  - Análisis temporal (hora, turno, día, mes, estacionalidad)
  - Cálculo de Safety Score dinámico (0-100)
  - Generación de insights contextuales personalizados
  - Pronóstico de riesgo por franja horaria
  - Cálculo de tendencias reales por distrito
  - Determinación de horarios seguros

Dependencias: pandas, numpy, scikit-learn (ya en requirements.txt)
=============================================================================
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from math import radians, cos, sin, asin, sqrt
from utils.time_helpers import get_turno, get_turno_weight, TURNOS, TURNO_WEIGHTS


# ═══════════════════════════════════════════════════════════════════════════
#  CONSTANTES TEMPORALES
# ═══════════════════════════════════════════════════════════════════════════

# Las constantes de turnos se importan desde utils.time_helpers

DIAS_SEMANA = {
    0: "lunes", 1: "martes", 2: "miercoles", 3: "jueves",
    4: "viernes", 5: "sabado", 6: "domingo"
}

# Radio máximo de influencia para scoring (metros)
MAX_INFLUENCE_RADIUS = 2000.0
# Período de análisis por defecto (días)
DEFAULT_ANALYSIS_DAYS = 365


# ═══════════════════════════════════════════════════════════════════════════
#  UTILIDADES GEOESPACIALES
# ═══════════════════════════════════════════════════════════════════════════

def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia Haversine entre dos puntos en metros."""
    R = 6371000
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))


# Las funciones get_turno y get_turno_weight se importan desde utils.time_helpers


# ═══════════════════════════════════════════════════════════════════════════
#  CLASE: TemporalAnalyzer — Análisis temporal de incidentes
# ═══════════════════════════════════════════════════════════════════════════

class TemporalAnalyzer:
    """
    Analiza patrones temporales de incidentes delictivos usando
    MongoDB aggregation pipelines optimizados.
    """

    @staticmethod
    def analyze_by_hour(db, distrito: Optional[str] = None, days: int = DEFAULT_ANALYSIS_DAYS) -> List[Dict]:
        """Distribución de incidentes por hora del día."""
        fecha_inicio = datetime.utcnow() - timedelta(days=days)
        match_stage = {
            "fecha_hecho": {"$gte": fecha_inicio},
            "estado_coord": {"$ne": "SIN COORDENADA"}
        }
        if distrito:
            match_stage["distrito"] = distrito.upper()

        pipeline = [
            {"$match": match_stage},
            {"$project": {
                "hora": {"$hour": "$fecha_hecho"},
                "subtipo_hecho": 1
            }},
            {"$group": {
                "_id": "$hora",
                "total": {"$sum": 1},
                "tipos": {"$push": "$subtipo_hecho"}
            }},
            {"$sort": {"_id": 1}}
        ]

        resultados = list(db.historial_delitos.aggregate(pipeline))
        
        # Llenar horas faltantes con 0
        horas_completas = []
        datos_por_hora = {r["_id"]: r["total"] for r in resultados}
        for h in range(24):
            horas_completas.append({
                "hora": h,
                "turno": get_turno(h),
                "total_incidentes": datos_por_hora.get(h, 0)
            })
        return horas_completas

    @staticmethod
    def analyze_by_day_of_week(db, distrito: Optional[str] = None, days: int = DEFAULT_ANALYSIS_DAYS) -> List[Dict]:
        """Distribución de incidentes por día de la semana."""
        fecha_inicio = datetime.utcnow() - timedelta(days=days)
        match_stage = {
            "fecha_hecho": {"$gte": fecha_inicio},
            "estado_coord": {"$ne": "SIN COORDENADA"}
        }
        if distrito:
            match_stage["distrito"] = distrito.upper()

        pipeline = [
            {"$match": match_stage},
            {"$project": {"dia_semana": {"$dayOfWeek": "$fecha_hecho"}}},
            {"$group": {"_id": "$dia_semana", "total": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]

        resultados = list(db.historial_delitos.aggregate(pipeline))
        datos_dia = {r["_id"]: r["total"] for r in resultados}

        dias = []
        # MongoDB: 1=Domingo, 2=Lunes, ..., 7=Sábado
        nombres = {1: "domingo", 2: "lunes", 3: "martes", 4: "miercoles",
                   5: "jueves", 6: "viernes", 7: "sabado"}
        for d in range(1, 8):
            dias.append({
                "dia": nombres.get(d, "desconocido"),
                "dia_numero": d,
                "total_incidentes": datos_dia.get(d, 0),
                "es_fin_semana": d in (1, 7)
            })
        return dias

    @staticmethod
    def analyze_by_turno(db, distrito: Optional[str] = None, days: int = DEFAULT_ANALYSIS_DAYS) -> List[Dict]:
        """Distribución de incidentes por turno (mañana/tarde/noche/madrugada)."""
        por_hora = TemporalAnalyzer.analyze_by_hour(db, distrito, days)
        turnos_agg = {}
        for h in por_hora:
            turno = h["turno"]
            if turno not in turnos_agg:
                turnos_agg[turno] = {"turno": turno, "total_incidentes": 0, "peso_riesgo": TURNO_WEIGHTS[turno]}
            turnos_agg[turno]["total_incidentes"] += h["total_incidentes"]

        return list(turnos_agg.values())

    @staticmethod
    def calculate_trend(db, distrito: Optional[str] = None, months: int = 6) -> Dict:
        """
        Calcula la tendencia de incidentes comparando periodos.
        Retorna slope y dirección (subiendo/bajando/estable).
        """
        now = datetime.utcnow()
        match_stage = {"estado_coord": {"$ne": "SIN COORDENADA"}}
        if distrito:
            match_stage["distrito"] = distrito.upper()

        pipeline = [
            {"$match": {**match_stage, "fecha_hecho": {"$gte": now - timedelta(days=months * 30)}}},
            {"$group": {
                "_id": {"anio": {"$year": "$fecha_hecho"}, "mes": {"$month": "$fecha_hecho"}},
                "total": {"$sum": 1}
            }},
            {"$sort": {"_id.anio": 1, "_id.mes": 1}}
        ]

        datos = list(db.historial_delitos.aggregate(pipeline))
        if len(datos) < 2:
            return {"tendencia": "estable", "slope": 0.0, "datos_meses": len(datos)}

        totales = [d["total"] for d in datos]
        x = np.arange(len(totales)).reshape(-1, 1)
        y = np.array(totales)

        # Regresión lineal simple para slope
        from sklearn.linear_model import LinearRegression
        model = LinearRegression().fit(x, y)
        slope = float(model.coef_[0])

        if slope > 2:
            tendencia = "subiendo"
        elif slope < -2:
            tendencia = "bajando"
        else:
            tendencia = "estable"

        return {
            "tendencia": tendencia,
            "slope": round(slope, 2),
            "datos_meses": len(datos),
            "ultimo_mes_total": totales[-1] if totales else 0
        }


# ═══════════════════════════════════════════════════════════════════════════
#  CLASE: SafetyScoreCalculator — Score dinámico 0-100
# ═══════════════════════════════════════════════════════════════════════════

class SafetyScoreCalculator:
    """
    Calcula un Safety Score contextual (0-100) basado en:
    - Proximidad a zonas DBSCAN existentes
    - Densidad de incidentes en radio cercano
    - Horario actual (factor temporal)
    - Tendencia del distrito
    """

    @staticmethod
    def calculate(db, lat: float, lng: float, hora: Optional[int] = None) -> Dict:
        """
        Calcula el Safety Score para una ubicación y hora dadas.
        
        Returns:
            Dict con score, nivel, factores detallados e interpretación.
        """
        if hora is None:
            hora = datetime.utcnow().hour

        # ── Factor 1: Proximidad a zonas de riesgo DBSCAN ──
        proximity_factor = SafetyScoreCalculator._calc_proximity_factor(db, lat, lng)

        # ── Factor 2: Densidad de incidentes en radio ──
        density_factor = SafetyScoreCalculator._calc_density_factor(db, lat, lng)

        # ── Factor 3: Horario actual ──
        temporal_factor = SafetyScoreCalculator._calc_temporal_factor(hora)

        # ── Factor 4: Tendencia del distrito más cercano ──
        trend_factor = SafetyScoreCalculator._calc_trend_factor(db, lat, lng)

        # ── Cálculo compuesto ──
        # Base score empieza en 100 y se reduce
        base = 100.0
        score = base * proximity_factor * density_factor * temporal_factor * trend_factor
        score = max(0.0, min(100.0, score))
        score = round(score, 1)

        # Interpretación
        if score >= 80:
            nivel = "seguro"
            color = "#43A047"
            mensaje = "Zona segura en este momento"
        elif score >= 50:
            nivel = "precaucion"
            color = "#FFB300"
            mensaje = "Precaución recomendada"
        else:
            nivel = "alto_riesgo"
            color = "#E53935"
            mensaje = "Alto riesgo — mantente alerta"

        return {
            "score": score,
            "nivel": nivel,
            "color": color,
            "mensaje": mensaje,
            "turno_actual": get_turno(hora),
            "hora_evaluada": hora,
            "factores": {
                "proximidad_zonas": round(proximity_factor, 3),
                "densidad_incidentes": round(density_factor, 3),
                "factor_temporal": round(temporal_factor, 3),
                "factor_tendencia": round(trend_factor, 3),
            }
        }

    @staticmethod
    def _calc_proximity_factor(db, lat: float, lng: float) -> float:
        """Factor 0.3-1.0 basado en cercanía a zonas de riesgo."""
        zonas = list(db.zonas_riesgo.find({}, {"centroide": 1, "radio_metros": 1, "nivel_riesgo": 1}))
        if not zonas:
            return 1.0  # Sin zonas = seguro

        min_ratio = float('inf')
        for zona in zonas:
            centroide = zona.get("centroide", {})
            coords = centroide.get("coordinates", [])
            if len(coords) != 2:
                continue
            z_lng, z_lat = coords[0], coords[1]
            radio = zona.get("radio_metros", 300)
            dist = haversine_meters(lat, lng, z_lat, z_lng)

            # Ratio: dist/radio. Si < 1 estás dentro de la zona
            ratio = dist / max(radio, 1)
            nivel = zona.get("nivel_riesgo", "bajo")
            # Penalización por severidad
            severity_mult = {"critico": 0.6, "alto": 0.7, "medio": 0.85, "bajo": 0.95}.get(nivel, 0.9)
            effective_ratio = ratio * severity_mult if ratio < 3.0 else ratio
            min_ratio = min(min_ratio, effective_ratio)

        if min_ratio <= 0.5:
            return 0.3  # Dentro del núcleo
        elif min_ratio <= 1.0:
            return 0.5  # Dentro de la zona
        elif min_ratio <= 2.0:
            return 0.7  # Zona periférica
        elif min_ratio <= 3.0:
            return 0.85
        return 1.0  # Lejos de todas las zonas

    @staticmethod
    def _calc_density_factor(db, lat: float, lng: float, radius_m: int = 1000) -> float:
        """Factor 0.4-1.0 basado en densidad de incidentes recientes en radio."""
        try:
            fecha_inicio = datetime.utcnow() - timedelta(days=90)
            count = db.historial_delitos.count_documents({
                "ubicacion": {
                    "$nearSphere": {
                        "$geometry": {"type": "Point", "coordinates": [lng, lat]},
                        "$maxDistance": radius_m
                    }
                },
                "fecha_hecho": {"$gte": fecha_inicio},
                "estado_coord": {"$ne": "SIN COORDENADA"}
            })
        except Exception:
            return 0.9  # Si falla la consulta geo, asumir riesgo bajo

        if count >= 50:
            return 0.4
        elif count >= 25:
            return 0.55
        elif count >= 10:
            return 0.7
        elif count >= 5:
            return 0.85
        return 1.0

    @staticmethod
    def _calc_temporal_factor(hora: int) -> float:
        """Factor 0.7-1.0 basado en turno horario."""
        weight = get_turno_weight(hora)
        # Invertir: weight alto = más riesgo = factor bajo
        return max(0.7, 1.0 - (weight - 0.7) * 0.3)

    @staticmethod
    def _calc_trend_factor(db, lat: float, lng: float) -> float:
        """Factor 0.8-1.0 basado en tendencia del distrito cercano."""
        # Buscar distrito más cercano por incidente
        try:
            nearest = db.historial_delitos.find_one(
                {
                    "ubicacion": {
                        "$nearSphere": {
                            "$geometry": {"type": "Point", "coordinates": [lng, lat]},
                            "$maxDistance": 5000
                        }
                    }
                },
                {"distrito": 1}
            )
            if not nearest or not nearest.get("distrito"):
                return 1.0

            trend = TemporalAnalyzer.calculate_trend(db, nearest["distrito"], months=3)
            if trend["tendencia"] == "subiendo":
                return 0.8
            elif trend["tendencia"] == "bajando":
                return 1.0
            return 0.9
        except Exception:
            return 0.95


# ═══════════════════════════════════════════════════════════════════════════
#  CLASE: InsightGenerator — Generación de insights automáticos
# ═══════════════════════════════════════════════════════════════════════════

class InsightGenerator:
    """Genera insights contextuales basados en data real."""

    @staticmethod
    def generate(db, lat: float, lng: float, hora: Optional[int] = None) -> List[Dict]:
        """
        Genera una lista de insights personalizados para la ubicación y hora.
        Cada insight tiene: tipo, icono, mensaje, severidad (info/warning/danger).
        """
        if hora is None:
            hora = datetime.utcnow().hour

        insights = []
        turno = get_turno(hora)

        # ── Insight 1: Análisis temporal del turno actual ──
        try:
            por_turno = TemporalAnalyzer.analyze_by_turno(db)
            turno_data = next((t for t in por_turno if t["turno"] == turno), None)
            if turno_data:
                total_general = sum(t["total_incidentes"] for t in por_turno)
                if total_general > 0:
                    pct = (turno_data["total_incidentes"] / total_general) * 100
                    if pct > 35:
                        insights.append({
                            "tipo": "temporal",
                            "icono": "schedule",
                            "mensaje": f"El turno {turno} concentra el {pct:.0f}% de incidentes. Mantén precaución extra.",
                            "severidad": "warning"
                        })
                    elif pct < 15:
                        insights.append({
                            "tipo": "temporal",
                            "icono": "verified_user",
                            "mensaje": f"Esta zona suele ser más segura durante la {turno}.",
                            "severidad": "info"
                        })
        except Exception:
            pass

        # ── Insight 2: Horario seguro recomendado ──
        try:
            por_hora = TemporalAnalyzer.analyze_by_hour(db)
            if por_hora:
                min_hora = min(por_hora, key=lambda x: x["total_incidentes"])
                max_hora = max(por_hora, key=lambda x: x["total_incidentes"])
                insights.append({
                    "tipo": "recomendacion",
                    "icono": "lightbulb",
                    "mensaje": f"Horario más seguro: {min_hora['hora']:02d}:00. Evitar las {max_hora['hora']:02d}:00.",
                    "severidad": "info"
                })
        except Exception:
            pass

        # ── Insight 3: Tendencia reciente ──
        try:
            nearest = db.historial_delitos.find_one(
                {"ubicacion": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": [lng, lat]}, "$maxDistance": 3000}}},
                {"distrito": 1}
            )
            if nearest and nearest.get("distrito"):
                trend = TemporalAnalyzer.calculate_trend(db, nearest["distrito"], months=3)
                if trend["tendencia"] == "subiendo":
                    insights.append({
                        "tipo": "tendencia",
                        "icono": "trending_up",
                        "mensaje": f"Se detectó incremento de incidentes en {nearest['distrito']} las últimas semanas.",
                        "severidad": "danger"
                    })
                elif trend["tendencia"] == "bajando":
                    insights.append({
                        "tipo": "tendencia",
                        "icono": "trending_down",
                        "mensaje": f"Los incidentes en {nearest['distrito']} muestran tendencia a la baja.",
                        "severidad": "info"
                    })
        except Exception:
            pass

        # ── Insight 4: Día de la semana ──
        try:
            dia_actual = datetime.utcnow().weekday()
            por_dia = TemporalAnalyzer.analyze_by_day_of_week(db)
            if por_dia:
                total_dias = sum(d["total_incidentes"] for d in por_dia)
                if total_dias > 0:
                    # MongoDB dayOfWeek: 1=Dom, Python weekday: 0=Lun → mapear
                    mongo_day = ((dia_actual + 1) % 7) + 1
                    dia_data = next((d for d in por_dia if d["dia_numero"] == mongo_day), None)
                    if dia_data and dia_data["es_fin_semana"]:
                        pct_fw = dia_data["total_incidentes"] / total_dias * 100
                        if pct_fw > 18:
                            insights.append({
                                "tipo": "dia_semana",
                                "icono": "event",
                                "mensaje": f"Los fines de semana concentran mayor incidencia. Hoy es {dia_data['dia']}.",
                                "severidad": "warning"
                            })
        except Exception:
            pass

        # ── Insight 5: Proximidad a zona de riesgo ──
        try:
            zonas = list(db.zonas_riesgo.find({}, {"centroide": 1, "radio_metros": 1, "nivel_riesgo": 1, "delito_predominante": 1}))
            for zona in zonas:
                coords = zona.get("centroide", {}).get("coordinates", [])
                if len(coords) != 2:
                    continue
                dist = haversine_meters(lat, lng, coords[1], coords[0])
                radio = zona.get("radio_metros", 300)
                if dist <= radio * 1.5:
                    nivel = zona.get("nivel_riesgo", "medio").upper()
                    delito = zona.get("delito_predominante", "incidentes")
                    insights.append({
                        "tipo": "proximidad",
                        "icono": "warning",
                        "mensaje": f"Zona de riesgo {nivel} cercana ({dist:.0f}m). Predomina: {delito}.",
                        "severidad": "danger" if nivel in ("CRITICO", "ALTO") else "warning"
                    })
                    break  # Solo el más cercano
        except Exception:
            pass

        return insights[:6]  # Máximo 6 insights


# ═══════════════════════════════════════════════════════════════════════════
#  CLASE: SafeHoursCalculator — Horarios seguros recomendados
# ═══════════════════════════════════════════════════════════════════════════

class SafeHoursCalculator:
    """Determina las franjas horarias más seguras para transitar."""

    @staticmethod
    def calculate(db, distrito: Optional[str] = None) -> Dict:
        """Retorna franjas horarias seguras y peligrosas."""
        por_hora = TemporalAnalyzer.analyze_by_hour(db, distrito)
        if not por_hora:
            return {"horarios_seguros": [], "horarios_riesgo": [], "recomendacion": "Sin datos suficientes"}

        totales = [h["total_incidentes"] for h in por_hora]
        if max(totales) == 0:
            return {"horarios_seguros": list(range(24)), "horarios_riesgo": [], "recomendacion": "Sin incidentes registrados"}

        avg = np.mean(totales)
        std = np.std(totales) if np.std(totales) > 0 else 1

        seguros = []
        riesgo = []
        for h in por_hora:
            z_score = (h["total_incidentes"] - avg) / std
            if z_score < -0.5:
                seguros.append(h["hora"])
            elif z_score > 0.5:
                riesgo.append(h["hora"])

        # Generar recomendación textual
        if seguros:
            rango_seguro = f"{min(seguros):02d}:00 - {max(seguros):02d}:00"
            recomendacion = f"Horario recomendado para transitar: {rango_seguro}"
        else:
            recomendacion = "No se identificó un rango horario con riesgo significativamente menor"

        return {
            "horarios_seguros": seguros,
            "horarios_riesgo": riesgo,
            "recomendacion": recomendacion,
            "distribucion_horas": por_hora
        }


# ═══════════════════════════════════════════════════════════════════════════
#  FUNCIÓN: Calcular tendencia real para el motor DBSCAN
# ═══════════════════════════════════════════════════════════════════════════

def calcular_tendencia_real(db, lat: float, lng: float, radio_m: int = 500) -> str:
    """
    Calcula la tendencia real de una zona comparando últimos 3 meses vs 3 meses anteriores.
    Usado por motor_ia_zonas_riesgo.py para reemplazar el hardcode 'subiendo'.
    """
    now = datetime.utcnow()
    periodo_reciente_inicio = now - timedelta(days=90)
    periodo_anterior_inicio = now - timedelta(days=180)

    base_query = {
        "ubicacion": {
            "$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": [lng, lat]},
                "$maxDistance": radio_m
            }
        },
        "estado_coord": {"$ne": "SIN COORDENADA"}
    }

    try:
        recientes = db.historial_delitos.count_documents({
            **base_query,
            "fecha_hecho": {"$gte": periodo_reciente_inicio}
        })
        anteriores = db.historial_delitos.count_documents({
            **base_query,
            "fecha_hecho": {"$gte": periodo_anterior_inicio, "$lt": periodo_reciente_inicio}
        })
    except Exception:
        return "estable"

    if anteriores == 0 and recientes == 0:
        return "estable"
    if anteriores == 0:
        return "subiendo"
    
    cambio = (recientes - anteriores) / max(anteriores, 1)
    if cambio > 0.15:
        return "subiendo"
    elif cambio < -0.15:
        return "bajando"
    return "estable"
