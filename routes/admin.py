from fastapi import APIRouter, HTTPException
from config.database import db
import asyncio
from services.analytics_service import calcular_prediccion

router = APIRouter(prefix="/api/admin", tags=["admin"])

@router.get("/dashboard_stats")
def obtener_dashboard_stats(filtro_tiempo: str = 'Todos'):
    try:
        from datetime import datetime, timedelta
        import pytz

        # Configurar filtro de tiempo
        query = {}
        now = datetime.now(pytz.utc)
        if filtro_tiempo == 'Mes Actual':
            inicio = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            query["creado_en"] = {"$gte": inicio}
        elif filtro_tiempo == 'Ultimos 3 Meses':
            inicio = now - timedelta(days=90)
            query["creado_en"] = {"$gte": inicio}
        elif filtro_tiempo == 'Ultimos 6 Meses':
            inicio = now - timedelta(days=180)
            query["creado_en"] = {"$gte": inicio}

        # Realizar agregaciones
        pipeline_total = [{"$match": query}, {"$count": "total"}]
        pipeline_estado = [{"$match": query}, {"$group": {"_id": "$estado", "count": {"$sum": 1}}}]
        pipeline_tipo = [{"$match": query}, {"$group": {"_id": "$subtipo_hecho", "count": {"$sum": 1}}}]

        total_cursor = list(db.reportes_ciudadano.aggregate(pipeline_total))
        total = total_cursor[0]["total"] if total_cursor else 0

        estado_cursor = list(db.reportes_ciudadano.aggregate(pipeline_estado))
        por_estado = {doc["_id"] or "desconocido": doc["count"] for doc in estado_cursor}

        tipo_cursor = list(db.reportes_ciudadano.aggregate(pipeline_tipo))
        por_tipo = {doc["_id"] or "desconocido": doc["count"] for doc in tipo_cursor}

        return {
            "status": "success",
            "stats": {
                "total": total,
                "por_estado": por_estado,
                "por_tipo": por_tipo
            }
        }
    except Exception as e:
        print("Error dashboard stats:", e)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sidpol_stats")
def obtener_sidpol_stats():
    try:
        pipeline_distrito = [
            {"$group": {"_id": "$distrito", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": 5}
        ]
        
        pipeline_tipo = [
            {"$group": {"_id": "$subtipo_hecho", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": 5}
        ]
        
        pipeline_tiempo = [
            {"$group": {
                "_id": {
                    "anio": {"$year": "$fecha_hecho"}, 
                    "mes": {"$month": "$fecha_hecho"}
                }, 
                "total": {"$sum": 1}
            }},
            {"$sort": {"_id.anio": 1, "_id.mes": 1}}
        ]

        distritos = list(db.historial_delitos.aggregate(pipeline_distrito))
        tipos = list(db.historial_delitos.aggregate(pipeline_tipo))
        tiempo = list(db.historial_delitos.aggregate(pipeline_tiempo))

        return {
            "status": "success",
            "stats": {
                "por_distrito": {d["_id"] if d["_id"] else "Desconocido": d["total"] for d in distritos},
                "por_tipo": {t["_id"] if t["_id"] else "Desconocido": t["total"] for t in tipos},
                "linea_tiempo": [{"anio": t["_id"]["anio"], "mes": t["_id"]["mes"], "total": t["total"]} for t in tiempo]
            }
        }
    except Exception as e:
        print("Error sidpol stats:", e)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sidpol_predict")
async def obtener_sidpol_prediccion():
    try:
        pipeline = [
            {"$group": {
                "_id": {
                    "distrito": "$distrito", 
                    "anio": {"$year": "$fecha_hecho"}, 
                    "mes": {"$month": "$fecha_hecho"}
                }, 
                "total": {"$sum": 1}
            }}
        ]
        data = list(db.historial_delitos.aggregate(pipeline))
        result = await asyncio.get_event_loop().run_in_executor(None, calcular_prediccion, data)
        return result
    except Exception as e:
        print("Error sidpol predict:", e)
        raise HTTPException(status_code=500, detail=str(e))