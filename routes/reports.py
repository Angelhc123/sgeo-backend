from fastapi import APIRouter, HTTPException, BackgroundTasks
from bson.objectid import ObjectId
from bson.errors import InvalidId

from config.database import db
from models.report_schemas import ReporteCiudadano
from firebase_service import send_push_notification
from motor_ia_zonas_riesgo import ejecutar_ia_zonas_riesgo
from services.report_service import (
    validar_limite_diario,
    construir_metadatos_reporte,
    confirmar_reporte_en_db,
    rechazar_reporte_en_db
)

router = APIRouter(prefix="/api/reportes", tags=["reports"])

@router.post("")
def crear_reporte(reporte: ReporteCiudadano):
    """
    Recibe un reporte del ciudadano desde la App y lo guarda como 'pendiente'.
    Limita a 5 reportes por dia por usuario (si esta logeado).
    """
    try:
        user_id_obj = None
        if reporte.usuario_id and reporte.usuario_id.strip():
            try:
                user_id_obj = ObjectId(reporte.usuario_id.strip())
                # Validar límite usando función extraída
                validar_limite_diario(db, user_id_obj)
            except InvalidId:
                # BUG 1 CORRECTION: No silenciar, lanzar excepción.
                raise HTTPException(status_code=400, detail="ID de usuario inválido")

        nuevo_reporte = construir_metadatos_reporte(reporte, user_id_obj)
        resultado = db.reportes_ciudadano.insert_one(nuevo_reporte)
        return {"status": "success", "id_reporte": str(resultado.inserted_id), "mensaje": "Reporte enviado con exito"}
    except HTTPException:
        raise
    except Exception as e:
        print("Error guardando reporte:", str(e))
        raise HTTPException(status_code=500, detail="Error guardando reporte: " + str(e))

@router.post("/confirmar/{reporte_id}")
def confirmar_reporte(reporte_id: str, background_tasks: BackgroundTasks):
    """
    Ruta para la Policia: Aprueba un reporte ciudadano y dispara la IA y un Push Notification con Coordenadas.
    """
    try:
        resultado = confirmar_reporte_en_db(db, reporte_id)
        
        lat = resultado.get("lat")
        lng = resultado.get("lng")
        subtipo = resultado.get("subtipo_hecho")

        # 2. Mandar la notificacion a los ciudadanos con el punto GPS exacto
        send_push_notification(
            title="🚔 ALERTA: Nuevo Incidente Confirmado",
            body=f"Se ha confirmado un incidente del tipo {subtipo}. Toca para ver.",
            tipo_alerta="incident",
            topic="alertas_ciudadanos",
            lat=lat,
            lng=lng
        )

        # 3. Lanzar la IA silenciosamente
        background_tasks.add_task(ejecutar_ia_zonas_riesgo)

        return {"status": "success", "mensaje": resultado["mensaje"]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/rechazar/{reporte_id}")
def rechazar_reporte(reporte_id: str):
    """
    Ruta para la Policia: Rechaza un reporte ciudadano.
    """
    try:
        resultado = rechazar_reporte_en_db(db, reporte_id)
        return resultado
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/mis_reportes/{user_id}")
def obtener_mis_reportes(user_id: str):
    try:
        try:
            user_obj_id = ObjectId(user_id)
        except InvalidId:
            raise HTTPException(status_code=400, detail="ID de usuario invalido")
            
        reportes = list(db.reportes_ciudadano.find({"usuario_id": user_obj_id}).sort("creado_en", -1))
        for r in reportes:
            r["_id"] = str(r["_id"])
            if "usuario_id" in r and r["usuario_id"]:
                r["usuario_id"] = str(r["usuario_id"])
            if "creado_en" in r:
                r["creado_en"] = r["creado_en"].isoformat()
            if "fecha_hecho" in r:
                r["fecha_hecho"] = r["fecha_hecho"].isoformat()
                
        return {"status": "success", "reportes": reportes}
    except HTTPException:
        raise
    except Exception as e:
        print("Error en obtener_mis_reportes:", e)
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{reporte_id}")
def eliminar_mi_reporte(reporte_id: str):
    """
    Ruta para el ciudadano: Permite eliminar un reporte 'pendiente'.
    """
    try:
        try:
            rep_obj_id = ObjectId(reporte_id)
        except InvalidId:
            raise HTTPException(status_code=400, detail="ID de reporte inválido")
            
        reporte = db.reportes_ciudadano.find_one({"_id": rep_obj_id})
        if not reporte:
            raise HTTPException(status_code=404, detail="Reporte no encontrado")
            
        if reporte.get("estado") != "pendiente":
            raise HTTPException(status_code=400, detail="Solo se pueden eliminar reportes pendientes.")

        resultado = db.reportes_ciudadano.delete_one({"_id": rep_obj_id})
        if resultado.deleted_count == 1:
            return {"status": "success", "message": "Reporte eliminado exitosamente."}
        else:
            raise HTTPException(status_code=500, detail="No se pudo eliminar el reporte.")
            
    except HTTPException:
        raise
    except Exception as e:
        print("Error al eliminar el reporte:", e)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/policia")
def obtener_reportes_policia():
    """
    Devuelve TODOS los reportes (pendientes, confirmados y rechazados) para el mapa y las
    validaciones del rol policia. Se agrego "rechazado" al filtro (antes se excluia por completo,
    lo que hacia imposible construir un tab de Historial o un contador de "rechazados hoy") y se
    proyectan gravedad/confirmado_en/rechazado_en para los contadores y el detalle del reporte.
    """
    try:
        reportes = list(db.reportes_ciudadano.find(
            {"estado": {"$in": ["pendiente", "confirmado", "rechazado"]}},
            {"_id": 1, "subtipo_hecho": 1, "ubicacion": 1, "estado": 1, "descripcion": 1, "direccion_hecho": 1,
             "fecha_hora_hecho": 1, "anonimo": 1, "modalidad_hecho": 1, "gravedad": 1,
             "confirmado_en": 1, "rechazado_en": 1}
        ))
        for rep in reportes:
            rep["_id"] = str(rep["_id"])

        return {"status": "success", "puntos": reportes, "reportes": reportes}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error obteniendo reportes: " + str(e))
