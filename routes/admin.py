# Se agregan endpoints de gestion de usuarios + flujo de aprobacion policial (con guard de rol por header).
from fastapi import APIRouter, HTTPException, Header, Depends
from bson.objectid import ObjectId
from bson.errors import InvalidId
import asyncio
from config.database import db
from services.analytics_service import calcular_prediccion
from services import email_service
from models.user_schemas import RechazoUsuarioRequest


def require_admin_role(x_user_role: str = Header(None)):
    """
    Validacion de rol "cosmetica": el cliente envia su propio rol (guardado en
    SharedPreferences) por header. No es seguridad real -cualquiera puede falsificar
    el header con curl- pero evita que la app llame estas rutas por accidente desde
    un rol equivocado. No hay sesion/token en el backend (ver routes/auth.py) para
    hacer una validacion real todavia.
    """
    if x_user_role != "admin":
        raise HTTPException(status_code=403, detail="Acceso solo para administradores")


router = APIRouter(prefix="/api/admin", tags=["admin"], dependencies=[Depends(require_admin_role)])

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


def _obj_id_o_400(user_id: str) -> ObjectId:
    try:
        return ObjectId(user_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="ID de usuario invalido")


@router.get("/usuarios")
def listar_usuarios(rol: str = None, pendiente: bool = None):
    """
    Lista usuarios (sin password_hash). ?pendiente=true tiene prioridad sobre ?rol y
    filtra exactamente los policias con aprobacion_pendiente=True.
    """
    query = {}
    if pendiente:
        query["rol"] = "policia"
        query["aprobacion_pendiente"] = True
    elif rol:
        query["rol"] = rol

    usuarios = list(db.usuarios.find(query, {"password_hash": 0}))
    for u in usuarios:
        u["_id"] = str(u["_id"])
        if u.get("creado_en"):
            u["creado_en"] = u["creado_en"].isoformat()

    return {"status": "success", "usuarios": usuarios}


@router.put("/usuarios/{user_id}/aprobar")
async def aprobar_usuario(user_id: str):
    user_obj_id = _obj_id_o_400(user_id)

    user = db.usuarios.find_one({"_id": user_obj_id})
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    db.usuarios.update_one(
        {"_id": user_obj_id},
        {"$set": {"aprobacion_pendiente": False, "activo": True}}
    )

    try:
        await email_service.notificar_policia_aprobado(email=user["email"], nombre=user["nombre"])
    except Exception as e:
        print(f"Aviso: fallo el envio de email de aprobacion a {user.get('email')}: {e}")

    return {"status": "success", "mensaje": "Cuenta aprobada y policía notificado"}


@router.put("/usuarios/{user_id}/rechazar")
async def rechazar_usuario(user_id: str, body: RechazoUsuarioRequest):
    user_obj_id = _obj_id_o_400(user_id)

    user = db.usuarios.find_one({"_id": user_obj_id})
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    db.usuarios.update_one(
        {"_id": user_obj_id},
        {"$set": {"activo": False, "aprobacion_pendiente": False, "motivo_rechazo": body.motivo}}
    )

    try:
        await email_service.notificar_policia_rechazado(email=user["email"], nombre=user["nombre"], motivo=body.motivo)
    except Exception as e:
        print(f"Aviso: fallo el envio de email de rechazo a {user.get('email')}: {e}")

    return {"status": "success", "mensaje": "Cuenta rechazada y policía notificado"}


@router.delete("/usuarios/{user_id}")
def eliminar_usuario(user_id: str):
    user_obj_id = _obj_id_o_400(user_id)

    resultado = db.usuarios.delete_one({"_id": user_obj_id})
    if resultado.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    return {"status": "success", "mensaje": "Usuario eliminado"}


@router.put("/usuarios/{user_id}/suspender")
def suspender_usuario(user_id: str):
    """Toggle: si activo==True pasa a False y viceversa."""
    user_obj_id = _obj_id_o_400(user_id)

    user = db.usuarios.find_one({"_id": user_obj_id})
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    nuevo_valor = not user.get("activo", True)
    db.usuarios.update_one({"_id": user_obj_id}, {"$set": {"activo": nuevo_valor}})

    return {"status": "success", "activo": nuevo_valor}
