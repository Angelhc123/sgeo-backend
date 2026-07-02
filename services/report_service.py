from datetime import datetime
from bson.objectid import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException
from utils.time_helpers import get_turno, get_local_time

def validar_limite_diario(db, user_id_obj: ObjectId):
    hoy_inicio = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    reportes_hoy = db.reportes_ciudadano.count_documents({
        "usuario_id": user_id_obj,
        "creado_en": {"$gte": hoy_inicio}
    })
    if reportes_hoy >= 5:
        raise HTTPException(status_code=429, detail="Has alcanzado el límite de 5 reportes por día.")
    return True

def construir_metadatos_reporte(reporte, user_id_obj: ObjectId | None) -> dict:
    server_now = datetime.utcnow()
    local_time = get_local_time(reporte.timezone)
    
    hora = local_time.hour
    turno_hecho = get_turno(hora)
        
    dias = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]
    dia_semana = dias[local_time.weekday()]

    nuevo_reporte = {
        "anonimo": user_id_obj is None,
        "usuario_id": user_id_obj,
        "tipo_hecho": "PATRIMONIO (DELITO)",
        "subtipo_hecho": reporte.subtipo_hecho.upper(),
        "modalidad_hecho": reporte.modalidad_hecho.upper() if reporte.modalidad_hecho else "NO ESPECIFICADO",
        "ubicacion": {
            "type": "Point",
            "coordinates": [reporte.longitud, reporte.latitud]
        },
        "direccion_hecho": reporte.direccion_hecho,
        "distrito_hecho": reporte.distrito_hecho.upper() if reporte.distrito_hecho else "TACNA",
        "provincia_hecho": reporte.provincia_hecho.upper(),
        "departamento_hecho": reporte.departamento_hecho.upper(),
        "descripcion": reporte.descripcion,
        "estado": "pendiente",
        "fuente": reporte.fuente,
        "gravedad": reporte.gravedad,
        
        # Temporalidad estricta (IA/SIDPOL Compatible)
        "fecha_hora_hecho": server_now,
        "timestamp_utc": server_now.isoformat() + "Z",
        "hora_local": local_time.strftime("%H:%M"),
        "anio": local_time.year,
        "mes": local_time.month,
        "dia": local_time.day,
        "dia_semana": dia_semana,
        "turno_hecho": turno_hecho,
        
        # Metadatos tecnicos y auditoria
        "precision_gps": reporte.precision_gps,
        "metadata_contextual": reporte.metadata_contextual,
        "device_timestamp": reporte.device_timestamp,
        "creado_en": server_now,
        "actualizado_en": server_now
    }
    return nuevo_reporte

def confirmar_reporte_en_db(db, reporte_id: str) -> dict:
    try:
        reporte_obj_id = ObjectId(reporte_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="ID de reporte inválido")
        
    # Verificar que el reporte existe y esta pendiente
    reporte = db.reportes_ciudadano.find_one({"_id": reporte_obj_id})
    if not reporte:
        raise HTTPException(status_code=404, detail="Reporte no encontrado")
        
    if reporte.get("estado") == "confirmado":
        raise HTTPException(status_code=400, detail="El reporte ya está confirmado")

    lat = None
    lng = None
    if "ubicacion" in reporte:
        coords = reporte["ubicacion"].get("coordinates", [])
        if len(coords) == 2:
            lng, lat = coords[0], coords[1]
            
            # Copiar este reporte validado a la tabla estandarizada HISTORIAL_DELITOS PRIMERO
            try:
                db.historial_delitos.insert_one({
                    "ubicacion": reporte["ubicacion"],
                    "direccion": reporte.get("direccion_hecho", "Ubicación reportada vía app"),
                    "tipo_via": "Otros",
                    "departamento_hecho": reporte.get("departamento_hecho", "TACNA"),
                    "provincia_hecho": reporte.get("provincia_hecho", "TACNA"),
                    "distrito_hecho": reporte.get("distrito_hecho", "TACNA"),
                    "ubigeo": reporte.get("ubigeo", ""),
                    "fecha_hecho": reporte.get("fecha_hora_hecho", datetime.utcnow()),
                    "turno_hecho": reporte.get("turno_hecho", "NO ESPECIFICADO"),
                    "tipo_hecho": reporte.get("tipo_hecho", "PATRIMONIO (DELITO)"),
                    "subtipo_hecho": reporte.get("subtipo_hecho", "DESCONOCIDO"),
                    "modalidad_hecho": reporte.get("modalidad_hecho", "NO ESPECIFICADO"),
                    "estado_coord": "VALIDADO APP",
                    "estado": "confirmado",
                    "fuente": "ciudadano",
                    "creado_en": datetime.utcnow(),
                    # Preservar metadatos temporales y analíticos para AI
                    "anio": reporte.get("anio"),
                    "mes": reporte.get("mes"),
                    "dia": reporte.get("dia"),
                    "dia_semana": reporte.get("dia_semana"),
                    "timestamp_utc": reporte.get("timestamp_utc"),
                    "hora_local": reporte.get("hora_local"),
                    "precision_gps": reporte.get("precision_gps"),
                    "gravedad": reporte.get("gravedad"),
                    "metadata_contextual": reporte.get("metadata_contextual")
                })
            except Exception as ex_import:
                print("No se pudo copiar el reporte validado al historial:", ex_import)
                # BUG 2 CORRECTION: si falla historial_delitos, no se confirma el reporte.
                raise HTTPException(status_code=500, detail="Error al validar reporte en historial_delitos: " + str(ex_import))

            # Agrupar reportes pendientes cercanos (radio de 500 metros) del mismo tipo
            try:
                db.reportes_ciudadano.update_many(
                    {
                        "_id": {"$ne": reporte_obj_id},
                        "estado": "pendiente",
                        "subtipo_hecho": reporte.get("subtipo_hecho"),
                        "ubicacion": {
                            "$near": {
                                "$geometry": {
                                    "type": "Point",
                                    "coordinates": [lng, lat]
                                },
                                "$maxDistance": 500 # 500 metros a la redonda
                            }
                        }
                    },
                    {
                        "$set": {
                            "estado": "agrupado",
                            "agrupado_con": str(reporte_obj_id),
                            "confirmado_en": datetime.utcnow()
                        }
                    }
                )
            except Exception as grouping_err:
                print("Aviso: No se pudo agrupar reportes cercanos:", grouping_err)

    # Ahora sí actualizamos el reporte original como confirmado
    resultado = db.reportes_ciudadano.update_one(
        {"_id": reporte_obj_id},
        {"$set": {"estado": "confirmado", "confirmado_en": datetime.utcnow()}}
    )
    
    if resultado.modified_count == 0:
        raise HTTPException(status_code=500, detail="No se pudo actualizar el estado del reporte")

    return {
        "status": "success", 
        "mensaje": "Reporte confirmado, IA recalculando zonas y Alerta enviada",
        "lat": lat,
        "lng": lng,
        "subtipo_hecho": reporte.get("subtipo_hecho", "Desconocido")
    }

def rechazar_reporte_en_db(db, reporte_id: str) -> dict:
    try:
        reporte_obj_id = ObjectId(reporte_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="ID de reporte inválido")
        
    resultado = db.reportes_ciudadano.update_one(
        {"_id": reporte_obj_id},
        {"$set": {"estado": "rechazado", "rechazado_en": datetime.utcnow()}}
    )
    if resultado.modified_count == 0:
        raise HTTPException(status_code=404, detail="Reporte no encontrado o ya procesado")

    reporte = db.reportes_ciudadano.find_one({"_id": reporte_obj_id})
    if reporte and "ubicacion" in reporte:
        coords = reporte["ubicacion"].get("coordinates", [])
        if len(coords) == 2:
            lng, lat = coords[0], coords[1]
            
            # Agrupar reportes "invalidados/falsos" en radio 500m
            try:
                db.reportes_ciudadano.update_many(
                    {
                        "_id": {"$ne": reporte_obj_id},
                        "estado": "pendiente",
                        "subtipo_hecho": reporte.get("subtipo_hecho"),
                        "ubicacion": {
                            "$near": {
                                "$geometry": {
                                    "type": "Point",
                                    "coordinates": [lng, lat]
                                },
                                "$maxDistance": 500
                            }
                        }
                    },
                    {
                        "$set": {
                            "estado": "rechazado",
                            "rechazado_con": str(reporte_obj_id)
                        }
                    }
                )
            except Exception as grouping_err:
                print("Aviso: No se pudo rechazar reportes cercanos:", grouping_err)
    
    return {"status": "success", "mensaje": "Rechazado correctamente"}
