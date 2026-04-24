from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pydantic import BaseModel, EmailStr
import bcrypt
import os
import time
import datetime as dt
import threading
from dotenv import load_dotenv

# Importar servicios de Notificaciones
from firebase_service import init_firebase, send_push_notification

# Importar el motor de IA
from motor_ia_espacial import ejecutar_ia_zonas_riesgo

# Cargar variables de entorno
load_dotenv()

app = FastAPI(title="SGEO API - Geolocalizaci�n de Inseguridad")

# Inicializamos Firebase al encender
init_firebase()

# Evento de inicio: Ejecutar la IA de fondo una vez cuando el servidor encienda
@app.on_event("startup")
def startup_event():
    print("?? Servidor iniciado. Ejecutando motor de IA espacial en segundo plano...")
    # Usamos un hilo para que la IA matem�tica no bloquee el encendido del servidor
    thread = threading.Thread(target=ejecutar_ia_zonas_riesgo)
    thread.start()

# Configuracion de CORS (para permitir que la app se comunique)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URL = os.getenv("MONGO_URL")

try:
    client = MongoClient(MONGO_URL)
    db = client["geocrimen_tacna"]
    # Asegurar el índice para consultas geográficas
    db.reportes_ciudadano.create_index([("ubicacion", "2dsphere")])
    print("Conectado exitosamente a MongoDB en Railway")
except Exception as e:
    print(f"Error conectando a la base de datos: {e}")

# ================================
# MODELOS PYDANTIC
# ================================
class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class RegisterRequest(BaseModel):
    nombre: str
    email: EmailStr
    password: str
    rol: str = "ciudadano"
    is_active: bool = True

# Funciones de utilidad para constrase�as
def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    pwd_bytes = password.encode('utf-8')
    return bcrypt.hashpw(pwd_bytes, salt).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(
            plain_password.encode('utf-8'),
            hashed_password.encode('utf-8')
        )
    except Exception:
        return False

# ================================
# RUTAS DE AUTENTICACION
# ================================
@app.post("/api/auth/login")
def login(req: LoginRequest):
    # Buscar usuario
    user = db.usuarios.find_one({"email": req.email})
    if not user:
        raise HTTPException(status_code=401, detail="Correo o contrase�a incorrectos")
    
    if not user.get("activo", True):
        raise HTTPException(status_code=403, detail="Tu cuenta est� inactiva")

    # Verificar password
    if not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Correo o contrase�a incorrectos")
    
    return {
        "status": "success",
        "usuario": {
            "id": str(user["_id"]),
            "nombre": user["nombre"],
            "email": user["email"],
            "rol": user["rol"]
        }
    }

@app.post("/api/auth/register")
def register(req: RegisterRequest):
    # Verificar si existe el email
    existing_user = db.usuarios.find_one({"email": req.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="Este correo ya est� registrado")
    
    # Verificar si existe el nombre de usuario
    existing_name = db.usuarios.find_one({"nombre": req.nombre})
    if existing_name:
        raise HTTPException(status_code=400, detail="Usuario inv�lido: ya hay otra cuenta con este nombre")
    
    # Crear usuario
    nuevo_usuario = {
        "nombre": req.nombre,
        "email": req.email,
        "password_hash": hash_password(req.password),
        "rol": req.rol,
        "activo": req.is_active,
        "creado_en": dt.datetime.now(dt.timezone.utc)
    }
    
    result = db.usuarios.insert_one(nuevo_usuario)
    return {
        "status": "success",
        "mensaje": "Usuario registrado correctamente",
        "usuario_id": str(result.inserted_id)
    }

# Rutas de prueba
@app.get("/")
def read_root():
    return {"mensaje": "Bienvenido al Backend de SGEO"}

@app.get("/test-db")
def test_db_connection():
    try:
        collections = db.list_collection_names()
        return {"status": "success", "colecciones": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ================================
# RUTAS DE MAPAS Y ZONAS DE RIESGO
# ================================
@app.post("/api/map/generar_zonas_ia")
def desencadenar_ia_zonas(background_tasks: BackgroundTasks):
    """
    Ruta administrativa silenciosa. 
    Lanza el motor matem�tico sin trabar la respuesta del servidor.
    Se llamar� autom�ticamente cada vez que un polic�a apruebe un nuevo incidente.
    """
    background_tasks.add_task(ejecutar_ia_zonas_riesgo)
    return {"status": "success", "mensaje": "IA iniciada en segundo plano."}

_cache_zonas = None
_cache_time = 0

@app.get("/api/map/zonas_riesgo")
def obtner_zonas_riesgo():
    global _cache_zonas, _cache_time
    if _cache_zonas is not None and (time.time() - _cache_time) < 600:
        return {"status": "success", "zonas": _cache_zonas, "cached": True}
        
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
        
        _cache_zonas = zonas
        _cache_time = time.time()
        
        return {"status": "success", "zonas": zonas, "cached": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error obteniendo zonas de riesgo: " + str(e))

# ==========================================
# RUTAS DE REPORTES E INCIDENTES
# ==========================================

from datetime import datetime

from typing import Optional
from bson.errors import InvalidId

class ReporteCiudadano(BaseModel):
    sub_tipo: str # "HURTO", "ROBO", "EXTORSION"
    modalidad: str = "PUNTEO/ARREBATO"
    latitud: float
    longitud: float
    direccion: str = "Ubicacion reportada por mapa"
    distrito: str = "TACNA"
    descripcion: str = ""
    relacion_incidente: str = "Fui testigo presencial" # "Fui testigo presencial", "Familiar / Conocido"
    usuario_id: Optional[str] = None

@app.post("/api/reportes")
def crear_reporte(reporte: ReporteCiudadano):
    """
    Recibe un reporte del ciudadano desde la App y lo guarda como 'pendiente'.
    Limita a 5 reportes por dia por usuario (si esta logeado).
    """
    try:
        user_id_obj = None
        if hasattr(reporte, 'usuario_id') and reporte.usuario_id:
            try:
                from bson.objectid import ObjectId
                from bson.errors import InvalidId
                user_id_obj = ObjectId(reporte.usuario_id)
                
                # Check rate limit (5 per day)
                hoy_inicio = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                reportes_hoy = db.reportes_ciudadano.count_documents({
                    "usuario_id": user_id_obj,
                    "creado_en": {"$gte": hoy_inicio}
                })
                if reportes_hoy >= 5:
                    from fastapi import HTTPException
                    raise HTTPException(status_code=429, detail="Has alcanzado el límite de 5 reportes por día.")
            except HTTPException:
                raise
            except Exception:
                pass
                
        nuevo_reporte = {
            "anonimo": True,
            "usuario_id": user_id_obj,
            "tipo": "PATRIMONIO (DELITO)",
            "sub_tipo": reporte.sub_tipo,
            "modalidad": reporte.modalidad,
            "ubicacion": {
                "type": "Point",
                "coordinates": [reporte.longitud, reporte.latitud] # GeoJSON pide primero Longitud, luego Latitud
            },
            "direccion": reporte.direccion,
            "distrito": reporte.distrito,
            "relacion_incidente": reporte.relacion_incidente, # NUEVO: Guardamos qui�n lo reporta
            "fecha_hecho": datetime.utcnow(),
            "descripcion": reporte.descripcion,
            "estado": "pendiente", # Siempre nace como pendiente hasta que un policia verifique
            "creado_en": datetime.utcnow()
        }
        resultado = db.reportes_ciudadano.insert_one(nuevo_reporte)
        return {"status": "success", "id_reporte": str(resultado.inserted_id), "mensaje": "Reporte enviado con �xito"}
    except Exception as e:
        print("Error guardando reporte:", str(e))
        raise HTTPException(status_code=500, detail="Error guardando reporte: " + str(e))
@app.post("/api/reportes/confirmar/{reporte_id}")
def confirmar_reporte(reporte_id: str, background_tasks: BackgroundTasks):
    """
    Ruta para la Policia: Aprueba un reporte ciudadano y dispara la IA y un Push Notification con Coordenadas.
    """
    from bson.objectid import ObjectId
    from bson.errors import InvalidId
    try:
        resultado = db.reportes_ciudadano.update_one(
            {"_id": ObjectId(reporte_id)},
            {"$set": {"estado": "confirmado", "confirmado_en": datetime.utcnow()}}
        )
        if resultado.modified_count == 0:
            raise HTTPException(status_code=404, detail="Reporte no encontrado o ya confirmado")

        # 1. Traer la info del reporte para sacar las coordenadas
        reporte = db.reportes_ciudadano.find_one({"_id": ObjectId(reporte_id)})
        lat = None
        lng = None
        if reporte and "ubicacion" in reporte:
            coords = reporte["ubicacion"].get("coordinates", [])
            if len(coords) == 2:
                lng, lat = coords[0], coords[1]
                
                # 1.5. Agrupar reportes pendientes cercanos (radio de 100 metros) del mismo tipo
                try:
                    db.reportes_ciudadano.update_many(
                        {
                            "_id": {"$ne": ObjectId(reporte_id)},
                            "estado": "pendiente",
                            "sub_tipo": reporte.get("sub_tipo"),
                            "ubicacion": {
                                "$near": {
                                    "$geometry": {
                                        "type": "Point",
                                        "coordinates": [lng, lat]
                                    },
                                    "$maxDistance": 100 # 100 metros a la redonda
                                }
                            }
                        },
                        {
                            "$set": {
                                "estado": "agrupado",
                                "agrupado_con": reporte_id,
                                "confirmado_en": datetime.utcnow()
                            }
                        }
                    )
                except Exception as grouping_err:
                    print("Aviso: No se pudo agrupar reportes cercanos:", grouping_err)

        # 2. Mandar la notificacion a los ciudadanos con el punto GPS exacto
        send_push_notification(
            title="🚔 ALERTA: Nuevo Incidente Confirmado",
            body=f"Se ha confirmado un incidente del tipo {reporte.get('sub_tipo', 'Desconocido')}. Toca para ver.",
            tipo_alerta="incident",
            topic="alertas_ciudadanos",
            lat=lat,
            lng=lng
        )

        # 3. Lanzar la IA silenciosamente
        background_tasks.add_task(ejecutar_ia_zonas_riesgo)

        return {"status": "success", "mensaje": "Reporte confirmado, IA recalculando zonas y Alerta enviada"}
    except InvalidId:
        raise HTTPException(status_code=400, detail="ID de reporte invlido")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/reportes/rechazar/{reporte_id}")
def rechazar_reporte(reporte_id: str):
    """
    Ruta para la Policia: Rechaza un reporte ciudadano.
    """
    from bson.objectid import ObjectId
    from bson.errors import InvalidId
    try:
        resultado = db.reportes_ciudadano.update_one(
            {"_id": ObjectId(reporte_id)},
            {"$set": {"estado": "rechazado"}}
        )
        if resultado.modified_count == 0:
            raise HTTPException(status_code=404, detail="Reporte no encontrado o ya procesado")
        
        return {"status": "success", "mensaje": "Rechazado correctamente"}
    except InvalidId:
        raise HTTPException(status_code=400, detail="ID de reporte invlido")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reportes/mis_reportes/{user_id}")
def obtener_mis_reportes(user_id: str):
    try:
        from bson.objectid import ObjectId
        from bson.errors import InvalidId
        try:
            user_obj_id = ObjectId(user_id)
        except InvalidId:
            raise HTTPException(status_code=400, detail="ID de usuario inv�lido")
            
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
    except Exception as e:
        print("Error en obtener_mis_reportes:", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/reportes/{reporte_id}")
def eliminar_mi_reporte(reporte_id: str):
    """
    Ruta para el ciudadano: Permite eliminar un reporte 'pendiente'.
    """
    try:
        from bson.objectid import ObjectId
        from bson.errors import InvalidId
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

@app.get("/api/map/puntos_exactos")
def obtener_puntos_exactos():
    """
    Devuelve SOLO los reportes que hayan sido CONFIRMADOS por la polic�a.
    Esto evita falsos positivos y sesgos en el mapa de calor de la IA.
    """
    try:
        # Filtro muy importante: {"estado": "confirmado"}
        reportes = list(db.reportes_ciudadano.find(
            {"estado": "confirmado"}, 
            {"_id": 1, "sub_tipo": 1, "ubicacion": 1, "estado": 1}
        ))
        for rep in reportes:
            rep["_id"] = str(rep["_id"])
        
        return {"status": "success", "puntos": reportes}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error obteniendo los puntos: " + str(e))

@app.get("/api/reportes/policia")
def obtener_reportes_policia():
    """
    Devuelve TODOS los reportes (pendientes y confirmados) para la vista del mapa de policia y validaciones.
    """
    try:
        reportes = list(db.reportes_ciudadano.find(
            {"estado": {"$in": ["pendiente", "confirmado"]}}, 
            {"_id": 1, "sub_tipo": 1, "ubicacion": 1, "estado": 1, "descripcion": 1, "direccion": 1, "fecha_hecho": 1, "anonimo": 1, "modalidad": 1}
        ))
        for rep in reportes:
            rep["_id"] = str(rep["_id"])
        
        return {"status": "success", "puntos": reportes, "reportes": reportes}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error obteniendo reportes: " + str(e))

from bson import ObjectId

class UpdateUser(BaseModel):
    nombre: str
    email: EmailStr
    telefono: str = ""

@app.get("/api/usuarios/{user_id}")
def obtener_usuario(user_id: str):
    try:
        user = db.usuarios.find_one({"_id": ObjectId(user_id)}, {"password_hash": 0})
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        user["_id"] = str(user["_id"])
        return {"status": "success", "user": user}
    except Exception as e:
        raise HTTPException(status_code=400, detail="ID Invalido o error: " + str(e))

@app.put("/api/usuarios/{user_id}")
def actualizar_usuario(user_id: str, data: UpdateUser):
    try:
        resultado = db.usuarios.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {
                "nombre": data.nombre,
                "email": data.email,
                "telefono": data.telefono
            }}
        )
        if resultado.matched_count == 0:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        return {"status": "success", "message": "Datos actualizados"}
    except Exception as e:
        raise HTTPException(status_code=400, detail="Error actualizando: " + str(e))


