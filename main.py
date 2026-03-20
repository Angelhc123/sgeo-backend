from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pydantic import BaseModel, EmailStr
import bcrypt
import os
import datetime as dt
import threading
from dotenv import load_dotenv

# Importar el motor de IA
from motor_ia_espacial import ejecutar_ia_zonas_riesgo

# Cargar variables de entorno
load_dotenv()

app = FastAPI(title="SGEO API - Geolocalización de Inseguridad")

# Evento de inicio: Ejecutar la IA de fondo una vez cuando el servidor encienda
@app.on_event("startup")
def startup_event():
    print("🚀 Servidor iniciado. Ejecutando motor de IA espacial en segundo plano...")
    # Usamos un hilo para que la IA matemática no bloquee el encendido del servidor
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

# Funciones de utilidad para constraseñas
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
        raise HTTPException(status_code=401, detail="Correo o contraseña incorrectos")
    
    if not user.get("activo", True):
        raise HTTPException(status_code=403, detail="Tu cuenta está inactiva")

    # Verificar password
    if not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Correo o contraseña incorrectos")
    
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
        raise HTTPException(status_code=400, detail="Este correo ya está registrado")
    
    # Crear usuario
    nuevo_usuario = {
        "nombre": req.nombre,
        "email": req.email,
        "password_hash": hash_password(req.password),
        "rol": "ciudadano", # Por defecto todo el que se registra es ciudadano
        "activo": True,
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
    Lanza el motor matemático sin trabar la respuesta del servidor.
    Se llamará automáticamente cada vez que un policía apruebe un nuevo incidente.
    """
    background_tasks.add_task(ejecutar_ia_zonas_riesgo)
    return {"status": "success", "mensaje": "IA iniciada en segundo plano."}

@app.get("/api/map/zonas_riesgo")
def obtner_zonas_riesgo():
    """
    Retorna los mapas de calor / zonas de riesgo generadas por la IA
    (basadas en estadísticas del SIDPOL).
    """
    try:
        zonas = list(db.zonas_riesgo.find({}))
        # Convertir ObjectIds y Fechas a strings para JSON
        for zona in zonas:
            zona["_id"] = str(zona["_id"])
            if "calculado_en" in zona:
                zona["calculado_en"] = zona["calculado_en"].isoformat()
            if "periodo_analizado" in zona:
                if "desde" in zona["periodo_analizado"]:
                    zona["periodo_analizado"]["desde"] = zona["periodo_analizado"]["desde"].isoformat()
                if "hasta" in zona["periodo_analizado"]:
                    zona["periodo_analizado"]["hasta"] = zona["periodo_analizado"]["hasta"].isoformat()
        
        return {"status": "success", "zonas": zonas}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error obteniendo zonas de riesgo: " + str(e))

