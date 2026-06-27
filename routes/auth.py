from fastapi import APIRouter, HTTPException, Depends, status
from models.auth_schemas import LoginRequest, RegisterRequest
from utils.crypto import hash_password, verify_password
from config.database import get_db
import datetime as dt

router = APIRouter(prefix="/api/auth", tags=["auth"])

# Nota: login solo verifica credenciales y retorna los datos del usuario; no emite token.
# No existe JWT ni middleware de sesion en ningun otro endpoint del backend (SECRET_KEY
# en .env esta declarada pero no se usa). Cualquier endpoint que reciba un user_id/usuario_id
# confia en el valor que envia el cliente, sin verificar que corresponda a una sesion autenticada.

@router.post("/login")
def login(req: LoginRequest, db = Depends(get_db)):
    # Buscar usuario
    user = db.usuarios.find_one({"email": req.email})
    if not user:
        raise HTTPException(status_code=401, detail="Correo o contraseña incorrectos")
    
    if not user.get("activo", True):
        raise HTTPException(status_code=403, detail="Tu cuenta esta inactiva")

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

@router.post("/register")
def register(req: RegisterRequest, db = Depends(get_db)):
    # Verificar si existe el email
    existing_user = db.usuarios.find_one({"email": req.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="Este correo ya esta registrado")
    
    # Verificar si existe el nombre de usuario
    existing_name = db.usuarios.find_one({"nombre": req.nombre})
    if existing_name:
        raise HTTPException(status_code=400, detail="Usuario invalido: ya hay otra cuenta con este nombre")
    
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
