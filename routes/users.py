# Fix: el except generico envolvia el HTTPException(404) propio y lo reempaquetaba como 400.
from fastapi import APIRouter, HTTPException
from bson.objectid import ObjectId
from config.database import db
from models.user_schemas import UpdateUser

router = APIRouter(prefix="/api/usuarios", tags=["users"])

@router.get("/{user_id}")
def obtener_usuario(user_id: str):
    try:
        user = db.usuarios.find_one({"_id": ObjectId(user_id)}, {"password_hash": 0})
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        user["_id"] = str(user["_id"])
        return {"status": "success", "user": user}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail="ID Invalido o error: " + str(e))

@router.put("/{user_id}")
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
