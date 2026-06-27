# Se agrega RechazoUsuarioRequest para el endpoint PUT /api/admin/usuarios/{id}/rechazar.
from pydantic import BaseModel, EmailStr

class UpdateUser(BaseModel):
    nombre: str
    email: EmailStr
    telefono: str = ""

class RechazoUsuarioRequest(BaseModel):
    motivo: str
