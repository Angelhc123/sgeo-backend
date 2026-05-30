from pydantic import BaseModel, EmailStr

class UpdateUser(BaseModel):
    nombre: str
    email: EmailStr
    telefono: str = ""
