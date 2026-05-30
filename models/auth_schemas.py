from pydantic import BaseModel, EmailStr

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class RegisterRequest(BaseModel):
    nombre: str
    email: EmailStr
    password: str
    rol: str = "ciudadano"
    is_active: bool = True
