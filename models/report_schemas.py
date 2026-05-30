from pydantic import BaseModel
from typing import Optional

class ReporteCiudadano(BaseModel):
    subtipo_hecho: str  # Solo "HURTO" o "ROBO"
    modalidad_hecho: Optional[str] = None
    latitud: float
    longitud: float
    direccion_hecho: str = "Ubicacion reportada por mapa"
    distrito_hecho: str = "TACNA"
    provincia_hecho: str = "TACNA"
    departamento_hecho: str = "TACNA"
    descripcion: str = ""
    usuario_id: Optional[str] = None
    precision_gps: float = 10.0
    fuente: str = "CIUDADANO_APP"
    gravedad: str = "MEDIA"
    device_timestamp: str = ""
    timezone: str = "America/Lima"
    metadata_contextual: dict = {}
