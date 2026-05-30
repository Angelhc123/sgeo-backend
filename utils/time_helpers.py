import pytz
from datetime import datetime, timezone

# Diccionario de turnos en mayúsculas
TURNOS = {
    "MADRUGADA": (0, 5),
    "MAÑANA":    (6, 11),
    "TARDE":     (12, 17),
    "NOCHE":     (18, 23),
}

# Pesos de turnos para el scoring predictivo
TURNO_WEIGHTS = {
    "MADRUGADA": 1.5,
    "MAÑANA":    0.7,
    "TARDE":     0.8,
    "NOCHE":     1.2,
}

def get_turno(hora: int) -> str:
    """
    Devuelve el turno unificado en MAYÚSCULAS según la hora (0-23).
    MADRUGADA (0-5), MAÑANA (6-11), TARDE (12-17), NOCHE (18-23).
    """
    if 0 <= hora <= 5:
        return "MADRUGADA"
    elif 6 <= hora <= 11:
        return "MAÑANA"
    elif 12 <= hora <= 17:
        return "TARDE"
    else:
        return "NOCHE"

def get_turno_weight(hora: int) -> float:
    """
    Peso de riesgo según el turno horario.
    """
    return TURNO_WEIGHTS.get(get_turno(hora), 1.0)

def get_local_time(tz_name: str = "America/Lima") -> datetime:
    """
    Obtiene la fecha y hora actual en la zona horaria indicada.
    Si ocurre algún error o la zona no es válida, usa 'America/Lima' por defecto.
    """
    server_now = datetime.utcnow()
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone("America/Lima")
    return server_now.replace(tzinfo=timezone.utc).astimezone(tz)
