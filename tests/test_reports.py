"""
Tests del flujo critico de reportes del rol ciudadano:
1) crear un reporte, 2) listar los reportes propios de un usuario,
3) limite de 5 reportes por dia (HTTP 429).
"""
from datetime import datetime

from bson.objectid import ObjectId


def _payload(usuario_id=None):
    data = {
        "subtipo_hecho": "ROBO",
        "latitud": -18.0146,
        "longitud": -70.2536,
        "descripcion": "Prueba automatizada",
    }
    if usuario_id:
        data["usuario_id"] = usuario_id
    return data


def test_crear_reporte_guarda_pendiente_en_la_bd(client, mongo_db):
    response = client.post("/api/reportes", json=_payload())

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert "id_reporte" in body

    guardado = mongo_db.reportes_ciudadano.find_one({"_id": ObjectId(body["id_reporte"])})
    assert guardado is not None
    assert guardado["estado"] == "pendiente"
    assert guardado["subtipo_hecho"] == "ROBO"


def test_obtener_mis_reportes_filtra_por_usuario(client, mongo_db):
    usuario_a = ObjectId()
    usuario_b = ObjectId()

    mongo_db.reportes_ciudadano.insert_many([
        {"usuario_id": usuario_a, "subtipo_hecho": "ROBO", "estado": "pendiente", "creado_en": datetime.utcnow()},
        {"usuario_id": usuario_a, "subtipo_hecho": "HURTO", "estado": "confirmado", "creado_en": datetime.utcnow()},
        {"usuario_id": usuario_b, "subtipo_hecho": "ROBO", "estado": "pendiente", "creado_en": datetime.utcnow()},
    ])

    response = client.get(f"/api/reportes/mis_reportes/{usuario_a}")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert len(body["reportes"]) == 2
    assert all(r["usuario_id"] == str(usuario_a) for r in body["reportes"])


def test_sexto_reporte_del_dia_devuelve_429(client, mongo_db):
    usuario_id = ObjectId()
    hoy = datetime.utcnow()

    # Se siembran 5 reportes ya creados hoy por el mismo usuario (el limite es 5/dia)
    mongo_db.reportes_ciudadano.insert_many([
        {"usuario_id": usuario_id, "subtipo_hecho": "ROBO", "estado": "pendiente", "creado_en": hoy}
        for _ in range(5)
    ])

    response = client.post("/api/reportes", json=_payload(usuario_id=str(usuario_id)))

    assert response.status_code == 429
    assert "alcanzado" in response.json()["detail"].lower()
