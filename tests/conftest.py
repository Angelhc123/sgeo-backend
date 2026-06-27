"""
Fixtures de pytest para el flujo critico de reportes (rol ciudadano).

Se usa mongomock (BD en memoria) en vez de una conexion real a MongoDB, y se monta
solo el router de reportes en una FastAPI nueva -en vez de importar main:app- para
evitar disparar el lifespan real de la app (conexion a la BD de produccion + el hilo
de la IA de zonas de riesgo en cada arranque, ver backend/main.py).
"""
import mongomock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import routes.reports as reports_module


@pytest.fixture
def mongo_db():
    client = mongomock.MongoClient()
    return client["test_geocrimen_tacna"]


@pytest.fixture
def client(monkeypatch, mongo_db):
    # routes/reports.py hizo "from config.database import db" al importarse, por lo que
    # el nombre `db` vive en el namespace del modulo: se reemplaza ahi por la BD de prueba.
    monkeypatch.setattr(reports_module, "db", mongo_db)

    test_app = FastAPI()
    test_app.include_router(reports_module.router)
    return TestClient(test_app)
