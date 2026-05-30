import os
import uvicorn
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from config.database import connect_db, close_db
from firebase_service import init_firebase
from motor_ia_zonas_riesgo import ejecutar_ia_zonas_riesgo

from routes.auth import router as auth_router
from routes.reports import router as reports_router
from routes.maps import router as maps_router
from routes.predictive import router as predictive_router
from routes.users import router as users_router
from routes.admin import router as admin_router

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Conectando a DB e iniciando IA...")
    connect_db()
    threading.Thread(target=ejecutar_ia_zonas_riesgo, daemon=True).start()
    yield
    print("🔌 Apagando servidor...")
    close_db()

app = FastAPI(title="SGEO API", lifespan=lifespan)

init_firebase()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(reports_router)
app.include_router(maps_router)
app.include_router(predictive_router)
app.include_router(users_router)
app.include_router(admin_router)

@app.get("/")
def health_check():
    return {"status": "ok", "mensaje": "SGEO API Funcionando"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)