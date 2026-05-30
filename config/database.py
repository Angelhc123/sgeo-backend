import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = "geocrimen_tacna"

class DatabaseManager:
    def __init__(self):
        self.client: MongoClient = None
        self.db = None

    def connect(self):
        if self.client is None:
            if not MONGO_URL:
                print("⚠️ ADVERTENCIA: MONGO_URL no está configurada en las variables de entorno.")
            self.client = MongoClient(MONGO_URL)
            self.db = self.client[DB_NAME]
            # Asegurar los índices para consultas geográficas y ordenamientos rápidos
            self.db.reportes_ciudadano.create_index([("ubicacion", "2dsphere")])
            self.db.reportes_ciudadano.create_index([("creado_en", -1)])
            self.db.historial_delitos.create_index([("ubicacion", "2dsphere")])
            self.db.historial_delitos.create_index([("creado_en", -1)])
            self.db.historial_delitos.create_index([("distrito", 1)])
            self.db.zonas_riesgo.create_index([("centroide", "2dsphere")])
            print("🚀 Conexión a MongoDB inicializada exitosamente.")

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None
            self.db = None
            print("🔌 Conexión a MongoDB cerrada.")

db_manager = DatabaseManager()

def connect_db():
    db_manager.connect()

def close_db():
    db_manager.close()

def get_db():
    if db_manager.db is None:
        db_manager.connect()
    return db_manager.db

# Proxy para la base de datos y colecciones para evitar conectarse en tiempo de importación
class DatabaseProxy:
    @property
    def _db(self):
        return get_db()

    def __getattr__(self, name):
        return getattr(self._db, name)

    def __getitem__(self, name):
        return self._db[name]

class CollectionProxy:
    def __init__(self, collection_name: str):
        self.collection_name = collection_name

    @property
    def _collection(self):
        return get_db()[self.collection_name]

    def __getattr__(self, name):
        return getattr(self._collection, name)

    def __getitem__(self, name):
        return self._collection[name]

# Exportar la base de datos proxy
db = DatabaseProxy()

# Colecciones expuestas como atributos accesibles
usuarios = CollectionProxy("usuarios")
reportes_ciudadano = CollectionProxy("reportes_ciudadano")
historial_delitos = CollectionProxy("historial_delitos")
zonas_riesgo = CollectionProxy("zonas_riesgo")
