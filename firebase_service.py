import firebase_admin
from firebase_admin import credentials, messaging
import os
import json

def init_firebase():
    # Sólo inicializamos la app si no ha sido inicializada antes
    if not firebase_admin._apps:
        
        # 1. Intentar descargar credenciales desde las VARIABLES DE ENTORNO de Railway
        firebase_json_str = os.getenv("FIREBASE_CREDENTIALS_JSON")
        
        if firebase_json_str:
            print("🚀 Conectando a FCM a través de Variable de Entorno (Railway)")
            try:
                cred_dict = json.loads(firebase_json_str)
                cred = credentials.Certificate(cred_dict)
                firebase_admin.initialize_app(cred)
            except Exception as e:
                print("❌ Error cargando el JSON de Firebase desde Railway:", e)
                
        # 2. Si no hay variable (estamos en tu PC Local), buscar el archivo .json
        else:
            cred_path = os.path.join(os.path.dirname(__file__), "sgeo-firebase-adminsdk.json")
            if os.path.exists(cred_path):
                print("🚀 Conectado con Firebase a través de Archivo Local (PC)")
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            else:
                print("⚠️ ADVERTENCIA: No se encontraron credenciales de Firebase en Local ni en Railway.")

def send_push_notification(title: str, body: str, tipo_alerta: str, topic="actualizaciones", lat: float = None, lng: float = None):
    """
    Envía una notificación push masiva a todos los usuarios o a un tema en específico.
    topic='actualizaciones' es al que tu app Flutter se suscribió en el main.dart.
    """
    if not firebase_admin._apps:
        print("Firebase no inicializado, no se pudo enviar el Push.")
        return False

    payload_data = {
        "type": tipo_alerta  # 'incident', 'risk_zone', 'update' (determina el color e icono en flutter)
    }

    if lat is not None and lng is not None:
        payload_data["lat"] = str(lat)
        payload_data["lng"] = str(lng)

    message = messaging.Message(
        notification=messaging.Notification(
            title=title,
            body=body,
        ),
        data=payload_data,
        topic=topic, # 'actualizaciones' llega a todos los usuarios
    )

    try:
        response = messaging.send(message)
        print("🔔 Notificación enviada con éxito:", response)
        return True
    except Exception as e:
        print("❌ Error al enviar notificación:", e)
        return False
