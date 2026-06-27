# Nuevo servicio: envia los emails del flujo de aprobacion policial via Resend (httpx async).
import os
import sys

import httpx

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", "noreply@sgeo.com")  # [CONFIGURAR] dominio verificado en Resend
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@sgeo.com")  # [CONFIGURAR] correo real del administrador

RESEND_URL = "https://api.resend.com/emails"


def _log(msg: str) -> None:
    """print() que nunca lanza, ni siquiera en consolas que no son UTF-8 (ej. cmd.exe con cp1252)."""
    try:
        print(msg)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "ascii"
        print(msg.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def _email_layout(accent_color: str, title: str, body_html: str) -> str:
    return f"""
    <div style="background-color:#0d1117;padding:32px;font-family:Arial,sans-serif;">
      <div style="max-width:480px;margin:0 auto;background-color:#161b22;border-radius:12px;padding:32px;border:1px solid #30363d;">
        <h2 style="color:{accent_color};margin-top:0;">{title}</h2>
        <div style="color:#c9d1d9;font-size:15px;line-height:1.6;">
          {body_html}
        </div>
        <p style="color:#8b949e;font-size:12px;margin-top:32px;">SGEO — Sistema de Geolocalización de Inseguridad Ciudadana</p>
      </div>
    </div>
    """


async def _enviar_email(to: list[str], subject: str, html: str, reply_to: str | None = None) -> None:
    """Envia un email via Resend. Nunca lanza excepcion: solo loggea si falla (ver llamadores)."""
    if not RESEND_API_KEY:
        _log(f"[email_service] RESEND_API_KEY no configurada — se omite envio a {to} ({subject})")
        return

    payload = {
        "from": RESEND_FROM_EMAIL,
        "to": to,
        "subject": subject,
        "html": html,
    }
    if reply_to:
        payload["reply_to"] = reply_to

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                RESEND_URL,
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            if response.status_code >= 400:
                _log(f"[email_service] Resend respondio {response.status_code}: {response.text}")
    except Exception as e:
        _log(f"[email_service] Error enviando email a {to}: {e}")


async def solicitar_datos_policia(email: str, nombre: str) -> None:
    body = f"""
    <p>Hola {nombre}, recibimos tu registro en SGEO.</p>
    <p>Para activar tu cuenta como efectivo policial, necesitamos verificar tu identidad.
    Por favor responde este correo con la siguiente información:</p>
    <ul>
      <li>DNI (8 dígitos)</li>
      <li>Número de placa o CUI policial</li>
      <li>Unidad / Comisaría a la que perteneces</li>
      <li>Teléfono de contacto</li>
    </ul>
    <p>El administrador revisará tus datos y recibirás una respuesta en tu correo.
    Tu cuenta permanecerá en espera hasta la aprobación.</p>
    """
    html = _email_layout("#1A73E8", "Completa tu verificación como efectivo policial", body)
    await _enviar_email(
        to=[email],
        subject="SGEO — Completa tu verificación como efectivo policial",
        html=html,
        reply_to=ADMIN_EMAIL,
    )


async def notificar_policia_aprobado(email: str, nombre: str) -> None:
    body = f"""
    <p>Hola {nombre}, tus datos han sido verificados y tu cuenta ha sido aprobada.</p>
    <p>Ya puedes iniciar sesión en la app SGEO con tu email y contraseña registrados.</p>
    <p>Bienvenido al sistema.</p>
    """
    html = _email_layout("#2ECC71", "Tu cuenta SGEO ha sido aprobada", body)
    await _enviar_email(to=[email], subject="✅ Tu cuenta SGEO ha sido aprobada", html=html)


async def notificar_policia_rechazado(email: str, nombre: str, motivo: str) -> None:
    body = f"""
    <p>Hola {nombre}, revisamos la información enviada y encontramos un inconveniente.</p>
    <p><strong>Motivo:</strong> {motivo}</p>
    <p>Si crees que es un error o deseas corregir tus datos, puedes registrarte
    nuevamente con la información correcta o contactar al administrador.</p>
    """
    html = _email_layout("#E53935", "Verificación de cuenta no completada", body)
    await _enviar_email(to=[email], subject="SGEO — Verificación de cuenta no completada", html=html)
