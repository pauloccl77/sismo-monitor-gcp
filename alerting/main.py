"""
Cloud Function 2ª gen — Notificador de alertas sísmicas
Trigger: Pub/Sub topic sismos-alertas (mensaje publicado por Dataflow)

Flujo:
  1. Decodifica el mensaje Pub/Sub (JSON del evento sísmico)
  2. Consulta BQ alertas_enviadas para evitar duplicados por id
  3. Envía Telegram + email
  4. Registra el id en alertas_enviadas
"""

import base64
import json
import logging
import os
from datetime import datetime

import functions_framework
from google.cloud import bigquery
from zoneinfo import ZoneInfo
import requests

CHILE_TZ = ZoneInfo("America/Santiago")

PROJECT_ID       = os.environ["GCP_PROJECT_ID"]
ALERT_EMAIL_FROM = os.environ["ALERT_EMAIL_FROM"]
ALERT_EMAIL_TO   = os.environ["ALERT_EMAIL_TO"]
SENDGRID_API_KEY    = os.environ["SENDGRID_API_KEY"]
TELEGRAM_BOT_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID    = os.environ["TELEGRAM_CHAT_ID"]

DATASET_ID    = "sismo_monitor"
TABLE_ALERTAS = "alertas_enviadas"

# Cliente BQ inicializado en cold start — reutilizado entre invocaciones
_bq = bigquery.Client(project=PROJECT_ID)


def _ya_alertado(event_id: str) -> bool:
    query = f"""
        SELECT COUNT(1) AS n
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ALERTAS}`
        WHERE id = @event_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("event_id", "STRING", event_id)]
    )
    rows = list(_bq.query(query, job_config=job_config).result())
    return rows[0]["n"] > 0


def _registrar_alerta(event_id: str, magnitud, lugar: str) -> None:
    row = {
        "id":               event_id,
        "timestamp_alerta": datetime.now(timezone.utc).isoformat(),
        "magnitud":         magnitud,
        "lugar":            lugar,
    }
    errors = _bq.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ALERTAS}", [row])
    if errors:
        logging.error("Error registrando alerta en BQ: %s", errors)


def _enviar_telegram(mensaje: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": mensaje, "parse_mode": "HTML"},
        timeout=10,
    )
    resp.raise_for_status()


def _enviar_email(asunto: str, cuerpo: str) -> None:
    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "personalizations": [{"to": [{"email": ALERT_EMAIL_TO}]}],
            "from": {"email": ALERT_EMAIL_FROM},
            "subject": asunto,
            "content": [{"type": "text/plain", "value": cuerpo}],
        },
        timeout=10,
    )
    if not resp.ok:
        logging.error("SendGrid %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()


@functions_framework.cloud_event
def notify(cloud_event) -> None:
    raw   = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    event = json.loads(raw)

    event_id    = event.get("id")
    magnitud    = event.get("magnitude")
    lugar       = event.get("place", "Lugar desconocido")
    region      = event.get("region_chile") or "Fuera de Chile"
    profundidad = event.get("depth_km")
    url         = event.get("url", "")

    if not event_id:
        logging.warning("Evento sin ID, descartado")
        return

    if _ya_alertado(event_id):
        logging.info("Evento %s ya fue alertado, omitiendo", event_id)
        return

    mag_str    = f"M{magnitud:.1f}" if magnitud is not None else "M?"
    hora_chile = datetime.now(CHILE_TZ).strftime("%Y-%m-%d %H:%M (Chile)")

    telegram_msg = (
        f"<b>\U0001f6a8 ALERTA SÍSMICA {mag_str}</b>\n"
        f"\U0001f4cd <b>Lugar:</b> {lugar}\n"
        f"\U0001f5fa️ <b>Región:</b> {region}\n"
        f"⬇️ <b>Profundidad:</b> {profundidad} km\n"
        f"\U0001f550 <b>Hora:</b> {hora_chile}\n"
        f"\U0001f517 {url}"
    )
    email_asunto = f"Alerta Sismica {mag_str} - {lugar}"
    email_cuerpo = (
        f"ALERTA SISMICA DETECTADA\n\n"
        f"Magnitud:    {mag_str}\n"
        f"Lugar:       {lugar}\n"
        f"Region:      {region}\n"
        f"Profundidad: {profundidad} km\n"
        f"Hora:        {hora_chile}\n"
        f"URL:         {url}\n"
    )

    try:
        _enviar_telegram(telegram_msg)
    except Exception as e:
        logging.error("Error Telegram: %s", e)

    try:
        _enviar_email(email_asunto, email_cuerpo)
    except Exception as e:
        logging.error("Error email: %s", e, exc_info=True)

    _registrar_alerta(event_id, magnitud, lugar)
    logging.info("Alerta procesada: %s (%s)", event_id, mag_str)
