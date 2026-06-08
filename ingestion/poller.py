"""
Poller USGS → Pub/Sub
Lee el feed GeoJSON de USGS, deduplica por event ID + timestamp_updated
y publica eventos nuevos o actualizados.

Deduplicación:
- La clave de estado es (id, updated) — no solo el id.
- Si USGS actualiza un evento (magnitud revisada, coordenadas corregidas),
  el campo `updated` cambia y el evento se republica.
- Esto garantiza que eventos publicados sin magnitud (preliminares) sean
  republicados cuando USGS los actualiza con datos completos.
"""

import json
import logging
import os
import time
from pathlib import Path

import requests
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Configuración ---
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
TOPIC_ID = os.environ.get("PUBSUB_TOPIC", "sismos-raw")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
STATE_FILE = Path(os.environ.get("STATE_FILE", Path(__file__).parent / "state" / "seen_ids.json"))

# Bounding box Chile: lat [-56, -18] / lon [-75, -66]
CHILE_LAT_MIN, CHILE_LAT_MAX = -56.0, -18.0
CHILE_LON_MIN, CHILE_LON_MAX = -75.0, -66.0


def load_seen_state() -> dict:
    """Carga el estado {id: updated} desde archivo."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_seen_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state))


def is_in_chile(lon: float, lat: float) -> bool:
    return CHILE_LAT_MIN <= lat <= CHILE_LAT_MAX and CHILE_LON_MIN <= lon <= CHILE_LON_MAX


def fetch_events() -> list:
    resp = requests.get(USGS_URL, timeout=30)
    resp.raise_for_status()
    return resp.json().get("features", [])


def build_message(feature: dict) -> dict:
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]  # [lon, lat, depth]
    return {
        "id": feature["id"],
        "magnitude": props.get("mag"),
        "place": props.get("place"),
        "lon": coords[0],
        "lat": coords[1],
        "depth_km": coords[2],
        "timestamp_event": props.get("time"),      # epoch ms
        "timestamp_updated": props.get("updated"), # epoch ms — cambia al revisar el evento
        "timestamp_ingested": int(time.time() * 1000),
        "url": props.get("url"),
    }


def poll_once(publisher, topic_path: str, seen_state: dict) -> dict:
    try:
        features = fetch_events()
    except Exception as e:
        log.error("Error fetching USGS feed: %s", e)
        return seen_state

    new_count = 0
    updated_count = 0

    for feature in features:
        event_id = feature["id"]
        props = feature["properties"]
        current_updated = props.get("updated")

        # Deduplicación por (id, updated):
        # - Nuevo evento: id no visto antes → publicar
        # - Evento actualizado: id conocido pero updated cambió → republicar
        prev_updated = seen_state.get(event_id)
        is_new = prev_updated is None
        is_updated = not is_new and prev_updated != current_updated

        if not is_new and not is_updated:
            continue

        coords = feature["geometry"]["coordinates"]
        lon, lat = coords[0], coords[1]

        # Filtro geográfico: solo Chile
        # TEMP: comentado para pruebas con mayor volumen de eventos
        # if not is_in_chile(lon, lat):
        #     seen_state[event_id] = current_updated
        #     continue

        msg = build_message(feature)
        msg["is_update"] = is_updated  # flag para trazabilidad en BQ
        data = json.dumps(msg).encode("utf-8")
        publisher.publish(topic_path, data=data)
        seen_state[event_id] = current_updated

        if is_new:
            new_count += 1
            log.info("Publicado: id=%s mag=%s lugar=%s", event_id, msg["magnitude"], msg["place"])
        else:
            updated_count += 1
            log.info("Actualizado: id=%s mag=%s lugar=%s", event_id, msg["magnitude"], msg["place"])

    log.info("Ciclo completo: %d nuevos, %d actualizados publicados", new_count, updated_count)
    return seen_state


def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    log.info("Poller iniciado — proyecto=%s topic=%s", PROJECT_ID, TOPIC_ID)

    seen_state = load_seen_state()

    while True:
        seen_state = poll_once(publisher, topic_path, seen_state)
        save_seen_state(seen_state)
        log.info("Esperando %ds para próximo ciclo...", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
