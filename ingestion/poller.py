"""
Poller USGS → Pub/Sub
Lee el feed GeoJSON de USGS, deduplica por event ID y publica eventos nuevos.
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


def load_seen_ids() -> set:
    if STATE_FILE.exists():
        return set(json.loads(STATE_FILE.read_text()))
    return set()


def save_seen_ids(seen: set) -> None:
    STATE_FILE.write_text(json.dumps(list(seen)))


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
        "timestamp_event": props.get("time"),   # epoch ms
        "timestamp_updated": props.get("updated"),
        "timestamp_ingested": int(time.time() * 1000),
        "url": props.get("url"),
    }


def poll_once(publisher, topic_path: str, seen_ids: set) -> set:
    try:
        features = fetch_events()
    except Exception as e:
        log.error("Error fetching USGS feed: %s", e)
        return seen_ids

    new_count = 0
    for feature in features:
        event_id = feature["id"]

        # Deduplicación: saltar eventos ya publicados
        if event_id in seen_ids:
            continue

        coords = feature["geometry"]["coordinates"]
        lon, lat = coords[0], coords[1]

        # Filtro geográfico: solo Chile
        # TEMP: comentado para pruebas con mayor volumen de eventos
        # if not is_in_chile(lon, lat):
        #     seen_ids.add(event_id)
        #     continue

        msg = build_message(feature)
        data = json.dumps(msg).encode("utf-8")
        publisher.publish(topic_path, data=data)
        seen_ids.add(event_id)
        new_count += 1
        log.info("Publicado: id=%s mag=%s lugar=%s", event_id, msg["magnitude"], msg["place"])

    log.info("Ciclo completo: %d eventos nuevos de Chile publicados", new_count)
    return seen_ids


def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    log.info("Poller iniciado — proyecto=%s topic=%s", PROJECT_ID, TOPIC_ID)

    seen_ids = load_seen_ids()

    while True:
        seen_ids = poll_once(publisher, topic_path, seen_ids)
        save_seen_ids(seen_ids)
        log.info("Esperando %ds para próximo ciclo...", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
