"""
Transformaciones reutilizables del pipeline sísmico.
Cada clase es un PTransform independiente y testeable.
"""

import json
import logging
import time
from typing import Optional

import apache_beam as beam

from config import (
    CHILE_LAT_MIN, CHILE_LAT_MAX,
    CHILE_LON_MIN, CHILE_LON_MAX,
    MAG_MIN_DISPLAY,
    MAG_ALERT_THRESHOLD,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REGIONES_CHILE = [
    ("Arica y Parinacota",  -18.5, -17.4, -70.5, -69.2),
    ("Tarapacá",            -21.6, -18.5, -70.6, -68.2),
    ("Antofagasta",         -26.1, -21.6, -70.6, -67.0),
    ("Atacama",             -29.2, -26.1, -71.5, -68.0),
    ("Coquimbo",            -32.3, -29.2, -72.0, -69.5),
    ("Valparaíso",          -33.9, -32.3, -72.0, -70.0),
    ("Metropolitana",       -34.4, -33.0, -71.8, -69.8),
    ("O'Higgins",           -35.2, -34.4, -72.5, -70.0),
    ("Maule",               -36.6, -35.2, -73.0, -70.5),
    ("Ñuble",               -37.5, -36.6, -73.5, -71.0),
    ("Biobío",              -38.5, -37.5, -74.0, -71.0),
    ("La Araucanía",        -39.8, -38.5, -74.0, -71.0),
    ("Los Ríos",            -40.6, -39.8, -74.0, -71.5),
    ("Los Lagos",           -44.0, -40.6, -74.5, -71.5),
    ("Aysén",               -49.0, -44.0, -75.0, -71.0),
    ("Magallanes",          -56.0, -49.0, -75.0, -66.0),
]


def get_region_chile(lat: float, lon: float) -> str:
    """Retorna el nombre de la región chilena dado lat/lon, o 'Chile (sin región)' si no coincide."""
    for nombre, lat_min, lat_max, lon_min, lon_max in REGIONES_CHILE:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return nombre
    return "Chile"


def is_in_chile(lat: float, lon: float) -> bool:
    """Verifica si las coordenadas están dentro del bounding box de Chile."""
    return (CHILE_LAT_MIN <= lat <= CHILE_LAT_MAX and
            CHILE_LON_MIN <= lon <= CHILE_LON_MAX)


def get_pais(place: str) -> Optional[str]:
    """Extrae el país/estado del campo place de USGS ('X km de Ciudad, País' → 'País')."""
    if not place:
        return None
    if "," in place:
        return place.split(",")[-1].strip()
    return None


# ---------------------------------------------------------------------------
# PTransforms
# ---------------------------------------------------------------------------

class ParseMessage(beam.DoFn):
    """
    Deserializa el mensaje JSON de Pub/Sub.
    Emite el dict del evento o lo descarta si el JSON es inválido.
    """

    def process(self, element):
        try:
            if isinstance(element, bytes):
                element = element.decode("utf-8")
            yield json.loads(element)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.warning("Mensaje inválido descartado: %s", e)


class FilterChile(beam.DoFn):
    """
    Filtra eventos fuera del bounding box Chile.
    Emite solo los eventos dentro de Chile con magnitud >= MAG_MIN_DISPLAY.
    """

    def process(self, element):
        lat = element.get("lat")
        lon = element.get("lon")
        mag = element.get("magnitude")

        if lat is None or lon is None or mag is None:
            log.warning("Evento sin coordenadas o magnitud: %s", element.get("id"))
            return

        if not is_in_chile(lat, lon):
            return

        if mag < MAG_MIN_DISPLAY:
            return

        yield element


class EnrichEvent(beam.DoFn):
    """
    Enriquece cada evento con:
    - region_chile: nombre de la región según coordenadas
    - es_alerta: True si magnitud >= MAG_ALERT_THRESHOLD
    - timestamp_procesado: epoch ms del momento de procesamiento
    """

    def process(self, element):
        lat = element.get("lat")
        lon = element.get("lon")
        mag = element.get("magnitude")

        if lat is not None and lon is not None and is_in_chile(lat, lon):
            element["region_chile"] = get_region_chile(lat, lon)
        else:
            element["region_chile"] = get_pais(element.get("place", ""))

        element["es_alerta"] = bool(mag and mag >= MAG_ALERT_THRESHOLD)
        element["timestamp_procesado"] = int(time.time() * 1000)

        yield element


class FormatForBigQuery(beam.DoFn):
    """
    Adapta el dict del evento al schema de BigQuery (tabla eventos_raw).
    Convierte timestamp_event de epoch ms a segundos para TIMESTAMP de BQ.
    """

    def process(self, element):
        yield {
            "id":                 element.get("id"),
            "magnitud":           element.get("magnitude"),
            "lugar":              element.get("place"),
            "lat":                element.get("lat"),
            "lon":                element.get("lon"),
            "profundidad_km":     element.get("depth_km"),
            "timestamp_evento":   element.get("timestamp_event", 0) / 1000.0,
            "timestamp_ingesta":  element.get("timestamp_ingested", 0) / 1000.0,
            "timestamp_procesado": element.get("timestamp_procesado", 0) / 1000.0,
            "region_chile":       element.get("region_chile"),
            "es_alerta":          element.get("es_alerta", False),
            "url":                element.get("url"),
            "is_update":          element.get("is_update", False),
        }


class FilterAlertas(beam.DoFn):
    """
    Emite todos los eventos con es_alerta=True, incluyendo is_update=True.
    La deduplicación real se hace en la Cloud Function via tabla BQ alertas_enviadas,
    lo que cubre también el caso donde un evento llega sin magnitud y USGS lo
    actualiza después a M5+.
    """

    def process(self, element):
        if element.get("es_alerta"):
            yield element
