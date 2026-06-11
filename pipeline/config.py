"""
Configuración central del pipeline sísmico.
Todos los parámetros operativos viven aquí — sin hardcode en pipeline.py ni transforms.py.
"""

# ---------------------------------------------------------------------------
# Fuente de datos
# ---------------------------------------------------------------------------
USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

# ---------------------------------------------------------------------------
# Filtro geográfico — Bounding box Chile
# lat [-56, -18] / lon [-75, -66]
# Cubre Chile continental + Isla de Pascua + zonas subantárticas relevantes
# ---------------------------------------------------------------------------
CHILE_LAT_MIN = -56.0
CHILE_LAT_MAX = -18.0
CHILE_LON_MIN = -75.0
CHILE_LON_MAX = -66.0

# ---------------------------------------------------------------------------
# Umbrales de magnitud
# ---------------------------------------------------------------------------
MAG_MIN_DISPLAY = 0.5      # Magnitud mínima para procesar y escribir a BigQuery
MAG_ALERT_THRESHOLD = 5.0  # Magnitud mínima para disparar alerta (email + Telegram)

# ---------------------------------------------------------------------------
# Watermark y windowing
# Latencia real del feed USGS para eventos fuera de EE.UU.: hasta 20 minutos
# Documentado en: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
# ---------------------------------------------------------------------------
WATERMARK_MINUTES = 20         # Tiempo máximo de retraso esperado de un evento
WINDOW_SIZE_MINUTES = 10       # Tamaño de la ventana de agregación

# ---------------------------------------------------------------------------
# GCP — se sobreescriben con variables de entorno en producción
# ---------------------------------------------------------------------------
import os

PROJECT_ID    = os.environ.get("GCP_PROJECT_ID", "sismo-monitor-pcl")
TOPIC_RAW     = os.environ.get("PUBSUB_TOPIC_RAW", "sismos-raw")
SUB_RAW       = os.environ.get("PUBSUB_SUB_RAW", "sismos-raw-sub")   # leer siempre desde suscripción
TOPIC_ALERTS  = os.environ.get("PUBSUB_TOPIC_ALERTS", "sismos-alertas")
DATASET_ID    = os.environ.get("BQ_DATASET", "sismo_monitor")
TABLE_EVENTS  = os.environ.get("BQ_TABLE_EVENTS", "eventos_raw")
TABLE_METRICS = os.environ.get("BQ_TABLE_METRICS", "metricas_ventana")

# ---------------------------------------------------------------------------
# Runner — DirectRunner local por defecto, DataflowRunner en producción
# ---------------------------------------------------------------------------
RUNNER = os.environ.get("BEAM_RUNNER", "DirectRunner")
