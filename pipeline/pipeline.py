"""
Pipeline principal — Monitor Sísmico Chile
Modo local:  --runner=DirectRunner (default)
Modo cloud:  --runner=DataflowRunner (Fase 3)

Uso local:
    python pipeline.py --input_file=../ingestion/state/test_events.json
    python pipeline.py --input_pubsub=projects/sismo-monitor-pcl/topics/sismos-raw
"""

import argparse
import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode

import config
from transforms import ParseMessage, FilterChile, EnrichEvent, FormatForBigQuery, FilterAlertas

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file",
        help="Archivo JSON local con eventos de prueba (uno por línea). Usa esto con DirectRunner.",
        default=None,
    )
    parser.add_argument(
        "--input_pubsub",
        help="Topic Pub/Sub de entrada. Formato: projects/PROJECT/topics/TOPIC",
        default=f"projects/{config.PROJECT_ID}/topics/{config.TOPIC_RAW}",
    )
    parser.add_argument(
        "--output_file",
        help="Archivo de salida local para resultados (DirectRunner).",
        default="output/eventos_procesados",
    )
    parser.add_argument(
        "--runner",
        default=config.RUNNER,
        help="Runner de Beam: DirectRunner o DataflowRunner",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Crear carpeta de output si no existe
    os.makedirs("output", exist_ok=True)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner = known_args.runner

    # Streaming solo si leemos de Pub/Sub
    use_pubsub = known_args.input_file is None
    if use_pubsub:
        options.view_as(StandardOptions).streaming = True

    log.info("Runner: %s | Modo: %s", known_args.runner, "streaming" if use_pubsub else "batch/archivo")

    with beam.Pipeline(options=options) as p:

        # ------------------------------------------------------------------
        # 1. INGESTA
        # ------------------------------------------------------------------
        if use_pubsub:
            raw = (
                p
                | "LeerPubSub" >> beam.io.ReadFromPubSub(
                    topic=known_args.input_pubsub
                )
            )
        else:
            raw = (
                p
                | "LeerArchivo" >> beam.io.ReadFromText(known_args.input_file)
            )

        # ------------------------------------------------------------------
        # 2. PARSEO Y FILTRO
        # ------------------------------------------------------------------
        eventos_chile = (
            raw
            | "Parsear"      >> beam.ParDo(ParseMessage())
            | "FiltrarChile" >> beam.ParDo(FilterChile())
            | "Enriquecer"   >> beam.ParDo(EnrichEvent())
        )

        # ------------------------------------------------------------------
        # 3. WINDOWING (solo streaming — Pub/Sub)
        # Ventanas fijas de 10 minutos con watermark de 20 minutos
        # ------------------------------------------------------------------
        if use_pubsub:
            eventos_chile = (
                eventos_chile
                | "Timestamps" >> beam.Map(
                    lambda e: beam.window.TimestampedValue(
                        e, e["timestamp_evento"]
                    )
                )
                | "VentanasFijas" >> beam.WindowInto(
                    FixedWindows(config.WINDOW_SIZE_MINUTES * 60),
                    allowed_lateness=config.WATERMARK_MINUTES * 60,
                    trigger=AfterWatermark(
                        late=AfterProcessingTime(config.WATERMARK_MINUTES * 60)
                    ),
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
            )

        # ------------------------------------------------------------------
        # 4. SALIDA
        # ------------------------------------------------------------------
        formatted = (
            eventos_chile
            | "FormatearBQ" >> beam.ParDo(FormatForBigQuery())
        )

        # Escribir a archivo local (DirectRunner) o BigQuery (DataflowRunner — Fase 3)
        if not use_pubsub or known_args.runner == "DirectRunner":
            (
                formatted
                | "SerializarJSON" >> beam.Map(json.dumps)
                | "EscribirArchivo" >> beam.io.WriteToText(
                    known_args.output_file,
                    file_name_suffix=".json",
                    shard_name_template="",
                )
            )

        # Alertas — separar eventos sobre umbral (preview Fase 4)
        alertas = (
            eventos_chile
            | "FiltrarAlertas" >> beam.ParDo(FilterAlertas())
            | "LogAlertas" >> beam.Map(
                lambda e: log.warning(
                    "🚨 ALERTA: M%.1f — %s (región: %s)",
                    e["magnitude"], e["place"], e["region_chile"]
                )
            )
        )


if __name__ == "__main__":
    run()
