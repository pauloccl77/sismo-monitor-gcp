"""
Pipeline principal — Monitor Sísmico Chile
Modo local:  --runner=DirectRunner (default) — escribe a archivo local
Modo cloud:  --runner=DataflowRunner         — escribe a BigQuery

Uso local (archivo):
    python pipeline.py --input_file=test_events.json

Uso streaming (Pub/Sub → BigQuery):
    python pipeline.py --runner=DataflowRunner
"""

import argparse
import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    WorkerOptions,
)
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import (
    AfterWatermark,
    AfterProcessingTime,
    AccumulationMode,
)

import config
from transforms import ParseMessage, FilterChile, EnrichEvent, FormatForBigQuery, FilterAlertas

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Schema BigQuery — debe coincidir con terraform/bigquery.tf
BQ_SCHEMA_EVENTOS = (
    "id:STRING,magnitud:FLOAT64,lugar:STRING,lat:FLOAT64,lon:FLOAT64,"
    "profundidad_km:FLOAT64,timestamp_evento:TIMESTAMP,timestamp_ingesta:TIMESTAMP,"
    "timestamp_procesado:TIMESTAMP,region_chile:STRING,es_alerta:BOOL,url:STRING"
)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file",
        help="Archivo JSON local con eventos de prueba (uno por línea).",
        default=None,
    )
    parser.add_argument(
        "--input_pubsub",
        help="Topic Pub/Sub de entrada.",
        default=f"projects/{config.PROJECT_ID}/topics/{config.TOPIC_RAW}",
    )
    parser.add_argument(
        "--output_file",
        help="Archivo de salida local (DirectRunner).",
        default="output/eventos_procesados",
    )
    parser.add_argument(
        "--output_bq",
        help="Tabla BigQuery de salida. Formato: PROJECT:DATASET.TABLE",
        default=f"{config.PROJECT_ID}:{config.DATASET_ID}.{config.TABLE_EVENTS}",
    )
    parser.add_argument(
        "--runner",
        default=config.RUNNER,
    )
    parser.add_argument(
        "--temp_location",
        help="GCS bucket para archivos temporales de Dataflow.",
        default=os.environ.get("DATAFLOW_TEMP_LOCATION", ""),
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    os.makedirs("output", exist_ok=True)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner = known_args.runner

    use_pubsub = known_args.input_file is None
    use_dataflow = known_args.runner == "DataflowRunner"

    if use_pubsub:
        options.view_as(StandardOptions).streaming = True

    if use_dataflow:
        gcp_options = options.view_as(GoogleCloudOptions)
        gcp_options.project = config.PROJECT_ID
        gcp_options.temp_location = known_args.temp_location
        # region y machine_type se pasan por --region y --worker_machine_type
        # NO hardcodear aquí para permitir cambiarlos sin modificar el código
        worker_options = options.view_as(WorkerOptions)
        worker_options.max_num_workers = 1

    log.info("Runner: %s | Entrada: %s | Salida: %s",
             known_args.runner,
             "Pub/Sub" if use_pubsub else "archivo",
             "BigQuery" if use_dataflow else "archivo local")

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
        # 2. PARSEO, FILTRO Y ENRIQUECIMIENTO
        # ------------------------------------------------------------------
        eventos_chile = (
            raw
            | "Parsear"      >> beam.ParDo(ParseMessage())
            # TEMP: FilterChile comentado para pruebas con eventos mundiales
            # | "FiltrarChile" >> beam.ParDo(FilterChile())
            | "Enriquecer"   >> beam.ParDo(EnrichEvent())
        )

        # ------------------------------------------------------------------
        # 3. WINDOWING — solo en modo streaming (Pub/Sub)
        # NOTA: con windowing los datos llegan a BQ después de ~30 min (ventana + watermark)
        # Para verificación inicial está desactivado — reactivar en producción
        # ------------------------------------------------------------------
        # if use_pubsub:
        #     eventos_chile = (
        #         eventos_chile
        #         | "Timestamps" >> beam.Map(
        #             lambda e: beam.window.TimestampedValue(
        #                 e, e["timestamp_event"] / 1000.0
        #             )
        #         )
        #         | "VentanasFijas" >> beam.WindowInto(
        #             FixedWindows(config.WINDOW_SIZE_MINUTES * 60),
        #             allowed_lateness=config.WATERMARK_MINUTES * 60,
        #             trigger=AfterWatermark(
        #                 late=AfterProcessingTime(config.WATERMARK_MINUTES * 60)
        #             ),
        #             accumulation_mode=AccumulationMode.DISCARDING,
        #         )
        #     )

        # ------------------------------------------------------------------
        # 4. FORMATEO
        # ------------------------------------------------------------------
        formatted = (
            eventos_chile
            | "FormatearBQ" >> beam.ParDo(FormatForBigQuery())
        )

        # ------------------------------------------------------------------
        # 5. SALIDA — BigQuery (Dataflow) o archivo local (DirectRunner)
        # ------------------------------------------------------------------
        if use_dataflow:
            (
                formatted
                | "EscribirBigQuery" >> beam.io.WriteToBigQuery(
                    known_args.output_bq,
                    schema=BQ_SCHEMA_EVENTOS,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                )
            )
        else:
            (
                formatted
                | "SerializarJSON"  >> beam.Map(json.dumps)
                | "EscribirArchivo" >> beam.io.WriteToText(
                    known_args.output_file,
                    file_name_suffix=".json",
                    shard_name_template="",
                )
            )

        # ------------------------------------------------------------------
        # 6. ALERTAS — rama paralela (preview Fase 4)
        # ------------------------------------------------------------------
        (
            eventos_chile
            | "FiltrarAlertas" >> beam.ParDo(FilterAlertas())
            | "LogAlertas"     >> beam.Map(
                lambda e: log.warning(
                    "🚨 ALERTA: M%.1f — %s (región: %s)",
                    e["magnitude"], e["place"], e["region_chile"]
                )
            )
        )


if __name__ == "__main__":
    run()
