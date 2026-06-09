resource "google_bigquery_dataset" "sismo_monitor" {
  dataset_id  = "sismo_monitor"
  description = "Dataset principal del Monitor Sísmico Chile"
  location    = var.region

  delete_contents_on_destroy = false # preservar datos entre sesiones de trabajo
}

resource "google_bigquery_table" "eventos_raw" {
  dataset_id          = google_bigquery_dataset.sismo_monitor.dataset_id
  table_id            = "eventos_raw"
  deletion_protection = false

  description = "Eventos sísmicos de Chile procesados por el pipeline Beam"

  schema = jsonencode([
    { name = "id",                   type = "STRING",    mode = "REQUIRED", description = "ID único del evento USGS" },
    { name = "magnitud",             type = "FLOAT64",   mode = "NULLABLE", description = "Magnitud del sismo" },
    { name = "lugar",                type = "STRING",    mode = "NULLABLE", description = "Descripción textual del lugar (USGS)" },
    { name = "lat",                  type = "FLOAT64",   mode = "NULLABLE", description = "Latitud del epicentro" },
    { name = "lon",                  type = "FLOAT64",   mode = "NULLABLE", description = "Longitud del epicentro" },
    { name = "profundidad_km",       type = "FLOAT64",   mode = "NULLABLE", description = "Profundidad del foco en km" },
    { name = "timestamp_evento",     type = "TIMESTAMP", mode = "NULLABLE", description = "Hora del evento según USGS" },
    { name = "timestamp_ingesta",    type = "TIMESTAMP", mode = "NULLABLE", description = "Hora en que el poller publicó a Pub/Sub" },
    { name = "timestamp_procesado",  type = "TIMESTAMP", mode = "NULLABLE", description = "Hora en que el pipeline procesó el evento" },
    { name = "region_chile",         type = "STRING",    mode = "NULLABLE", description = "Región administrativa de Chile" },
    { name = "es_alerta",            type = "BOOL",      mode = "NULLABLE", description = "True si magnitud >= umbral de alerta" },
    { name = "url",                  type = "STRING",    mode = "NULLABLE", description = "URL del evento en USGS" },
    { name = "is_update",            type = "BOOL",      mode = "NULLABLE", description = "True si es republicación por actualización de USGS" }
  ])

  time_partitioning {
    type  = "DAY"
    field = "timestamp_evento"
  }
}

resource "google_bigquery_table" "metricas_ventana" {
  dataset_id          = google_bigquery_dataset.sismo_monitor.dataset_id
  table_id            = "metricas_ventana"
  deletion_protection = false

  description = "Métricas agregadas por región y ventana de 10 minutos"

  schema = jsonencode([
    { name = "region",          type = "STRING",    mode = "REQUIRED", description = "Región chilena" },
    { name = "ventana_inicio",  type = "TIMESTAMP", mode = "REQUIRED", description = "Inicio de la ventana de 10 minutos" },
    { name = "ventana_fin",     type = "TIMESTAMP", mode = "REQUIRED", description = "Fin de la ventana de 10 minutos" },
    { name = "conteo",          type = "INT64",     mode = "NULLABLE", description = "Cantidad de eventos en la ventana" },
    { name = "magnitud_max",    type = "FLOAT64",   mode = "NULLABLE", description = "Magnitud máxima en la ventana" },
    { name = "magnitud_promedio", type = "FLOAT64", mode = "NULLABLE", description = "Magnitud promedio en la ventana" }
  ])

  time_partitioning {
    type  = "DAY"
    field = "ventana_inicio"
  }
}
