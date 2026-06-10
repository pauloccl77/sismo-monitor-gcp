resource "google_bigquery_data_transfer_config" "metricas_ventana" {
  display_name           = "Agregar métricas por ventana de 10 min"
  location               = var.region
  data_source_id         = "scheduled_query"
  schedule               = "every 10 minutes"
  destination_dataset_id = google_bigquery_dataset.sismo_monitor.dataset_id
  service_account_name   = google_service_account.cf_notifier.email

  params = {
    query = <<-SQL
      SELECT
        COALESCE(region_chile, 'Mundial') AS region,
        TIMESTAMP_TRUNC(timestamp_evento, MINUTE)
          - INTERVAL MOD(EXTRACT(MINUTE FROM timestamp_evento), 10) MINUTE
          AS ventana_inicio,
        TIMESTAMP_TRUNC(timestamp_evento, MINUTE)
          - INTERVAL MOD(EXTRACT(MINUTE FROM timestamp_evento), 10) MINUTE
          + INTERVAL 10 MINUTE
          AS ventana_fin,
        COUNT(*)      AS conteo,
        MAX(magnitud) AS magnitud_max,
        AVG(magnitud) AS magnitud_promedio
      FROM `${var.project_id}.sismo_monitor.eventos_raw`
      WHERE timestamp_evento IS NOT NULL
        AND magnitud IS NOT NULL
        AND (is_update IS NULL OR is_update = FALSE)
      GROUP BY 1, 2, 3
    SQL
    destination_table_name_template = "metricas_ventana"
    write_disposition               = "WRITE_TRUNCATE"
  }

  depends_on = [
    google_bigquery_table.eventos_raw,
    google_bigquery_table.metricas_ventana,
  ]
}
