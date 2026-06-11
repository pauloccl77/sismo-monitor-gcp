# BQ Data Transfer Service necesita impersonar cf-notifier para ejecutar scripts DML
resource "google_service_account_iam_member" "dts_impersonate_cf_notifier" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/cf-notifier@${var.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${var.project_number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}

resource "google_bigquery_data_transfer_config" "metricas_ventana" {
  display_name         = "Agregar métricas por ventana de 10 min"
  location             = var.region
  data_source_id       = "scheduled_query"
  schedule             = "every 10 minutes"
  service_account_name = google_service_account.cf_notifier.email

  params = {
    query = <<-SQL
      DELETE FROM `${var.project_id}.sismo_monitor.metricas_ventana`
      WHERE ventana_inicio >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY);

      INSERT INTO `${var.project_id}.sismo_monitor.metricas_ventana`
      SELECT
        COALESCE(region_chile, 'Sin lugar') AS region,
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
      WHERE timestamp_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
        AND timestamp_evento IS NOT NULL
        AND magnitud IS NOT NULL
        AND (is_update IS NULL OR is_update = FALSE)
      GROUP BY 1, 2, 3;
    SQL
  }

  depends_on = [
    google_bigquery_table.eventos_raw,
    google_bigquery_table.metricas_ventana,
  ]
}
