output "project_id" {
  description = "ID del proyecto GCP"
  value       = var.project_id
}

output "region" {
  description = "Región principal del proyecto"
  value       = var.region
}

output "dataflow_temp_bucket" {
  description = "GCS bucket para archivos temporales de Dataflow"
  value       = "gs://${google_storage_bucket.dataflow_temp.name}/temp"
}
