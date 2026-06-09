resource "google_storage_bucket" "cf_source" {
  name          = "${var.project_id}-cf-source"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "dataflow_temp" {
  name          = "${var.project_id}-dataflow-temp"
  location      = var.region
  force_destroy = true # permite terraform destroy limpio

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1 # eliminar archivos temporales después de 1 día
    }
    action {
      type = "Delete"
    }
  }
}
