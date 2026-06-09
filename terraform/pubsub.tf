resource "google_pubsub_topic" "sismos_raw" {
  name = "sismos-raw"

  message_retention_duration = "86600s" # 24 horas
}

resource "google_pubsub_topic" "sismos_alertas" {
  name = "sismos-alertas"
}

resource "google_pubsub_subscription" "sismos_raw_sub" {
  name  = "sismos-raw-sub"
  topic = google_pubsub_topic.sismos_raw.name

  ack_deadline_seconds = 60

  expiration_policy {
    ttl = "604800s" # 7 días — debe ser >= message_retention_duration
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }
}
