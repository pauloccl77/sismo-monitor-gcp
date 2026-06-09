# ---------------------------------------------------------------------------
# Secretos — creados via gcloud antes del primer terraform apply (ver CLAUDE.md)
# Referenciados aquí como data sources para IAM y la Cloud Function
# ---------------------------------------------------------------------------
data "google_secret_manager_secret" "sendgrid_api_key" {
  secret_id = "sendgrid-api-key"
}

data "google_secret_manager_secret" "telegram_bot_token" {
  secret_id = "telegram-bot-token"
}

data "google_secret_manager_secret" "telegram_chat_id" {
  secret_id = "telegram-chat-id"
}

# ---------------------------------------------------------------------------
# Service account dedicado a la Cloud Function
# ---------------------------------------------------------------------------
resource "google_service_account" "cf_notifier" {
  account_id   = "cf-notifier"
  display_name = "Sismo Monitor — Cloud Function Notifier"
}

resource "google_bigquery_dataset_iam_member" "cf_bq_editor" {
  dataset_id = google_bigquery_dataset.sismo_monitor.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.cf_notifier.email}"
}

resource "google_project_iam_member" "cf_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.cf_notifier.email}"
}

resource "google_secret_manager_secret_iam_member" "cf_sendgrid" {
  secret_id = data.google_secret_manager_secret.sendgrid_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cf_notifier.email}"
}

resource "google_secret_manager_secret_iam_member" "cf_telegram_token" {
  secret_id = data.google_secret_manager_secret.telegram_bot_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cf_notifier.email}"
}

resource "google_secret_manager_secret_iam_member" "cf_telegram_chat" {
  secret_id = data.google_secret_manager_secret.telegram_chat_id.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cf_notifier.email}"
}

# ---------------------------------------------------------------------------
# Fuente — zip de alerting/ subido a GCS
# Se regenera automáticamente cuando cambia el código
# ---------------------------------------------------------------------------
data "archive_file" "cf_source" {
  type        = "zip"
  source_dir  = "${path.module}/../alerting"
  output_path = "${path.module}/cf-notifier.zip"
}

resource "google_storage_bucket_object" "cf_source" {
  name   = "cf-notifier-${data.archive_file.cf_source.output_md5}.zip"
  bucket = google_storage_bucket.cf_source.name
  source = data.archive_file.cf_source.output_path
}

# ---------------------------------------------------------------------------
# Cloud Function 2ª gen
# ---------------------------------------------------------------------------
resource "google_cloudfunctions2_function" "notifier" {
  name     = "sismo-notifier"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "notify"
    source {
      storage_source {
        bucket = google_storage_bucket.cf_source.name
        object = google_storage_bucket_object.cf_source.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    min_instance_count    = 0
    available_memory      = "256M"
    timeout_seconds       = 60
    service_account_email = google_service_account.cf_notifier.email

    environment_variables = {
      GCP_PROJECT_ID   = var.project_id
      ALERT_EMAIL_FROM = var.alert_email_from
      ALERT_EMAIL_TO   = var.alert_email_to
    }

    secret_environment_variables {
      key        = "SENDGRID_API_KEY"
      project_id = var.project_id
      secret     = data.google_secret_manager_secret.sendgrid_api_key.secret_id
      version    = "latest"
    }

    secret_environment_variables {
      key        = "TELEGRAM_BOT_TOKEN"
      project_id = var.project_id
      secret     = data.google_secret_manager_secret.telegram_bot_token.secret_id
      version    = "latest"
    }

    secret_environment_variables {
      key        = "TELEGRAM_CHAT_ID"
      project_id = var.project_id
      secret     = data.google_secret_manager_secret.telegram_chat_id.secret_id
      version    = "latest"
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.sismos_alertas.id
    retry_policy   = "RETRY_POLICY_DO_NOT_RETRY"
  }
}
