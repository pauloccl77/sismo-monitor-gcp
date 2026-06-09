variable "project_id" {
  description = "ID del proyecto GCP (ej: sismo-monitor-pcl)"
  type        = string
}

variable "project_number" {
  description = "Número numérico del proyecto GCP (para budget filter)"
  type        = string
}

variable "billing_account_id" {
  description = "ID de la cuenta de facturación GCP (formato: XXXXXX-XXXXXX-XXXXXX)"
  type        = string
}

variable "region" {
  description = "Región principal GCP"
  type        = string
  default     = "us-central1"
}

variable "notification_channels" {
  description = "Lista de Monitoring Notification Channel IDs para alertas de presupuesto"
  type        = list(string)
  default     = []
}

variable "alert_email_from" {
  description = "Email verificado en SendGrid (sender) para las alertas sísmicas"
  type        = string
}

variable "alert_email_to" {
  description = "Email destinatario de las alertas sísmicas"
  type        = string
}
