# Alertas de presupuesto para el proyecto sismo-monitor
# CRÍTICO: este archivo debe existir antes de cualquier terraform apply
# Umbral total del proyecto: < $25 USD

resource "google_billing_budget" "sismo_monitor_budget" {
  billing_account = var.billing_account_id
  display_name    = "sismo-monitor-budget"

  budget_filter {
    projects = ["projects/${var.project_number}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = "25"
    }
  }

  # Alerta al 20% ($5) — primeras pruebas de Pub/Sub
  threshold_rules {
    threshold_percent = 0.2
    spend_basis       = "CURRENT_SPEND"
  }

  # Alerta al 60% ($15) — sesión Dataflow activa
  threshold_rules {
    threshold_percent = 0.6
    spend_basis       = "CURRENT_SPEND"
  }

  # Alerta al 100% ($25) — límite total del proyecto
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }

  all_updates_rule {
    monitoring_notification_channels = var.notification_channels
    disable_default_iam_recipients   = false
  }
}
