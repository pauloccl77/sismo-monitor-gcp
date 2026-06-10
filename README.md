# 🌎 Monitor Sísmico Chile — GCP Streaming Pipeline

> Pipeline near real-time que ingesta sismos desde USGS, los procesa con Apache Beam y entrega alertas automáticas por email y Telegram.

---

## Por qué existe este proyecto

Vivo en Placilla, Valparaíso — una de las zonas sísmicas más activas del planeta.
La región de Valparaíso tuvo un enjambre de ~640 sismos en 8 días en 2017. El norte
de Chile registra actividad recurrente: enjambre Huasco marzo 2026, M6.9 Antofagasta
mayo 2026.

Este proyecto nace de querer conectar mi oficio con mi entorno: construir el mismo
tipo de pipeline event-driven que implementé en producción en Entel para KPIs
near real-time de clientes B2B, ahora sobre stack nativo GCP y con datos sísmicos reales.

**Aclaración honesta:** el sistema monitorea y visualiza actividad sísmica en tiempo real.
No predice terremotos — eso no es posible con los datos disponibles.

---

## Arquitectura

```
USGS GeoJSON Feed (actualizado cada 60s)
        │
        ▼
[Cloud Scheduler] → dispara cada 60s
        │
        ▼
[Cloud Run Job — Poller Python]
  · Descarga feed USGS
  · Deduplica por event ID
  · Filtra: bounding box Chile
  · Publica a Pub/Sub
        │
        ▼
[Pub/Sub — topic: sismos-raw]
        │
        ▼
[Dataflow — Apache Beam Streaming]
  · Filtro geográfico + magnitud mínima
  · Windowing: ventanas fijas de 10 minutos
  · Watermark: 20 min (latencia real USGS para Chile)
  · Enriquecimiento: nombre de región chilena
  · Escribe a BigQuery
  · Publica alertas si magnitud ≥ 5.0
        │
        ├─────────────────────────────┐
        ▼                             ▼
[BigQuery]                 [Pub/Sub — sismos-alertas]
  · eventos_raw                       │
  · metricas_ventana                  ▼
        │                  [Cloud Function — Notificador]
        ▼                    · Email vía SendGrid
[Looker Studio]              · Telegram Bot API
  · Mapa de epicentros
  · Serie temporal
  · Últimos eventos
```

---

## Stack GCP

| Servicio | Rol |
|---|---|
| Cloud Scheduler | Trigger periódico del poller (cada 60s) |
| Cloud Run Jobs | Poller de ingesta Python |
| Pub/Sub | Desacople ingesta / procesamiento |
| Dataflow (Apache Beam) | Pipeline streaming — filtro, windowing, escritura |
| BigQuery | Persistencia: histórico + métricas agregadas |
| Cloud Functions | Notificador de alertas |
| Looker Studio | Dashboard conectado a BigQuery |
| Terraform | Infraestructura como código (IaC) |

---

## Decisiones técnicas

**¿Por qué DirectRunner antes de Dataflow?**
Todo el pipeline se desarrolló y verificó localmente con `DirectRunner` antes de
desplegar a Dataflow. Esto garantiza que la lógica es correcta y evita gastar en
Dataflow mientras hay bugs. Dataflow streaming cobra por vCPU/hora — cada ciclo de
debug en la nube tiene costo real.

**¿Por qué watermark de 20 minutos?**
El feed USGS actualiza cada 60 segundos, pero los eventos fuera de EE.UU. pueden
tardar hasta 15-20 minutos en aparecer. El watermark le dice a Beam que espere
ese tiempo antes de cerrar una ventana, evitando descartar eventos tardíos válidos.

**¿Por qué ventanas de 10 minutos?**
Balance entre latencia de las métricas agregadas y volumen de eventos. Chile registra
~50-200 eventos/día — ventanas de 10 minutos dan resolución suficiente sin generar
demasiadas filas en `metricas_ventana`.

**¿Por qué Dataflow lee desde la suscripción y no desde el topic?**
Apache Beam permite apuntar `ReadFromPubSub` al topic directamente — Beam crea una
suscripción temporal. El problema: esa suscripción no existe hasta que Dataflow arranca,
por lo que cualquier mensaje publicado mientras el job estaba caído se pierde.
Leyendo desde `sismos-raw-sub` (suscripción fija creada por Terraform), los mensajes se
acumulan con retención de 7 días independientemente del estado del job. Al reiniciar
Dataflow, procesa todo lo que esperaba en la cola — sin pérdida de eventos.

**¿Por qué `metricas_ventana` se puebla con Scheduled Query y no con windowing en Beam?**
Apache Beam tiene soporte nativo para ventanas de tiempo (`FixedWindows`), pero activarlo
añade ~30 minutos de latencia antes de que los datos lleguen a BigQuery (ventana + watermark).
Para el volumen de este proyecto esa latencia no aporta valor. En su lugar, una Scheduled
Query en BigQuery agrega `eventos_raw` cada 10 minutos — mismo resultado, sin complejidad
adicional en el pipeline.

Esto introduce además una capa de agregación pre-calculada: Looker Studio consulta
`metricas_ventana` en vez de agregar en tiempo real sobre los datos crudos. En BigQuery
se cobra por datos escaneados — una tabla de hechos agregados puede reducir el volumen
escaneado en más de un 99% respecto a la tabla de eventos. Patrón equivalente a la capa
Gold en Medallion Architecture o las tablas de métricas en Microsoft Fabric.

**¿Por qué `terraform destroy` al terminar cada sesión?**
Sin créditos GCP disponibles, todo gasto es real. Dataflow streaming cuesta ~$0.056/vCPU-hora.
Dejar el pipeline corriendo sin supervisión puede generar cargos innecesarios. La infra
se levanta en minutos con `terraform apply` cuando se necesita.

---

## Conexión con experiencia previa

| Lo que hice en Entel (Microsoft Fabric) | Equivalente en este proyecto (GCP) |
|---|---|
| Pipelines near real-time para KPIs B2B | Dataflow streaming con ventanas de 10 min |
| Ingesta multifuente (APIs, JSON, BD) | Poller USGS → Pub/Sub (API REST + JSON) |
| Reducción latencia de días a horas | Alertas en menos de 3 min desde evento |
| DWH con capas raw > staging > marts | BigQuery: eventos_raw + metricas_ventana |
| Monitoreo y alertas operativas | Cloud Monitoring + notificaciones automáticas |

---

## Estructura del repositorio

```
sismo-monitor-gcp/
├── terraform/          # Infraestructura como código
│   ├── main.tf         # Provider GCP
│   ├── variables.tf    # Variables del proyecto
│   ├── budget.tf       # Alertas de presupuesto $5/$15/$25 USD
│   └── pubsub.tf       # Topics y subscriptions
├── ingestion/
│   ├── poller.py       # Poller USGS → Pub/Sub
│   └── Dockerfile      # Imagen para Cloud Run
├── pipeline/
│   ├── pipeline.py     # Pipeline Apache Beam principal
│   ├── transforms.py   # Transformaciones reutilizables
│   └── config.py       # Umbrales y parámetros configurables
├── alerting/
│   └── notifier.py     # Cloud Function — email + Telegram
└── queries/
    └── dashboard_queries.sql
```

---

## Cómo correr el proyecto

### Prerequisitos
- Python 3.11+
- Terraform >= 1.5
- gcloud CLI configurado (`gcloud auth application-default login`)
- Proyecto GCP con billing activo

### Setup
```bash
# 1. Clonar el repo
git clone https://github.com/pauloccl77/sismo-monitor-gcp.git
cd sismo-monitor-gcp

# 2. Configurar variables
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# editar terraform.tfvars con tu project_id y billing_account_id

# 3. Levantar infraestructura
cd terraform
terraform init
terraform apply

# 4. Instalar dependencias del poller
cd ../ingestion
pip install -r requirements.txt

# 5. Correr el poller localmente
GCP_PROJECT_ID=tu-project-id python poller.py

# 6. Probar el pipeline con DirectRunner
cd ../pipeline
pip install -r requirements.txt
python pipeline.py --input_file=test_events.json
```

### Destruir infraestructura al terminar
```bash
cd terraform && terraform destroy
```

---

## Estado actual

| Fase | Estado |
|---|---|
| 0 — Fundaciones (GCP + Terraform + presupuesto) | ✅ Completada |
| 1 — Ingesta a Pub/Sub | ✅ Completada |
| 2 — Pipeline Beam DirectRunner local | ✅ Completada |
| 3 — BigQuery + Dataflow | 🔄 En construcción |
| 4 — Alertas email + Telegram | ⏳ Pendiente |
| 5 — Dashboard Looker Studio | ⏳ Pendiente |

---

## Limitaciones conocidas

- **Latencia USGS para Chile:** hasta 20 minutos post-evento. No es un sistema de alerta temprana — es un monitor de actividad.

- **Frecuencia del feed:** USGS actualiza cada 60 segundos. No hay datos en tiempo real con sub-segundo de latencia.

- **Cobertura:** solo eventos registrados por la red de sensores USGS. Eventos muy locales o superficiales pueden no aparecer.

- **Eventos preliminares sin magnitud:** USGS publica algunos eventos inmediatamente después del sismo sin magnitud calculada (`magnitude: null`). El poller los captura en ese estado. Solución implementada: el poller rastrea el campo `updated` de cada evento — si USGS lo actualiza con magnitud revisada, el evento se republica automáticamente al pipeline. El campo `is_update: true` en BigQuery identifica estas republicaciones.

- **Duplicados en BigQuery:** dado que los eventos actualizados se repubican, puede haber múltiples filas para el mismo `id` en `eventos_raw` — una con datos preliminares y otra con datos revisados. Para análisis, filtrar por `is_update = false` o usar `MAX(timestamp_procesado)` por `id`.

---

*Desarrollado por Paulo César Contreras Ledezma — Data Engineer Senior*
*Valparaíso, Chile — 2026*
