# 🌎 Monitor Sísmico Chile — GCP Streaming Pipeline

[![CI](https://github.com/pauloccl77/sismo-monitor-gcp/actions/workflows/ci.yml/badge.svg)](https://github.com/pauloccl77/sismo-monitor-gcp/actions/workflows/ci.yml)

> Pipeline near real-time que ingesta sismos desde USGS, los procesa con Apache Beam y entrega alertas automáticas por email y Telegram.

**[Ver Dashboard en vivo →](https://datastudio.google.com/u/0/reporting/9ba90f6b-b7b5-4b0f-ac13-61a0925ad942/page/Lys0F?s=pPzL-zN4qug)**

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
[Poller Python — local / CLI]
  · Descarga feed USGS cada 60s
  · Deduplica por (event ID, updated timestamp)
  · Publica a Pub/Sub
        │
        ▼
[Pub/Sub — topic: sismos-raw]
        │
        ▼
[Dataflow — Apache Beam Streaming]
  · Filtro geográfico + magnitud mínima
  · Enriquecimiento: región chilena o país USGS
  · Escribe a BigQuery
  · Publica alertas si magnitud ≥ 5.0
        │
        ├─────────────────────────────┐
        ▼                             ▼
[BigQuery]                 [Pub/Sub — sismos-alertas]
  · eventos_raw                       │
  · metricas_ventana ←────────        ▼
        │            Scheduled [Cloud Function — Notificador]
        │              Query     · Dedup via alertas_enviadas
        ▼             (10 min)   · Email vía SendGrid
[Looker Studio]                  · Telegram Bot API
  · Mapa de epicentros
  · Serie temporal por región
  · Scorecards en tiempo real
```

> **Mejora futura (Fase 8 — optativa):** desplegar el poller como Cloud Run Job + Cloud Scheduler para eliminar la dependencia de una máquina local corriendo.

---

## Stack GCP

| Servicio | Rol |
|---|---|
| Pub/Sub | Desacople ingesta / procesamiento |
| Dataflow (Apache Beam) | Pipeline streaming — filtro, enriquecimiento, escritura |
| BigQuery | Persistencia: histórico (`eventos_raw`) + métricas (`metricas_ventana`) |
| BigQuery Data Transfer | Scheduled Query — agrega métricas cada 10 min |
| Cloud Functions (2ª gen) | Notificador de alertas (Telegram + email) |
| Secret Manager | Credenciales SendGrid y Telegram |
| Looker Studio | Dashboard conectado a BigQuery |
| Terraform | Infraestructura como código (IaC) |

### Desarrollo y CI/CD

| Herramienta | Rol |
|---|---|
| Docker | Containeriza el poller (`ingestion/Dockerfile`) — base para migrarlo a Cloud Run Job en Fase 8 |
| GitHub Actions | CI: lint, tests, build de imagen Docker, `terraform plan` en cada push a `main` |
| Workload Identity Federation | Autenticación de GitHub Actions contra GCP sin llaves de larga duración |
| ruff | Linter Python (`pyproject.toml`) |
| pytest | Tests unitarios de la lógica de deduplicación y parsing del poller |

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

**¿Por qué el dashboard usa dos fuentes de datos distintas?**
Looker Studio ejecuta una query independiente por cada chart al cargar la página.
Para mantener el dashboard rápido y el costo de BQ bajo se usan dos fuentes:

- `metricas_ventana` para todos los charts agregados (serie temporal, barras por región,
  scorecards). Esta tabla es pequeña y siempre rápida — el trabajo pesado ya lo hizo
  la Scheduled Query.
- `eventos_raw` solo para el mapa de epicentros, filtrado a los últimos 7 días.
  Como la tabla está particionada por `timestamp_evento`, BQ aplica partition pruning
  y escanea solo las 7 particiones del período — no el histórico completo.

Looker Studio cachea los resultados (por defecto 12h), por lo que visitas repetidas
al dashboard no generan queries adicionales a BigQuery.

**¿Por qué las fuentes de Looker Studio usan consulta personalizada y no la tabla directa?**
Los campos TIMESTAMP en BigQuery se almacenan en UTC. Conectar la tabla directamente
muestra las horas en UTC en el dashboard. Para mostrar hora Chile, las fuentes usan
consultas personalizadas con `DATETIME(campo, 'America/Santiago')` — la conversión
ocurre en BigQuery antes de que Looker Studio reciba los datos.

Consulta fuente `metricas_ventana`:
```sql
SELECT
  region,
  DATETIME(ventana_inicio, 'America/Santiago') AS ventana_inicio,
  DATETIME(ventana_fin,    'America/Santiago') AS ventana_fin,
  conteo,
  magnitud_max,
  magnitud_promedio
FROM `sismo-monitor-pcl.sismo_monitor.metricas_ventana`
```

Consulta fuente `eventos_raw` (mapa — últimos 7 días):
```sql
SELECT
  id, magnitud, lugar,
  COALESCE(region_chile, 'Mundial') AS region,
  lat, lon, profundidad_km,
  DATETIME(timestamp_evento, 'America/Santiago') AS timestamp_evento,
  es_alerta, url
FROM `sismo-monitor-pcl.sismo_monitor.eventos_raw`
WHERE timestamp_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND (is_update IS NULL OR is_update = FALSE)
  AND lat IS NOT NULL
  AND lon IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_procesado DESC) = 1
```

**Alcance mundial vs. producción Chile — decisión de visualización**
El objetivo del proyecto es la detección y registro de sismos en Chile. Sin embargo,
para demostrar el pipeline con datos reales y un dashboard visualmente más rico,
el sistema opera en modo mundial durante el desarrollo:

| Parámetro | Modo demo (actual) | Producción Chile |
|---|---|---|
| `MAG_ALERT_THRESHOLD` | 5.0 | 5.0 |
| Filtro geográfico | Mundial (FilterChile comentado) | Solo Chile |
| Campo `region_chile` | País del evento (de USGS `place`) | Región administrativa de Chile |

**Para pasar a producción Chile**, descomentar en `pipeline/pipeline.py`:
```python
# | "FiltrarChile" >> beam.ParDo(FilterChile())
```
El resto del pipeline opera sin cambios — `EnrichEvent` ya asigna la región chilena
correcta cuando las coordenadas están dentro del bounding box de Chile.

**¿Por qué `terraform destroy` al terminar cada sesión?**
Sin créditos GCP disponibles, todo gasto es real. Dataflow streaming cuesta ~$0.056/vCPU-hora.
Dejar el pipeline corriendo sin supervisión puede generar cargos innecesarios. La infra
se levanta en minutos con `terraform apply` cuando se necesita.

**¿Por qué GitHub Actions se autentica contra GCP con Workload Identity Federation y no con una service account key?**
La alternativa clásica es generar un archivo JSON de credenciales y guardarlo como secret
en GitHub — funciona, pero es una credencial de larga duración: si se filtra, sigue siendo
válida hasta que alguien la revoque manualmente. Con WIF, GCP confía directamente en los
tokens OIDC de corta duración que GitHub firma para este repositorio (`pauloccl77/sismo-monitor-gcp`);
no hay ninguna llave que guardar, rotar ni pueda filtrarse. La service account que usa el
CI (`ci-terraform-plan`) es además de solo lectura — el job de CI únicamente corre
`terraform plan`, nunca `apply`.

**¿Por qué el smoke test del contenedor solo importa el módulo y no corre `main()`?**
Ejecutar el loop completo del poller requeriría credenciales GCP reales (para instanciar
`PublisherClient`) y acceso de red al feed de USGS — ninguno de los dos está disponible
en el runner de CI, y no tiene sentido darle credenciales de escritura a un job de build.
El smoke test valida lo que sí se puede validar sin salir del contenedor: que la imagen
buildea y que el módulo importa y ejecuta sin errores de sintaxis o dependencias faltantes.

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
├── .github/
│   └── workflows/
│       └── ci.yml            # Lint, test, docker build, terraform plan (WIF)
├── terraform/
│   ├── main.tf              # Provider GCP
│   ├── variables.tf         # Variables del proyecto
│   ├── budget.tf            # Alertas de presupuesto $5/$15/$25 USD
│   ├── pubsub.tf            # Topics sismos-raw y sismos-alertas + suscripción
│   ├── bigquery.tf          # Dataset, tablas eventos_raw, metricas_ventana, alertas_enviadas
│   ├── storage.tf           # Buckets Dataflow temp y CF source
│   ├── cloudfunction.tf     # Cloud Function notificador + SA cf-notifier + IAM
│   ├── scheduled_query.tf   # Scheduled Query metricas_ventana (cada 10 min)
│   └── outputs.tf
├── ingestion/
│   ├── poller.py            # Poller USGS → Pub/Sub (dedup por id+updated)
│   ├── test_poller.py       # Tests unitarios (dedup, parsing, filtro geográfico)
│   ├── Dockerfile           # Imagen del poller — base para Cloud Run Job (Fase 8)
│   └── .dockerignore
├── pipeline/
│   ├── pipeline.py          # Pipeline Apache Beam principal
│   ├── transforms.py        # ParseMessage, EnrichEvent, FilterAlertas, etc.
│   ├── config.py            # Umbrales y parámetros configurables
│   └── setup.py             # Empaquetado de módulos para workers Dataflow
├── alerting/
│   └── main.py              # Cloud Function — Telegram + email vía SendGrid REST
└── pyproject.toml           # Configuración de ruff (lint)
```

---

## Cómo correr el proyecto

### Prerequisitos
- Python 3.11+
- Terraform >= 1.5
- gcloud CLI configurado (`gcloud auth application-default login`)
- Proyecto GCP con billing activo
- Docker (opcional — solo para correr el poller containerizado)

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

# 5. Correr el poller localmente (Terminal 1)
GCP_PROJECT_ID=tu-project-id python poller.py

# 5b. Alternativa: correr el poller en Docker
docker build -t sismo-poller .
docker run --rm \
  -e GCP_PROJECT_ID=tu-project-id \
  -v ~/.config/gcloud:/home/poller/.config/gcloud:ro \
  sismo-poller

# 6. Correr el pipeline (Terminal 2)
cd ../pipeline
pip install -r requirements.txt

# Modo local (sin costo, para desarrollo)
python pipeline.py

# Modo Dataflow (streaming en GCP)
python pipeline.py \
  --runner=DataflowRunner \
  --temp_location=gs://TU_BUCKET/temp \
  --region=us-east1 \
  --worker_machine_type=e2-standard-2 \
  --max_num_workers=1 \
  --setup_file=$(pwd)/setup.py
```

### Destruir infraestructura al terminar
```bash
# Destroy selectivo — preserva BigQuery con datos históricos
cd terraform
terraform destroy \
  -target=google_pubsub_subscription.sismos_raw_sub \
  -target=google_pubsub_topic.sismos_raw \
  -target=google_storage_bucket.dataflow_temp
```

---

## Evidencia del sistema en producción

### Poller — publicando eventos a Pub/Sub
![Poller USGS corriendo](docs/00_1%20poller.png)

### Pub/Sub — topic `sismos-raw`
![Topic sismos-raw con mensajes](docs/00_2%20pub_sub%20-%20sismos_raw.png)

### BigQuery — tabla `eventos_raw`
![Registros en eventos_raw](docs/01%20registro%20BQ%20-%20tabla%20eventos_raw.png)

### BigQuery — Scheduled Query `metricas_ventana`
![Scheduled Query ejecutándose](docs/02%20BQ%20-%20Scheduled%20Query.png)

### BigQuery — tabla `metricas_ventana`
![Tabla metricas_ventana](docs/02_1%20BQ%20-%20metricas_ventana.png)

### Dataflow — Gráfico del pipeline
![Dataflow grafo del pipeline](docs/03_1%20Dataflow%20job%20-%20diagrama.png)
![Dataflow grafo detalle](docs/03_2%20Dataflow%20job%20-%20diagrama.png)

### Dataflow — Métricas en tiempo real
![Dataflow métricas por etapa](docs/03_3%20Dataflow%20job%20-%20grafico.png)
![Dataflow watermark age](docs/03_4%20Dataflow%20job%20-%20grafico.png)

### Dashboard Looker Studio
![Dashboard Looker Studio](docs/04%20Dashboard.png)

### Cloud Function — sismo-notifier
![Cloud Function sismo-notifier](docs/05_1%20CF%20-%20sismo-notifier.png)

### Pub/Sub — topic `sismos-alertas`
![Topic sismos-alertas](docs/05_2%20pubsub%20-%20sismos-alertas.png)

### Alerta Telegram
![Alerta recibida en Telegram](docs/06_1%20Alerta%20Telegram.png)

### Alerta Email
![Alerta recibida por email](docs/06_2%20Alerta%20Email.png)

---

## Estado actual

| Fase | Estado |
|---|---|
| 0 — Fundaciones (GCP + Terraform + presupuesto) | ✅ Completada |
| 1 — Ingesta a Pub/Sub | ✅ Completada |
| 2 — Pipeline Beam DirectRunner local | ✅ Completada |
| 3 — BigQuery + Dataflow streaming | ✅ Completada |
| 4 — Alertas email + Telegram | ✅ Completada |
| 5 — Dashboard Looker Studio | ✅ Completada |
| 6 — Evidencia + destroy final | ✅ Completada |
| 7 — README final | ✅ Completada |
| 8 — Poller en Cloud Run Job (optativa) | ⏳ Pendiente |
| — Docker + CI/CD (GitHub Actions) | ✅ Completada |

---

## Limitaciones conocidas

- **Latencia USGS para Chile:** hasta 20 minutos post-evento. No es un sistema de alerta temprana — es un monitor de actividad.

- **Frecuencia del feed:** USGS actualiza cada 60 segundos. No hay datos en tiempo real con sub-segundo de latencia.

- **Cobertura:** solo eventos registrados por la red de sensores USGS. Eventos muy locales o superficiales pueden no aparecer.

- **Eventos preliminares sin magnitud:** USGS publica algunos eventos inmediatamente después del sismo sin magnitud calculada (`magnitude: null`). El poller los captura en ese estado. Solución implementada: el poller rastrea el campo `updated` de cada evento — si USGS lo actualiza con magnitud revisada, el evento se republica automáticamente al pipeline. El campo `is_update: true` en BigQuery identifica estas republicaciones.

- **Duplicados en BigQuery:** dado que los eventos actualizados se repubican, puede haber múltiples filas para el mismo `id` en `eventos_raw` — una con datos preliminares y otra con datos revisados. Para análisis, filtrar por `is_update = false` o usar `MAX(timestamp_procesado)` por `id`.

- **Docker build validado en CI, no localmente:** la imagen del poller se construye y corre con éxito en cada push (ver badge de CI arriba), pero no se validó `docker build`/`docker run` en la máquina de desarrollo por no tener Docker/WSL2 instalado. El smoke test del contenedor solo confirma que el módulo importa — no ejecuta el loop de polling completo, que requiere credenciales GCP reales.

---

*Desarrollado por Paulo César Contreras Ledezma — Data Engineer Senior*
*Valparaíso, Chile — 2026*
