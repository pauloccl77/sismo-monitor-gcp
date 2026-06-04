# 🌎 Proyecto: Monitor Sísmico Chile — Pipeline Streaming en GCP
> Brief técnico para construcción incremental con Claude Code

---

## Contexto y narrativa

Vivo en Placilla, Valparaíso — una de las zonas sísmicas más activas del planeta.
La región de Valparaíso tuvo un enjambre de ~640 sismos en 8 días en 2017. El norte
de Chile registra actividad recurrente (enjambre Huasco marzo 2026, M6.9 Antofagasta
mayo 2026). Este proyecto nace de querer conectar mi oficio con mi entorno.

**Objetivo técnico:** construir un sistema que ingeste datos sísmicos en tiempo real,
los procese con lógica de negocio y entregue alertas y visualizaciones automáticas.

**Objetivo profesional:** demostrar dominio de arquitectura event-driven sobre GCP
(Pub/Sub + Dataflow + BigQuery), el mismo patrón que implementé en producción en Entel
para KPIs near real-time de clientes B2B, ahora sobre stack nativo GCP.

**Aclaración honesta (va en el README):** el proyecto no predice terremotos — eso
no es posible. Monitorea y visualiza actividad en tiempo real. Esta distinción es
parte deliberada de la narrativa técnica madura.

---

## Arquitectura — flujo completo

```
USGS GeoJSON Feed (cachéado 60s)
        │
        ▼
[Cloud Scheduler] — dispara cada 60s
        │
        ▼
[Cloud Run Job / Cloud Function — Poller Python]
  - Lee feed USGS
  - Deduplica por event ID (estado en Firestore o memoria)
  - Filtra: solo eventos nuevos o actualizados
  - Publica a Pub/Sub
        │
        ▼
[Pub/Sub — Topic: sismos-raw]
        │
        ▼
[Dataflow — Apache Beam Streaming Pipeline]
  - Ingesta desde Pub/Sub
  - Filtro geográfico: bounding box Chile (-18°, -56° lat / -66°, -75° lon)
  - Filtro por magnitud mínima configurable (default: 2.5)
  - Windowing: conteo de eventos por región cada 10 minutos
  - Manejo de datos tardíos (watermark 20 min — latencia real USGS para Chile)
  - Manejo de duplicados (por event ID)
  - Escribe a BigQuery
  - Publica a topic de alertas si magnitud > umbral_alerta (default: 5.0)
        │
        ├──────────────────────────────────────┐
        ▼                                      ▼
[BigQuery]                          [Pub/Sub — Topic: sismos-alertas]
  - tabla: eventos_raw                         │
  - tabla: metricas_ventana                    ▼
        │                           [Cloud Function — Notificador]
        ▼                             - Email (SendGrid) o Telegram Bot
[Looker Studio Dashboard]
  - Mapa de epicentros (lat/lon/magnitud)
  - Serie temporal de actividad
  - Conteo por región
  - Últimos N eventos
```

**Fuente de datos:** USGS Earthquake GeoJSON Feed — gratuita, sin auth.
- URL base: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/`
- Endpoint recomendado: `all_hour.geojson` (todos los sismos última hora)
- Refresh real del feed: cada 60 segundos (respetar este límite)
- Latencia eventos fuera de EE.UU.: hasta 15-20 minutos post-evento

**Fuente complementaria (opcional Fase posterior):**
Centro Sismológico Nacional U. de Chile — `https://www.sismologia.cl`

---

## Stack GCP utilizado

| Servicio | Rol en el proyecto |
|---|---|
| Cloud Scheduler | Trigger periódico del poller (cada 60s) |
| Cloud Run Jobs | Poller de ingesta — Python |
| Pub/Sub | Desacople ingesta / procesamiento |
| Dataflow (Apache Beam) | Pipeline streaming — filtro, windowing, escritura |
| BigQuery | Persistencia: histórico + métricas agregadas |
| Cloud Functions | Notificador de alertas |
| Looker Studio | Dashboard conectado a BigQuery |
| Terraform | Toda la infraestructura como código (IaC) |
| Artifact Registry | Imagen Docker del poller (si se usa Cloud Run) |
| Cloud Build | CI/CD opcional (Fase avanzada) |

## Estrategia de costos (sin créditos GCP disponibles)

Los $300 de crédito inicial ya están consumidos. Todo gasto es real desde el día uno.
El proyecto es perfectamente viable con inversión controlada — la clave es construir
en ráfagas, no dejar infraestructura corriendo entre sesiones.

**Estimación realista por fase:**

| Fase | Costo estimado | Qué gasta |
|---|---|---|
| 0 — Fundaciones | $0 | Solo configuración |
| 1 — Pub/Sub + poller | < $0.10 | Pub/Sub: primeros 10 GB/mes son gratuitos siempre |
| 2 — DirectRunner local | $0 | Corre en tu máquina, sin GCP |
| 3 — Dataflow + BigQuery | $3–8 por sesión de trabajo | Dataflow streaming: ~$0.056/vCPU-hora |
| 4 — Alertas | $0 | Cloud Functions: 2M invocaciones/mes gratis siempre |
| 5 — Looker Studio | $0 | Gratis sobre BigQuery |
| 6 — Cosecha + destroy | Para el medidor | `terraform destroy` apenas capturas evidencia |

**Costo total estimado del proyecto: $10–25 USD**, asumiendo disciplina de destroy.

**Servicios con capa gratuita permanente (nunca cobran aunque no haya créditos):**
- BigQuery: 1 TiB queries/mes + 10 GB storage
- Cloud Functions: 2M invocaciones/mes
- Cloud Scheduler: 3 jobs
- Pub/Sub: primeros 10 GB/mes

**El único componente caro: Dataflow streaming.**
Para este volumen de datos (sismos Chile = ~50–200 eventos/día), correrlo 24/7 es
innecesario y costoso (~$40/mes). La estrategia correcta para un proyecto portfolio:
- Levantar → capturar evidencia (dashboard vivo, alerta llegando) → destruir
- El pipeline demuestra las mismas competencias corriendo 4 horas que corriendo un mes

**Presupuesto sugerido:** configurar alerta en GCP a $5 / $15 / $25 USD.
Si llegas a $25, algo está mal — revisar antes de continuar.

---

## Estructura de carpetas del repositorio

```
sismo-monitor-gcp/
├── README.md                  # Narrativa, arquitectura, decisiones técnicas
├── docs/
│   └── arquitectura.png       # Diagrama (generar con draw.io o similar)
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── pubsub.tf
│   ├── bigquery.tf
│   ├── scheduler.tf
│   ├── cloudrun.tf
│   └── budget.tf              # CRÍTICO: alertas de presupuesto
├── ingestion/
│   ├── poller.py              # Lógica de ingesta USGS → Pub/Sub
│   ├── requirements.txt
│   └── Dockerfile
├── pipeline/
│   ├── pipeline.py            # Apache Beam pipeline principal
│   ├── transforms.py          # Transformaciones reutilizables
│   ├── config.py              # Umbrales, bounding box, etc.
│   └── requirements.txt
├── alerting/
│   └── notifier.py            # Cloud Function notificador
├── queries/
│   └── dashboard_queries.sql  # Queries base para Looker Studio
└── scripts/
    ├── setup.sh               # Setup inicial gcloud
    ├── deploy.sh              # Deploy completo
    └── destroy.sh             # Limpieza segura de infraestructura
```

---

## Plan de construcción por fases

### ✅ Fase 0 — Fundaciones (sin gasto de crédito)
**Objetivo:** cuenta GCP lista, repo inicializado, presupuesto protegido.

Tareas:
- [ ] Crear proyecto GCP: `sismo-monitor-[iniciales]`
- [ ] Activar APIs: Pub/Sub, Dataflow, BigQuery, Cloud Run, Scheduler, Functions
- [ ] Configurar `gcloud` CLI localmente
- [ ] Crear alerta de presupuesto en GCP: 50% / 90% / 100% de $300
- [ ] Inicializar repo Git en GitHub con estructura de carpetas
- [ ] Crear `terraform/budget.tf` como primer archivo

**Criterio de éxito:** `gcloud projects list` retorna el proyecto, repo en GitHub existe.

---

### ✅ Fase 1 — Ingesta a Pub/Sub
**Objetivo:** mensajes reales de USGS llegando a Pub/Sub.

Tareas:
- [ ] `terraform/pubsub.tf`: topic `sismos-raw` + subscription
- [ ] `ingestion/poller.py`: fetch USGS → deduplicar por `id` → publicar a Pub/Sub
- [ ] Probar poller localmente con credenciales (`gcloud auth application-default login`)
- [ ] Verificar mensajes en consola GCP → Pub/Sub → topic → ver mensajes

**Criterio de éxito:** mensajes JSON de sismos visibles en consola Pub/Sub.

**Nota técnica:** el campo `id` del GeoJSON USGS es el identificador único del evento.
El campo `updated` cambia cuando el evento se revisa. Guardar ambos para deduplicar.

---

### ✅ Fase 2 — Pipeline Beam en LOCAL (DirectRunner — sin costo)
**Objetivo:** toda la lógica de procesamiento probada y funcionando antes de Dataflow.

Tareas:
- [ ] `pipeline/config.py`: bounding box Chile, umbrales configurables
- [ ] `pipeline/pipeline.py`: leer Pub/Sub → filtrar → windowing → escribir (a archivo local primero)
- [ ] `pipeline/transforms.py`: filtro geográfico, parseo, enriquecimiento (nombre región)
- [ ] Correr con `--runner=DirectRunner` y mensajes de prueba
- [ ] Probar manejo de duplicados con mismo `id` publicado dos veces
- [ ] Probar datos tardíos bajando el watermark

**Criterio de éxito:** pipeline local procesa eventos de prueba, filtra correctamente
Chile vs fuera de Chile, y agrupa en ventanas de 10 minutos.

**Regla:** NO pasar a Fase 3 hasta que DirectRunner funcione sin errores.

---

### ✅ Fase 3 — BigQuery + Dataflow (primer gasto real)
**Objetivo:** pipeline corriendo en cloud, datos en BigQuery.

Tareas:
- [ ] `terraform/bigquery.tf`: dataset `sismo_monitor`, tablas `eventos_raw` y `metricas_ventana`
- [ ] Schema `eventos_raw`: id, magnitud, lugar, lat, lon, profundidad, timestamp_evento, timestamp_ingesta, region_chile
- [ ] Schema `metricas_ventana`: region, ventana_inicio, ventana_fin, conteo, magnitud_max
- [ ] Modificar pipeline para escribir a BigQuery (en vez de archivo local)
- [ ] `terraform/cloudrun.tf` + `terraform/scheduler.tf`: desplegar poller
- [ ] Desplegar pipeline a DataflowRunner
- [ ] Verificar datos en BigQuery con query simple

**Criterio de éxito:** query `SELECT * FROM sismo_monitor.eventos_raw LIMIT 10`
retorna filas reales.

**Control de costos:** Dataflow cobra por vCPU/hora. Para este volumen, usar
`--machine_type=n1-standard-1` y `--max_num_workers=1`. Monitorear en consola.

---

### ✅ Fase 4 — Alertas event-driven
**Objetivo:** notificación real cuando sismo supera umbral.

Tareas:
- [ ] `terraform/pubsub.tf`: agregar topic `sismos-alertas`
- [ ] Modificar pipeline: si magnitud > 5.0 Y región Chile → publicar a `sismos-alertas`
- [ ] `alerting/notifier.py`: Cloud Function triggered por `sismos-alertas`
  - Canal 1 — Email vía SendGrid (free tier: 100 emails/día sin tarjeta)
  - Canal 2 — Telegram vía Bot API (completamente gratis, sin límite práctico)
  - Ambos canales activos simultáneamente: la Function itera sobre los dos
- [ ] Probar bajando umbral a 2.5 para gatillar con actividad normal

**Criterio de éxito:** sismo real de Chile → notificación llega en menos de 3 minutos.
Este es el **money shot #1** — capturar screenshot/video de la notificación llegando.

---

### ✅ Fase 5 — Dashboard Looker Studio
**Objetivo:** visualización pública del pipeline.

Tareas:
- [ ] Conectar Looker Studio a BigQuery (nativo, sin configuración extra)
- [ ] `queries/dashboard_queries.sql`: queries base para cada visualización
- [ ] Gráficos:
  - Mapa de burbujas: epicentros lat/lon, tamaño = magnitud
  - Serie temporal: actividad últimas 24h por región
  - Tabla: últimos 10 eventos con magnitud, lugar, hora
  - Contador: total eventos hoy en Chile
- [ ] Configurar refresh automático (cada 15 min en Looker Studio gratuito)

**Criterio de éxito:** dashboard público accesible por URL, se actualiza solo.
Este es el **money shot #2** — capturar screenshot del dashboard con datos reales.

---

### ✅ Fase 6 — Cosecha de evidencia + `terraform destroy`
**Objetivo:** capturar toda la evidencia antes de destruir infra.

Tareas:
- [ ] Video corto (1-2 min) mostrando: dashboard vivo + notificación llegando
- [ ] Screenshots del dashboard, de Dataflow corriendo, de BigQuery con datos
- [ ] Guardar URL pública de Looker Studio (persiste sin la infra)
- [ ] `scripts/destroy.sh`: `terraform destroy` ordenado
- [ ] Verificar en consola GCP que no quedaron recursos activos

**Regla de oro:** la infra se levanta con `terraform apply` cuando necesites mostrarla.
No dejarla corriendo "por si acaso".

---

### ✅ Fase 7 — Documentación del README
**Objetivo:** el repo cuenta la historia solo, sin que tengas que explicarlo.

Secciones del README:
1. **¿Qué es esto?** — narrativa personal (Valparaíso, conexión con trabajo en Entel)
2. **Arquitectura** — diagrama + descripción de cada componente GCP
3. **Decisiones técnicas** — por qué DirectRunner primero, por qué el watermark de 20 min
4. **Limitaciones conocidas** — latencia USGS para eventos fuera de EE.UU., frecuencia del feed
5. **Cómo correr el proyecto** — prerequisitos, `terraform apply`, `destroy`
6. **Comparación Fabric ↔ GCP** — tabla mostrando equivalencias (Fabric Lakehouse = BigQuery + GCS, etc.)
7. **Screenshots / demo** — evidencia visual

---

## Reglas de desarrollo (no negociables)

```
COSTOS (sin créditos GCP — todo gasto es real)
- Alerta de presupuesto en GCP: $5 / $15 / $25 USD ANTES de cualquier deploy
- Dataflow: siempre --max_num_workers=1 en desarrollo
- Fase 2 completa en DirectRunner antes de tocar Dataflow
- terraform destroy al terminar CADA sesión de trabajo — sin excepción
- Meta: proyecto completo < $25 USD total

CÓDIGO
- Todo parametrizado en config.py / variables.tf (sin hardcode de IDs)
- Deduplicación obligatoria por event ID en el poller
- Watermark de 20 minutos en el pipeline (latencia real de la fuente)
- Manejo explícito de duplicados documentado en código y README

REPOSITORIO
- Commits por fase completada
- .gitignore incluye: credenciales, terraform.tfstate, archivos .env
- NUNCA commitear service account keys ni archivos de credenciales

HONESTIDAD TÉCNICA (narrativa)
- El sistema monitorea, NO predice
- Las limitaciones de la fuente van documentadas
- La comparación Fabric vs GCP va en el README
```

---

## Conexión con experiencia previa (para el pitch en entrevistas)

| Lo que hice en Entel (Fabric) | Equivalente en este proyecto (GCP) |
|---|---|
| Pipelines near real-time para KPIs B2B | Dataflow streaming con ventanas de 10 min |
| Ingesta multifuente (APIs, JSON, BD) | Poller USGS → Pub/Sub (API REST + JSON) |
| Reducción latencia de días a horas | Alertas en menos de 3 min desde evento |
| DWH con capas raw > staging > marts | BigQuery: eventos_raw + metricas_ventana |
| Gobierno de datos DAMA | Documentación de linaje, fuente y calidad en README |
| Monitoreo y alertas operativas | Cloud Monitoring + notificaciones automáticas |

---

## Recursos útiles

- USGS Feed docs: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
- Apache Beam Python docs: https://beam.apache.org/documentation/sdks/python/
- Dataflow pricing: https://cloud.google.com/dataflow/pricing
- Terraform GCP provider: https://registry.terraform.io/providers/hashicorp/google/latest
- CSN Chile (fuente complementaria): https://www.sismologia.cl

---

*Generado como brief de proyecto — Paulo César Contreras Ledezma — Mayo 2026*
