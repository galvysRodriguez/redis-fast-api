# Procesador Desestructurado Asíncrono

Microservicio que recibe reportes complejos en PDF, extrae **tablas con Docling**, procesa el
**texto con Unstructured** y lo **indexa en micro-batches** a través de **Kafka** hacia
Elasticsearch.

## Arquitectura

```
                 POST /reports                       pdf.ingest (6 part.)
  cliente ───────────────────────►  api (FastAPI) ─────────────────────────┐
     ▲            202 + job_id          │  guarda PDF en volumen "uploads"  │
     │                                  │  job = queued                     ▼
     │  GET /reports/{id}               │                      ┌──────────────────────┐
     │  GET /search?q=                  │                      │ extractor (N réplicas)│
     │                                  │                      │  ├─ Docling → tablas  │  (en paralelo,
     │                                  ▼                      │  └─ Unstructured →    │   en hilos)
     │                          ┌───────────────┐              │     texto + chunking  │
     └──────────────────────────│ Elasticsearch │              └──────────┬───────────┘
                                │ report-chunks │                         │ pdf.chunks (12 part., key=job_id)
                                │ report-jobs   │◄── bulk ──┐             │ started → chunk… → completed
                                └───────────────┘           │             ▼
                                                    ┌───────┴──────────────────────┐
                                                    │ indexer (M réplicas)          │
                                                    │ micro-batch: 500 msgs / 2 s   │
                                                    │ 1 bulk por lote + estado jobs │
                                                    └───────────────┬──────────────┘
                                                                    │ fallos permanentes
                                                                    ▼
                                                                 pdf.dlq
```

| Servicio    | Código                              | Rol |
|-------------|-------------------------------------|-----|
| `api`       | `pdfproc/api/main.py`               | Valida y guarda el PDF, crea el job y publica en `pdf.ingest`. Expone estado y búsqueda. |
| `extractor` | `pdfproc/extractor/worker.py`       | Consume `pdf.ingest` de a 1 mensaje. Docling (TableFormer, modo ACCURATE) y Unstructured (`partition_pdf` + `chunk_by_title`) corren en paralelo. Publica chunks en `pdf.chunks`. |
| `indexer`   | `pdfproc/indexer/worker.py`         | Acumula micro-batches (`BATCH_MAX_RECORDS` o `BATCH_MAX_WAIT_MS`, lo que llegue antes), hace **un** `_bulk` por lote y actualiza el estado de los jobs. |

### Topics

| Topic        | Key      | Contenido |
|--------------|----------|-----------|
| `pdf.ingest` | `job_id` | `IngestRequest` (ruta del PDF) |
| `pdf.chunks` | `job_id` | `job_started` → `chunk`* → `job_completed` \| `job_failed` |
| `pdf.dlq`    | `job_id` | `DeadLetter`: mensajes inválidos, PDFs que no se pudieron procesar, chunks rechazados por ES |

Los contratos están en `pdfproc/schemas.py` (Pydantic, unión discriminada por `type`).

## Garantías de entrega

- **At-least-once** extremo a extremo: los offsets se confirman manualmente *después* de
  publicar/indexar. Producer con `acks=all` + `enable_idempotence`.
- **Idempotencia**: `chunk_id = {job_id}-{text|table}-{seq}` es el `_id` en Elasticsearch, así
  que un reprocesamiento sobrescribe en lugar de duplicar.
- **Orden por job**: todos los mensajes de un job comparten key → misma partición. Cuando el
  indexador ve `job_completed`, todos los chunks de ese job ya pasaron por un bulk; entonces
  cuenta lo indexado y marca `completed` o `completed_with_errors`.
- **Reintentos**: errores 429/5xx del bulk se reintentan con backoff exponencial solo para los
  documentos fallidos. Errores 4xx (mapping, etc.) van a la DLQ. Si ES sigue caído tras
  `BULK_MAX_RETRIES`, el worker termina sin confirmar el lote y Docker lo reinicia.
- **PDFs lentos**: `max_poll_interval_ms` del extractor es 1 h y el trabajo pesado corre fuera
  del event loop, así que los heartbeats de Kafka no se cortan.

## Puesta en marcha

Requisitos: Docker + Docker Compose. ~4 GB de RAM libres (Elasticsearch 1 GB, Docling ~2 GB).

```bash
cp .env.example .env      # opcional
make up                   # la 1ª vez construye el extractor (torch CPU + modelos Docling)
make ps
```

| URL                         | Qué |
|-----------------------------|-----|
| http://localhost:8000/docs  | Swagger de la API |
| http://localhost:8080       | Kafka UI (topics, consumer lag) |
| http://localhost:9200       | Elasticsearch |
| `localhost:29092`           | Kafka desde el host |

## Uso

```bash
# Enviar y esperar a que termine
make submit FILE=reporte.pdf

# O a mano
curl -F "file=@reporte.pdf" http://localhost:8000/reports
# {"job_id":"9f1c…","status":"queued","status_url":"/reports/9f1c…"}

curl http://localhost:8000/reports/9f1c…                       # estado del job
curl "http://localhost:8000/reports/9f1c…/chunks?kind=table"   # tablas estructuradas
curl "http://localhost:8000/search?q=ingresos%20operativos&kind=table"
```

Estados del job: `queued` → `processing` → `completed` | `completed_with_errors` | `failed`.

Cada chunk de tabla incluye `table.columns`, `table.rows`, `table.markdown` y `table.caption`;
el campo `text` (caption + markdown) es el que se indexa para búsqueda.

## Escalado

```bash
make scale EXTRACTORS=4 INDEXERS=2
```

- Los extractores escalan hasta el número de particiones de `pdf.ingest` (6).
- Los indexadores escalan hasta las particiones de `pdf.chunks` (12).
- Ajusta `EXTRACTOR_THREADS` (OMP) según núcleos por réplica.
- Lag de los consumer groups `pdf-extractor` / `pdf-indexer` visible en Kafka UI.

## Configuración

Todas las variables están en `pdfproc/config.py` (sobrescribibles por entorno):

| Variable                     | Defecto | Descripción |
|------------------------------|---------|-------------|
| `UNSTRUCTURED_STRATEGY`      | `fast`  | `hi_res` usa modelo de layout (más lento, mejor en PDFs complejos) |
| `DOCLING_DO_OCR`             | `false` | Activar para PDFs escaneados |
| `CHUNK_MAX_CHARS`            | `1500`  | Tamaño máximo de chunk de texto |
| `CHUNK_COMBINE_UNDER_CHARS`  | `300`   | Une secciones cortas consecutivas |
| `BATCH_MAX_RECORDS`          | `500`   | Tamaño máximo del micro-batch |
| `BATCH_MAX_WAIT_MS`          | `2000`  | Ventana máxima del micro-batch |
| `BULK_MAX_RETRIES`           | `5`     | Reintentos de bulk ante 429/5xx |
| `MAX_UPLOAD_MB`              | `100`   | Límite de subida |

## Desarrollo

```bash
make venv && make test   # tests unitarios (sin Kafka/ES/Docling)
make logs
make dlq                 # inspeccionar dead-letter queue
```

## Hacia producción

- **Almacenamiento**: el volumen compartido `uploads` sirve en un solo host. En multi-host,
  sustituirlo por S3/MinIO y mandar la URI en `IngestRequest.path`.
- **Kafka**: 3 brokers, `replication.factor=3`, `min.insync.replicas=2`.
- **Elasticsearch**: activar seguridad (`xpack.security`) y réplicas.
- **Embeddings**: el indexador es el punto natural para añadir vectores (`dense_vector`) por lote.
- **Retención**: borrar el PDF del volumen tras `completed` si no se necesita conservarlo.
