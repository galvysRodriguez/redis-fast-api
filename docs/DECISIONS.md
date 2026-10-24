# Decisiones técnicas y lecciones aprendidas

## Decisiones de diseño

### 1. Kafka entre cada etapa, no llamadas directas

La extracción de un PDF complejo tarda de segundos a minutos y consume mucha CPU/RAM; la
indexación es rápida y se beneficia de agrupar. Separarlas con Kafka permite:

- responder `202` al instante y absorber picos de subida sin perder trabajos;
- escalar extractores e indexadores por separado (perfiles de recursos muy distintos);
- reprocesar desde los offsets si una etapa falla.

### 2. Docling para tablas, Unstructured para texto

- **Docling** (TableFormer) reconstruye la estructura de las tablas (celdas, encabezados, celdas
  combinadas) y la exporta a DataFrame/Markdown. En las pruebas extrajo ambas tablas del
  ejemplo con columnas y filas exactas.
- **Unstructured** clasifica el texto (`Title`, `NarrativeText`, `ListItem`…) y hace *chunking
  semántico* con `chunk_by_title`, que respeta las secciones del documento: ideal para búsqueda
  y RAG.

Las dos corren **en paralelo** en hilos sobre el mismo archivo. El tiempo total es el de la más
lenta, no la suma.

### 3. `job_id` como key de partición

Garantiza el orden `job_started → chunk* → job_completed` dentro de cada job sin coordinación
extra. Así el indexador puede cerrar un job contando sus documentos al ver `job_completed`.

### 4. Idempotencia por `_id` determinista

`chunk_id = {job_id}-{text|table}-{seq:05d}`. Reprocesar un job (por crash, rebalanceo o
reentrega) sobrescribe los mismos documentos. Esto convierte *at-least-once* en resultados
*effectively-once* sin transacciones de Kafka.

### 5. Estado del job derivado de los datos

En lugar de llevar contadores incrementales, que se duplican al reprocesar, el indexador hace
`count(job_id)` al recibir `job_completed` y lo compara con `total_chunks`. El resultado es
idempotente por construcción.

### 6. Micro-batches con ventana temporal

Un `_bulk` de 500 documentos es mucho más eficiente que 500 `index`. La ventana de 2 s acota la
latencia en carga baja. Ambos valores se configuran por entorno.

### 7. Solo el indexador escribe chunks en Elasticsearch

El extractor no conoce Elasticsearch: publica también los eventos de estado (`job_started`,
`job_failed`) por Kafka. Hay un único escritor del índice de chunks, y cambiar de backend
(OpenSearch, un vector store) solo toca el indexador.

## Problemas reales encontrados al ejecutarlo

Estos fallos no aparecían en los tests unitarios. Salieron al levantar el stack completo con un
PDF real.

### Unstructured devolvía 0 elementos sin lanzar error

**Síntoma:** jobs `completed` con `text_chunks: 0` en un PDF lleno de párrafos.

**Diagnóstico:** con logging en DEBUG dentro del contenedor:

```
INFO:unstructured.nlp.tokenize:Downloading spaCy model en_core_web_sm 3.8.0 …
DEBUG:unstructured:Failed to install en_core_web_sm to /usr/local/lib/python3.12/site-packages:
      [Errno 13] Permission denied
INFO:unstructured:PDF text extraction failed, skip text extraction...
```

Unstructured 0.27 intenta instalar el modelo de spaCy en tiempo de ejecución. El contenedor
corre como usuario sin privilegios (buena práctica) y no puede escribir en `site-packages`, así
que la extracción se **salta en silencio**.

**Solución:** instalar `en_core_web_sm` en la imagen durante el build, como root.

**Lección:** una librería que "degrada con elegancia" puede esconder un fallo total. Conviene
probar el pipeline de punta a punta con un documento conocido y verificar los conteos, no solo
el estado.

### Tablas indexadas dos veces

**Síntoma:** buscar `131.2` devolvía el chunk de la tabla **y** un chunk de texto con las celdas
sueltas (`Región\n\nQ1\n\nQ2…`).

**Causa:** la estrategia `fast` de Unstructured no detecta tablas; sus celdas llegan como
`UncategorizedText`, así que filtrar por la categoría `Table` no bastaba.

**Solución:** Docling devuelve el *bounding box* de cada tabla. Se normaliza a fracciones de
página (0..1), porque Unstructured usa puntos PDF en `fast` y píxeles en `hi_res`, y se descartan
los elementos cuyo centro cae dentro de una tabla. En el PDF de ejemplo se filtraron 30 de 37
elementos y la búsqueda de `131.2` pasó a devolver solo la tabla.

### Caption duplicado en el texto de la tabla

`export_to_markdown()` de Docling ya incluye el caption en la versión actual; anteponerlo otra
vez lo duplicaba. Ahora solo se antepone si no está presente. Hay un test que lo cubre.

### Build frágil ante cortes de red

La descarga de modelos de Docling desde Hugging Face y de torch desde PyPI se cortó varias veces
(`Read timed out`, `CAS Client Error`), y cada corte tiraba un build de más de 30 minutos.

- Los **modelos de Docling** se descargan al arrancar el extractor, en un volumen, con 5
  reintentos. Un corte ya no rompe la imagen, y los modelos se comparten entre reinicios y
  réplicas.
- **pip** usa una caché de BuildKit (`--mount=type=cache`) y `PIP_RETRIES`. Un reintento continúa
  desde lo ya descargado: el segundo intento de build tardó menos de un minuto en descargas.
- Las variables de entorno que cambian a menudo están **después** de las capas pesadas, para no
  invalidar la caché de torch.

### APIs que cambiaron entre versiones

`easyocr` dejó de venir con Docling y `nltk` dejó de ser dependencia de Unstructured. El OCR
ahora usa Tesseract, que ya está en la imagen con el paquete de español.
