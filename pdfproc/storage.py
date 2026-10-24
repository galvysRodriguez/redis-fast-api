"""Elasticsearch: conexión, índices y estado de los jobs."""

import asyncio
import logging

from elasticsearch import AsyncElasticsearch, BadRequestError

from pdfproc.config import Settings

log = logging.getLogger(__name__)

CHUNKS_MAPPING = {
    "properties": {
        "chunk_id": {"type": "keyword"},
        "job_id": {"type": "keyword"},
        "filename": {"type": "keyword"},
        "kind": {"type": "keyword"},
        "seq": {"type": "integer"},
        "text": {"type": "text"},
        "page_numbers": {"type": "integer"},
        "element_types": {"type": "keyword"},
        # La tabla estructurada se guarda tal cual; se busca por `text` (markdown).
        "table": {"type": "object", "enabled": False},
        "extracted_at": {"type": "date"},
        "indexed_at": {"type": "date"},
    }
}

JOBS_MAPPING = {
    "properties": {
        "job_id": {"type": "keyword"},
        "filename": {"type": "keyword"},
        "status": {"type": "keyword"},
        "error": {"type": "text"},
        "submitted_at": {"type": "date"},
        "started_at": {"type": "date"},
        "finished_at": {"type": "date"},
        "total_chunks": {"type": "integer"},
        "text_chunks": {"type": "integer"},
        "table_chunks": {"type": "integer"},
        "indexed_chunks": {"type": "integer"},
        "extraction_seconds": {"type": "float"},
    }
}


async def connect_es(settings: Settings, attempts: int = 60, delay: float = 2.0) -> AsyncElasticsearch:
    es = AsyncElasticsearch(settings.elasticsearch_url, request_timeout=30)
    for attempt in range(1, attempts + 1):
        if await es.ping():
            log.info("Conectado a Elasticsearch")
            return es
        log.warning("Elasticsearch no disponible (%d/%d)", attempt, attempts)
        await asyncio.sleep(delay)
    await es.close()
    raise RuntimeError("No se pudo conectar a Elasticsearch")


async def ensure_indices(es: AsyncElasticsearch, settings: Settings) -> None:
    for name, mapping in (
        (settings.index_chunks, CHUNKS_MAPPING),
        (settings.index_jobs, JOBS_MAPPING),
    ):
        if await es.indices.exists(index=name):
            continue
        try:
            await es.indices.create(
                index=name, mappings=mapping, settings={"number_of_replicas": 0}
            )
            log.info("Índice creado: %s", name)
        except BadRequestError as exc:
            # Carrera entre api e indexer creando el mismo índice.
            if exc.error != "resource_already_exists_exception":
                raise


async def update_job(es: AsyncElasticsearch, settings: Settings, job_id: str, **fields) -> None:
    await es.update(
        index=settings.index_jobs,
        id=job_id,
        doc={"job_id": job_id, **fields},
        doc_as_upsert=True,
        retry_on_conflict=3,
    )
