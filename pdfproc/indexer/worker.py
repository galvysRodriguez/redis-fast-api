"""Worker indexador: consume pdf.chunks en micro-batches y los indexa en
Elasticsearch con la Bulk API. También mantiene el estado de los jobs."""

import asyncio
import logging
import time
from collections import Counter

from aiokafka import AIOKafkaProducer, ConsumerRecord
from aiokafka.errors import CommitFailedError
from elasticsearch import ApiError, AsyncElasticsearch, TransportError
from pydantic import ValidationError

from pdfproc.config import Settings, get_settings
from pdfproc.indexer.batching import (
    BulkOutcome,
    build_bulk_operations,
    chunk_to_document,
    classify_bulk_response,
    collect_batch,
)
from pdfproc.messaging import send_to_dlq, start_consumer, start_producer
from pdfproc.runtime import configure_logging, install_signal_handlers
from pdfproc.schemas import (
    Chunk,
    ChunkTopicMessage,
    JobCompleted,
    JobFailed,
    JobStarted,
    chunk_topic_adapter,
    utcnow,
)
from pdfproc.storage import connect_es, ensure_indices, update_job

log = logging.getLogger("pdfproc.indexer")

GROUP_ID = "pdf-indexer"


class IndexingUnavailable(RuntimeError):
    """Elasticsearch no aceptó el lote tras todos los reintentos."""


async def bulk_index(
    es: AsyncElasticsearch, settings: Settings, docs: dict[str, dict]
) -> dict[str, str]:
    """Indexa `docs` reintentando con backoff los fallos transitorios.

    Devuelve los fallos permanentes (id -> error). Si los transitorios persisten
    lanza IndexingUnavailable: el lote no se confirma en Kafka y se reprocesa.
    """
    pending = docs
    permanent: dict[str, str] = {}
    for attempt in range(settings.bulk_max_retries + 1):
        try:
            response = await es.bulk(
                operations=build_bulk_operations(settings.index_chunks, pending)
            )
            outcome = classify_bulk_response(response.body)
        except (TransportError, ApiError) as exc:
            outcome = BulkOutcome(retryable={doc_id: str(exc) for doc_id in pending})

        permanent.update(outcome.permanent)
        if not outcome.retryable:
            return permanent
        pending = {doc_id: pending[doc_id] for doc_id in outcome.retryable}
        delay = min(0.5 * 2**attempt, 30)
        log.warning("Bulk: %d docs reintentables, nuevo intento en %.1fs", len(pending), delay)
        await asyncio.sleep(delay)
    raise IndexingUnavailable(f"{len(pending)} documentos sin indexar tras reintentos")


async def apply_job_event(
    es: AsyncElasticsearch, settings: Settings, event: ChunkTopicMessage
) -> None:
    match event:
        case JobStarted():
            await update_job(
                es,
                settings,
                event.job_id,
                status="processing",
                started_at=event.started_at.isoformat(),
            )
        case JobCompleted():
            # Los chunks del job van antes en la misma partición, así que ya
            # están indexados; se cuentan para detectar pérdidas (DLQ).
            await es.indices.refresh(index=settings.index_chunks)
            indexed = (
                await es.count(
                    index=settings.index_chunks, query={"term": {"job_id": event.job_id}}
                )
            )["count"]
            status = "completed" if indexed >= event.total_chunks else "completed_with_errors"
            await update_job(
                es,
                settings,
                event.job_id,
                status=status,
                total_chunks=event.total_chunks,
                text_chunks=event.text_chunks,
                table_chunks=event.table_chunks,
                indexed_chunks=indexed,
                extraction_seconds=event.extraction_seconds,
                finished_at=event.finished_at.isoformat(),
            )
            log.info("Job %s %s (%d/%d chunks)", event.job_id, status, indexed, event.total_chunks)
        case JobFailed():
            await update_job(
                es,
                settings,
                event.job_id,
                status="failed",
                error=event.error,
                finished_at=event.failed_at.isoformat(),
            )


async def handle_batch(
    records: list[ConsumerRecord],
    es: AsyncElasticsearch,
    producer: AIOKafkaProducer,
    settings: Settings,
) -> None:
    started = time.perf_counter()
    events: list[ChunkTopicMessage] = []
    raw_chunks: dict[str, ConsumerRecord] = {}
    docs: dict[str, dict] = {}
    now = utcnow()

    for record in records:
        try:
            message = chunk_topic_adapter.validate_json(record.value)
        except ValidationError as exc:
            await send_to_dlq(
                producer,
                settings,
                stage="indexer",
                source_topic=record.topic,
                key=record.key.decode() if record.key else None,
                payload=record.value,
                error=f"mensaje inválido: {exc}",
            )
            continue
        if isinstance(message, Chunk):
            # Si un chunk llega duplicado dentro del lote, gana el último.
            docs[message.chunk_id] = chunk_to_document(message, now)
            raw_chunks[message.chunk_id] = record
        else:
            events.append(message)

    # 1) Todos los chunks del lote en un único bulk...
    if docs:
        failures = await bulk_index(es, settings, docs)
        for chunk_id, error in failures.items():
            record = raw_chunks[chunk_id]
            await send_to_dlq(
                producer,
                settings,
                stage="indexer",
                source_topic=record.topic,
                key=record.key.decode() if record.key else None,
                payload=record.value,
                error=error,
            )

    # 2) ...y después los eventos de estado, en orden de llegada.
    for event in events:
        await apply_job_event(es, settings, event)

    kinds = Counter(doc["kind"] for doc in docs.values())
    log.info(
        "Lote: %d registros -> %d chunks (%d texto, %d tablas), %d eventos en %.0f ms",
        len(records),
        len(docs),
        kinds["text"],
        kinds["table"],
        len(events),
        (time.perf_counter() - started) * 1000,
    )


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    stop = install_signal_handlers()
    es = await connect_es(settings)
    await ensure_indices(es, settings)
    producer = await start_producer(settings)
    consumer = await start_consumer(
        settings, settings.topic_chunks, GROUP_ID, max_poll_records=settings.batch_max_records
    )
    log.info(
        "Indexador listo (batch=%d registros / %d ms)",
        settings.batch_max_records,
        settings.batch_max_wait_ms,
    )
    try:
        while not stop.is_set():
            records = await collect_batch(
                consumer, settings.batch_max_records, settings.batch_max_wait_ms, stop
            )
            if not records:
                continue
            await handle_batch(records, es, producer, settings)
            try:
                await consumer.commit()
            except CommitFailedError:
                # Rebalanceo: el lote se volverá a entregar; es idempotente (_id fijo).
                log.warning("Commit rechazado por rebalanceo; el lote se reprocesará")
    finally:
        await consumer.stop()
        await producer.stop()
        await es.close()
        log.info("Indexador detenido")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
