"""Worker extractor: consume pdf.ingest, extrae tablas (Docling) y texto
(Unstructured) en paralelo y publica los chunks en pdf.chunks."""

import asyncio
import logging
import time
from pathlib import Path

from aiokafka import AIOKafkaProducer, ConsumerRecord
from pydantic import ValidationError

from pdfproc.config import Settings, get_settings
from pdfproc.extractor.pipeline import TableExtractor, TextExtractor, build_chunks
from pdfproc.messaging import encode, send_to_dlq, start_consumer, start_producer
from pdfproc.runtime import configure_logging, install_signal_handlers
from pdfproc.schemas import IngestRequest, JobCompleted, JobFailed, JobStarted

log = logging.getLogger("pdfproc.extractor")

GROUP_ID = "pdf-extractor"


async def process_report(
    req: IngestRequest,
    tables_ex: TableExtractor,
    text_ex: TextExtractor,
    producer: AIOKafkaProducer,
    settings: Settings,
) -> None:
    topic = settings.topic_chunks
    await producer.send_and_wait(topic, encode(JobStarted(job_id=req.job_id)), key=req.job_id)

    if not Path(req.path).is_file():
        raise FileNotFoundError(req.path)

    started = time.perf_counter()
    # Docling y Unstructured son CPU-bound y bloqueantes: van a hilos para no
    # congelar el event loop (heartbeats de Kafka) y corren en paralelo.
    tables, elements = await asyncio.gather(
        asyncio.to_thread(tables_ex.extract, req.path),
        asyncio.to_thread(text_ex.partition, req.path),
    )
    # El texto que cae dentro de una tabla de Docling ya está en el chunk de
    # tabla; se descarta para no indexarlo dos veces.
    regions = [region for table in tables for region in table.regions]
    texts = await asyncio.to_thread(text_ex.chunk, elements, regions)
    elapsed = time.perf_counter() - started
    chunks = build_chunks(req, tables, texts)

    # send() solo encola: el producer agrupa los mensajes (linger_ms) y los
    # envía en lotes; esperamos a todas las confirmaciones antes de cerrar el job.
    deliveries = [await producer.send(topic, encode(c), key=req.job_id) for c in chunks]
    await asyncio.gather(*deliveries)

    done = JobCompleted(
        job_id=req.job_id,
        total_chunks=len(chunks),
        text_chunks=len(texts),
        table_chunks=len(tables),
        extraction_seconds=round(elapsed, 3),
    )
    await producer.send_and_wait(topic, encode(done), key=req.job_id)
    log.info(
        "Job %s extraído en %.1fs: %d texto, %d tablas",
        req.job_id, elapsed, len(texts), len(tables),
    )


async def handle_record(
    record: ConsumerRecord,
    tables_ex: TableExtractor,
    text_ex: TextExtractor,
    producer: AIOKafkaProducer,
    settings: Settings,
) -> None:
    key = record.key.decode() if record.key else None
    dlq = dict(stage="extractor", source_topic=record.topic, key=key, payload=record.value)
    try:
        req = IngestRequest.model_validate_json(record.value)
    except ValidationError as exc:
        await send_to_dlq(producer, settings, error=f"mensaje inválido: {exc}", **dlq)
        return

    try:
        await process_report(req, tables_ex, text_ex, producer, settings)
    except Exception as exc:
        log.exception("Falló la extracción del job %s", req.job_id)
        error = f"{type(exc).__name__}: {exc}"
        failed = JobFailed(job_id=req.job_id, error=error)
        await producer.send_and_wait(settings.topic_chunks, encode(failed), key=req.job_id)
        await send_to_dlq(producer, settings, error=error, **dlq)


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    # Imports pesados (torch, modelos) solo en el proceso del extractor.
    from pdfproc.extractor.docling_tables import DoclingTableExtractor, ensure_models
    from pdfproc.extractor.unstructured_text import UnstructuredTextExtractor

    if settings.docling_artifacts_path:
        await asyncio.to_thread(ensure_models, settings.docling_artifacts_path)
    log.info("Cargando modelos de Docling...")
    tables_ex = DoclingTableExtractor(
        do_ocr=settings.docling_do_ocr, artifacts_path=settings.docling_artifacts_path
    )
    text_ex = UnstructuredTextExtractor(
        strategy=settings.unstructured_strategy,
        max_characters=settings.chunk_max_chars,
        new_after_n_chars=settings.chunk_new_after_chars,
        combine_text_under_n_chars=settings.chunk_combine_under_chars,
    )

    stop = install_signal_handlers()
    producer = await start_producer(settings)
    consumer = await start_consumer(
        settings,
        settings.topic_ingest,
        GROUP_ID,
        max_poll_records=1,
        max_poll_interval_ms=settings.extractor_max_poll_interval_ms,
    )
    log.info("Extractor listo, escuchando %s", settings.topic_ingest)
    try:
        while not stop.is_set():
            batch = await consumer.getmany(timeout_ms=1000, max_records=1)
            for tp, records in batch.items():
                for record in records:
                    await handle_record(record, tables_ex, text_ex, producer, settings)
                    # At-least-once: se confirma el offset solo tras publicar el resultado.
                    await consumer.commit({tp: record.offset + 1})
    finally:
        await consumer.stop()
        await producer.stop()
        log.info("Extractor detenido")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
