"""API de ingesta: recibe PDFs, los deja en el volumen compartido y publica en Kafka."""

import logging
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

import anyio
from elasticsearch import NotFoundError
from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel

from pdfproc.config import get_settings
from pdfproc.messaging import encode, start_producer
from pdfproc.runtime import configure_logging
from pdfproc.schemas import IngestRequest, utcnow
from pdfproc.storage import connect_es, ensure_indices, update_job

log = logging.getLogger("pdfproc.api")

READ_CHUNK_BYTES = 1024 * 1024
PDF_MAGIC = b"%PDF-"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    Path(settings.upload_dir).mkdir(parents=True, exist_ok=True)

    es = await connect_es(settings)
    await ensure_indices(es, settings)
    producer = await start_producer(settings)

    app.state.settings = settings
    app.state.es = es
    app.state.producer = producer
    try:
        yield
    finally:
        await producer.stop()
        await es.close()


app = FastAPI(title="Procesador Desestructurado Asíncrono", version="1.0.0", lifespan=lifespan)


class ReportAccepted(BaseModel):
    job_id: str
    status: str
    status_url: str


async def _save_upload(file: UploadFile, dest: Path, max_bytes: int) -> int:
    head = await file.read(len(PDF_MAGIC))
    if head != PDF_MAGIC:
        raise HTTPException(415, "El archivo no es un PDF válido")
    size = len(head)
    try:
        async with await anyio.open_file(dest, "wb") as out:
            await out.write(head)
            while data := await file.read(READ_CHUNK_BYTES):
                size += len(data)
                if size > max_bytes:
                    raise HTTPException(413, f"El PDF supera {max_bytes // 1024 // 1024} MB")
                await out.write(data)
    except BaseException:
        dest.unlink(missing_ok=True)
        raise
    return size


@app.post("/reports", status_code=202, response_model=ReportAccepted)
async def submit_report(request: Request, file: Annotated[UploadFile, File()]) -> ReportAccepted:
    settings = request.app.state.settings
    es = request.app.state.es
    producer = request.app.state.producer

    job_id = uuid.uuid4().hex
    filename = Path(file.filename or "report.pdf").name
    dest = Path(settings.upload_dir) / f"{job_id}.pdf"
    size = await _save_upload(file, dest, settings.max_upload_mb * 1024 * 1024)

    ingest = IngestRequest(job_id=job_id, filename=filename, path=str(dest))
    await update_job(
        es,
        settings,
        job_id,
        filename=filename,
        status="queued",
        submitted_at=ingest.submitted_at.isoformat(),
    )
    try:
        await producer.send_and_wait(settings.topic_ingest, encode(ingest), key=job_id)
    except Exception as exc:
        log.exception("No se pudo publicar el job %s", job_id)
        dest.unlink(missing_ok=True)
        await update_job(es, settings, job_id, status="failed", error=f"kafka: {exc}")
        raise HTTPException(503, "Cola de procesamiento no disponible") from exc

    log.info("Job %s encolado (%s, %d bytes)", job_id, filename, size)
    return ReportAccepted(job_id=job_id, status="queued", status_url=f"/reports/{job_id}")


@app.get("/reports/{job_id}")
async def get_report(request: Request, job_id: str) -> dict:
    settings = request.app.state.settings
    try:
        doc = await request.app.state.es.get(index=settings.index_jobs, id=job_id)
    except NotFoundError:
        raise HTTPException(404, "Job no encontrado") from None
    return doc["_source"]


@app.get("/reports/{job_id}/chunks")
async def get_report_chunks(
    request: Request,
    job_id: str,
    kind: Literal["text", "table"] | None = None,
    size: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    settings = request.app.state.settings
    filters = [{"term": {"job_id": job_id}}]
    if kind:
        filters.append({"term": {"kind": kind}})
    resp = await request.app.state.es.search(
        index=settings.index_chunks,
        query={"bool": {"filter": filters}},
        sort=[{"kind": "asc"}, {"seq": "asc"}],
        size=size,
        from_=offset,
    )
    return {
        "total": resp["hits"]["total"]["value"],
        "items": [hit["_source"] for hit in resp["hits"]["hits"]],
    }


@app.get("/search")
async def search(
    request: Request,
    q: str = Query(..., min_length=1),
    kind: Literal["text", "table"] | None = None,
    job_id: str | None = None,
    size: int = Query(10, ge=1, le=100),
) -> dict:
    settings = request.app.state.settings
    filters = []
    if kind:
        filters.append({"term": {"kind": kind}})
    if job_id:
        filters.append({"term": {"job_id": job_id}})
    resp = await request.app.state.es.search(
        index=settings.index_chunks,
        query={"bool": {"must": [{"match": {"text": q}}], "filter": filters}},
        highlight={"fields": {"text": {}}},
        source_excludes=["table"],
        size=size,
    )
    return {
        "total": resp["hits"]["total"]["value"],
        "items": [
            {
                "score": hit["_score"],
                "highlight": hit.get("highlight", {}).get("text", []),
                **hit["_source"],
            }
            for hit in resp["hits"]["hits"]
        ],
    }


@app.get("/health")
async def health(request: Request) -> dict:
    es_ok = await request.app.state.es.ping()
    if not es_ok:
        raise HTTPException(503, "Elasticsearch no disponible")
    return {"status": "ok", "time": utcnow().isoformat()}
