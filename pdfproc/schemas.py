"""Contratos de los mensajes que viajan por Kafka."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter


def utcnow() -> datetime:
    return datetime.now(UTC)


# --- topic: pdf.ingest -------------------------------------------------------


class IngestRequest(BaseModel):
    job_id: str
    filename: str
    path: str
    submitted_at: datetime = Field(default_factory=utcnow)


# --- topic: pdf.chunks -------------------------------------------------------
# Todos los mensajes de un job usan job_id como key, así caen en la misma
# partición y el indexador los ve en orden: started -> chunks... -> completed.


class TablePayload(BaseModel):
    columns: list[str]
    rows: list[list[str]]
    markdown: str
    caption: str | None = None


class Chunk(BaseModel):
    type: Literal["chunk"] = "chunk"
    chunk_id: str
    job_id: str
    filename: str
    kind: Literal["text", "table"]
    seq: int
    text: str
    page_numbers: list[int] = Field(default_factory=list)
    element_types: list[str] = Field(default_factory=list)
    table: TablePayload | None = None
    extracted_at: datetime = Field(default_factory=utcnow)


class JobStarted(BaseModel):
    type: Literal["job_started"] = "job_started"
    job_id: str
    started_at: datetime = Field(default_factory=utcnow)


class JobCompleted(BaseModel):
    type: Literal["job_completed"] = "job_completed"
    job_id: str
    total_chunks: int
    text_chunks: int
    table_chunks: int
    extraction_seconds: float
    finished_at: datetime = Field(default_factory=utcnow)


class JobFailed(BaseModel):
    type: Literal["job_failed"] = "job_failed"
    job_id: str
    error: str
    failed_at: datetime = Field(default_factory=utcnow)


ChunkTopicMessage = Annotated[
    Chunk | JobStarted | JobCompleted | JobFailed, Field(discriminator="type")
]
chunk_topic_adapter: TypeAdapter[ChunkTopicMessage] = TypeAdapter(ChunkTopicMessage)


# --- topic: pdf.dlq ----------------------------------------------------------


class DeadLetter(BaseModel):
    stage: Literal["extractor", "indexer"]
    source_topic: str
    key: str | None
    payload: str
    error: str
    failed_at: datetime = Field(default_factory=utcnow)
